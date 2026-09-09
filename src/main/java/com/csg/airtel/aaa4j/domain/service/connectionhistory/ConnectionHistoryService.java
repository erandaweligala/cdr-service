package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.csg.airtel.aaa4j.common.DateTimeUtil;
import com.csg.airtel.aaa4j.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.model.PageDetails;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionSearchCriteria;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;
import com.csg.airtel.aaa4j.domain.util.exceptions.BaseException;
import com.csg.airtel.aaa4j.domain.util.exceptions.ServiceExceptionHandler;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.http.HttpStatus;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jspecify.annotations.NonNull;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@ApplicationScoped
public class ConnectionHistoryService {

    private static final Logger log = Logger.getLogger(ConnectionHistoryService.class);

    private static final String METHOD_FETCH_SESSION_DETAILS = "fetchSessionDetails";
    private static final String METHOD_MAP_FAILURE = "mapFailure";
    private static final String METHOD_LOG_ES_ERROR = "logElasticsearchError";

    private static final DateTimeFormatter INDEX_DATE_SUFFIX = DateTimeFormatter.ofPattern("yyyy.MM.dd");

    private static final LocalTime END_OF_DAY = LocalTime.of(23, 59, 59);

    /** Range bounds arrive as a date, a local date-time, or a date-time with an offset. */
    private static final DateTimeFormatter BOUND_FORMAT = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral('T')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .optionalStart()
            .appendOffsetId()
            .optionalEnd()
            .optionalEnd()
            .toFormatter();

    @ConfigProperty(name = "sessions-data")
    String sessionsIndex;

    @ConfigProperty(name = "app.timezone", defaultValue = "UTC")
    String timezone;

    private final ElasticsearchAsyncClient client;
    private final ServiceExceptionHandler handler;

    @Inject
    Instance<ExceptionMetricsService> metrics;

    public ConnectionHistoryService(ElasticsearchAsyncClient client, ServiceExceptionHandler handler) {
        this.client = client;
        this.handler = handler;
    }

    public Uni<BaseResponse<Session>> fetchSessionDetails(SessionSearchCriteria criteria) {
        LoggingUtil.logInfo(log, METHOD_FETCH_SESSION_DETAILS, "Start fetching session info list.");

        ZoneId zone = deploymentZone();
        String startTime = criteria.startDate() != null ? criteria.startDate() : LocalDate.now(zone).minusDays(7).toString();
        String endTime = criteria.endDate() != null ? criteria.endDate() : LocalDate.now(zone).toString();

        List<String> targetIndices = getTargetIndices(startTime, endTime);

        return filterExistingIndices(targetIndices)
                .flatMap(existingIndices -> existingIndices.isEmpty()
                        ? emptySessionResponse(criteria.page(), startTime, endTime)
                        : searchSessions(criteria, existingIndices, startTime, endTime))
                .onFailure().transform(this::mapFailure);
    }

    private Uni<BaseResponse<Session>> emptySessionResponse(int page, String startTime, String endTime) {
        LoggingUtil.logWarn(log, METHOD_FETCH_SESSION_DETAILS,
                "No existing indices found in range [%s - %s]. Returning empty result.", startTime, endTime);
        return Uni.createFrom().item(BaseResponse.success(
                ResponseCodeEnum.SUCCESSFUL.description(),
                Collections.<Session>emptyList(),
                new PageDetails(0, page, 0)
        ));
    }

    private Uni<BaseResponse<Session>> searchSessions(
            SessionSearchCriteria criteria, List<String> existingIndices, String startTime, String endTime) {
        Instant startInstant = parseDate(startTime);
        Instant endInstant = parseEndOfDay(endTime);
        int pageSize = criteria.pageSize();
        int page = criteria.page();

        return Uni.createFrom().completionStage(() -> client.search(s -> s
                                .index(existingIndices)
                                .from((pageSize * page) - pageSize)
                                .size(pageSize)
                                .sort(srt -> srt
                                        .field(f -> f.field("startTime").order(SortOrder.Desc))
                                )
                                .query(q -> q.bool(b -> buildSessionFilters(b, criteria, startInstant, endInstant))),
                        Session.class
                ))
                .map(response -> toSessionResponse(response, page));
    }

    private BoolQuery.Builder buildSessionFilters(
            BoolQuery.Builder b, SessionSearchCriteria criteria, Instant startInstant, Instant endInstant) {
        addTermFilter(b, "userName.keyword", criteria.username());
        addTermFilter(b, "connectionStatus.keyword", criteria.connectionStatus());
        addTermFilter(b, "sessionId.keyword", criteria.sessionId());
        addTermFilter(b, "groupId.keyword", criteria.groupId());

        b.filter(filterQ -> filterQ.range(rangeQ -> rangeQ
                .field("startTime")
                .gte(JsonData.of(startInstant.toEpochMilli()))
                .lte(JsonData.of(endInstant.toEpochMilli()))));

        return b;
    }

    private void addTermFilter(BoolQuery.Builder b, String field, String value) {
        if (value != null && !value.isEmpty()) {
            b.must(query -> query.term(term -> term.field(field).value(value)));
        }
    }

    private BaseResponse<Session> toSessionResponse(SearchResponse<Session> response, int page) {
        List<Session> data = response.hits().hits().isEmpty()
                ? new ArrayList<>()
                : response.hits().hits().stream().map(Hit::source).toList();

        assert response.hits().total() != null;
        PageDetails pageDetails = new PageDetails(
                response.hits().total().value(),
                page,
                data.size()
        );

        LoggingUtil.logInfo(log, METHOD_FETCH_SESSION_DETAILS,
                "Session data fetched successfully. Total records: %s", pageDetails.getTotalRecords());

        return BaseResponse.success(
                ResponseCodeEnum.SUCCESSFUL.description(),
                data,
                pageDetails
        );
    }

    public Uni<BaseResponse<SessionInstanceInfo>> fetchSessionInstances(String sessionId) {
        LoggingUtil.logInfo(log, "fetchSessionInstances", "Fetching session instances for sessionId: %s", sessionId);

        return Uni.createFrom().completionStage(() -> client.search(s -> s
                                .index(getSearchIndex())
                                .allowNoIndices(true)
                                .ignoreUnavailable(true)
                                .size(1)
                                .query(q -> q.term(t -> t
                                        .field("uniqueId.keyword")
                                        .value(sessionId)
                                )),
                        Session.class
                ))
                .map(response -> {
                    List<SessionInstanceInfo> data = getSessionInstanceInfos(response);
                    LoggingUtil.logInfo(log, "fetchSessionInstances",
                            "Refined list of session instances: %s records", data.size());
                    return BaseResponse.success(
                            ResponseCodeEnum.SUCCESSFUL.description(),
                            data,
                            null
                    );
                })
                .onFailure().transform(this::mapFailure);
    }

    private Throwable mapFailure(Throwable ex) {
        Throwable cause = unwrapCompletionCause(ex);

        if (cause instanceof BaseException be) {
            recordMetric(be, ExceptionMetricsService.Layer.SERVICE, ExceptionMetricsService.Source.INTERNAL);
            return be;
        }
        if (cause instanceof ElasticsearchException ese) {
            LoggingUtil.logError(log, METHOD_MAP_FAILURE, ese, "Elasticsearch error: ");
            logElasticsearchError(ese);
            recordMetric(ese, ExceptionMetricsService.Layer.CLIENT, ExceptionMetricsService.Source.ELASTICSEARCH);
            return handler.elasticsearchExceptionHandler(ese);
        }
        if (cause instanceof java.io.IOException) {
            LoggingUtil.logError(log, METHOD_MAP_FAILURE, cause, "IO error communicating with Elasticsearch: ");
            recordMetric(cause, ExceptionMetricsService.Layer.CLIENT, ExceptionMetricsService.Source.ELASTICSEARCH);
            return handler.elasticsearchExceptionHandler(cause);
        }
        LoggingUtil.logError(log, METHOD_MAP_FAILURE, cause, "Unexpected error: ");
        recordMetric(cause, ExceptionMetricsService.Layer.SERVICE, ExceptionMetricsService.Source.INTERNAL);
        return handler.serviceLayerExceptionHandler(cause);
    }

    /** Unwraps the real failure from the {@code CompletionException}/{@code ExecutionException} the async client wraps it in. */
    private Throwable unwrapCompletionCause(Throwable ex) {
        boolean isWrapped = ex instanceof java.util.concurrent.CompletionException
                || ex instanceof java.util.concurrent.ExecutionException;
        if (!isWrapped) {
            return ex;
        }
        return ex.getCause() != null ? ex.getCause() : ex;
    }

    private void recordMetric(Throwable t,
                              ExceptionMetricsService.Layer layer,
                              ExceptionMetricsService.Source source) {
        if (metrics == null || metrics.isUnsatisfied()) {
            return;
        }
        metrics.get().recordException(t, layer, source);
    }

    private static @NonNull List<SessionInstanceInfo> getSessionInstanceInfos(SearchResponse<Session> response) {
        if (response.hits().hits().isEmpty()) {
            throw new BaseException(
                    ResponseCodeEnum.SESSION_INSTANCE_NOT_FOUND.description(),
                    ResponseCodeEnum.SESSION_INSTANCE_NOT_FOUND.description(),
                    HttpStatus.SC_NOT_FOUND,
                    ResponseCodeEnum.SESSION_INSTANCE_NOT_FOUND.code()
            );
        }

        Session session = response.hits().hits().get(0).source();
        return (session != null && session.getSessionInstances() != null)
                ? session.getSessionInstances()
                : Collections.emptyList();
    }

    private void logElasticsearchError(ElasticsearchException e) {
        LoggingUtil.logError(log, METHOD_LOG_ES_ERROR, null, "=== Elasticsearch Exception Details ===");
        LoggingUtil.logError(log, METHOD_LOG_ES_ERROR, null, "Status: %s", e.status());

        if (e.error() != null) {
            LoggingUtil.logError(log, METHOD_LOG_ES_ERROR, null, "Error type: %s", e.error().type());
            LoggingUtil.logError(log, METHOD_LOG_ES_ERROR, null, "Error reason: %s", e.error().reason());

            if (e.error().rootCause() != null && !e.error().rootCause().isEmpty()) {
                LoggingUtil.logError(log, METHOD_LOG_ES_ERROR, null, "Root causes:");
                e.error().rootCause().forEach(cause ->
                        LoggingUtil.logError(log, METHOD_LOG_ES_ERROR, null,
                                "  - Type: %s, Reason: %s", cause.type(), cause.reason())
                );
            }

            if (e.error().metadata() != null) {
                LoggingUtil.logError(log, METHOD_LOG_ES_ERROR, null, "Metadata: %s", e.error().metadata());
            }
        }
        LoggingUtil.logError(log, METHOD_LOG_ES_ERROR, null, "=====================================");
    }

    /**
     * The instant a requested range bound refers to.
     *
     * <p>The console sends its bounds the way it displays timestamps: wall-clock date-times with
     * no offset (a plain {@code yyyy-MM-dd} is also accepted). They are read in the deployment
     * zone, the zone those displayed timestamps are in. Reading them as UTC searched a window
     * shifted by the zone's offset instead: sessions shown on the selected day fell outside it,
     * and sessions from the neighbouring day fell inside it. A bound that does carry an offset is
     * honoured as given.
     */
    public Instant parseDate(String date) {
        return parseBound(date, LocalTime.MIDNIGHT);
    }

    /**
     * The instant the end of the requested day refers to. As before, an end bound covers the whole
     * of its day (23:59:59), whatever time of day was supplied with it.
     */
    private Instant parseEndOfDay(String date) {
        ZoneId zone = deploymentZone();
        return LocalDate.ofInstant(parseBound(date, END_OF_DAY), zone)
                .atTime(END_OF_DAY)
                .atZone(zone)
                .toInstant();
    }

    private Instant parseBound(String date, LocalTime defaultTime) {
        ZoneId zone = deploymentZone();
        TemporalAccessor parsed = BOUND_FORMAT.parseBest(date,
                OffsetDateTime::from, LocalDateTime::from, LocalDate::from);

        if (parsed instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        }
        if (parsed instanceof LocalDateTime localDateTime) {
            return localDateTime.atZone(zone).toInstant();
        }
        return ((LocalDate) parsed).atTime(defaultTime).atZone(zone).toInstant();
    }

    private ZoneId deploymentZone() {
        return DateTimeUtil.zoneOf(timezone);
    }

    private String getSearchIndex() {
        return sessionsIndex + "-*";
    }

    private List<String> getTargetIndices(String startTime, String endTime) {
        ZoneId zone = deploymentZone();
        LocalDate from = LocalDate.ofInstant(parseDate(startTime), zone);
        LocalDate to   = LocalDate.ofInstant(parseEndOfDay(endTime), zone);

        List<String> indices = new ArrayList<>();
        LocalDate current = from;
        while (!current.isAfter(to)) {
            indices.add(sessionsIndex + "-" + current.format(INDEX_DATE_SUFFIX));
            current = current.plusDays(1);
        }
        return indices;
    }

    private Uni<List<String>> filterExistingIndices(List<String> indices) {
        if (indices.isEmpty()) {
            return Uni.createFrom().item(Collections.emptyList());
        }
        return Multi.createFrom().iterable(indices)
                .onItem().transformToUniAndConcatenate(index ->
                        Uni.createFrom().completionStage(() -> client.indices().exists(e -> e.index(index)))
                                .map(resp -> resp.value() ? index : null)
                                .onFailure().recoverWithItem(err -> {
                                    LoggingUtil.logWarn(log, "filterExistingIndices",
                                            "Could not check existence of index %s, skipping: %s", index, err.getMessage());
                                    return null;
                                })
                                .onItem().invoke(result -> {
                                    if (result == null) {
                                        LoggingUtil.logWarn(log, "filterExistingIndices",
                                                "Index does not exist, skipping: %s", index);
                                    }
                                })
                )
                .filter(Objects::nonNull)
                .collect().asList();
    }
}
