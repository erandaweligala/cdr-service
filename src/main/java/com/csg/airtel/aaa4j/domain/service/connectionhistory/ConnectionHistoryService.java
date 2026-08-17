package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.mapping.FieldType;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.csg.airtel.aaa4j.common.DateTimeUtil;
import com.csg.airtel.aaa4j.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.model.PageDetails;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
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
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@ApplicationScoped
public class ConnectionHistoryService {

    private static final Logger log = Logger.getLogger(ConnectionHistoryService.class);

    @ConfigProperty(name = "sessions-data")
    String sessionsIndex;

    private final ElasticsearchAsyncClient client;
    private final ServiceExceptionHandler handler;

    @Inject
    Instance<ExceptionMetricsService> metrics;

    private final ZoneId deploymentZone;

    public ConnectionHistoryService(
            ElasticsearchAsyncClient client,
            ServiceExceptionHandler handler,
            @ConfigProperty(name = "TZ", defaultValue = "UTC") String timezone) {
        this.client = client;
        this.handler = handler;
        this.deploymentZone = ZoneId.of(timezone);
    }

    public Uni<BaseResponse<Session>> fetchSessionDetails(
            String username,
            String connectionStatus,
            String sessionId,
            String groupId,
            String startDate,
            String endDate,
            int pageSize,
            int page
    ) {
        LoggingUtil.logInfo(log, "fetchSessionDetails", "Start fetching session info list.");

        String startTime = startDate != null ? startDate : LocalDate.now(deploymentZone).minusDays(7).toString();
        String endTime = endDate != null ? endDate : LocalDate.now(deploymentZone).toString();

        List<String> targetIndices = getTargetIndices(startTime, endTime);

        return filterExistingIndices(targetIndices)
                .flatMap(existingIndices -> {
                    if (existingIndices.isEmpty()) {
                        LoggingUtil.logWarn(log, "fetchSessionDetails",
                                "No existing indices found in range [%s - %s]. Returning empty result.", startTime, endTime);
                        return Uni.createFrom().item(BaseResponse.success(
                                ResponseCodeEnum.SUCCESSFUL.description(),
                                Collections.<Session>emptyList(),
                                new PageDetails(0, page, 0)
                        ));
                    }

                    String startFormatted = parseToStartLocalDateTime(startTime).format(DateTimeUtil.LOCAL_DATE_TIME_FORMATTER);
                    String endFormatted = parseToEndLocalDateTime(endTime).format(DateTimeUtil.LOCAL_DATE_TIME_FORMATTER);

                    return Uni.createFrom().completionStage(() -> client.search(s -> s
                                            .index(existingIndices)
                                            // The indices were filtered for existence above, but a
                                            // daily index can still be dropped by retention between
                                            // that check and this search; skip it rather than fail
                                            // the whole request.
                                            .ignoreUnavailable(true)
                                            .allowNoIndices(true)
                                            .from((pageSize * page) - pageSize)
                                            .size(pageSize)
                                            // unmappedType keeps the sort working across the day
                                            // boundary: an index created before any session was
                                            // written to it has no startTime mapping, and without
                                            // this the sort fails the search on all indices.
                                            .sort(srt -> srt
                                                    .field(f -> f.field("startTime")
                                                            .order(SortOrder.Desc)
                                                            .unmappedType(FieldType.Date))
                                            )
                                            .query(q -> q.bool(b -> {

                                                if (username != null && !username.isEmpty())
                                                    b.must(query -> query.term(term -> term
                                                            .field("userName.keyword")
                                                            .value(username)));

                                                if (connectionStatus != null && !connectionStatus.isEmpty())
                                                    b.must(query -> query.term(term -> term
                                                            .field("connectionStatus.keyword")
                                                            .value(connectionStatus)));

                                                if (sessionId != null && !sessionId.isEmpty())
                                                    b.must(query -> query.term(term -> term
                                                            .field("sessionId.keyword")
                                                            .value(sessionId)));

                                                if (groupId != null && !groupId.isEmpty())
                                                    b.must(query -> query.term(term -> term
                                                            .field("groupId.keyword")
                                                            .value(groupId)));

                                                b.filter(filterQ -> filterQ.range(rangeQ -> {
                                                    rangeQ.field("startTime");
                                                    rangeQ.gte(JsonData.of(startFormatted));
                                                    rangeQ.lte(JsonData.of(endFormatted));
                                                    return rangeQ;
                                                }));

                                                return b;
                                            })),
                                    Session.class
                            ))
                            .map(response -> {
                                List<Session> data = new ArrayList<>();
                                if (!response.hits().hits().isEmpty()) {
                                    data = response.hits().hits().stream()
                                            .map(Hit::source)
                                            .toList();
                                }

                                assert response.hits().total() != null;
                                PageDetails pageDetails = new PageDetails(
                                        response.hits().total().value(),
                                        page,
                                        data.size()
                                );

                                LoggingUtil.logInfo(log, "fetchSessionDetails",
                                        "Session data fetched successfully. Total records: %s", pageDetails.getTotalRecords());

                                return BaseResponse.success(
                                        ResponseCodeEnum.SUCCESSFUL.description(),
                                        data,
                                        pageDetails
                                );
                            });
                })
                .onFailure().transform(this::mapFailure);
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
        Throwable cause = (ex instanceof java.util.concurrent.CompletionException
                || ex instanceof java.util.concurrent.ExecutionException)
                ? (ex.getCause() != null ? ex.getCause() : ex)
                : ex;

        if (cause instanceof BaseException be) {
            recordMetric(be, ExceptionMetricsService.Layer.SERVICE, ExceptionMetricsService.Source.INTERNAL);
            return be;
        }
        if (cause instanceof ElasticsearchException ese) {
            LoggingUtil.logError(log, "mapFailure", ese, "Elasticsearch error: ");
            logElasticsearchError(ese);
            recordMetric(ese, ExceptionMetricsService.Layer.CLIENT, ExceptionMetricsService.Source.ELASTICSEARCH);
            return handler.elasticsearchExceptionHandler(ese);
        }
        if (cause instanceof java.io.IOException) {
            LoggingUtil.logError(log, "mapFailure", cause, "IO error communicating with Elasticsearch: ");
            recordMetric(cause, ExceptionMetricsService.Layer.CLIENT, ExceptionMetricsService.Source.ELASTICSEARCH);
            return handler.elasticsearchExceptionHandler(cause);
        }
        LoggingUtil.logError(log, "mapFailure", cause, "Unexpected error: ");
        recordMetric(cause, ExceptionMetricsService.Layer.SERVICE, ExceptionMetricsService.Source.INTERNAL);
        return handler.serviceLayerExceptionHandler(cause);
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
        LoggingUtil.logError(log, "logElasticsearchError", null, "=== Elasticsearch Exception Details ===");
        LoggingUtil.logError(log, "logElasticsearchError", null, "Status: %s", e.status());

        if (e.error() != null) {
            LoggingUtil.logError(log, "logElasticsearchError", null, "Error type: %s", e.error().type());
            LoggingUtil.logError(log, "logElasticsearchError", null, "Error reason: %s", e.error().reason());

            if (e.error().rootCause() != null && !e.error().rootCause().isEmpty()) {
                LoggingUtil.logError(log, "logElasticsearchError", null, "Root causes:");
                e.error().rootCause().forEach(cause ->
                        LoggingUtil.logError(log, "logElasticsearchError", null,
                                "  - Type: %s, Reason: %s", cause.type(), cause.reason())
                );
            }

            if (e.error().metadata() != null) {
                LoggingUtil.logError(log, "logElasticsearchError", null, "Metadata: %s", e.error().metadata());
            }
        }
        LoggingUtil.logError(log, "logElasticsearchError", null, "=====================================");
    }

    /**
     * Parse an input date string to an Instant representing the start of the interval.
     * Accepts ISO instant (with zone), ISO local date-time, or ISO date (yyyy-MM-dd).
     * If the input is null, returns start of day for (now - 7 days) in UTC.
     */
    public Instant parseToStartInstant(String date) {
        if (date == null) {
            return LocalDate.now().minusDays(7).atStartOfDay(ZoneOffset.UTC).toInstant();
        }
        try {
            return Instant.parse(date);
        } catch (DateTimeParseException ignored) {
        }
        try {
            LocalDateTime ldt = LocalDateTime.parse(date);
            return ldt.atZone(ZoneOffset.UTC).toInstant();
        } catch (DateTimeParseException ignored) {
        }
        try {
            LocalDate ld = LocalDate.parse(date);
            return ld.atStartOfDay(ZoneOffset.UTC).toInstant();
        } catch (DateTimeParseException e) {
            throw new BaseException(
                    "Invalid date format: " + date,
                    ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(),
                    HttpStatus.SC_BAD_REQUEST,
                    ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code()
            );
        }
    }

    /**
     * Parse an input date string to an Instant representing the end of the interval (23:59:59 UTC).
     * Accepts ISO instant (with zone), ISO local date-time, or ISO date (yyyy-MM-dd).
     * If the input is null, returns end of today in UTC.
     */
    public Instant parseToEndInstant(String date) {
        if (date == null) {
            return LocalDate.now().atTime(23, 59, 59).atZone(ZoneOffset.UTC).toInstant();
        }
        try {
            return Instant.parse(date);
        } catch (DateTimeParseException ignored) {
        }
        try {
            LocalDateTime ldt = LocalDateTime.parse(date);
            // if a time was provided, use it; otherwise, caller's string included time portion
            return ldt.atZone(ZoneOffset.UTC).toInstant();
        } catch (DateTimeParseException ignored) {
        }
        try {
            LocalDate ld = LocalDate.parse(date);
            return ld.atTime(23, 59, 59).atZone(ZoneOffset.UTC).toInstant();
        } catch (DateTimeParseException e) {
            throw new BaseException(
                    "Invalid date format: " + date,
                    ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(),
                    HttpStatus.SC_BAD_REQUEST,
                    ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code()
            );
        }
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

    /**
     * Parse an input date string to the start-of-range LocalDateTime, in the pod's
     * deployment zone. Accepts a zoned Instant string (converted via deploymentZone),
     * an already-zone-less LocalDateTime string, or a plain date (start of day).
     */
    public LocalDateTime parseToStartLocalDateTime(String date) {
        if (date == null) {
            return LocalDate.now(deploymentZone).minusDays(7).atStartOfDay();
        }
        try {
            return DateTimeUtil.toLocal(Instant.parse(date), deploymentZone);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return LocalDateTime.parse(date);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return LocalDate.parse(date).atStartOfDay();
        } catch (DateTimeParseException e) {
            throw new BaseException(
                    "Invalid date format: " + date,
                    ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(),
                    HttpStatus.SC_BAD_REQUEST,
                    ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code()
            );
        }
    }

    /**
     * Parse an input date string to the end-of-range LocalDateTime (23:59:59),
     * in the pod's deployment zone.
     */
    public LocalDateTime parseToEndLocalDateTime(String date) {
        if (date == null) {
            return LocalDate.now(deploymentZone).atTime(23, 59, 59);
        }
        try {
            return DateTimeUtil.toLocal(Instant.parse(date), deploymentZone);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return LocalDateTime.parse(date);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return LocalDate.parse(date).atTime(23, 59, 59);
        } catch (DateTimeParseException e) {
            throw new BaseException(
                    "Invalid date format: " + date,
                    ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(),
                    HttpStatus.SC_BAD_REQUEST,
                    ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code()
            );
        }
    }

    // Backwards-compatible alias used by some tests / callers
    public LocalDateTime parseDate(String date) {
        return parseToStartLocalDateTime(date);
    }

    private String getSearchIndex() {
        return sessionsIndex + "-*";
    }

    private List<String> getTargetIndices(String startTime, String endTime) {
        LocalDate from = parseToStartLocalDateTime(startTime).toLocalDate();
        LocalDate to   = parseToEndLocalDateTime(endTime).toLocalDate();

        List<String> indices = new ArrayList<>();
        LocalDate current = from;
        while (!current.isAfter(to)) {
            indices.add(sessionsIndex + "-" + current.format(DateTimeFormatter.ofPattern("yyyy.MM.dd")));
            current = current.plusDays(1);
        }
        return indices;
    }

}
