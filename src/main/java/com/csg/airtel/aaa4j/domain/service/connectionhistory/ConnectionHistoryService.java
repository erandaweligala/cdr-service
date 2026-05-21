package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.model.PageDetails;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;
import com.csg.airtel.aaa4j.domain.util.exceptions.BaseException;
import com.csg.airtel.aaa4j.domain.util.exceptions.ServiceExceptionHandler;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.http.HttpStatus;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@ApplicationScoped
public class ConnectionHistoryService {

    private static final Logger log = LoggerFactory.getLogger(ConnectionHistoryService.class);

    @ConfigProperty(name = "sessions-data")
    String sessionsIndex;

    private final ElasticsearchAsyncClient client;
    private final ServiceExceptionHandler handler;

    public ConnectionHistoryService(ElasticsearchAsyncClient client, ServiceExceptionHandler handler) {
        this.client = client;
        this.handler = handler;
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
        log.info("Start fetching session info list.");

        String startTime = startDate != null ? startDate : LocalDate.now().minusDays(7).toString();
        String endTime = endDate != null ? endDate : LocalDate.now().toString();

        List<String> targetIndices = getTargetIndices(startTime, endTime);

        Instant endInstant = LocalDateTime.parse(endTime)
                .toLocalDate()
                .atTime(23, 59, 59)
                .atZone(ZoneId.of("UTC"))
                .toInstant();

        return Uni.createFrom().completionStage(() ->
                        client.search(s -> s
                                        .index(targetIndices)
                                        .from((pageSize * page) - pageSize)
                                        .size(pageSize)
                                        .sort(srt -> srt
                                                .field(f -> f.field("startTime").order(SortOrder.Desc))
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
                                                rangeQ.gte(JsonData.of(parseDate(startTime).toEpochMilli()));
                                                rangeQ.lte(JsonData.of(endInstant.toEpochMilli()));
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

                    log.info("Session data fetched successfully. Total records: {}", pageDetails.getTotalRecords());

                    return BaseResponse.success(
                            ResponseCodeEnum.SUCCESSFUL.description(),
                            data,
                            pageDetails
                    );
                })
                .onFailure().transform(this::mapFailure);
    }

    public Uni<BaseResponse<SessionInstanceInfo>> fetchSessionInstances(String sessionId) {
        log.info("Fetching session instances for sessionId: {}", sessionId);

        return Uni.createFrom().completionStage(() ->
                        client.get(g -> g
                                        .index(getSearchIndex())
                                        .id(sessionId),
                                Session.class
                        ))
                .map(response -> {
                    log.debug("Elasticsearch GET response: found={}, source={}",
                            response.found(), response.source());

                    if (!response.found() || response.source() == null) {
                        throw new BaseException(
                                ResponseCodeEnum.SESSION_INSTANCE_NOT_FOUND.description(),
                                ResponseCodeEnum.SESSION_INSTANCE_NOT_FOUND.description(),
                                HttpStatus.SC_NOT_FOUND,
                                ResponseCodeEnum.SESSION_INSTANCE_NOT_FOUND.code()
                        );
                    }

                    Session session = response.source();
                    List<SessionInstanceInfo> data = (session.getSessionInstances() != null)
                            ? session.getSessionInstances()
                            : Collections.emptyList();

                    log.info("Refined list of session instances: {} records", data.size());

                    return BaseResponse.success(
                            ResponseCodeEnum.SUCCESSFUL.description(),
                            data,
                            null
                    );
                })
                .onFailure().transform(this::mapFailure);
    }

    private Throwable mapFailure(Throwable ex) {
        Throwable cause = (ex.getCause() != null
                && (ex instanceof java.util.concurrent.CompletionException
                || ex instanceof java.util.concurrent.ExecutionException))
                ? ex.getCause() : ex;

        if (cause instanceof BaseException be) {
            return be;
        }

        if (cause instanceof ElasticsearchException ese) {
            log.error("Elasticsearch error: ", ese);
            logElasticsearchError(ese);
            return handler.elasticsearchExceptionHandler(ese);
        }

        if (cause instanceof java.io.IOException) {
            log.error("IO error communicating with Elasticsearch: ", cause);
            return handler.elasticsearchExceptionHandler(cause);
        }

        log.error("Unexpected error: ", cause);
        return handler.serviceLayerExceptionHandler(cause);
    }

    /**
     * Log detailed Elasticsearch error information
     */
    private void logElasticsearchError(ElasticsearchException e) {
        log.error("=== Elasticsearch Exception Details ===");
        log.error("Status: {}", e.status());

        if (e.error() != null) {
            log.error("Error type: {}", e.error().type());
            log.error("Error reason: {}", e.error().reason());

            if (e.error().rootCause() != null && !e.error().rootCause().isEmpty()) {
                log.error("Root causes:");
                e.error().rootCause().forEach(cause ->
                        log.error("  - Type: {}, Reason: {}", cause.type(), cause.reason())
                );
            }

            if (e.error().metadata() != null && !e.error().metadata().isEmpty()) {
                String metadataStr = e.error().metadata().entrySet().stream()
                        .map(entry -> entry.getKey() + "="
                                + (entry.getValue() != null ? entry.getValue().toJson() : "null"))
                        .collect(java.util.stream.Collectors.joining(", ", "{", "}"));
                log.error("Metadata: {}", metadataStr);
            }
        }
        log.error("=====================================");
    }

    public Instant parseDate(String date) {
        return LocalDateTime
                .parse(date)
                .atZone(ZoneId.of("UTC"))
                .toInstant();
    }

    private String getSearchIndex() {
        return sessionsIndex + "-*";
    }

    private List<String> getTargetIndices(String startTime, String endTime) {
        LocalDate from = parseDate(startTime).atZone(ZoneOffset.UTC).toLocalDate();
        LocalDate to = parseDate(endTime).atZone(ZoneOffset.UTC).toLocalDate();

        List<String> indices = new ArrayList<>();
        LocalDate current = from;
        while (!current.isAfter(to)) {
            indices.add(sessionsIndex + "-" + current.format(DateTimeFormatter.ofPattern("yyyy.MM.dd")));
            current = current.plusDays(1);
        }
        return indices;
    }
}
