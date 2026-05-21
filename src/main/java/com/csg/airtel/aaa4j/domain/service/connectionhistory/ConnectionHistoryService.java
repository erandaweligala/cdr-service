package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.model.PageDetails;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;
import com.csg.airtel.aaa4j.domain.util.exceptions.BaseException;
import com.csg.airtel.aaa4j.domain.util.exceptions.ServiceExceptionHandler;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.http.HttpStatus;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    private final ElasticsearchClient client;
    private final ServiceExceptionHandler handler;

    public ConnectionHistoryService(ElasticsearchClient client, ServiceExceptionHandler handler) {
        this.client = client;
        this.handler = handler;
    }

    public BaseResponse<Session> fetchSessionDetails(
            String username,
            String connectionStatus,
            String sessionId,
            String groupId,
            String startDate,
            String endDate,
            int pageSize,
            int page
    ) throws BaseException {

        try {
            log.info("Start fetching session info list.");

            String startTime = startDate != null ? startDate : LocalDate.now().minusDays(7).toString();
            String endTime = endDate != null ? endDate : LocalDate.now().toString();

            List<String> targetIndices = getTargetIndices(startTime, endTime);

            // ✅ Filter to only indices that actually exist
            List<String> existingIndices = filterExistingIndices(targetIndices);

            // ✅ If no indices exist at all, return empty result gracefully
            if (existingIndices.isEmpty()) {
                log.warn("No existing indices found in range [{} - {}]. Returning empty result.", startTime, endTime);
                return BaseResponse.success(
                        ResponseCodeEnum.SUCCESSFUL.description(),
                        Collections.emptyList(),
                        new PageDetails(0, page, 0)
                );
            }

            Instant endInstant = LocalDateTime.parse(endTime)
                    .toLocalDate()
                    .atTime(23, 59, 59)
                    .atZone(ZoneId.of("UTC"))
                    .toInstant();

            SearchResponse<Session> response = client.search(s -> s
                            .index(existingIndices)   // ✅ only existing indices
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

                                // startTime and endTime always present now
                                b.filter(filterQ -> filterQ.range(rangeQ -> {
                                    rangeQ.field("startTime");
                                    rangeQ.gte(JsonData.of(parseDate(startTime).toEpochMilli()));
                                    rangeQ.lte(JsonData.of(endInstant.toEpochMilli()));
                                    return rangeQ;
                                }));

                                return b;
                            })),
                    Session.class
            );

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

        } catch (ElasticsearchException ex) {
            log.error("Elasticsearch error: ", ex);
            logElasticsearchError(ex);
            throw handler.elasticsearchExceptionHandler(ex);

        } catch (IOException ex) {
            log.error("IO error communicating with Elasticsearch: ", ex);
            throw handler.elasticsearchExceptionHandler(ex);

        } catch (BaseException ex) {
            throw ex;

        } catch (Exception ex) {
            log.error("Unexpected error: ", ex);
            throw handler.serviceLayerExceptionHandler(ex);
        }
    }

    public BaseResponse<SessionInstanceInfo> fetchSessionInstances(String sessionId) throws BaseException {

        try {
            log.info("Fetching session instances for sessionId: {}", sessionId);

            // ✅ Use SEARCH with wildcard index instead of GET (which requires a specific index).
            //    This tolerates missing daily indices — Elasticsearch skips them automatically
            //    when allow_no_indices=true and ignore_unavailable=true.
            SearchResponse<Session> response = client.search(s -> s
                            .index(getSearchIndex())               // "sessions-data-*" wildcard
                            .allowNoIndices(true)
                            .ignoreUnavailable(true)
                            .size(1)
                            .query(q -> q.term(t -> t
                                    .field("uniqueId.keyword")
                                    .value(sessionId)
                            )),
                    Session.class
            );

            List<SessionInstanceInfo> data = getSessionInstanceInfos(response);

            log.info("Refined list of session instances: {} records", data.size());

            return BaseResponse.success(
                    ResponseCodeEnum.SUCCESSFUL.description(),
                    data,
                    null
            );

        } catch (ElasticsearchException ex) {
            log.error("Elasticsearch error: ", ex);
            logElasticsearchError(ex);
            throw handler.elasticsearchExceptionHandler(ex);

        } catch (IOException ex) {
            log.error("IO error: ", ex);
            throw handler.elasticsearchExceptionHandler(ex);

        } catch (BaseException ex) {
            throw ex;

        } catch (Exception ex) {
            log.error("Unexpected error: ", ex);
            throw handler.serviceLayerExceptionHandler(ex);
        }
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
        List<SessionInstanceInfo> data = (session != null && session.getSessionInstances() != null)
                ? session.getSessionInstances()
                : Collections.emptyList();
        return data;
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

            // Log the failed shard info if available
            if (e.error().metadata() != null) {
                log.error("Metadata: {}", e.error().metadata());
            }
        }
        log.error("=====================================");
    }

    public Instant parseDate(String date){
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
        LocalDate to   = parseDate(endTime).atZone(ZoneOffset.UTC).toLocalDate();

        List<String> indices = new ArrayList<>();
        LocalDate current = from;
        while (!current.isAfter(to)) {
            indices.add(sessionsIndex + "-" + current.format(DateTimeFormatter.ofPattern("yyyy.MM.dd")));
            current = current.plusDays(1);
        }
        return indices;
    }

    private List<String> filterExistingIndices(List<String> indices) {
        List<String> existing = new ArrayList<>();
        for (String index : indices) {
            try {
                boolean exists = client.indices().exists(e -> e.index(index)).value();
                if (exists) {
                    existing.add(index);
                } else {
                    log.warn("Index does not exist, skipping: {}", index);
                }
            } catch (Exception e) {
                log.warn("Could not check existence of index {}, skipping: {}", index, e.getMessage());
            }
        }
        return existing;
    }
}