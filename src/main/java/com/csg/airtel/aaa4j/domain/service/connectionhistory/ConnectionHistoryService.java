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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
            String startTime,
            String endTime,
            int pageSize,
            int page
    ) throws BaseException {

        try {
            log.info("Start fetching session info list.");
            log.debug("Fetching sessions - userName: {}, status: {}, sessionId: {}, groupId: {}, page: {}/{}",
                    username, connectionStatus, sessionId, groupId, page, pageSize);

            // Check if index exists first
            if (!indexExists()) {
                log.error("Index does not exist: {}", sessionsIndex);
                throw new BaseException(
                        "Elasticsearch index not found: " + sessionsIndex,
                        "INDEX_NOT_FOUND",
                        HttpStatus.SC_SERVICE_UNAVAILABLE,
                        "ES_001"
                );
            }

            SearchResponse<Session> response = client.search(s -> s
                            .index(sessionsIndex)
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

                                if (startTime != null || endTime != null) {
                                    b.filter(filterQ -> filterQ.range(rangeQ -> {
                                        rangeQ.field("startTime");
                                        if (startTime != null)
                                            rangeQ.gte(JsonData.of(parseDate(startTime).toEpochMilli()));
                                        if (endTime != null)
                                            rangeQ.lte(JsonData.of(parseDate(endTime).toEpochMilli()));
                                        return rangeQ;
                                    }));
                                }

                                return b;
                            })),
                    Session.class  // CHANGED: Use Session.class instead of Object.class
            );

            log.debug("Elasticsearch response - total hits: {}",
                    response.hits().total() != null ? response.hits().total().value() : 0);

            List<Session> data = new ArrayList<>();

            if (! response.hits().hits().isEmpty()) {
                data = response.hits().hits().stream()
                        .map(Hit::source)
                        .toList();
            }

            log.debug("Refined list of session data: {} records", data.size());

            assert response.hits().total() != null;
            PageDetails pageDetails = new PageDetails(
                    response.hits().total().value(),
                    page,
                    data.size()
            );

            log.info("Session data fetched successfully. Total records: {}",
                    pageDetails.getTotalRecords());

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

            // Use GET API instead of SEARCH API for ID lookup
            GetResponse<Session> response = client.get(g -> g
                            .index(sessionsIndex)
                            .id(sessionId),
                    Session.class
            );

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

            // Get session instances
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

    /**
     * Check if index exists
     */
    private boolean indexExists() {
        try {
            return client.indices().exists(e -> e.index(sessionsIndex)).value();
        } catch (Exception e) {
            log.error("Error checking if index exists: ", e);
            return false;
        }
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
}