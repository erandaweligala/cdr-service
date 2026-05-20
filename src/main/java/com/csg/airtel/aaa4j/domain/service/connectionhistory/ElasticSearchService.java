package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.IOException;

@ApplicationScoped
public class ElasticSearchService {

    private static final Logger LOG = Logger.getLogger(ElasticSearchService.class);

    @Inject
    ElasticsearchClient elasticsearchClient;

    @ConfigProperty(name = "sessions-data")
    String sessionsIndex;

    /**
     * Index a session (works for both active and completed sessions)
     */
    public void indexSession(Session session, String id) {
        try {
            IndexRequest<Session> request = IndexRequest.of(i -> i
                    .index(sessionsIndex)
                    .id(id)
                    .document(session)
            );

            IndexResponse response = elasticsearchClient.index(request);
            LOG.infof("Session indexed: %s, status: %s, result: %s",
                    session.getSessionId(), session.getConnectionStatus(), response.result());
        } catch (IOException e) {
            LOG.errorf(e, "Failed to index session: %s", session.getSessionId());
            throw new RuntimeException("Failed to index session", e);
        }
    }
}