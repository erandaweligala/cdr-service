package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@ApplicationScoped
public class ElasticSearchService {

    private static final Logger LOG = Logger.getLogger(ElasticSearchService.class);

    @Inject
    ElasticsearchAsyncClient elasticsearchClient;

    @ConfigProperty(name = "sessions-data")
    String sessionsIndex;

    public Uni<Void> indexSession(Session session, String id, String targetIndex) {
        IndexRequest<Session> request = IndexRequest.of(i -> i
                .index(targetIndex)
                .id(id)
                .document(session)
        );

        return Uni.createFrom().completionStage(() -> elasticsearchClient.index(request))
                .invoke(response -> LOG.debugf("Session indexed: %s, status: %s, result: %s",
                        session.getSessionId(), session.getConnectionStatus(), response.result()))
                .onFailure().invoke(e ->
                        LOG.errorf((Throwable) e, "Failed to index session: %s", session.getSessionId()))
                .replaceWithVoid();
    }

    public String getCurrentIndex() {
        return sessionsIndex + "-" + LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));
    }
}
