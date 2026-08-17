package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.json.JsonData;
import com.csg.airtel.aaa4j.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ElasticSearchService {

    private static final Logger LOG = Logger.getLogger(ElasticSearchService.class);

    /**
     * Painless script that appends a single SessionInstanceInfo to the session document and
     * refreshes the scalar top-level fields, without the application ever re-sending the whole
     * (unbounded, ever-growing) sessionInstances list.
     *
     * Previously every event re-indexed the full document, so a session with N events did
     * O(N^2) work in app-side serialization + payload bytes — the dominant cause of Kafka lag
     * at ~2000 TPS on a single Vert.x event loop. Here each event sends exactly one instance.
     *
     * The messageId guard keeps the operation idempotent under at-least-once redelivery and
     * across the primary + mirror topics, which both target the same document id.
     */
    private static final String APPEND_INSTANCE_SCRIPT =
            "def existing = ctx._source.sessionInstances;" +
            "if (existing == null) { existing = new ArrayList(); }" +
            "boolean dup = false;" +
            "for (int i = 0; i < existing.size(); i++) {" +
            "  if (existing.get(i).messageId == params.instance.messageId) { dup = true; break; }" +
            "}" +
            "ctx._source.putAll(params.session);" +
            "if (!dup) { existing.add(params.instance); }" +
            "ctx._source.sessionInstances = existing;";

    private static final DateTimeFormatter INDEX_DATE_SUFFIX = DateTimeFormatter.ofPattern("yyyy.MM.dd");

    @Inject
    ElasticsearchAsyncClient elasticsearchClient;

    @Inject
    Instance<ExceptionMetricsService> metrics;

    @ConfigProperty(name = "sessions-data")
    String sessionsIndex;

    @ConfigProperty(name = "TZ", defaultValue = "UTC")
    String timezone;

    /**
     * Append one event's instance info to the session document, creating the document on the
     * first event (upsert) and appending on subsequent events (scripted update). Replaces the
     * former full-document re-index so per-event cost stays O(1) regardless of session length.
     */
    public Uni<Void> appendInstance(Session session, String id, String targetIndex, SessionInstanceInfo instanceInfo) {
        // The upsert document (used only when the document does not yet exist) must already
        // carry the first instance; for existing documents the script appends it instead.
        session.setSessionInstances(new ArrayList<>(List.of(instanceInfo)));

        Map<String, JsonData> params = new HashMap<>();
        params.put("session", JsonData.of(session));
        params.put("instance", JsonData.of(instanceInfo));

        UpdateRequest<Session, Session> request = new UpdateRequest.Builder<Session, Session>()
                .index(targetIndex)
                .id(id)
                .script(s -> s.inline(i -> i
                        .lang("painless")
                        .source(APPEND_INSTANCE_SCRIPT)
                        .params(params)))
                .upsert(session)
                .retryOnConflict(3)
                .build();

        return Uni.createFrom().completionStage(() -> elasticsearchClient.update(request, Session.class))
                .invoke(response -> LoggingUtil.logDebug(LOG, "appendInstance",
                        "Session instance appended: %s, status: %s, result: %s",
                        session.getSessionId(), session.getConnectionStatus(), response.result()))
                .onFailure().invoke(e -> {
                    LoggingUtil.logError(LOG, "appendInstance", e,
                            "Failed to append session instance: %s", session.getSessionId());
                    if (metrics != null && !metrics.isUnsatisfied()) {
                        metrics.get().recordException(
                                (Throwable) e,
                                ExceptionMetricsService.Layer.CLIENT,
                                ExceptionMetricsService.Source.ELASTICSEARCH);
                    }
                })
                .replaceWithVoid();
    }

    /**
     * Name of the daily index the current event belongs to.
     *
     * <p>The suffix is derived in the deployment zone, not UTC, because that is the zone the
     * timestamps on the document itself are converted to (see {@code SessionService}) and the zone
     * ConnectionHistoryService uses to expand a requested date range into index names. Deriving it
     * in UTC instead put documents written either side of local midnight into the neighbouring
     * day's index, where a search scoped to their own {@code startTime} date could not find them.
     */
    public String getCurrentIndex() {
        return sessionsIndex + "-" + LocalDate.now(ZoneId.of(timezone)).format(INDEX_DATE_SUFFIX);
    }
}
