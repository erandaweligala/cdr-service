package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.put_index_template.IndexTemplateMapping;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateRequest;
import com.csg.airtel.aaa4j.common.LoggingUtil;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Owns the Elasticsearch mapping of the daily {@code radius-sessions-*} indices.
 *
 * <p>The indices are created implicitly by the first write of the day, so without a template
 * their mapping is whatever dynamic mapping infers from the first document that happens to
 * arrive. That made the mapping a function of deployment history rather than of the model: an
 * earlier revision serialized the session timestamps as epoch numbers, so dynamic mapping typed
 * {@code startTime} as {@code long}, and every later write of the same field as an ISO-8601
 * string was rejected with
 * <pre>document_parsing_exception ... failed to parse field [startTime] of type [long]</pre>
 * while the range query in {@link ConnectionHistoryService} failed on the same index with
 * <pre>query_shard_exception: failed to create query: For input string: "2026-08-16T00:00:00"</pre>
 * — the numeric field cannot parse a date bound.
 *
 * <p>Declaring the mapping up front removes that coupling: whichever revision creates tomorrow's
 * index, the date fields are {@code date} and the fields the search path filters on keep their
 * {@code .keyword} sub-field. The template is applied at index creation time only, so it cannot
 * repair an index that already exists with a conflicting type; startup therefore also inspects
 * the current index and logs the reindex needed to clear a legacy mapping.
 */
@ApplicationScoped
public class SessionIndexTemplateService {

    private static final Logger LOG = Logger.getLogger(SessionIndexTemplateService.class);

    /**
     * Accepts both the ISO-8601 local date-times the service writes today and the epoch
     * milliseconds written by older revisions, so documents already in flight or replayed from
     * Kafka by an older pod during a rolling upgrade still index.
     */
    static final String DATE_FORMAT = "strict_date_optional_time||epoch_millis";

    /** Above the default (0) so this template wins over any catch-all template on the cluster. */
    static final long TEMPLATE_PRIORITY = 200L;

    private static final long STARTUP_TIMEOUT_SECONDS = 20L;

    @Inject
    ElasticsearchAsyncClient elasticsearchClient;

    @Inject
    ElasticSearchService elasticSearchService;

    @ConfigProperty(name = "sessions-data")
    String sessionsIndex;

    @ConfigProperty(name = "elasticsearch.index-template.enabled", defaultValue = "true")
    boolean templateEnabled;

    /**
     * Install the template before the Kafka consumer starts writing, so the first event of the
     * day creates its index with the declared mapping.
     *
     * <p>This blocks the startup thread briefly on purpose — the template has to exist before the
     * first write — but never fails the boot: a cluster that is unreachable at startup should
     * leave the service running (and retrying its writes) rather than crash-loop the pod.
     */
    void onStart(@Observes StartupEvent event) {
        if (!templateEnabled) {
            LoggingUtil.logInfo(LOG, "onStart",
                    "Session index template installation disabled by configuration.");
            return;
        }
        try {
            applyTemplate().get(STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            LoggingUtil.logInfo(LOG, "onStart",
                    "Session index template '%s' applied for pattern '%s'.",
                    templateName(), indexPattern());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LoggingUtil.logWarn(LOG, "onStart",
                    "Interrupted while applying session index template '%s'.", templateName());
            return;
        } catch (Exception e) {
            LoggingUtil.logError(LOG, "onStart", e,
                    "Failed to apply session index template '%s'. Daily indices created before the "
                            + "next successful startup will fall back to dynamic mapping.", templateName());
            return;
        }

        verifyCurrentIndexMapping();
    }

    private java.util.concurrent.CompletableFuture<?> applyTemplate() {
        return elasticsearchClient.indices().putIndexTemplate(buildTemplateRequest());
    }

    /**
     * Warn — with the exact remediation — when the index being written to today predates the
     * template and still carries an incompatible {@code startTime} type. Nothing is mutated:
     * an existing field mapping cannot be changed in place, so clearing it is a reindex, which
     * is an operator decision rather than something to do implicitly on every pod start.
     */
    private void verifyCurrentIndexMapping() {
        String index = elasticSearchService.getCurrentIndex();
        try {
            boolean exists = elasticsearchClient.indices().exists(e -> e.index(index))
                    .get(STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .value();
            if (!exists) {
                return;
            }

            GetMappingResponse mapping = elasticsearchClient.indices().getMapping(m -> m.index(index))
                    .get(STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            String conflicting = firstNonDateDateField(mapping, index);
            if (conflicting != null) {
                LoggingUtil.logError(LOG, "verifyCurrentIndexMapping", null,
                        "Index %s maps [%s] with a non-date type, so writes and date range queries "
                                + "against it will keep failing. Reindex it into a new index created "
                                + "from template '%s' (the mapping of an existing field cannot be "
                                + "changed in place).",
                        index, conflicting, templateName());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LoggingUtil.logWarn(LOG, "verifyCurrentIndexMapping",
                    "Could not verify the mapping of index %s: %s", index, e.getMessage());
        }
    }

    /**
     * @return the name of the first top-level date field of the model that the given index maps
     * as something other than {@code date}, or null when the mapping is compatible.
     */
    static String firstNonDateDateField(GetMappingResponse mapping, String index) {
        var record = mapping.result().get(index);
        if (record == null || record.mappings() == null) {
            return null;
        }
        Map<String, Property> properties = record.mappings().properties();
        for (String field : List.of("startTime", "endTime", "updatedTime")) {
            Property property = properties.get(field);
            if (property != null && !property.isDate()) {
                return field;
            }
        }
        return null;
    }

    /**
     * The mapping mirrors the indexed model: {@code Session} plus its {@code sessionInstances}
     * objects. Date fields are pinned so they can never be inferred as numbers again, and the
     * fields the search path filters on are declared as text with a {@code keyword} sub-field
     * because the queries address them as {@code userName.keyword}, {@code sessionId.keyword},
     * {@code groupId.keyword}, {@code connectionStatus.keyword} and {@code uniqueId.keyword}.
     * {@code date_detection} is switched off so a future string field cannot be silently typed
     * as a date either.
     */
    PutIndexTemplateRequest buildTemplateRequest() {
        Map<String, Property> instanceProperties = new LinkedHashMap<>();
        instanceProperties.put("dateTime", dateProperty());
        instanceProperties.put("messageId", textWithKeyword());
        instanceProperties.put("messageType", textWithKeyword());
        instanceProperties.put("serviceId", textWithKeyword());
        instanceProperties.put("bucketId", textWithKeyword());
        instanceProperties.put("usage", longProperty());

        Map<String, Property> properties = new LinkedHashMap<>();
        properties.put("uniqueId", textWithKeyword());
        properties.put("sessionId", textWithKeyword());
        properties.put("userName", textWithKeyword());
        properties.put("groupId", textWithKeyword());
        properties.put("connectionStatus", textWithKeyword());
        properties.put("indexName", textWithKeyword());
        properties.put("startTime", dateProperty());
        properties.put("endTime", dateProperty());
        properties.put("updatedTime", dateProperty());
        properties.put("usage", longProperty());
        properties.put("sessionInstances", Property.of(p -> p.object(o -> o.properties(instanceProperties))));

        TypeMapping mappings = TypeMapping.of(m -> m
                .dateDetection(false)
                .properties(properties));

        return PutIndexTemplateRequest.of(t -> t
                .name(templateName())
                .indexPatterns(indexPattern())
                .priority(TEMPLATE_PRIORITY)
                .template(IndexTemplateMapping.of(tm -> tm.mappings(mappings))));
    }

    private static Property dateProperty() {
        return Property.of(p -> p.date(d -> d.format(DATE_FORMAT)));
    }

    private static Property longProperty() {
        return Property.of(p -> p.long_(l -> l));
    }

    private static Property textWithKeyword() {
        return Property.of(p -> p.text(t -> t
                .fields("keyword", f -> f.keyword(k -> k.ignoreAbove(256)))));
    }

    String templateName() {
        return sessionsIndex + "-template";
    }

    String indexPattern() {
        return sessionsIndex + "-*";
    }
}
