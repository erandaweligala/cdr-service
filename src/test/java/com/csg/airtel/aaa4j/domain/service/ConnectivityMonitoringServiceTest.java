package com.csg.airtel.aaa4j.domain.service;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.csg.airtel.aaa4j.application.config.ConnectivityMonitoringConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConnectivityMonitoringServiceTest {

    private static final int FAILURE_THRESHOLD = 3;
    private static final String SERVICE_NAME = "cdr-service";

    private MeterRegistry registry;
    private ReactiveRedisDataSource redisDataSource;
    private ElasticsearchAsyncClient elasticsearchClient;
    private ConnectivityMonitoringConfig config;
    private ConnectivityMonitoringService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        redisDataSource = mock(ReactiveRedisDataSource.class);
        elasticsearchClient = mock(ElasticsearchAsyncClient.class);
        config = mock(ConnectivityMonitoringConfig.class);
        when(config.serviceName()).thenReturn(SERVICE_NAME);
        when(config.enabled()).thenReturn(true);
        when(config.failureThreshold()).thenReturn(FAILURE_THRESHOLD);
        when(config.probeTimeoutMs()).thenReturn(500L);
        when(config.probeRedis()).thenReturn(true);
        when(config.probeKafka()).thenReturn(false);
        when(config.probeElasticsearch()).thenReturn(true);
        service = new ConnectivityMonitoringService(
                registry, redisDataSource, elasticsearchClient, config, "localhost:9092");
    }

    // ---- State machine ----

    @Test
    void startsWithEveryDependencyUp() {
        assertTrue(service.allUp());
        for (ConnectivityMonitoringService.Dependency dependency : ConnectivityMonitoringService.Dependency.values()) {
            assertTrue(service.isUp(dependency));
            assertEquals(1.0, upGauge(dependency));
        }
    }

    @Test
    void everyMeterCarriesTheServiceTag() {
        service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS,
                new ConnectException("Connection refused"));

        assertNotNull(registry.find("dependency_up")
                .tags(Tags.of("service", SERVICE_NAME, "dependency", "redis")).gauge());
        assertNotNull(registry.find("dependency_connectivity_failure_count")
                .tags(Tags.of("service", SERVICE_NAME, "dependency", "redis")).counter());
        assertNotNull(registry.find("dependency_outage_count")
                .tags(Tags.of("service", SERVICE_NAME, "dependency", "redis")).counter());
    }

    @Test
    void failuresBelowThresholdKeepDependencyUp() {
        for (int i = 0; i < FAILURE_THRESHOLD - 1; i++) {
            service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS,
                    new ConnectException("Connection refused"));
        }

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.REDIS));
        assertEquals(1.0, upGauge(ConnectivityMonitoringService.Dependency.REDIS));
        assertEquals(FAILURE_THRESHOLD - 1.0, gauge("dependency_consecutive_failure_count", "redis"));
        assertEquals(FAILURE_THRESHOLD - 1.0,
                failureCounter("redis", ConnectivityFailureReason.CONNECTION_REFUSED).count());
    }

    @Test
    void thresholdFailuresMarkDependencyDownAndCountTheOutage() {
        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.recordFailure(ConnectivityMonitoringService.Dependency.ELASTICSEARCH,
                    new java.io.IOException("No living connections"));
        }

        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.ELASTICSEARCH));
        assertFalse(service.allUp());
        assertEquals(0.0, upGauge(ConnectivityMonitoringService.Dependency.ELASTICSEARCH));
        assertEquals(1.0, registry.find("dependency_outage_count")
                .tags(Tags.of("dependency", "elasticsearch")).counter().count());
        assertTrue(gauge("dependency_last_failure_timestamp_seconds", "elasticsearch") > 0.0);
        // Other dependencies are unaffected
        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.REDIS));
    }

    @Test
    void successRestoresDependencyAndRecordsOutageDuration() {
        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.recordFailure(ConnectivityMonitoringService.Dependency.KAFKA,
                    new RuntimeException("Topic cdr-events not present in metadata after 60000 ms"));
        }
        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.KAFKA));

        service.recordSuccess(ConnectivityMonitoringService.Dependency.KAFKA);

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.KAFKA));
        assertEquals(1.0, upGauge(ConnectivityMonitoringService.Dependency.KAFKA));
        assertEquals(0.0, gauge("dependency_consecutive_failure_count", "kafka"));
        assertEquals(0.0, gauge("dependency_downtime_seconds", "kafka"));
        assertTrue(gauge("dependency_last_success_timestamp_seconds", "kafka") > 0.0);
        assertEquals(1L, registry.find("dependency.outage.duration").tags(Tags.of("dependency", "kafka"))
                .timer().count());
    }

    @Test
    void interleavedSuccessResetsTheFailureStreak() {
        service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, new ConnectException("Connection refused"));
        service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, new ConnectException("Connection refused"));
        service.recordSuccess(ConnectivityMonitoringService.Dependency.REDIS);
        service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, new ConnectException("Connection refused"));

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.REDIS));
        assertEquals(1.0, gauge("dependency_consecutive_failure_count", "redis"));
    }

    @Test
    void applicationErrorsAreCountedButNeverTakeADependencyDown() {
        for (int i = 0; i < FAILURE_THRESHOLD * 2; i++) {
            ConnectivityFailureReason reason = service.recordFailure(
                    ConnectivityMonitoringService.Dependency.ELASTICSEARCH,
                    new IllegalArgumentException("session id must not be null"));
            assertEquals(ConnectivityFailureReason.APPLICATION_ERROR, reason);
        }

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.ELASTICSEARCH));
        assertEquals(0.0, gauge("dependency_consecutive_failure_count", "elasticsearch"));
        assertEquals(FAILURE_THRESHOLD * 2.0,
                errorCounter("elasticsearch", ConnectivityFailureReason.APPLICATION_ERROR).count());
        // ...and no connectivity failure series was created for it
        assertEquals(0, registry.find("dependency_connectivity_failure_count")
                .tags(Tags.of("dependency", "elasticsearch")).counters().size());
    }

    @Test
    void nullArgumentsAreIgnored() {
        assertEquals(ConnectivityFailureReason.APPLICATION_ERROR, service.recordFailure(null, new ConnectException()));
        assertEquals(ConnectivityFailureReason.APPLICATION_ERROR,
                service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, null));
        service.recordSuccess(null);
        assertTrue(service.allUp());
    }

    // ---- Probes ----

    @Test
    void redisProbeDrivesRedisState() {
        when(redisDataSource.execute("PING")).thenReturn(Uni.createFrom().failure(new ConnectException("Connection refused")));

        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.probeRedis();
        }
        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.REDIS));
        assertNotNull(registry.find("dependency.probe.latency")
                .tags(Tags.of("dependency", "redis", "outcome", "failure")).timer());

        when(redisDataSource.execute("PING")).thenReturn(Uni.createFrom().item(mock(Response.class)));
        service.probeRedis();

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.REDIS));
        assertNotNull(registry.find("dependency.probe.latency")
                .tags(Tags.of("dependency", "redis", "outcome", "success")).timer());
    }

    @Test
    void elasticsearchProbeFailureCountsTowardsTheOutage() {
        when(elasticsearchClient.ping())
                .thenReturn(CompletableFuture.failedFuture(new ConnectException("Connection refused")));

        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.probeElasticsearch();
        }

        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.ELASTICSEARCH));
        assertEquals(FAILURE_THRESHOLD * 1.0,
                failureCounter("elasticsearch", ConnectivityFailureReason.CONNECTION_REFUSED).count());
        assertNotNull(registry.find("dependency.probe.latency")
                .tags(Tags.of("dependency", "elasticsearch", "outcome", "failure")).timer());
    }

    @Test
    void elasticsearchProbeSuccessMarksDependencyUpAgain() {
        when(elasticsearchClient.ping())
                .thenReturn(CompletableFuture.failedFuture(new ConnectException("Connection refused")));
        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.probeElasticsearch();
        }
        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.ELASTICSEARCH));

        when(elasticsearchClient.ping())
                .thenReturn(CompletableFuture.completedFuture(new BooleanResponse(true)));
        service.probeElasticsearch();

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.ELASTICSEARCH));
        assertNotNull(registry.find("dependency.probe.latency")
                .tags(Tags.of("dependency", "elasticsearch", "outcome", "success")).timer());
    }

    @Test
    void probeTreatsAnUnusableClientAsAnOutageRatherThanThrowing() {
        // e.g. the REST transport has already been closed, so ping() throws rather than returning.
        when(elasticsearchClient.ping()).thenThrow(new IllegalStateException("Connection pool shut down"));

        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.probeElasticsearch();
        }

        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.ELASTICSEARCH));
        assertEquals(FAILURE_THRESHOLD * 1.0,
                failureCounter("elasticsearch", ConnectivityFailureReason.CONNECTION_CLOSED).count());
    }

    @Test
    void probeFailureWithoutARecognisableCauseStillCountsAsUnavailable() {
        when(elasticsearchClient.ping())
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("probe could not run")));

        service.probeElasticsearch();

        assertEquals(1.0, failureCounter("elasticsearch", ConnectivityFailureReason.SERVICE_UNAVAILABLE).count());
        assertEquals(1.0, gauge("dependency_consecutive_failure_count", "elasticsearch"));
    }

    @Test
    void schedulerProbesOnlyEnabledDependencies() {
        when(redisDataSource.execute("PING")).thenReturn(Uni.createFrom().item(mock(Response.class)));
        when(elasticsearchClient.ping())
                .thenReturn(CompletableFuture.completedFuture(new BooleanResponse(true)));
        when(config.probeElasticsearch()).thenReturn(false);

        service.probeDependencies();

        verify(redisDataSource).execute("PING");
        verify(elasticsearchClient, never()).ping();
    }

    @Test
    void schedulerDoesNothingWhenMonitoringIsDisabled() {
        when(config.enabled()).thenReturn(false);

        service.probeDependencies();

        verify(redisDataSource, never()).execute(anyString());
        verify(elasticsearchClient, never()).ping();
    }

    // ---- Read model / housekeeping ----

    @Test
    void snapshotReportsPerDependencyState() {
        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, new ConnectException("Connection refused"));
        }

        Map<String, ConnectivityMonitoringService.DependencyStatus> snapshot = service.snapshot();
        assertEquals(3, snapshot.size());

        ConnectivityMonitoringService.DependencyStatus redis = snapshot.get("redis");
        assertFalse(redis.up());
        assertEquals(FAILURE_THRESHOLD, redis.consecutiveFailures());
        assertEquals(FAILURE_THRESHOLD, redis.connectivityFailureCount());
        assertEquals(FAILURE_THRESHOLD, redis.dailyConnectivityFailureCount());
        assertEquals(1L, redis.outageCount());
        assertEquals(ConnectivityFailureReason.CONNECTION_REFUSED.label(), redis.lastFailureReason());
        assertTrue(redis.lastFailureEpochSeconds() > 0);

        assertTrue(snapshot.get("elasticsearch").up());
        assertEquals(0L, snapshot.get("elasticsearch").connectivityFailureCount());
    }

    @Test
    void dailyResetClearsDailyCountsButKeepsLifetimeCounts() {
        service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, new ConnectException("Connection refused"));
        assertEquals(1.0, gauge("dependency_connectivity_failure_daily_count", "redis"));

        service.resetDailyCounters();

        assertEquals(0.0, gauge("dependency_connectivity_failure_daily_count", "redis"));
        assertEquals(1.0, failureCounter("redis", ConnectivityFailureReason.CONNECTION_REFUSED).count());
        assertEquals(1L, service.snapshot().get("redis").connectivityFailureCount());
    }

    // ---- Helpers ----

    private double upGauge(ConnectivityMonitoringService.Dependency dependency) {
        return gauge("dependency_up", dependency.label());
    }

    private double gauge(String name, String dependency) {
        Gauge g = registry.find(name).tags(Tags.of("dependency", dependency)).gauge();
        assertNotNull(g, "gauge not registered: " + name + "{dependency=" + dependency + "}");
        return g.value();
    }

    private Counter failureCounter(String dependency, ConnectivityFailureReason reason) {
        Counter counter = registry.find("dependency_connectivity_failure_count")
                .tags(Tags.of("dependency", dependency, "reason", reason.label())).counter();
        assertNotNull(counter, "counter not registered for " + dependency + "/" + reason.label());
        return counter;
    }

    private Counter errorCounter(String dependency, ConnectivityFailureReason reason) {
        Counter counter = registry.find("dependency_error_count")
                .tags(Tags.of("dependency", dependency, "reason", reason.label())).counter();
        assertNotNull(counter, "counter not registered for " + dependency + "/" + reason.label());
        return counter;
    }
}
