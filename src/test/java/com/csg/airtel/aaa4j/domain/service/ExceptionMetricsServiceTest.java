package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.config.ConnectivityMonitoringConfig;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExceptionMetricsServiceTest {

    private static final String SERVICE = "cdr-service";

    private MeterRegistry registry;
    private ConnectivityMonitoringService connectivityMonitoringService;
    private ErrorCatalog errorCatalog;
    private ExceptionMetricsService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        connectivityMonitoringService = mock(ConnectivityMonitoringService.class);

        ConnectivityMonitoringConfig config = mock(ConnectivityMonitoringConfig.class);
        when(config.serviceName()).thenReturn(SERVICE);
        errorCatalog = new ErrorCatalog(registry, config);

        service = new ExceptionMetricsService(registry, connectivityMonitoringService, errorCatalog);
        service.init();
    }

    @Test
    void nullThrowableIsIgnored() {
        service.recordException(null, ExceptionMetricsService.Layer.SERVICE);

        assertEquals(0L, service.getTotalRootCount());
        verify(connectivityMonitoringService, never()).recordFailure(any(), any());
    }

    @Test
    void nullLayerIsIgnored() {
        service.recordException(new RuntimeException("boom"), null, ExceptionMetricsService.Source.KAFKA);

        assertEquals(0L, service.getTotalRootCount());
    }

    @Test
    void twoArgOverloadDefaultsToUnknownSourceAndDoesNotTouchConnectivity() {
        service.recordException(new RuntimeException("boom"), ExceptionMetricsService.Layer.SERVICE);

        assertEquals(1L, service.getTotalRootCount());
        verify(connectivityMonitoringService, never()).recordFailure(any(), any());
    }

    @Test
    void nullSourceIsTreatedAsUnknown() {
        service.recordException(new RuntimeException("boom"), ExceptionMetricsService.Layer.SERVICE, null);

        assertEquals(1L, service.getTotalRootCount());
        verify(connectivityMonitoringService, never()).recordFailure(any(), any());
    }

    @Test
    void redisSourceIsForwardedToConnectivityMonitor() {
        RuntimeException failure = new RuntimeException("connection refused");

        service.recordException(failure, ExceptionMetricsService.Layer.CLIENT, ExceptionMetricsService.Source.REDIS);

        verify(connectivityMonitoringService).recordFailure(ConnectivityMonitoringService.Dependency.REDIS, failure);
    }

    @Test
    void kafkaSourceIsForwardedToConnectivityMonitor() {
        RuntimeException failure = new RuntimeException("broker down");

        service.recordException(failure, ExceptionMetricsService.Layer.PRODUCER, ExceptionMetricsService.Source.KAFKA);

        verify(connectivityMonitoringService).recordFailure(ConnectivityMonitoringService.Dependency.KAFKA, failure);
    }

    @Test
    void elasticsearchSourceIsForwardedToConnectivityMonitor() {
        RuntimeException failure = new RuntimeException("es down");

        service.recordException(failure, ExceptionMetricsService.Layer.CLIENT, ExceptionMetricsService.Source.ELASTICSEARCH);

        verify(connectivityMonitoringService).recordFailure(ConnectivityMonitoringService.Dependency.ELASTICSEARCH, failure);
    }

    @Test
    void internalSourceIsNotForwardedToConnectivityMonitor() {
        service.recordException(new RuntimeException("boom"), ExceptionMetricsService.Layer.SERVICE,
                ExceptionMetricsService.Source.INTERNAL);

        verify(connectivityMonitoringService, never()).recordFailure(any(), any());
    }

    @Test
    void sameThrowableRecordedTwiceIsCountedOnce() {
        RuntimeException failure = new RuntimeException("boom");

        // Different layer/source so the dedup-cache key differs; markRecorded's own
        // suppressed-sentinel gate is what must stop the second call here.
        service.recordException(failure, ExceptionMetricsService.Layer.SERVICE, ExceptionMetricsService.Source.INTERNAL);
        service.recordException(failure, ExceptionMetricsService.Layer.CLIENT, ExceptionMetricsService.Source.KAFKA);

        assertEquals(1L, service.getTotalRootCount());
    }

    @Test
    void retryOfSameFaultWithinDedupWindowIsCountedOnce() {
        service.recordException(new RuntimeException("boom"), ExceptionMetricsService.Layer.SERVICE,
                ExceptionMetricsService.Source.INTERNAL);
        service.recordException(new RuntimeException("boom"), ExceptionMetricsService.Layer.SERVICE,
                ExceptionMetricsService.Source.INTERNAL);

        assertEquals(1L, service.getTotalRootCount());
    }

    @Test
    void exceptionDuringRecordingIsCaughtAndDoesNotPropagate() {
        ErrorCatalog throwingCatalog = mock(ErrorCatalog.class);
        doThrow(new RuntimeException("catalog exploded"))
                .when(throwingCatalog).recordOccurrence(any(), any(), any(), any());
        ExceptionMetricsService faulty = new ExceptionMetricsService(registry, connectivityMonitoringService, throwingCatalog);
        faulty.init();

        faulty.recordException(new RuntimeException("boom"), ExceptionMetricsService.Layer.SERVICE);

        // The exception is swallowed after counters were already incremented, since
        // recordInCatalog() runs after incrementCounters() in the recording pipeline.
        assertEquals(1L, faulty.getTotalRootCount());
    }

    @Test
    void nullErrorCatalogAndConnectivityMonitorAreToleratedDefensively() {
        ExceptionMetricsService bare = new ExceptionMetricsService(registry, null, null);
        bare.init();

        bare.recordException(new RuntimeException("boom"), ExceptionMetricsService.Layer.SERVICE,
                ExceptionMetricsService.Source.REDIS);

        assertEquals(1L, bare.getTotalRootCount());
    }

    @Test
    void rootCauseIsResolvedThroughTheCauseChain() {
        Exception root = new IllegalStateException("root cause");
        Exception wrapper = new RuntimeException("wrapper", root);

        assertEquals("IllegalStateException", ExceptionMetricsService.resolveRootCauseType(wrapper));
    }

    @Test
    void causeChainWalkIsBoundedByMaxDepth() {
        // Build a chain deeper than MAX_CAUSE_DEPTH (16): the walk must stop at hop 16
        // rather than reaching the true deepest cause.
        Throwable[] chain = new Throwable[21];
        chain[0] = new TrueRootMarker();
        for (int i = 1; i <= 20; i++) {
            if (i == 4) {
                chain[i] = new DepthMarker(chain[i - 1]);
            } else {
                chain[i] = new RuntimeException("level-" + i, chain[i - 1]);
            }
        }
        Throwable top = chain[20];

        assertEquals("DepthMarker", ExceptionMetricsService.resolveRootCauseType(top));
    }

    @Test
    void anonymousExceptionFallsBackToFullyQualifiedName() {
        RuntimeException anonymous = new RuntimeException("boom") { };

        String type = ExceptionMetricsService.resolveRootCauseType(anonymous);

        assertTrue(type.contains("ExceptionMetricsServiceTest"));
    }

    @Test
    void snapshotIsSortedByDescendingCountAndIncludesPercentageAndDailyCount() {
        // Distinct layers per call: the dedup key is (type, layer, source, threadContext), not
        // message content, so same-layer/source repeats within the dedup window would collapse.
        service.recordException(new IllegalStateException("frequent-0"), ExceptionMetricsService.Layer.SERVICE,
                ExceptionMetricsService.Source.INTERNAL);
        service.recordException(new IllegalStateException("frequent-1"), ExceptionMetricsService.Layer.CLIENT,
                ExceptionMetricsService.Source.INTERNAL);
        service.recordException(new IllegalStateException("frequent-2"), ExceptionMetricsService.Layer.PRODUCER,
                ExceptionMetricsService.Source.INTERNAL);
        service.recordException(new IllegalArgumentException("rare"), ExceptionMetricsService.Layer.RESOURCE,
                ExceptionMetricsService.Source.INTERNAL);

        Map<String, ExceptionMetricsService.ExceptionStats> snapshot = service.snapshot();

        assertEquals(2, snapshot.size());
        assertTrue(snapshot.containsKey("IllegalStateException"));
        assertTrue(snapshot.containsKey("IllegalArgumentException"));

        ExceptionMetricsService.ExceptionStats frequent = snapshot.get("IllegalStateException");
        assertEquals(3L, frequent.count());
        assertEquals(75.0, frequent.percentage(), 0.001);
        assertEquals(3L, frequent.dailyCount());

        // Iteration order follows descending count: the more frequent type comes first.
        assertEquals("IllegalStateException", snapshot.keySet().iterator().next());
    }

    @Test
    void resetDailyCountersZeroesDailyCountButKeepsLifetimeTotal() {
        service.recordException(new RuntimeException("boom"), ExceptionMetricsService.Layer.SERVICE,
                ExceptionMetricsService.Source.INTERNAL);

        service.resetDailyCounters();

        ExceptionMetricsService.ExceptionStats stats = service.snapshot().get("RuntimeException");
        assertEquals(0L, stats.dailyCount());
        assertEquals(1L, service.getTotalRootCount());
    }

    @Test
    void evictDedupCacheRunsWithoutErrorWhenCacheIsEmpty() {
        assertDoesNotThrow(() -> service.evictDedupCache());
    }

    @Test
    void refreshPercentagesIsNoOpBeforeAnyExceptionIsRecorded() {
        assertDoesNotThrow(() -> service.refreshPercentages());
    }

    @Test
    void refreshPercentagesComputesRowsAfterExceptionsAreRecorded() {
        service.recordException(new RuntimeException("boom"), ExceptionMetricsService.Layer.SERVICE,
                ExceptionMetricsService.Source.INTERNAL);

        service.refreshPercentages();

        Gauge gauge = registry.find("application_exception_percentage")
                .tag("exception_type", "RuntimeException").gauge();
        assertNotNull(gauge, "the percentage gauge row must be registered after refreshPercentages()");
        assertEquals(100.0, gauge.value(), 0.001);
    }

    private static final class DepthMarker extends RuntimeException {
        DepthMarker(Throwable cause) {
            super("depth-marker", cause);
        }
    }

    private static final class TrueRootMarker extends RuntimeException {
        TrueRootMarker() {
            super("true-root");
        }
    }
}
