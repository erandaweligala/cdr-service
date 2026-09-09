package com.csg.airtel.aaa4j.domain.resource;

import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ExceptionMetricsResourceTest {

    @Test
    void snapshotReturnsTotalAndPerTypeBreakdown() {
        ExceptionMetricsService exceptionMetrics = mock(ExceptionMetricsService.class);
        when(exceptionMetrics.getTotalRootCount()).thenReturn(42L);
        Map<String, ExceptionMetricsService.ExceptionStats> stats = new LinkedHashMap<>();
        stats.put("SQLException", new ExceptionMetricsService.ExceptionStats(30L, 71.4, 5L));
        when(exceptionMetrics.snapshot()).thenReturn(stats);

        ExceptionMetricsResource resource = new ExceptionMetricsResource(exceptionMetrics);

        Response response = resource.snapshot();

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(42L, body.get("total"));
        assertEquals(stats, body.get("byType"));
    }
}
