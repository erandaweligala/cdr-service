package com.csg.airtel.aaa4j.domain.resource;

import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;

@Path("/api/aaa/admin-console/metrics/exceptions")
@Produces(MediaType.APPLICATION_JSON)
public class ExceptionMetricsResource {

    private final ExceptionMetricsService exceptionMetrics;

    public ExceptionMetricsResource(ExceptionMetricsService exceptionMetrics) {
        this.exceptionMetrics = exceptionMetrics;
    }

    @GET
    public Response snapshot() {
        Map<String, ExceptionMetricsService.ExceptionStats> data = exceptionMetrics.snapshot();
        return Response.ok(Map.of(
                "total", exceptionMetrics.getTotalRootCount(),
                "byType", data
        )).build();
    }
}
