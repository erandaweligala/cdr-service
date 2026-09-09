package com.csg.airtel.aaa4j.domain.resource;

import com.csg.airtel.aaa4j.domain.service.ConnectivityMonitoringService;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConnectivityResourceTest {

    @Test
    void statusReturnsOkWhenAllDependenciesAreUp() {
        ConnectivityMonitoringService monitoringService = mock(ConnectivityMonitoringService.class);
        when(monitoringService.allUp()).thenReturn(true);
        when(monitoringService.snapshot()).thenReturn(Collections.emptyMap());
        ConnectivityResource resource = new ConnectivityResource(monitoringService);

        Response response = resource.status();

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        Object entity = response.getEntity();
        assertNotNull(entity);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) entity;
        assertEquals("UP", body.get("status"));
    }

    @Test
    void statusReturnsServiceUnavailableWhenAnyDependencyIsDown() {
        ConnectivityMonitoringService monitoringService = mock(ConnectivityMonitoringService.class);
        when(monitoringService.allUp()).thenReturn(false);
        when(monitoringService.snapshot()).thenReturn(Collections.emptyMap());
        ConnectivityResource resource = new ConnectivityResource(monitoringService);

        Response response = resource.status();

        assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals("DOWN", body.get("status"));
    }
}
