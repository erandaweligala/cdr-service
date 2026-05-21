package com.csg.airtel.aaa4j.domain.resource;

import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
import com.csg.airtel.aaa4j.domain.service.connectionhistory.ConnectionHistoryService;
import com.csg.airtel.aaa4j.domain.util.exceptions.BaseException;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ConnectionHistoryResourceTest {

    @Mock
    private ConnectionHistoryService connectionHistoryService;

    @InjectMocks
    private ConnectionHistoryResource connectionHistoryResource;

    private BaseResponse<Session> sessionResponse;
    private BaseResponse<SessionInstanceInfo> sessionInstanceResponse;

    @BeforeEach
    void setUp() {
        sessionResponse = new BaseResponse<>(null, null, null, null);
        sessionInstanceResponse = new BaseResponse<>(null, null, null, null);
    }

    @Test
    void getSessions_success() throws BaseException {
        String username = "user1";
        String connectionStatus = "ACTIVE";
        String sessionId = "SID123";
        String groupId = "GRP1";
        String startTime = "2025-01-01T00:00:00Z";
        String endTime = "2025-01-02T00:00:00Z";
        int pageSize = 10;
        int page = 1;

        when(connectionHistoryService.fetchSessionDetails(
                username, connectionStatus, sessionId, groupId,
                startTime, endTime, pageSize, page
        )).thenReturn(Uni.createFrom().item(sessionResponse));

        Response response = connectionHistoryResource.getSessions(
                username, connectionStatus, sessionId, groupId,
                startTime, endTime, pageSize, page
        ).await().indefinitely();

        assertNotNull(response);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertEquals(sessionResponse, response.getEntity());

        verify(connectionHistoryService, times(1))
                .fetchSessionDetails(
                        username, connectionStatus, sessionId, groupId,
                        startTime, endTime, pageSize, page
                );
        verifyNoMoreInteractions(connectionHistoryService);
    }

    @Test
    void getSessionInstances_success() throws BaseException {
        String sessionId = "SID123";

        when(connectionHistoryService.fetchSessionInstances(sessionId))
                .thenReturn(Uni.createFrom().item(sessionInstanceResponse));

        Response response = connectionHistoryResource.getSessionInstances(sessionId)
                .await().indefinitely();

        assertNotNull(response);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertEquals(sessionInstanceResponse, response.getEntity());

        verify(connectionHistoryService, times(1))
                .fetchSessionInstances(sessionId);
        verifyNoMoreInteractions(connectionHistoryService);
    }
}
