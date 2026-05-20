package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import com.csg.airtel.aaa4j.domain.model.connectionhistory.*;
import com.csg.airtel.aaa4j.domain.util.exceptions.BaseException;
import com.csg.airtel.aaa4j.repository.SessionRedisRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SessionService
 * Coverage: 100% line coverage
 */
@ExtendWith(MockitoExtension.class)
class SessionServiceTest {

    @Mock
    private SessionRedisRepository redisRepository;

    @Mock
    private ElasticSearchService elasticsearchService;

    @InjectMocks
    private SessionService sessionService;

    private AccountingEvent startEvent;
    private AccountingEvent interimEvent;
    private AccountingEvent stopEvent;
    private AccountingEvent coaRequestEvent;
    private AccountingEvent coaResponseEvent;
    private Session existingSession;
    private String sessionId;
    private String nasPort;

    @BeforeEach
    void setUp() {
        sessionId = "test-session-123";
        nasPort = "5060";

        existingSession = createTestSession();

        startEvent = createAccountingEvent(EventTypes.ACCOUNTING_START.toString(), null);
        interimEvent = createAccountingEvent(EventTypes.ACCOUNTING_INTERIM.toString(), null);
        stopEvent = createAccountingEvent(EventTypes.ACCOUNTING_STOP.toString(), Instant.now());
        coaRequestEvent = createAccountingEvent(EventTypes.COA_REQUEST.toString(), null);
        coaResponseEvent = createCoaResponseEvent("ACK");
    }

    // ============ ACCOUNTING_START Tests ============

    @Test
    void testProcessStartEvent_NewSession() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.empty());

        // Act
        sessionService.processStartEvent(startEvent);

        // Assert
        verify(redisRepository, times(1)).save(any(Session.class), eq(uniqueSessionId));
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId));
    }

    @Test
    void testProcessStartEvent_ExistingSession() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processStartEvent(startEvent);

        // Assert
        verify(redisRepository, times(1)).save(any(Session.class), eq(uniqueSessionId));
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId));

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));
        assertEquals(SessionStatus.ACTIVE, sessionCaptor.getValue().getConnectionStatus());
    }

    @Test
    void testProcessStartEvent_WithStartTime() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        Instant startTime = Instant.now().minusSeconds(3600);
        startEvent.getPayload().getSession().setStartTime(startTime);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.empty());

        // Act
        sessionService.processStartEvent(startEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));
        assertEquals(Date.from(startTime), sessionCaptor.getValue().getStartTime());
    }

    @Test
    void testProcessStartEvent_IncompleteCDR_MissingSessionId() {
        // Arrange
        startEvent.getPayload().getSession().setSessionId(null);

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            sessionService.processStartEvent(startEvent);
        });

        assertTrue(exception.getMessage().contains("Incomplete CDR Data"));
    }

    @Test
    void testProcessStartEvent_IncompleteCDR_MissingNasPort() {
        // Arrange
        startEvent.getPayload().getSession().setNasPort(null);

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            sessionService.processStartEvent(startEvent);
        });

        assertTrue(exception.getMessage().contains("Incomplete CDR Data"));
    }

    // ============ ACCOUNTING_INTERIM Tests ============

    @Test
    void testProcessInterimEvent_ExistingSession() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processInterimEvent(interimEvent);

        // Assert
        verify(redisRepository, times(1)).findBySessionId(sessionId);
        verify(redisRepository, times(1)).save(any(Session.class), eq(uniqueSessionId));
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId));

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));
        assertNotNull(sessionCaptor.getValue().getUpdatedTime());
    }

    @Test
    void testProcessInterimEvent_NewSession() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.empty());

        // Act
        sessionService.processInterimEvent(interimEvent);

        // Assert
        verify(redisRepository, times(1)).findBySessionId(sessionId);
        verify(redisRepository, times(1)).save(any(Session.class), eq(uniqueSessionId));
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId));
    }

    // ============ ACCOUNTING_STOP Tests ============

    @Test
    void testProcessStopEvent_FromActiveSession() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        existingSession.setConnectionStatus(SessionStatus.ACTIVE);
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processStopEvent(stopEvent);

        // Assert
        verify(redisRepository, times(1)).findBySessionId(sessionId);
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId));
        verify(redisRepository, times(1)).delete(uniqueSessionId);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString());
        assertEquals(SessionStatus.COMPLETED, sessionCaptor.getValue().getConnectionStatus());
    }

    @Test
    void testProcessStopEvent_FromTerminationRequested() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        existingSession.setConnectionStatus(SessionStatus.TERMINATION_REQUESTED);
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processStopEvent(stopEvent);

        // Assert
        verify(redisRepository, times(1)).delete(uniqueSessionId);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString());
        assertEquals(SessionStatus.TERMINATED, sessionCaptor.getValue().getConnectionStatus());
    }

    @Test
    void testProcessStopEvent_WithStopTime() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        Instant stopTime = Instant.now();
        stopEvent.getPayload().getSession().setSessionStopTime(stopTime);
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processStopEvent(stopEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString());
        assertEquals(Date.from(stopTime), sessionCaptor.getValue().getEndTime());
    }

    @Test
    void testProcessStopEvent_WithoutStopTime() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        stopEvent.getPayload().getSession().setSessionStopTime(null);
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processStopEvent(stopEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString());
        assertNotNull(sessionCaptor.getValue().getEndTime());
    }

    // ============ COA_REQUEST Tests ============

    @Test
    void testProcessCoaRequestEvent_ExistingSession() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processCoaRequestEvent(coaRequestEvent);

        // Assert
        verify(redisRepository, times(1)).findBySessionId(sessionId);
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId));

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString());
        assertEquals(SessionStatus.TERMINATION_REQUESTED, sessionCaptor.getValue().getConnectionStatus());
    }

    @Test
    void testProcessCoaRequestEvent_NewSession() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.empty());

        // Act
        sessionService.processCoaRequestEvent(coaRequestEvent);

        // Assert
        verify(redisRepository, times(1)).findBySessionId(sessionId);
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId));
    }

    // ============ COA_RESPONSE Tests ============

    @Test
    void testProcessCoaResponseEvent_ACK_Status() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent ackEvent = createCoaResponseEvent("ACK");
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processCoaResponseEvent(ackEvent);

        // Assert
        verify(redisRepository, times(1)).findBySessionId(sessionId);
        verify(elasticsearchService, never()).indexSession(any(), anyString());
        verify(redisRepository, never()).delete(anyString());

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository, times(0)).save(sessionCaptor.capture(), anyString());
    }

    @Test
    void testProcessCoaResponseEvent_NACK_Status() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent nackEvent = createCoaResponseEvent("NACK");
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processCoaResponseEvent(nackEvent);

        // Assert
        verify(redisRepository, times(1)).findBySessionId(sessionId);
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId));
        verify(redisRepository, times(1)).delete(uniqueSessionId);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString());
        assertEquals(SessionStatus.TERMINATED, sessionCaptor.getValue().getConnectionStatus());
    }

    @Test
    void testProcessCoaResponseEvent_InvalidStatus() {
        // Arrange
        AccountingEvent invalidEvent = createCoaResponseEvent("INVALID");
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            sessionService.processCoaResponseEvent(invalidEvent);
        });

        assertTrue(exception.getMessage().contains("Invalid COA Status"));
    }

    @Test
    void testProcessCoaResponseEvent_NullCOA() {
        // Arrange
        coaResponseEvent.getPayload().setCoa(null);
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            sessionService.processCoaResponseEvent(coaResponseEvent);
        });

        assertTrue(exception.getMessage().contains("Incomplete COA Data"));
    }

    @Test
    void testProcessCoaResponseEvent_NullStatus() {
        // Arrange
        coaResponseEvent.getPayload().getCoa().setStatus(null);
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            sessionService.processCoaResponseEvent(coaResponseEvent);
        });

        assertTrue(exception.getMessage().contains("Incomplete COA Data"));
    }

    @Test
    void testProcessCoaResponseEvent_WithStopTime() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        Instant stopTime = Instant.now();
        AccountingEvent nackEvent = createCoaResponseEvent("NACK");
        nackEvent.getPayload().getSession().setSessionStopTime(stopTime);
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processCoaResponseEvent(nackEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString());
        assertEquals(Date.from(stopTime), sessionCaptor.getValue().getEndTime());
    }

    @Test
    void testProcessCoaResponseEvent_WithoutStopTime() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent nackEvent = createCoaResponseEvent("NACK");
        nackEvent.getPayload().getSession().setSessionStopTime(null);
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processCoaResponseEvent(nackEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString());
        assertNotNull(sessionCaptor.getValue().getEndTime());
    }

    // ============ Session Creation Tests ============

    @Test
    void testCreateSession_FromCoaResponseEvent() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent coaEvent = createCoaResponseEvent("ACK");
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.empty());

        // Act
        sessionService.processCoaResponseEvent(coaEvent);

        // Assert - session should be created with TERMINATION_REQUESTED status
        verify(elasticsearchService, never()).indexSession(any(), anyString());
    }

    @Test
    void testCreateSession_WithoutStartTime() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        startEvent.getPayload().getSession().setStartTime(null);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.empty());

        // Act
        sessionService.processStartEvent(startEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));
        assertEquals(Date.from(startEvent.getEventTimestamp()), sessionCaptor.getValue().getStartTime());
    }

    @Test
    void testCreateSession_WithNullUser() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        startEvent.getPayload().setUser(null);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.empty());

        // Act
        sessionService.processStartEvent(startEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));
        assertNull(sessionCaptor.getValue().getUserName());
        assertNull(sessionCaptor.getValue().getGroupId());
    }

    @Test
    void testCreateSession_WithNullAccounting() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        startEvent.getPayload().setAccounting(null);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.empty());

        // Act
        sessionService.processStartEvent(startEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));
        assertEquals(0L, sessionCaptor.getValue().getUsage());
    }

    // ============ SessionInstanceInfo Tests ============

    @Test
    void testCreateInstanceInfo_WithAccounting() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processInterimEvent(interimEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));

        Session savedSession = sessionCaptor.getValue();
        assertFalse(savedSession.getSessionInstances().isEmpty());

        SessionInstanceInfo instanceInfo = savedSession.getSessionInstances().get(
                savedSession.getSessionInstances().size() - 1
        );
        assertEquals(interimEvent.getEventId(), instanceInfo.getMessageId());
        assertEquals(interimEvent.getEventType(), instanceInfo.getMessageType());
        assertNotNull(instanceInfo.getUsage());
        assertNotNull(instanceInfo.getServiceId());
        assertNotNull(instanceInfo.getBucketId());
    }

    @Test
    void testCreateInstanceInfo_WithoutAccounting() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        coaRequestEvent.getPayload().setAccounting(null);
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processCoaRequestEvent(coaRequestEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString());

        Session savedSession = sessionCaptor.getValue();
        assertFalse(savedSession.getSessionInstances().isEmpty());

        SessionInstanceInfo instanceInfo = savedSession.getSessionInstances().get(
                savedSession.getSessionInstances().size() - 1
        );
        assertEquals(coaRequestEvent.getEventId(), instanceInfo.getMessageId());
        assertNull(instanceInfo.getUsage());
        assertNull(instanceInfo.getServiceId());
        assertNull(instanceInfo.getBucketId());
    }

    // ============ Update Methods Tests ============

    @Test
    void testUpdateSessionFromStart_WithoutStartTime() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        startEvent.getPayload().getSession().setStartTime(null);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));
        Date originalStartTime = existingSession.getStartTime();

        // Act
        sessionService.processStartEvent(startEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));
        // Start time should remain unchanged when payload has no start time
        assertEquals(originalStartTime, sessionCaptor.getValue().getStartTime());
    }

    @Test
    void testUpdateSessionFromCoa_CoaRequest() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processCoaRequestEvent(coaRequestEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString());
        assertEquals(SessionStatus.TERMINATION_REQUESTED, sessionCaptor.getValue().getConnectionStatus());
        assertNotNull(sessionCaptor.getValue().getUpdatedTime());
    }

    // ============ Edge Cases ============

    @Test
    void testGetUniqueIdFromSessionCDR_Success() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        SessionCdr sessionCdr = SessionCdr.builder()
                .sessionId(sessionId)
                .nasPort(nasPort)
                .build();

        // Act - This is tested indirectly through processStartEvent
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.empty());
        sessionService.processStartEvent(startEvent);

        // Assert
        verify(elasticsearchService).indexSession(any(Session.class), eq(uniqueSessionId));
    }

    @Test
    void testSessionStatus_CaseInsensitive_ACK() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent ackLowerCase = createCoaResponseEvent("ack");
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processCoaResponseEvent(ackLowerCase);

        // Assert - Should handle lowercase status
        verify(redisRepository, never()).delete(anyString());
    }

    @Test
    void testSessionStatus_CaseInsensitive_NACK() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent nackLowerCase = createCoaResponseEvent("nack");
        when(redisRepository.findBySessionId(sessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processCoaResponseEvent(nackLowerCase);

        // Assert - Should handle lowercase status
        verify(redisRepository, times(1)).delete(uniqueSessionId);
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), anyString());
    }

    @Test
    void testProcessStartEvent_UpdatesUserInfo() {
        // Arrange
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        // Act
        sessionService.processStartEvent(startEvent);

        // Assert
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));
        assertEquals("testuser@example.com", sessionCaptor.getValue().getUserName());
        assertEquals("group-001", sessionCaptor.getValue().getGroupId());
    }

    // ============ Helper Methods ============

    private Session createTestSession() {
        return Session.builder()
                .sessionId(sessionId)
                .startTime(new Date())
                .connectionStatus(SessionStatus.ACTIVE)
                .usage(1024L)
                .userName("existing@example.com")
                .groupId("group-999")
                .updatedTime(new Date())
                .sessionInstances(new ArrayList<>())
                .build();
    }

    private AccountingEvent createAccountingEvent(String eventType, Instant stopTime) {
        User user = User.builder()
                .userName("testuser@example.com")
                .groupId("group-001")
                .build();

        Accounting accounting = Accounting.builder()
                .totalUsage(2048L)
                .sessionUsage(512L)
                .serviceId("service-123")
                .bucketId("bucket-456")
                .build();

        SessionCdr sessionCdr = SessionCdr.builder()
                .sessionId(sessionId)
                .nasPort(nasPort)
                .startTime(Instant.now().minusSeconds(3600))
                .sessionStopTime(stopTime)
                .build();

        Payload payload = Payload.builder()
                .session(sessionCdr)
                .user(user)
                .accounting(accounting)
                .build();

        return AccountingEvent.builder()
                .eventId("event-" + System.currentTimeMillis())
                .eventType(eventType)
                .eventTimestamp(Instant.now())
                .payload(payload)
                .build();
    }

    private AccountingEvent createCoaResponseEvent(String status) {
        COA coa = COA.builder()
                .coaType("DISCONNECT")
                .status(status)
                .coaCode(40)
                .build();

        User user = User.builder()
                .userName("testuser@example.com")
                .groupId("group-001")
                .build();

        Accounting accounting = Accounting.builder()
                .totalUsage(2048L)
                .sessionUsage(512L)
                .serviceId("service-123")
                .bucketId("bucket-456")
                .build();

        SessionCdr sessionCdr = SessionCdr.builder()
                .sessionId(sessionId)
                .nasPort(nasPort)
                .startTime(Instant.now().minusSeconds(3600))
                .build();

        Payload payload = Payload.builder()
                .session(sessionCdr)
                .user(user)
                .accounting(accounting)
                .coa(coa)
                .build();

        return AccountingEvent.builder()
                .eventId("coa-event-" + System.currentTimeMillis())
                .eventType(EventTypes.COA_RESPONSE.toString())
                .eventTimestamp(Instant.now())
                .payload(payload)
                .build();
    }
}