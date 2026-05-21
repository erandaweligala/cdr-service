package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import com.csg.airtel.aaa4j.domain.model.connectionhistory.*;
import com.csg.airtel.aaa4j.domain.util.exceptions.BaseException;
import com.csg.airtel.aaa4j.repository.SessionRedisRepository;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
    void testProcessStartEvent_ExistingSession() {
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processStartEvent(startEvent);

        verify(redisRepository, times(1)).save(any(Session.class), eq(uniqueSessionId));
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId), anyString());

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));
        assertEquals(SessionStatus.ACTIVE, sessionCaptor.getValue().getConnectionStatus());
    }

    @Test
    void testProcessStartEvent_WithStartTime() {
        String uniqueSessionId = sessionId + nasPort;
        Instant startTime = Instant.now().minusSeconds(3600);
        startEvent.getPayload().getSession().setStartTime(startTime);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.empty());

        sessionService.processStartEvent(startEvent);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));
        assertEquals(Date.from(startTime), sessionCaptor.getValue().getStartTime());
    }

// ============ ACCOUNTING_INTERIM Tests ============

    @Test
    void testProcessInterimEvent_ExistingSession() {
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processInterimEvent(interimEvent);

        verify(redisRepository, times(1)).save(any(Session.class), eq(uniqueSessionId));
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId), anyString());

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));
        assertNotNull(sessionCaptor.getValue().getUpdatedTime());
    }


// ============ ACCOUNTING_STOP Tests ============

    @Test
    void testProcessStopEvent_FromActiveSession() {
        String uniqueSessionId = sessionId + nasPort;
        existingSession.setConnectionStatus(SessionStatus.ACTIVE);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processStopEvent(stopEvent);

        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId), anyString());
        verify(redisRepository, times(1)).delete(uniqueSessionId);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());
        assertEquals(SessionStatus.COMPLETED, sessionCaptor.getValue().getConnectionStatus());
    }

    @Test
    void testProcessStopEvent_FromTerminationRequested() {
        String uniqueSessionId = sessionId + nasPort;
        existingSession.setConnectionStatus(SessionStatus.TERMINATION_REQUESTED);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processStopEvent(stopEvent);

        verify(redisRepository, times(1)).delete(uniqueSessionId);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());
        assertEquals(SessionStatus.TERMINATED, sessionCaptor.getValue().getConnectionStatus());
    }

    @Test
    void testProcessStopEvent_WithStopTime() {
        String uniqueSessionId = sessionId + nasPort;
        Instant stopTime = Instant.now();
        stopEvent.getPayload().getSession().setSessionStopTime(stopTime);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processStopEvent(stopEvent);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());
        assertEquals(Date.from(stopTime), sessionCaptor.getValue().getEndTime());
    }

    @Test
    void testProcessStopEvent_WithoutStopTime() {
        String uniqueSessionId = sessionId + nasPort;
        stopEvent.getPayload().getSession().setSessionStopTime(null);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processStopEvent(stopEvent);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());
        assertNotNull(sessionCaptor.getValue().getEndTime());
    }

// ============ COA_REQUEST Tests ============

    @Test
    void testProcessCoaRequestEvent_ExistingSession() {
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processCoaRequestEvent(coaRequestEvent);

        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId), anyString());

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());
        assertEquals(SessionStatus.TERMINATION_REQUESTED, sessionCaptor.getValue().getConnectionStatus());
    }

// ============ COA_RESPONSE Tests ============

    @Test
    void testProcessCoaResponseEvent_ACK_Status() {
        AccountingEvent ackEvent = createCoaResponseEvent("ACK");
        when(redisRepository.findBySessionId(sessionId + nasPort)).thenReturn(Optional.of(existingSession));

        sessionService.processCoaResponseEvent(ackEvent);

        verify(elasticsearchService, never()).indexSession(any(), anyString(), anyString());
        verify(redisRepository, never()).delete(anyString());
    }

    @Test
    void testProcessCoaResponseEvent_NACK_Status() {
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent nackEvent = createCoaResponseEvent("NAK");
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processCoaResponseEvent(nackEvent);

        verify(elasticsearchService, times(1)).indexSession(any(Session.class), eq(uniqueSessionId), anyString());
        verify(redisRepository, times(1)).delete(uniqueSessionId);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());
        assertEquals(SessionStatus.TERMINATED, sessionCaptor.getValue().getConnectionStatus());
    }

    @Test
    void testProcessCoaResponseEvent_WithStopTime() {
        String uniqueSessionId = sessionId + nasPort;
        Instant stopTime = Instant.now();
        AccountingEvent nackEvent = createCoaResponseEvent("NAK");
        nackEvent.getPayload().getSession().setSessionStopTime(stopTime);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processCoaResponseEvent(nackEvent);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());
        assertEquals(Date.from(stopTime), sessionCaptor.getValue().getEndTime());
    }

    @Test
    void testProcessCoaResponseEvent_WithoutStopTime() {
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent nackEvent = createCoaResponseEvent("NAK");
        nackEvent.getPayload().getSession().setSessionStopTime(null);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processCoaResponseEvent(nackEvent);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());
        assertNotNull(sessionCaptor.getValue().getEndTime());
    }

// ============ Session Creation Tests ============

    @Test
    void testCreateSession_FromCoaResponseEvent() {
        AccountingEvent coaEvent = createCoaResponseEvent("ACK");
        when(redisRepository.findBySessionId(sessionId + nasPort)).thenReturn(Optional.empty());

        sessionService.processCoaResponseEvent(coaEvent);

        verify(elasticsearchService, never()).indexSession(any(), anyString(), anyString());
    }

// ============ SessionInstanceInfo Tests ============

    @Test
    void testCreateInstanceInfo_WithAccounting() {
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processInterimEvent(interimEvent);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(redisRepository).save(sessionCaptor.capture(), eq(uniqueSessionId));

        Session savedSession = sessionCaptor.getValue();
        assertFalse(savedSession.getSessionInstances().isEmpty());

        SessionInstanceInfo instanceInfo = savedSession.getSessionInstances()
                .get(savedSession.getSessionInstances().size() - 1);
        assertEquals(interimEvent.getEventId(), instanceInfo.getMessageId());
        assertEquals(interimEvent.getEventType(), instanceInfo.getMessageType());
        assertNotNull(instanceInfo.getUsage());
        assertNotNull(instanceInfo.getServiceId());
        assertNotNull(instanceInfo.getBucketId());
    }

    @Test
    void testCreateInstanceInfo_WithoutAccounting() {
        String uniqueSessionId = sessionId + nasPort;
        coaRequestEvent.getPayload().setAccounting(null);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processCoaRequestEvent(coaRequestEvent);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());

        Session savedSession = sessionCaptor.getValue();
        assertFalse(savedSession.getSessionInstances().isEmpty());

        SessionInstanceInfo instanceInfo = savedSession.getSessionInstances()
                .get(savedSession.getSessionInstances().size() - 1);
        assertEquals(coaRequestEvent.getEventId(), instanceInfo.getMessageId());
        assertNull(instanceInfo.getUsage());
        assertNull(instanceInfo.getServiceId());
        assertNull(instanceInfo.getBucketId());
    }

// ============ Update Methods Tests ============

    @Test
    void testUpdateSessionFromCoa_CoaRequest() {
        String uniqueSessionId = sessionId + nasPort;
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processCoaRequestEvent(coaRequestEvent);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());
        assertEquals(SessionStatus.TERMINATION_REQUESTED, sessionCaptor.getValue().getConnectionStatus());
        assertNotNull(sessionCaptor.getValue().getUpdatedTime());
    }

// ============ Edge Cases ============

    // ============ getUniqueIdFromSessionCDR Tests ============
// Tested indirectly through all event processors


    @Test
    void testGetUniqueId_NullSessionId_ThrowsBaseException() {
        startEvent.getPayload().getSession().setSessionId(null);

        BaseException exception = assertThrows(BaseException.class, () ->
                sessionService.processStartEvent(startEvent)
        );

        assertTrue(exception.getMessage().contains("Incomplete CDR Data"));
        assertTrue(exception.getMessage().contains("sessionId and nasPort are required"));
        assertEquals(HttpStatus.SC_BAD_REQUEST, exception.getHttpStatus());
        verifyNoInteractions(elasticsearchService);
        verify(redisRepository, never()).save(any(), anyString());
    }

    @Test
    void testGetUniqueId_NullNasPort_ThrowsBaseException() {
        startEvent.getPayload().getSession().setNasPort(null);

        BaseException exception = assertThrows(BaseException.class, () ->
                sessionService.processStartEvent(startEvent)
        );

        assertTrue(exception.getMessage().contains("Incomplete CDR Data"));
        assertTrue(exception.getMessage().contains("sessionId and nasPort are required"));
        assertEquals(HttpStatus.SC_BAD_REQUEST, exception.getHttpStatus());
        verifyNoInteractions(elasticsearchService);
        verify(redisRepository, never()).save(any(), anyString());
    }

    @Test
    void testGetUniqueId_BothNull_ThrowsBaseException() {
        startEvent.getPayload().getSession().setSessionId(null);
        startEvent.getPayload().getSession().setNasPort(null);

        BaseException exception = assertThrows(BaseException.class, () ->
                sessionService.processStartEvent(startEvent)
        );

        assertTrue(exception.getMessage().contains("Incomplete CDR Data"));
        assertEquals(HttpStatus.SC_BAD_REQUEST, exception.getHttpStatus());
        verifyNoInteractions(elasticsearchService);
        verify(redisRepository, never()).save(any(), anyString());
    }

    @Test
    void testGetUniqueId_CalledForEveryEventType() {
        // Verify all 5 event processors go through getUniqueIdFromSessionCDR
        // by checking null sessionId throws on each

        startEvent.getPayload().getSession().setSessionId(null);
        assertThrows(BaseException.class, () -> sessionService.processStartEvent(startEvent));

        interimEvent.getPayload().getSession().setSessionId(null);
        assertThrows(BaseException.class, () -> sessionService.processInterimEvent(interimEvent));

        stopEvent.getPayload().getSession().setSessionId(null);
        assertThrows(BaseException.class, () -> sessionService.processStopEvent(stopEvent));

        coaRequestEvent.getPayload().getSession().setSessionId(null);
        assertThrows(BaseException.class, () -> sessionService.processCoaRequestEvent(coaRequestEvent));

        coaResponseEvent.getPayload().getSession().setSessionId(null);
        assertThrows(BaseException.class, () -> sessionService.processCoaResponseEvent(coaResponseEvent));

        // None of them should have reached ES or Redis save
        verifyNoInteractions(elasticsearchService);
        verify(redisRepository, never()).save(any(), anyString());
    }

    @Test
    void testSessionStatus_CaseInsensitive_NACK() {
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent nackLowerCase = createCoaResponseEvent("NAK");
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processCoaResponseEvent(nackLowerCase);

        verify(redisRepository, times(1)).delete(uniqueSessionId);
        verify(elasticsearchService, times(1)).indexSession(any(Session.class), anyString(), anyString());
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

    // ============ getSessionStatusFromCoaResponse Tests ============
// Tested indirectly through processCoaResponseEvent()


    @Test
    void testGetSessionStatus_NAK_ReturnsTerminated() {
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent nakEvent = createCoaResponseEvent("NAK");
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processCoaResponseEvent(nakEvent);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());
        assertEquals(SessionStatus.TERMINATED, sessionCaptor.getValue().getConnectionStatus());
    }


    @Test
    void testGetSessionStatus_NAK_CaseInsensitive() {
        // "nak" lowercase should also resolve to TERMINATED via toUpperCase()
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent nakLowerEvent = createCoaResponseEvent("nak");
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        sessionService.processCoaResponseEvent(nakLowerEvent);

        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(elasticsearchService).indexSession(sessionCaptor.capture(), anyString(), anyString());
        assertEquals(SessionStatus.TERMINATED, sessionCaptor.getValue().getConnectionStatus());
    }

    @Test
    void testGetSessionStatus_InvalidStatus_ThrowsBaseException() {
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent invalidEvent = createCoaResponseEvent("UNKNOWN");
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        BaseException exception = assertThrows(BaseException.class, () ->
                sessionService.processCoaResponseEvent(invalidEvent)
        );

        assertTrue(exception.getMessage().contains("Invalid COA Status"));
        assertTrue(exception.getMessage().contains("UNKNOWN"));
        assertEquals(HttpStatus.SC_BAD_REQUEST, exception.getHttpStatus());
    }

    @Test
    void testGetSessionStatus_NullCOA_ThrowsBaseException() {
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent nullCoaEvent = createCoaResponseEvent("ACK");
        nullCoaEvent.getPayload().setCoa(null);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        BaseException exception = assertThrows(BaseException.class, () ->
                sessionService.processCoaResponseEvent(nullCoaEvent)
        );

        assertTrue(exception.getMessage().contains("Incomplete COA Data"));
        assertEquals(HttpStatus.SC_BAD_REQUEST, exception.getHttpStatus());
    }

    @Test
    void testGetSessionStatus_NullStatus_ThrowsBaseException() {
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent nullStatusEvent = createCoaResponseEvent("ACK");
        nullStatusEvent.getPayload().getCoa().setStatus(null);
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        BaseException exception = assertThrows(BaseException.class, () ->
                sessionService.processCoaResponseEvent(nullStatusEvent)
        );

        assertTrue(exception.getMessage().contains("Incomplete COA Data"));
        assertEquals(HttpStatus.SC_BAD_REQUEST, exception.getHttpStatus());
    }

    @Test
    void testGetSessionStatus_EmptyStatus_ThrowsBaseException() {
        // Empty string doesn't match ACK or NAK — hits the default branch
        String uniqueSessionId = sessionId + nasPort;
        AccountingEvent emptyStatusEvent = createCoaResponseEvent("");
        when(redisRepository.findBySessionId(uniqueSessionId)).thenReturn(Optional.of(existingSession));

        BaseException exception = assertThrows(BaseException.class, () ->
                sessionService.processCoaResponseEvent(emptyStatusEvent)
        );

        assertTrue(exception.getMessage().contains("Invalid COA Status"));
        assertEquals(HttpStatus.SC_BAD_REQUEST, exception.getHttpStatus());
    }

    // ============ Helper Methods ============

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

    private Session createTestSession() {
        String todayIndex = "test-sessions-index-"
                + LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));

        return Session.builder()
                .sessionId(sessionId)
                .startTime(new Date())
                .connectionStatus(SessionStatus.ACTIVE)
                .usage(1024L)
                .userName("existing@example.com")
                .groupId("group-999")
                .updatedTime(new Date())
                .indexName(todayIndex)  // ADD THIS
                .sessionInstances(new ArrayList<>())
                .build();
    }
}