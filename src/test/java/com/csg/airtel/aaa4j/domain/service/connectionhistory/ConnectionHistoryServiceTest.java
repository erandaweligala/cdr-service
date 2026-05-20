package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.json.JsonData;
import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionStatus;
import com.csg.airtel.aaa4j.domain.util.exceptions.BaseException;
import com.csg.airtel.aaa4j.domain.util.exceptions.ServiceExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ConnectionHistoryService
 * Coverage: 100% line coverage
 */
@ExtendWith(MockitoExtension.class)
class ConnectionHistoryServiceTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ElasticsearchClient client;

    @Mock
    private ServiceExceptionHandler handler;

    @InjectMocks
    private ConnectionHistoryService service;

    private String sessionsIndex = "test-sessions-index";

    @BeforeEach
    void setUp() throws Exception {
        reset(client, handler);
        setPrivateField(service, "sessionsIndex", sessionsIndex);
    }

    // ============ fetchSessionDetails Success Tests ============

    @Test
    void testFetchSessionDetails_Success_AllParameters() throws Exception {
        // Arrange
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act
        BaseResponse<Session> result = service.fetchSessionDetails(
                "user@test.com", "ACTIVE", "session-123", "group-001",
                "2024-01-01T00:00:00", "2024-01-31T23:59:59", 10, 1
        );

        // Assert
        assertNotNull(result);
        assertEquals(1, result.getData().size());
        assertEquals(1L, result.getPageDetails().getTotalRecords());
    }

    @Test
    void testFetchSessionDetails_Success_MinimalParameters() throws Exception {
        // Arrange
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act
        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, null, null, null, 10, 1
        );

        // Assert
        assertNotNull(result);
        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_EmptyResults() throws Exception {
        // Arrange
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.emptyList(), 0L);

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act
        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, null, null, null, 10, 1
        );

        // Assert
        assertNotNull(result);
        assertTrue(result.getData().isEmpty());
        assertEquals(0L, result.getPageDetails().getTotalRecords());
    }

    @Test
    void testFetchSessionDetails_Success_WithUsername() throws Exception {
        // Arrange
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act
        BaseResponse<Session> result = service.fetchSessionDetails(
                "user@test.com", null, null, null, null, null, 10, 1
        );

        // Assert
        assertNotNull(result);
        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_WithConnectionStatus() throws Exception {
        // Arrange
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act
        BaseResponse<Session> result = service.fetchSessionDetails(
                null, "ACTIVE", null, null, null, null, 10, 1
        );

        // Assert
        assertNotNull(result);
        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_WithSessionId() throws Exception {
        // Arrange
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act
        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, "session-123", null, null, null, 10, 1
        );

        // Assert
        assertNotNull(result);
        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_WithGroupId() throws Exception {
        // Arrange
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act
        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, "group-001", null, null, 10, 1
        );

        // Assert
        assertNotNull(result);
        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_WithStartTimeOnly() throws Exception {
        // Arrange
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act
        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, null, "2024-01-01T00:00:00", null, 10, 1
        );

        // Assert
        assertNotNull(result);
        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_WithEndTimeOnly() throws Exception {
        // Arrange
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act
        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, null, null, "2024-01-31T23:59:59", 10, 1
        );

        // Assert
        assertNotNull(result);
        assertEquals(1, result.getData().size());
    }

    // ============ fetchSessionDetails Error Tests ============

    @Test
    void testFetchSessionDetails_IndexNotFound() throws Exception {
        // Arrange
        when(client.indices().exists(any(Function.class)).value()).thenReturn(false);

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            service.fetchSessionDetails(null, null, null, null, null, null, 10, 1);
        });

        assertTrue(exception.getMessage().contains("Elasticsearch index not found"));
        assertEquals(503, exception.getHttpStatus());
    }

    @Test
    void testFetchSessionDetails_IndexCheckException() throws Exception {
        // Arrange
        when(client.indices().exists(any(Function.class))).thenThrow(new IOException("Connection error"));

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            service.fetchSessionDetails(null, null, null, null, null, null, 10, 1);
        });

        assertTrue(exception.getMessage().contains("Elasticsearch index not found"));
    }

    @Test
    void testFetchSessionDetails_ElasticsearchException() throws Exception {
        // Arrange
        ElasticsearchException esException = createElasticsearchException();
        BaseException handledException = new BaseException("ES Error", "ES_ERROR", 503, "ES_001");

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenThrow(esException);
        when(handler.elasticsearchExceptionHandler(any(ElasticsearchException.class))).thenReturn(handledException);

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            service.fetchSessionDetails(null, null, null, null, null, null, 10, 1);
        });

        assertEquals("ES Error", exception.getMessage());
        verify(handler).elasticsearchExceptionHandler(any(ElasticsearchException.class));
    }

    @Test
    void testFetchSessionDetails_IOException() throws Exception {
        // Arrange
        IOException ioException = new IOException("Network error");
        BaseException handledException = new BaseException("IO Error", "IO_ERROR", 500, "IO_001");

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenThrow(ioException);
        when(handler.elasticsearchExceptionHandler(any(IOException.class))).thenReturn(handledException);

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            service.fetchSessionDetails(null, null, null, null, null, null, 10, 1);
        });

        assertEquals("IO Error", exception.getMessage());
    }

    @Test
    void testFetchSessionDetails_BaseException_Rethrown() throws Exception {
        // Arrange
        BaseException baseException = new BaseException("Custom Error", "CUSTOM", 400, "C001");

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenThrow(baseException);

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            service.fetchSessionDetails(null, null, null, null, null, null, 10, 1);
        });

        assertEquals("Custom Error", exception.getMessage());
        verify(handler, never()).elasticsearchExceptionHandler(any());
    }

    @Test
    void testFetchSessionDetails_UnexpectedException() throws Exception {
        // Arrange
        RuntimeException unexpectedException = new RuntimeException("Unexpected");
        BaseException handledException = new BaseException("Service Error", "SERVICE_ERROR", 500, "S001");

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenThrow(unexpectedException);
        when(handler.serviceLayerExceptionHandler(any(RuntimeException.class))).thenReturn(handledException);

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            service.fetchSessionDetails(null, null, null, null, null, null, 10, 1);
        });

        assertEquals("Service Error", exception.getMessage());
    }

    // ============ fetchSessionInstances Tests ============

    @Test
    void testFetchSessionInstances_Success_WithInstances() throws Exception {
        // Arrange
        String sessionId = "session-123";
        Session session = createTestSession(sessionId);

        SessionInstanceInfo instance1 = new SessionInstanceInfo();
        instance1.setMessageId("msg-1");
        SessionInstanceInfo instance2 = new SessionInstanceInfo();
        instance2.setMessageId("msg-2");
        session.setSessionInstances(Arrays.asList(instance1, instance2));

        GetResponse<Session> mockResponse = createMockGetResponse(session, true);
        when(client.get(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act
        BaseResponse<SessionInstanceInfo> result = service.fetchSessionInstances(sessionId);

        // Assert
        assertNotNull(result);
        assertEquals(2, result.getData().size());
    }

    @Test
    void testFetchSessionInstances_Success_NoInstances() throws Exception {
        // Arrange
        String sessionId = "session-123";
        Session session = createTestSession(sessionId);
        session.setSessionInstances(null);

        GetResponse<Session> mockResponse = createMockGetResponse(session, true);
        when(client.get(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act
        BaseResponse<SessionInstanceInfo> result = service.fetchSessionInstances(sessionId);

        // Assert
        assertNotNull(result);
        assertTrue(result.getData().isEmpty());
    }

    @Test
    void testFetchSessionInstances_NotFound() throws Exception {
        // Arrange
        String sessionId = "session-123";
        GetResponse<Session> mockResponse = createMockGetResponse(null, false);
        when(client.get(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            service.fetchSessionInstances(sessionId);
        });

        assertTrue(exception.getMessage().contains("No session instances found"));
        assertEquals(404, exception.getHttpStatus());
    }

    @Test
    void testFetchSessionInstances_FoundButNullSource() throws Exception {
        // Arrange
        String sessionId = "session-123";
        GetResponse<Session> mockResponse = createMockGetResponse(null, true);
        when(client.get(any(Function.class), eq(Session.class))).thenReturn(mockResponse);

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            service.fetchSessionInstances(sessionId);
        });

        assertTrue(exception.getMessage().contains("No session instances found"));
    }

    @Test
    void testFetchSessionInstances_ElasticsearchException() throws Exception {
        // Arrange
        String sessionId = "session-123";
        ElasticsearchException esException = createElasticsearchException();
        BaseException handledException = new BaseException("ES Error", "ES_ERROR", 503, "ES_001");

        when(client.get(any(Function.class), eq(Session.class))).thenThrow(esException);
        when(handler.elasticsearchExceptionHandler(any(ElasticsearchException.class))).thenReturn(handledException);

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            service.fetchSessionInstances(sessionId);
        });

        assertEquals("ES Error", exception.getMessage());
    }

    @Test
    void testFetchSessionInstances_IOException() throws Exception {
        // Arrange
        String sessionId = "session-123";
        IOException ioException = new IOException("Network error");
        BaseException handledException = new BaseException("IO Error", "IO_ERROR", 500, "IO_001");

        when(client.get(any(Function.class), eq(Session.class))).thenThrow(ioException);
        when(handler.elasticsearchExceptionHandler(any(IOException.class))).thenReturn(handledException);

        // Act & Assert
        BaseException exception = assertThrows(BaseException.class, () -> {
            service.fetchSessionInstances(sessionId);
        });

        assertEquals("IO Error", exception.getMessage());
    }

    // ============ parseDate Tests ============

    @Test
    void testParseDate_Success() {
        String dateString = "2024-01-15T10:30:00";
        Instant result = service.parseDate(dateString);
        assertNotNull(result);
    }

    @Test
    void testParseDate_DifferentDate() {
        String dateString = "2025-12-31T23:59:59";
        Instant result = service.parseDate(dateString);
        assertNotNull(result);
    }

    // ============ logElasticsearchError Coverage Tests ============

    @Test
    void testLogElasticsearchError_WithRootCause() throws Exception {
        ElasticsearchException esException = createElasticsearchExceptionWithRootCause();
        BaseException handledException = new BaseException("ES Error", "ES_ERROR", 503, "ES_001");

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenThrow(esException);
        when(handler.elasticsearchExceptionHandler(any(ElasticsearchException.class))).thenReturn(handledException);

        assertThrows(BaseException.class, () -> {
            service.fetchSessionDetails(null, null, null, null, null, null, 10, 1);
        });
    }

    @Test
    void testLogElasticsearchError_WithMetadata() throws Exception {
        ElasticsearchException esException = createElasticsearchExceptionWithMetadata();
        BaseException handledException = new BaseException("ES Error", "ES_ERROR", 503, "ES_001");

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenThrow(esException);
        when(handler.elasticsearchExceptionHandler(any(ElasticsearchException.class))).thenReturn(handledException);

        assertThrows(BaseException.class, () -> {
            service.fetchSessionDetails(null, null, null, null, null, null, 10, 1);
        });
    }

    @Test
    void testLogElasticsearchError_NullError() throws Exception {
        ElasticsearchException esException = mock(ElasticsearchException.class);
        when(esException.status()).thenReturn(500);
        when(esException.error()).thenReturn(null);

        BaseException handledException = new BaseException("ES Error", "ES_ERROR", 503, "ES_001");

        when(client.indices().exists(any(Function.class)).value()).thenReturn(true);
        when(client.search(any(Function.class), eq(Session.class))).thenThrow(esException);
        when(handler.elasticsearchExceptionHandler(any(ElasticsearchException.class))).thenReturn(handledException);

        assertThrows(BaseException.class, () -> {
            service.fetchSessionDetails(null, null, null, null, null, null, 10, 1);
        });
    }

    // ============ Helper Methods ============

    private Session createTestSession(String sessionId) {
        Session session = new Session();
        session.setSessionId(sessionId);
        session.setUserName("test@example.com");
        session.setGroupId("group-001");
        session.setConnectionStatus(SessionStatus.ACTIVE);
        session.setStartTime(new Date());
        session.setUsage(1024L);
        session.setSessionInstances(new ArrayList<>());
        return session;
    }

    @SuppressWarnings("unchecked")
    private SearchResponse<Session> createMockSearchResponse(List<Session> sessions, long totalHits) {
        SearchResponse<Session> response = mock(SearchResponse.class);
        HitsMetadata<Session> hitsMetadata = mock(HitsMetadata.class);
        TotalHits total = mock(TotalHits.class);

        List<Hit<Session>> hits = new ArrayList<>();
        for (Session session : sessions) {
            Hit<Session> hit = mock(Hit.class);
            when(hit.source()).thenReturn(session);
            hits.add(hit);
        }

        when(total.value()).thenReturn(totalHits);
        when(hitsMetadata.total()).thenReturn(total);
        when(hitsMetadata.hits()).thenReturn(hits);
        when(response.hits()).thenReturn(hitsMetadata);

        return response;
    }

    @SuppressWarnings("unchecked")
    private GetResponse<Session> createMockGetResponse(Session session, boolean found) {
        GetResponse<Session> response = mock(GetResponse.class);
        when(response.found()).thenReturn(found);
        when(response.source()).thenReturn(session);
        return response;
    }

    private ElasticsearchException createElasticsearchException() {
        ElasticsearchException exception = mock(ElasticsearchException.class);
        ErrorCause errorCause = mock(ErrorCause.class);

        when(exception.status()).thenReturn(500);
        when(exception.error()).thenReturn(errorCause);
        when(errorCause.type()).thenReturn("search_exception");
        when(errorCause.reason()).thenReturn("search failed");
        when(errorCause.rootCause()).thenReturn(Collections.emptyList());
        when(errorCause.metadata()).thenReturn(null);

        return exception;
    }

    private ElasticsearchException createElasticsearchExceptionWithRootCause() {
        ElasticsearchException exception = mock(ElasticsearchException.class);
        ErrorCause errorCause = mock(ErrorCause.class);
        ErrorCause rootCause = mock(ErrorCause.class);

        when(exception.status()).thenReturn(500);
        when(exception.error()).thenReturn(errorCause);
        when(errorCause.type()).thenReturn("search_exception");
        when(errorCause.reason()).thenReturn("search failed");
        when(errorCause.rootCause()).thenReturn(Collections.singletonList(rootCause));
        when(rootCause.type()).thenReturn("root_exception");
        when(rootCause.reason()).thenReturn("root cause");
        when(errorCause.metadata()).thenReturn(null);

        return exception;
    }

    private ElasticsearchException createElasticsearchExceptionWithMetadata() {
        ElasticsearchException exception = mock(ElasticsearchException.class);
        ErrorCause errorCause = mock(ErrorCause.class);
        Map<String, JsonData> metadata = new HashMap<>();
        metadata.put("index", JsonData.of("test-index"));

        when(exception.status()).thenReturn(500);
        when(exception.error()).thenReturn(errorCause);
        when(errorCause.type()).thenReturn("search_exception");
        when(errorCause.reason()).thenReturn("search failed");
        when(errorCause.rootCause()).thenReturn(Collections.emptyList());
        when(errorCause.metadata()).thenReturn(metadata);

        return exception;
    }

    private void setPrivateField(Object target, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field: " + fieldName, e);
        }
    }
}