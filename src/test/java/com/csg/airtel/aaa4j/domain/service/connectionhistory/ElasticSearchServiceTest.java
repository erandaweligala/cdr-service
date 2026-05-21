package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ElasticSearchServiceTest {

    @Mock
    private ElasticsearchClient elasticsearchClient;

    @InjectMocks
    private ElasticSearchService elasticSearchService;

    private Session testSession;
    private String testSessionId;
    private String testUniqueId;
    private String testTargetIndex; // the daily rolling index passed to indexSession()

    private static final String BASE_INDEX = "test-sessions-index";

    @BeforeEach
    void setUp() {
        setPrivateField(elasticSearchService, "sessionsIndex", BASE_INDEX);

        testSessionId  = "test-session-123";
        testUniqueId   = "test-session-123-port-5060";

        // Simulate the index name that SessionService would have stored at session creation
        testTargetIndex = BASE_INDEX + "-"
                + LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));

        testSession = Session.builder()
                .sessionId(testSessionId)
                .startTime(new Date())
                .connectionStatus(SessionStatus.ACTIVE)
                .usage(1024L)
                .userName("testuser@example.com")
                .groupId("group-001")
                .updatedTime(new Date())
                .sessionInstances(new ArrayList<>())
                .build();
    }

    // ============ getCurrentIndex Tests ============

    @Test
    void testGetCurrentIndex_ReturnsCorrectDailyPattern() {
        // Arrange
        String expectedPattern = BASE_INDEX + "-"
                + LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));

        // Act
        String result = elasticSearchService.getCurrentIndex();

        // Assert
        assertEquals(expectedPattern, result);
        assertTrue(result.matches("test-sessions-index-\\d{4}\\.\\d{2}\\.\\d{2}"),
                "Index name should match pattern: base-yyyy.MM.dd");
    }

    @Test
    void testGetCurrentIndex_ContainsBaseIndexName() {
        String result = elasticSearchService.getCurrentIndex();
        assertTrue(result.startsWith(BASE_INDEX + "-"));
    }

    // ============ indexSession Success Tests ============

    @Test
    void testIndexSession_Success_ActiveSession() throws IOException {
        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        // Pass targetIndex as third argument
        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex);

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_Success_CompletedSession() throws IOException {
        testSession.setConnectionStatus(SessionStatus.COMPLETED);
        testSession.setEndTime(new Date());

        IndexResponse mockResponse = createMockIndexResponse("updated");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex);

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_Success_TerminatedSession() throws IOException {
        testSession.setConnectionStatus(SessionStatus.TERMINATED);
        testSession.setEndTime(new Date());

        IndexResponse mockResponse = createMockIndexResponse("updated");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex);

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_Success_TerminationRequestedSession() throws IOException {
        testSession.setConnectionStatus(SessionStatus.TERMINATION_REQUESTED);

        IndexResponse mockResponse = createMockIndexResponse("updated");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex);

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_ResultCreated() throws IOException {
        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex);

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_ResultUpdated() throws IOException {
        IndexResponse mockResponse = createMockIndexResponse("updated");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex);

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_NullSessionFields() throws IOException {
        Session sessionWithNulls = Session.builder()
                .sessionId(testSessionId)
                .connectionStatus(SessionStatus.ACTIVE)
                .build();

        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        elasticSearchService.indexSession(sessionWithNulls, testUniqueId, testTargetIndex);

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_UnknownStatus() throws IOException {
        testSession.setConnectionStatus(SessionStatus.UNKNOWN);

        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex);

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_DifferentUniqueIds() throws IOException {
        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        elasticSearchService.indexSession(testSession, "session-1-port-1", testTargetIndex);
        elasticSearchService.indexSession(testSession, "session-2-port-2", testTargetIndex);

        verify(elasticsearchClient, times(2)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_MultipleSequentialCalls() throws IOException {
        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex);
        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex);
        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex);

        verify(elasticsearchClient, times(3)).index(any(IndexRequest.class));
    }

    // ============ Rolling Index Correctness Test ============

    @Test
    void testIndexSession_UsesProvidedTargetIndex_NotCurrentDay() throws IOException {
        // Simulates a session started yesterday — its stored indexName should be used,
        // not today's index from getCurrentIndex()
        String yesterdayIndex = BASE_INDEX + "-"
                + LocalDate.now(ZoneOffset.UTC).minusDays(1)
                .format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));

        IndexResponse mockResponse = createMockIndexResponse("updated");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        elasticSearchService.indexSession(testSession, testUniqueId, yesterdayIndex);

        // Capture and verify the actual IndexRequest used yesterday's index
        ArgumentCaptor<IndexRequest<Session>> captor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(elasticsearchClient).index(captor.capture());
        assertEquals(yesterdayIndex, captor.getValue().index(),
                "STOP event should write to the session's birth index, not today's index");
    }

    @Test
    void testIndexSession_CorrectIndexName() throws IOException {
        // Verifies that whatever targetIndex is passed, it is forwarded to Elasticsearch as-is
        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenReturn(mockResponse);

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex);

        ArgumentCaptor<IndexRequest<Session>> captor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(elasticsearchClient).index(captor.capture());
        assertEquals(testTargetIndex, captor.getValue().index(),
                "IndexRequest should use the targetIndex passed by the caller");
    }

    // ============ Error Tests ============

    @Test
    void testIndexSession_IOExceptionThrown() throws IOException {
        IOException ioException = new IOException("Connection timeout");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenThrow(ioException);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
        );

        assertEquals("Failed to index session", exception.getMessage());
        assertEquals(ioException, exception.getCause());
        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_IOExceptionWithCustomMessage() throws IOException {
        IOException ioException = new IOException("Index not found");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenThrow(ioException);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
        );

        assertEquals("Failed to index session", exception.getMessage());
        assertNotNull(exception.getCause());
        assertTrue(exception.getCause().getMessage().contains("Index not found"));
    }

    @Test
    void testIndexSession_ExceptionWrapping() throws IOException {
        IOException originalException = new IOException("Network error");
        when(elasticsearchClient.index(any(IndexRequest.class))).thenThrow(originalException);

        RuntimeException thrown = assertThrows(RuntimeException.class, () ->
                elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
        );

        assertSame(originalException, thrown.getCause());
        assertEquals("Failed to index session", thrown.getMessage());
    }

    // ============ Helper Methods ============

    private IndexResponse createMockIndexResponse(String result) {
        IndexResponse mockResponse = mock(IndexResponse.class);
        when(mockResponse.result()).thenReturn(
                co.elastic.clients.elasticsearch._types.Result.valueOf(
                        result.substring(0, 1).toUpperCase() + result.substring(1))
        );
        return mockResponse;
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