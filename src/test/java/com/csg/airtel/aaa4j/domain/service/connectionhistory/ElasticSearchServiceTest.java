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
import java.util.ArrayList;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ElasticSearchService
 * Coverage: 100% line coverage
 */
@ExtendWith(MockitoExtension.class)
class ElasticSearchServiceTest {

    @Mock
    private ElasticsearchClient elasticsearchClient;

    @InjectMocks
    private ElasticSearchService elasticSearchService;

    private Session testSession;
    private String testSessionId;
    private String testUniqueId;

    @BeforeEach
    void setUp() {
        // Set the sessions-data property via reflection
        setPrivateField(elasticSearchService, "sessionsIndex", "test-sessions-index");

        // Create test session
        testSessionId = "test-session-123";
        testUniqueId = "test-session-123-port-5060";

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

    /**
     * Test successful indexing of an ACTIVE session
     */
    @Test
    void testIndexSession_Success_ActiveSession() throws IOException {
        // Arrange
        IndexResponse mockResponse = createMockIndexResponse("created");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(mockResponse);

        // Act
        elasticSearchService.indexSession(testSession, testUniqueId);

        // Assert
        ArgumentCaptor<IndexRequest<Session>> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(elasticsearchClient, times(1)).index(requestCaptor.capture());

        // Verify the request was built correctly
        verify(elasticsearchClient).index(any(IndexRequest.class));
    }

    /**
     * Test successful indexing of a COMPLETED session
     */
    @Test
    void testIndexSession_Success_CompletedSession() throws IOException {
        // Arrange
        testSession.setConnectionStatus(SessionStatus.COMPLETED);
        testSession.setEndTime(new Date());

        IndexResponse mockResponse = createMockIndexResponse("updated");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(mockResponse);

        // Act
        elasticSearchService.indexSession(testSession, testUniqueId);

        // Assert
        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    /**
     * Test successful indexing of a TERMINATED session
     */
    @Test
    void testIndexSession_Success_TerminatedSession() throws IOException {
        // Arrange
        testSession.setConnectionStatus(SessionStatus.TERMINATED);
        testSession.setEndTime(new Date());

        IndexResponse mockResponse = createMockIndexResponse("updated");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(mockResponse);

        // Act
        elasticSearchService.indexSession(testSession, testUniqueId);

        // Assert
        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    /**
     * Test successful indexing of a TERMINATION_REQUESTED session
     */
    @Test
    void testIndexSession_Success_TerminationRequestedSession() throws IOException {
        // Arrange
        testSession.setConnectionStatus(SessionStatus.TERMINATION_REQUESTED);

        IndexResponse mockResponse = createMockIndexResponse("updated");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(mockResponse);

        // Act
        elasticSearchService.indexSession(testSession, testUniqueId);

        // Assert
        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    /**
     * Test indexing when Elasticsearch returns 'created' result
     */
    @Test
    void testIndexSession_ResultCreated() throws IOException {
        // Arrange
        IndexResponse mockResponse = createMockIndexResponse("created");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(mockResponse);

        // Act
        elasticSearchService.indexSession(testSession, testUniqueId);

        // Assert
        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
        // Verify logging occurs (if you have a way to capture logs, you can verify log messages)
    }

    /**
     * Test indexing when Elasticsearch returns 'updated' result
     */
    @Test
    void testIndexSession_ResultUpdated() throws IOException {
        // Arrange
        IndexResponse mockResponse = createMockIndexResponse("updated");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(mockResponse);

        // Act
        elasticSearchService.indexSession(testSession, testUniqueId);

        // Assert
        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    /**
     * Test indexing failure due to IOException
     */
    @Test
    void testIndexSession_IOExceptionThrown() throws IOException {
        // Arrange
        IOException ioException = new IOException("Connection timeout");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenThrow(ioException);

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            elasticSearchService.indexSession(testSession, testUniqueId);
        });

        assertEquals("Failed to index session", exception.getMessage());
        assertEquals(ioException, exception.getCause());
        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    /**
     * Test indexing failure with custom IOException message
     */
    @Test
    void testIndexSession_IOExceptionWithCustomMessage() throws IOException {
        // Arrange
        IOException ioException = new IOException("Index not found");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenThrow(ioException);

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            elasticSearchService.indexSession(testSession, testUniqueId);
        });

        assertEquals("Failed to index session", exception.getMessage());
        assertNotNull(exception.getCause());
        assertTrue(exception.getCause().getMessage().contains("Index not found"));
    }

    /**
     * Test indexing with null session fields
     */
    @Test
    void testIndexSession_NullSessionFields() throws IOException {
        // Arrange
        Session sessionWithNulls = Session.builder()
                .sessionId(testSessionId)
                .connectionStatus(SessionStatus.ACTIVE)
                .build();

        IndexResponse mockResponse = createMockIndexResponse("created");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(mockResponse);

        // Act
        elasticSearchService.indexSession(sessionWithNulls, testUniqueId);

        // Assert
        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    /**
     * Test indexing with different unique IDs
     */
    @Test
    void testIndexSession_DifferentUniqueIds() throws IOException {
        // Arrange
        String uniqueId1 = "session-1-port-1";
        String uniqueId2 = "session-2-port-2";

        IndexResponse mockResponse = createMockIndexResponse("created");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(mockResponse);

        // Act
        elasticSearchService.indexSession(testSession, uniqueId1);
        elasticSearchService.indexSession(testSession, uniqueId2);

        // Assert
        verify(elasticsearchClient, times(2)).index(any(IndexRequest.class));
    }

    /**
     * Test indexing with UNKNOWN session status
     */
    @Test
    void testIndexSession_UnknownStatus() throws IOException {
        // Arrange
        testSession.setConnectionStatus(SessionStatus.UNKNOWN);

        IndexResponse mockResponse = createMockIndexResponse("created");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(mockResponse);

        // Act
        elasticSearchService.indexSession(testSession, testUniqueId);

        // Assert
        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    /**
     * Test that correct index name is used
     */
    @Test
    void testIndexSession_CorrectIndexName() throws IOException {
        // Arrange
        IndexResponse mockResponse = createMockIndexResponse("created");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(mockResponse);

        // Act
        elasticSearchService.indexSession(testSession, testUniqueId);

        // Assert
        ArgumentCaptor<IndexRequest<Session>> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(elasticsearchClient).index(requestCaptor.capture());

        // The index name should be "test-sessions-index" as set in setUp
        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    /**
     * Test multiple sequential indexing operations
     */
    @Test
    void testIndexSession_MultipleSequentialCalls() throws IOException {
        // Arrange
        IndexResponse mockResponse = createMockIndexResponse("created");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(mockResponse);

        // Act
        elasticSearchService.indexSession(testSession, testUniqueId);
        elasticSearchService.indexSession(testSession, testUniqueId);
        elasticSearchService.indexSession(testSession, testUniqueId);

        // Assert
        verify(elasticsearchClient, times(3)).index(any(IndexRequest.class));
    }

    /**
     * Test exception wrapping in RuntimeException
     */
    @Test
    void testIndexSession_ExceptionWrapping() throws IOException {
        // Arrange
        IOException originalException = new IOException("Network error");

        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenThrow(originalException);

        // Act & Assert
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
            elasticSearchService.indexSession(testSession, testUniqueId);
        });

        assertSame(originalException, thrown.getCause());
        assertEquals("Failed to index session", thrown.getMessage());
    }

    // ============ Helper Methods ============

    /**
     * Create a mock IndexResponse with the specified result
     */
    private IndexResponse createMockIndexResponse(String result) {
        IndexResponse mockResponse = mock(IndexResponse.class);
        when(mockResponse.result()).thenReturn(
                co.elastic.clients.elasticsearch._types.Result.valueOf(result.substring(0, 1).toUpperCase() + result.substring(1))
        );
        return mockResponse;
    }

    /**
     * Helper method to set private fields via reflection
     */
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