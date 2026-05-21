package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ElasticSearchServiceTest {

    @Mock
    private ElasticsearchAsyncClient elasticsearchClient;

    @InjectMocks
    private ElasticSearchService elasticSearchService;

    private Session testSession;
    private String testSessionId;
    private String testUniqueId;
    private String testTargetIndex;

    private static final String BASE_INDEX = "test-sessions-index";

    @BeforeEach
    void setUp() {
        setPrivateField(elasticSearchService, "sessionsIndex", BASE_INDEX);

        testSessionId  = "test-session-123";
        testUniqueId   = "test-session-123-port-5060";

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
        String expectedPattern = BASE_INDEX + "-"
                + LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));

        String result = elasticSearchService.getCurrentIndex();

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
    void testIndexSession_Success_ActiveSession() {
        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                .await().indefinitely();

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_Success_CompletedSession() {
        testSession.setConnectionStatus(SessionStatus.COMPLETED);
        testSession.setEndTime(new Date());

        IndexResponse mockResponse = createMockIndexResponse("updated");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                .await().indefinitely();

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_Success_TerminatedSession() {
        testSession.setConnectionStatus(SessionStatus.TERMINATED);
        testSession.setEndTime(new Date());

        IndexResponse mockResponse = createMockIndexResponse("updated");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                .await().indefinitely();

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_Success_TerminationRequestedSession() {
        testSession.setConnectionStatus(SessionStatus.TERMINATION_REQUESTED);

        IndexResponse mockResponse = createMockIndexResponse("updated");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                .await().indefinitely();

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_ResultCreated() {
        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                .await().indefinitely();

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_ResultUpdated() {
        IndexResponse mockResponse = createMockIndexResponse("updated");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                .await().indefinitely();

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_NullSessionFields() {
        Session sessionWithNulls = Session.builder()
                .sessionId(testSessionId)
                .connectionStatus(SessionStatus.ACTIVE)
                .build();

        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(sessionWithNulls, testUniqueId, testTargetIndex)
                .await().indefinitely();

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_UnknownStatus() {
        testSession.setConnectionStatus(SessionStatus.UNKNOWN);

        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                .await().indefinitely();

        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_DifferentUniqueIds() {
        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(testSession, "session-1-port-1", testTargetIndex)
                .await().indefinitely();
        elasticSearchService.indexSession(testSession, "session-2-port-2", testTargetIndex)
                .await().indefinitely();

        verify(elasticsearchClient, times(2)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_MultipleSequentialCalls() {
        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                .await().indefinitely();
        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                .await().indefinitely();
        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                .await().indefinitely();

        verify(elasticsearchClient, times(3)).index(any(IndexRequest.class));
    }

    // ============ Rolling Index Correctness Test ============

    @Test
    void testIndexSession_UsesProvidedTargetIndex_NotCurrentDay() {
        String yesterdayIndex = BASE_INDEX + "-"
                + LocalDate.now(ZoneOffset.UTC).minusDays(1)
                .format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));

        IndexResponse mockResponse = createMockIndexResponse("updated");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(testSession, testUniqueId, yesterdayIndex)
                .await().indefinitely();

        ArgumentCaptor<IndexRequest<Session>> captor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(elasticsearchClient).index(captor.capture());
        assertEquals(yesterdayIndex, captor.getValue().index(),
                "STOP event should write to the session's birth index, not today's index");
    }

    @Test
    void testIndexSession_CorrectIndexName() {
        IndexResponse mockResponse = createMockIndexResponse("created");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                .await().indefinitely();

        ArgumentCaptor<IndexRequest<Session>> captor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(elasticsearchClient).index(captor.capture());
        assertEquals(testTargetIndex, captor.getValue().index(),
                "IndexRequest should use the targetIndex passed by the caller");
    }

    // ============ Error Tests ============

    @Test
    void testIndexSession_IOExceptionThrown() {
        IOException ioException = new IOException("Connection timeout");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(ioException));

        Throwable thrown = assertThrows(Throwable.class, () ->
                elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                        .await().indefinitely()
        );

        Throwable root = unwrap(thrown);
        assertSame(ioException, root);
        verify(elasticsearchClient, times(1)).index(any(IndexRequest.class));
    }

    @Test
    void testIndexSession_IOExceptionWithCustomMessage() {
        IOException ioException = new IOException("Index not found");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(ioException));

        Throwable thrown = assertThrows(Throwable.class, () ->
                elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                        .await().indefinitely()
        );

        Throwable root = unwrap(thrown);
        assertTrue(root.getMessage().contains("Index not found"));
    }

    @Test
    void testIndexSession_ExceptionWrapping() {
        IOException originalException = new IOException("Network error");
        when(elasticsearchClient.index(any(IndexRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(originalException));

        Throwable thrown = assertThrows(Throwable.class, () ->
                elasticSearchService.indexSession(testSession, testUniqueId, testTargetIndex)
                        .await().indefinitely()
        );

        assertSame(originalException, unwrap(thrown));
    }

    // ============ Helper Methods ============

    private Throwable unwrap(Throwable t) {
        Throwable current = t;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

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
