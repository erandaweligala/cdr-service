package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch.core.UpdateResponse;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ElasticSearchServiceTest {

    @Mock
    private ElasticsearchAsyncClient elasticsearchClient;

    @InjectMocks
    private ElasticSearchService elasticSearchService;

    private Session testSession;
    private SessionInstanceInfo testInstance;
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

        testInstance = new SessionInstanceInfo(
                new Date(), "msg-1", "ACCOUNTING_INTERIM", "service-123", 512L, "bucket-456");
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

    // ============ appendInstance Success Tests ============

    @Test
    void testAppendInstance_Success_ActiveSession() {
        mockUpdate("updated");

        elasticSearchService.appendInstance(testSession, testUniqueId, testTargetIndex, testInstance)
                .await().indefinitely();

        verify(elasticsearchClient, times(1)).update(any(UpdateRequest.class), eq(Session.class));
    }

    @Test
    void testAppendInstance_Success_CompletedSession() {
        testSession.setConnectionStatus(SessionStatus.COMPLETED);
        testSession.setEndTime(new Date());
        mockUpdate("updated");

        elasticSearchService.appendInstance(testSession, testUniqueId, testTargetIndex, testInstance)
                .await().indefinitely();

        verify(elasticsearchClient, times(1)).update(any(UpdateRequest.class), eq(Session.class));
    }

    @Test
    void testAppendInstance_NewDocument_Created() {
        mockUpdate("created");

        elasticSearchService.appendInstance(testSession, testUniqueId, testTargetIndex, testInstance)
                .await().indefinitely();

        verify(elasticsearchClient, times(1)).update(any(UpdateRequest.class), eq(Session.class));
    }

    @Test
    void testAppendInstance_CarriesInstanceInUpsert_WithoutMutatingCaller() {
        mockUpdate("created");

        elasticSearchService.appendInstance(testSession, testUniqueId, testTargetIndex, testInstance)
                .await().indefinitely();

        // The caller's session must NOT be mutated (so it can be serialized to Redis concurrently).
        assertTrue(testSession.getSessionInstances().isEmpty(),
                "appendInstance must not mutate the caller's session");

        // The upsert document, however, must carry exactly the one new instance so a first-time
        // session is created with its history seeded.
        ArgumentCaptor<UpdateRequest<Session, Session>> captor = captureUpdate();
        verify(elasticsearchClient).update(captor.capture(), eq(Session.class));
        Session upsertDoc = captor.getValue().upsert();
        assertNotNull(upsertDoc, "scripted update must provide an upsert document");
        assertEquals(1, upsertDoc.getSessionInstances().size());
        assertSame(testInstance, upsertDoc.getSessionInstances().get(0));
    }

    @Test
    void testAppendInstance_MultipleSequentialCalls() {
        mockUpdate("updated");

        elasticSearchService.appendInstance(testSession, testUniqueId, testTargetIndex, testInstance)
                .await().indefinitely();
        elasticSearchService.appendInstance(testSession, testUniqueId, testTargetIndex, testInstance)
                .await().indefinitely();
        elasticSearchService.appendInstance(testSession, testUniqueId, testTargetIndex, testInstance)
                .await().indefinitely();

        verify(elasticsearchClient, times(3)).update(any(UpdateRequest.class), eq(Session.class));
    }

    // ============ Rolling Index / Routing Correctness ============

    @Test
    void testAppendInstance_UsesProvidedTargetIndex_NotCurrentDay() {
        String yesterdayIndex = BASE_INDEX + "-"
                + LocalDate.now(ZoneOffset.UTC).minusDays(1)
                .format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));
        mockUpdate("updated");

        elasticSearchService.appendInstance(testSession, testUniqueId, yesterdayIndex, testInstance)
                .await().indefinitely();

        ArgumentCaptor<UpdateRequest<Session, Session>> captor = captureUpdate();
        verify(elasticsearchClient).update(captor.capture(), eq(Session.class));
        assertEquals(yesterdayIndex, captor.getValue().index(),
                "Append should write to the session's birth index, not today's index");
    }

    @Test
    void testAppendInstance_CorrectIndexAndId() {
        mockUpdate("created");

        elasticSearchService.appendInstance(testSession, testUniqueId, testTargetIndex, testInstance)
                .await().indefinitely();

        ArgumentCaptor<UpdateRequest<Session, Session>> captor = captureUpdate();
        verify(elasticsearchClient).update(captor.capture(), eq(Session.class));
        assertEquals(testTargetIndex, captor.getValue().index(),
                "UpdateRequest should use the targetIndex passed by the caller");
        assertEquals(testUniqueId, captor.getValue().id(),
                "UpdateRequest should be keyed by the unique session id");
    }

    // ============ Error Tests ============

    @Test
    void testAppendInstance_IOExceptionThrown() {
        IOException ioException = new IOException("Connection timeout");
        when(elasticsearchClient.update(any(UpdateRequest.class), eq(Session.class)))
                .thenReturn(CompletableFuture.failedFuture(ioException));

        Throwable thrown = assertThrows(Throwable.class, () ->
                elasticSearchService.appendInstance(testSession, testUniqueId, testTargetIndex, testInstance)
                        .await().indefinitely()
        );

        assertSame(ioException, unwrap(thrown));
        verify(elasticsearchClient, times(1)).update(any(UpdateRequest.class), eq(Session.class));
    }

    @Test
    void testAppendInstance_ExceptionWrapping() {
        IOException originalException = new IOException("Network error");
        when(elasticsearchClient.update(any(UpdateRequest.class), eq(Session.class)))
                .thenReturn(CompletableFuture.failedFuture(originalException));

        Throwable thrown = assertThrows(Throwable.class, () ->
                elasticSearchService.appendInstance(testSession, testUniqueId, testTargetIndex, testInstance)
                        .await().indefinitely()
        );

        assertSame(originalException, unwrap(thrown));
    }

    // ============ Helper Methods ============

    @SuppressWarnings("unchecked")
    private void mockUpdate(String result) {
        UpdateResponse<Session> mockResponse = mock(UpdateResponse.class);
        when(mockResponse.result()).thenReturn(
                Result.valueOf(result.substring(0, 1).toUpperCase() + result.substring(1)));
        when(elasticsearchClient.update(any(UpdateRequest.class), eq(Session.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ArgumentCaptor<UpdateRequest<Session, Session>> captureUpdate() {
        return (ArgumentCaptor) ArgumentCaptor.forClass(UpdateRequest.class);
    }

    private Throwable unwrap(Throwable t) {
        Throwable current = t;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
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
