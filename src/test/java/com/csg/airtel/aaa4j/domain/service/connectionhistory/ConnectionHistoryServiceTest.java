package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionStatus;
import com.csg.airtel.aaa4j.domain.util.exceptions.BaseException;
import com.csg.airtel.aaa4j.domain.util.exceptions.ServiceExceptionHandler;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ConnectionHistoryServiceTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ElasticsearchAsyncClient client;

    @Mock
    private ServiceExceptionHandler handler;

    @InjectMocks
    private ConnectionHistoryService service;

    private static final String START_TIME = "2024-01-01T00:00:00";
    private static final String END_TIME   = "2024-01-31T23:59:59";

    /** UTC+03:00 all year, so a bound read in it differs from the same bound read as UTC. */
    private static final String DEPLOYMENT_ZONE = "Africa/Nairobi";

    private final String sessionsIndex = "test-sessions-index";

    @BeforeEach
    void setUp() throws Exception {
        reset(client, handler);
        setPrivateField(service, "sessionsIndex", sessionsIndex);
        setPrivateField(service, "timezone", "UTC");
    }

    // ============ fetchSessionDetails Success Tests ============

    @Test
    void testFetchSessionDetails_Success_AllParameters() {
        stubAllIndicesExist(true);
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<Session> result = service.fetchSessionDetails(
                "user@test.com", "ACTIVE", "session-123", "group-001",
                START_TIME, END_TIME, 10, 1
        ).await().indefinitely();

        assertNotNull(result);
        assertEquals(1, result.getData().size());
        assertEquals(1L, result.getPageDetails().getTotalRecords());
    }

    @Test
    void testFetchSessionDetails_Success_OnlyMandatoryDates() {
        stubAllIndicesExist(true);
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, null,
                START_TIME, END_TIME, 10, 1
        ).await().indefinitely();

        assertNotNull(result);
        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_EmptyResults() {
        stubAllIndicesExist(true);
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.emptyList(), 0L);
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, null,
                START_TIME, END_TIME, 10, 1
        ).await().indefinitely();

        assertNotNull(result);
        assertTrue(result.getData().isEmpty());
        assertEquals(0L, result.getPageDetails().getTotalRecords());
    }

    @Test
    void testFetchSessionDetails_Success_WithUsername() {
        stubAllIndicesExist(true);
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<Session> result = service.fetchSessionDetails(
                "user@test.com", null, null, null,
                START_TIME, END_TIME, 10, 1
        ).await().indefinitely();

        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_WithConnectionStatus() {
        stubAllIndicesExist(true);
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<Session> result = service.fetchSessionDetails(
                null, "ACTIVE", null, null,
                START_TIME, END_TIME, 10, 1
        ).await().indefinitely();

        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_WithSessionId() {
        stubAllIndicesExist(true);
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, "session-123", null,
                START_TIME, END_TIME, 10, 1
        ).await().indefinitely();

        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_WithGroupId() {
        stubAllIndicesExist(true);
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, "group-001",
                START_TIME, END_TIME, 10, 1
        ).await().indefinitely();

        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_MultiDayRange() {
        stubAllIndicesExist(true);
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, null,
                "2024-01-01T00:00:00", "2024-01-03T23:59:59",
                10, 1
        ).await().indefinitely();

        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_Success_SameDayRange() {
        stubAllIndicesExist(true);
        Session session = createTestSession("session-1");
        SearchResponse<Session> mockResponse = createMockSearchResponse(Collections.singletonList(session), 1L);
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, null,
                "2024-01-15T00:00:00", "2024-01-15T23:59:59",
                10, 1
        ).await().indefinitely();

        assertEquals(1, result.getData().size());
    }

    @Test
    void testFetchSessionDetails_NoIndicesExist_ReturnsEmpty() {
        stubAllIndicesExist(false);

        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, null,
                START_TIME, END_TIME, 10, 1
        ).await().indefinitely();

        assertNotNull(result);
        assertTrue(result.getData().isEmpty());
        assertEquals(0L, result.getPageDetails().getTotalRecords());
        verify(client, never()).search(any(Function.class), eq(Session.class));
    }

    // ============ fetchSessionDetails Error Tests ============

    @Test
    void testFetchSessionDetails_ElasticsearchException() {
        stubAllIndicesExist(true);
        ElasticsearchException esException = createElasticsearchException();
        BaseException handledException = new BaseException("ES Error", "ES_ERROR", 503, "ES_001");

        stubSearch(CompletableFuture.failedFuture(esException));
        when(handler.elasticsearchExceptionHandler(any(Throwable.class))).thenReturn(handledException);

        Throwable failure = service.fetchSessionDetails(
                null, null, null, null, START_TIME, END_TIME, 10, 1
        ).subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();

        assertInstanceOf(BaseException.class, failure);
        assertEquals("ES Error", failure.getMessage());
        verify(handler).elasticsearchExceptionHandler(any(Throwable.class));
    }

    @Test
    void testFetchSessionDetails_IOException() {
        stubAllIndicesExist(true);
        IOException ioException = new IOException("Network error");
        BaseException handledException = new BaseException("IO Error", "IO_ERROR", 500, "IO_001");

        stubSearch(CompletableFuture.failedFuture(ioException));
        when(handler.elasticsearchExceptionHandler(any(Throwable.class))).thenReturn(handledException);

        Throwable failure = service.fetchSessionDetails(
                null, null, null, null, START_TIME, END_TIME, 10, 1
        ).subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();

        assertInstanceOf(BaseException.class, failure);
        assertEquals("IO Error", failure.getMessage());
    }

    @Test
    void testFetchSessionDetails_BaseException_Rethrown() {
        stubAllIndicesExist(true);
        BaseException baseException = new BaseException("Custom Error", "CUSTOM", 400, "C001");

        stubSearch(CompletableFuture.failedFuture(baseException));

        Throwable failure = service.fetchSessionDetails(
                null, null, null, null, START_TIME, END_TIME, 10, 1
        ).subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();

        assertInstanceOf(BaseException.class, failure);
        assertEquals("Custom Error", failure.getMessage());
        verify(handler, never()).elasticsearchExceptionHandler(any());
    }

    @Test
    void testFetchSessionDetails_UnexpectedException() {
        stubAllIndicesExist(true);
        RuntimeException unexpectedException = new RuntimeException("Unexpected");
        BaseException handledException = new BaseException("Service Error", "SERVICE_ERROR", 500, "S001");

        stubSearch(CompletableFuture.failedFuture(unexpectedException));
        when(handler.serviceLayerExceptionHandler(any(Throwable.class))).thenReturn(handledException);

        Throwable failure = service.fetchSessionDetails(
                null, null, null, null, START_TIME, END_TIME, 10, 1
        ).subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();

        assertInstanceOf(BaseException.class, failure);
        assertEquals("Service Error", failure.getMessage());
    }

    // ============ fetchSessionInstances Tests ============

    @Test
    void testFetchSessionInstances_Success_WithInstances() {
        String sessionId = "session-123";
        Session session = createTestSession(sessionId);

        SessionInstanceInfo instance1 = new SessionInstanceInfo();
        instance1.setMessageId("msg-1");
        SessionInstanceInfo instance2 = new SessionInstanceInfo();
        instance2.setMessageId("msg-2");
        session.setSessionInstances(Arrays.asList(instance1, instance2));

        SearchResponse<Session> mockResponse = createMockSessionHitsOnly(Collections.singletonList(session));
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<SessionInstanceInfo> result =
                service.fetchSessionInstances(sessionId).await().indefinitely();

        assertNotNull(result);
        assertEquals(2, result.getData().size());
    }

    @Test
    void testFetchSessionInstances_Success_NoInstances() {
        String sessionId = "session-123";
        Session session = createTestSession(sessionId);
        session.setSessionInstances(null);

        SearchResponse<Session> mockResponse = createMockSessionHitsOnly(Collections.singletonList(session));
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<SessionInstanceInfo> result =
                service.fetchSessionInstances(sessionId).await().indefinitely();

        assertNotNull(result);
        assertTrue(result.getData().isEmpty());
    }

    @Test
    void testFetchSessionInstances_NotFound() {
        SearchResponse<Session> mockResponse = createMockSessionHitsOnly(Collections.emptyList());
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        Throwable failure = service.fetchSessionInstances("session-123")
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();

        assertInstanceOf(BaseException.class, failure);
        BaseException exception = (BaseException) failure;
        assertTrue(exception.getMessage().contains("No session instances found"));
        assertEquals(404, exception.getHttpStatus());
    }

    @Test
    void testFetchSessionInstances_FoundButNullSource() {
        SearchResponse<Session> mockResponse = createMockSessionHitsOnly(Collections.singletonList(null));
        stubSearch(CompletableFuture.completedFuture(mockResponse));

        BaseResponse<SessionInstanceInfo> result =
                service.fetchSessionInstances("session-123").await().indefinitely();

        assertNotNull(result);
        assertTrue(result.getData().isEmpty());
    }

    @Test
    void testFetchSessionInstances_ElasticsearchException() {
        ElasticsearchException esException = createElasticsearchException();
        BaseException handledException = new BaseException("ES Error", "ES_ERROR", 503, "ES_001");

        stubSearch(CompletableFuture.failedFuture(esException));
        when(handler.elasticsearchExceptionHandler(any(Throwable.class))).thenReturn(handledException);

        Throwable failure = service.fetchSessionInstances("session-123")
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();

        assertInstanceOf(BaseException.class, failure);
        assertEquals("ES Error", failure.getMessage());
    }

    @Test
    void testFetchSessionInstances_IOException() {
        IOException ioException = new IOException("Network error");
        BaseException handledException = new BaseException("IO Error", "IO_ERROR", 500, "IO_001");

        stubSearch(CompletableFuture.failedFuture(ioException));
        when(handler.elasticsearchExceptionHandler(any(Throwable.class))).thenReturn(handledException);

        Throwable failure = service.fetchSessionInstances("session-123")
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();

        assertInstanceOf(BaseException.class, failure);
        assertEquals("IO Error", failure.getMessage());
    }

    // ============ parseDate Tests ============

    @Test
    void testParseDate_Success() {
        Instant result = service.parseDate("2024-01-15T10:30:00");
        assertEquals(Instant.parse("2024-01-15T10:30:00Z"), result);
    }

    @Test
    void testParseDate_DifferentDate() {
        Instant result = service.parseDate("2025-12-31T23:59:59");
        assertEquals(Instant.parse("2025-12-31T23:59:59Z"), result);
    }

    @Test
    void testParseDate_ReadsBoundInDeploymentTimezone() {
        setPrivateField(service, "timezone", DEPLOYMENT_ZONE);

        // 10:30 on the console's clock is 07:30 UTC, not 10:30 UTC.
        assertEquals(Instant.parse("2024-01-15T07:30:00Z"), service.parseDate("2024-01-15T10:30:00"));
    }

    @Test
    void testParseDate_AcceptsPlainDateAsStartOfDay() {
        setPrivateField(service, "timezone", DEPLOYMENT_ZONE);

        assertEquals(Instant.parse("2024-01-14T21:00:00Z"), service.parseDate("2024-01-15"));
    }

    @Test
    void testParseDate_HonoursAnExplicitOffset() {
        setPrivateField(service, "timezone", DEPLOYMENT_ZONE);

        assertEquals(Instant.parse("2024-01-15T10:30:00Z"), service.parseDate("2024-01-15T10:30:00+00:00"));
    }

    @Test
    void testParseDate_FallsBackToUtcOnUnknownTimezone() {
        setPrivateField(service, "timezone", "Not/AZone");

        assertEquals(Instant.parse("2024-01-15T10:30:00Z"), service.parseDate("2024-01-15T10:30:00"));
    }

    // ============ Default date range Tests ============

    @Test
    void testFetchSessionDetails_DefaultRangeUsesDeploymentTimezoneDates() {
        setPrivateField(service, "timezone", DEPLOYMENT_ZONE);
        stubAllIndicesExist(true);
        stubSearch(CompletableFuture.completedFuture(createMockSearchResponse(Collections.emptyList(), 0L)));

        BaseResponse<Session> result = service.fetchSessionDetails(
                null, null, null, null, null, null, 10, 1
        ).await().indefinitely();

        assertNotNull(result);
        // The default range is the last 8 local days, one index per day.
        verify(client.indices(), times(8)).exists(any(Function.class));
    }

    // ============ logElasticsearchError Coverage Tests ============

    @Test
    void testLogElasticsearchError_WithRootCause() {
        stubAllIndicesExist(true);
        ElasticsearchException esException = createElasticsearchExceptionWithRootCause();
        BaseException handledException = new BaseException("ES Error", "ES_ERROR", 503, "ES_001");

        stubSearch(CompletableFuture.failedFuture(esException));
        when(handler.elasticsearchExceptionHandler(any(Throwable.class))).thenReturn(handledException);

        Throwable failure = service.fetchSessionDetails(
                null, null, null, null, START_TIME, END_TIME, 10, 1
        ).subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();

        assertInstanceOf(BaseException.class, failure);
    }

    @Test
    void testLogElasticsearchError_WithMetadata() {
        stubAllIndicesExist(true);
        ElasticsearchException esException = createElasticsearchExceptionWithMetadata();
        BaseException handledException = new BaseException("ES Error", "ES_ERROR", 503, "ES_001");

        stubSearch(CompletableFuture.failedFuture(esException));
        when(handler.elasticsearchExceptionHandler(any(Throwable.class))).thenReturn(handledException);

        Throwable failure = service.fetchSessionDetails(
                null, null, null, null, START_TIME, END_TIME, 10, 1
        ).subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();

        assertInstanceOf(BaseException.class, failure);
    }

    @Test
    void testLogElasticsearchError_NullError() {
        stubAllIndicesExist(true);
        ElasticsearchException esException = mock(ElasticsearchException.class);
        when(esException.status()).thenReturn(500);
        when(esException.error()).thenReturn(null);

        BaseException handledException = new BaseException("ES Error", "ES_ERROR", 503, "ES_001");

        stubSearch(CompletableFuture.failedFuture(esException));
        when(handler.elasticsearchExceptionHandler(any(Throwable.class))).thenReturn(handledException);

        Throwable failure = service.fetchSessionDetails(
                null, null, null, null, START_TIME, END_TIME, 10, 1
        ).subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
                .getFailure();

        assertInstanceOf(BaseException.class, failure);
    }

    // ============ Helper Methods ============

    @SuppressWarnings("unchecked")
    private void stubSearch(CompletableFuture<SearchResponse<Session>> future) {
        when(client.search(any(Function.class), eq(Session.class))).thenReturn(future);
    }

    private void stubAllIndicesExist(boolean exists) {
        BooleanResponse booleanResponse = mock(BooleanResponse.class);
        when(booleanResponse.value()).thenReturn(exists);
        when(client.indices().exists(any(Function.class)))
                .thenReturn(CompletableFuture.completedFuture(booleanResponse));
    }

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
    private SearchResponse<Session> createMockSessionHitsOnly(List<Session> sessions) {
        SearchResponse<Session> response = mock(SearchResponse.class);
        HitsMetadata<Session> hitsMetadata = mock(HitsMetadata.class);

        List<Hit<Session>> hits = new ArrayList<>();
        for (Session session : sessions) {
            Hit<Session> hit = mock(Hit.class);
            when(hit.source()).thenReturn(session);
            hits.add(hit);
        }

        when(hitsMetadata.hits()).thenReturn(hits);
        when(response.hits()).thenReturn(hitsMetadata);
        return response;
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
