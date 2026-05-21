package com.csg.airtel.aaa4j.domain.util.exceptions;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ServiceExceptionHandlerTest {

    private final ServiceExceptionHandler handler = new ServiceExceptionHandler();

    @Test
    void serviceLayerExceptionHandler_executionExceptionWithBaseCause_unwrapsInnerBaseException() {
        BaseException inner = new BaseException(
                "inner-msg",
                "inner-reason",
                HttpStatus.SC_BAD_GATEWAY,
                "INNER-CODE"
        );

        ExecutionException ex = new ExecutionException("wrapper-msg", inner);

        BaseException result = handler.serviceLayerExceptionHandler(ex);

        assertSame(inner, result);
        assertEquals(inner.getMessage(), result.getMessage());
        assertEquals(inner.getReason(), result.getReason());
        assertEquals(inner.getHttpStatus(), result.getHttpStatus());
        assertEquals(inner.getResultCode(), result.getResultCode());
    }

    @Test
    void serviceLayerExceptionHandler_completionExceptionWithBaseCause_unwrapsInnerBaseException() {
        BaseException inner = new BaseException(
                "inner-msg-2",
                "inner-reason-2",
                HttpStatus.SC_BAD_GATEWAY,
                "INNER-CODE-2"
        );

        CompletionException ex = new CompletionException("completion-wrapper", inner);

        BaseException result = handler.serviceLayerExceptionHandler(ex);

        assertSame(inner, result);
        assertEquals(inner.getMessage(), result.getMessage());
        assertEquals(inner.getReason(), result.getReason());
        assertEquals(inner.getHttpStatus(), result.getHttpStatus());
        assertEquals(inner.getResultCode(), result.getResultCode());
    }

    @Test
    void serviceLayerExceptionHandler_executionExceptionWithNonBaseCause_usesServiceLayerDefaults() {
        Exception cause = new IllegalStateException("illegal");
        ExecutionException ex = new ExecutionException("exec-msg", cause);

        BaseException result = handler.serviceLayerExceptionHandler(ex);

        assertNotNull(result);
        assertEquals("illegal", result.getMessage());
        assertEquals(ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(), result.getReason());
        assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, result.getHttpStatus());
        assertEquals(ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code(), result.getResultCode());
    }

    @Test
    void serviceLayerExceptionHandler_nonExecutionOrCompletionException_usesServiceLayerDefaults() {
        Exception ex = new Exception("plain-ex");

        BaseException result = handler.serviceLayerExceptionHandler(ex);

        assertNotNull(result);
        assertEquals("plain-ex", result.getMessage());
        assertEquals(ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(), result.getReason());
        assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, result.getHttpStatus());
        assertEquals(ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code(), result.getResultCode());
    }

    // ───────────────────── elasticsearchExceptionHandler tests ─────────────────────

    @Test
    void elasticsearchExceptionHandler_elasticsearchException_usesServiceUnavailable() {
        ElasticsearchException esEx = mock(ElasticsearchException.class);
        when(esEx.getMessage()).thenReturn("es-error");

        BaseException result = handler.elasticsearchExceptionHandler(esEx);

        assertNotNull(result);
        assertEquals("es-error", result.getMessage());
        assertEquals(ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.description(), result.getReason());
        assertEquals(HttpStatus.SC_SERVICE_UNAVAILABLE, result.getHttpStatus());
        assertEquals(ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.code(), result.getResultCode());
    }

    @Test
    void elasticsearchExceptionHandler_otherException_usesInternalServerError() {
        Exception ex = new Exception("other-error");

        BaseException result = handler.elasticsearchExceptionHandler(ex);

        assertNotNull(result);
        assertEquals("other-error", result.getMessage());
        assertEquals(ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.description(), result.getReason());
        assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, result.getHttpStatus());
        assertEquals(ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.code(), result.getResultCode());
    }
}
