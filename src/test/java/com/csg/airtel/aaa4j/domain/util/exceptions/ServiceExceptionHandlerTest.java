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
    void serviceLayerExceptionHandler_executionExceptionWithBaseCause_wrapsInnerBaseException() {
        BaseException inner = new BaseException(
                "inner-msg",
                "inner-reason",
                HttpStatus.SC_BAD_GATEWAY,
                "INNER-CODE"
        );

        ExecutionException ex = new ExecutionException("wrapper-msg", inner);

        BaseException thrown = assertThrows(
                BaseException.class,
                () -> handler.serviceLayerExceptionHandler(ex)
        );

        assertEquals("wrapper-msg", thrown.getMessage());
        assertEquals(inner.getReason(), thrown.getReason());
        assertEquals(inner.getHttpStatus(), thrown.getHttpStatus());
        assertEquals(inner.getResultCode(), thrown.getResultCode());
    }

    @Test
    void serviceLayerExceptionHandler_completionExceptionWithBaseCause_wrapsInnerBaseException() {
        BaseException inner = new BaseException(
                "inner-msg-2",
                "inner-reason-2",
                HttpStatus.SC_BAD_GATEWAY,
                "INNER-CODE-2"
        );

        CompletionException ex = new CompletionException("completion-wrapper", inner);

        BaseException thrown = assertThrows(
                BaseException.class,
                () -> handler.serviceLayerExceptionHandler(ex)
        );

        assertEquals("completion-wrapper", thrown.getMessage());
        assertEquals(inner.getReason(), thrown.getReason());
        assertEquals(inner.getHttpStatus(), thrown.getHttpStatus());
        assertEquals(inner.getResultCode(), thrown.getResultCode());
    }

    @Test
    void serviceLayerExceptionHandler_executionExceptionWithNonBaseCause_usesServiceLayerDefaults() {
        Exception cause = new IllegalStateException("illegal");
        ExecutionException ex = new ExecutionException("exec-msg", cause);

        BaseException thrown = assertThrows(
                BaseException.class,
                () -> handler.serviceLayerExceptionHandler(ex)
        );

        assertEquals("exec-msg", thrown.getMessage());
        assertEquals(ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(), thrown.getReason());
        assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, thrown.getHttpStatus());
        assertEquals(ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code(), thrown.getResultCode());
    }

    @Test
    void serviceLayerExceptionHandler_nonExecutionOrCompletionException_usesServiceLayerDefaults() {
        Exception ex = new Exception("plain-ex");

        BaseException thrown = assertThrows(
                BaseException.class,
                () -> handler.serviceLayerExceptionHandler(ex)
        );

        assertEquals("plain-ex", thrown.getMessage());
        assertEquals(ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(), thrown.getReason());
        assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, thrown.getHttpStatus());
        assertEquals(ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code(), thrown.getResultCode());
    }

    // ───────────────────── elasticsearchExceptionHandler tests ─────────────────────

    @Test
    void elasticsearchExceptionHandler_elasticsearchException_usesServiceUnavailable() {
        ElasticsearchException esEx = mock(ElasticsearchException.class);
        when(esEx.getMessage()).thenReturn("es-error");

        BaseException thrown = assertThrows(
                BaseException.class,
                () -> handler.elasticsearchExceptionHandler(esEx)
        );

        assertEquals("es-error", thrown.getMessage());
        assertEquals(ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.description(), thrown.getReason());
        assertEquals(HttpStatus.SC_SERVICE_UNAVAILABLE, thrown.getHttpStatus());
        assertEquals(ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.code(), thrown.getResultCode());
    }

    @Test
    void elasticsearchExceptionHandler_otherException_usesInternalServerError() {
        Exception ex = new Exception("other-error");

        BaseException thrown = assertThrows(
                BaseException.class,
                () -> handler.elasticsearchExceptionHandler(ex)
        );

        assertEquals("other-error", thrown.getMessage());
        assertEquals(ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.description(), thrown.getReason());
        assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, thrown.getHttpStatus());
        assertEquals(ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.code(), thrown.getResultCode());
    }
}
