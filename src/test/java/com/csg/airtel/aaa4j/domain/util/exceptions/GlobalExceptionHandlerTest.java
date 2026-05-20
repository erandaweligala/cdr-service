package com.csg.airtel.aaa4j.domain.util.exceptions;

import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void toResponse_sessionNotFoundException_returns404WithProperBody() {
        // given
        String message = "Session not found for id=xyz";
        Exception ex = new SessionNotFoundException(message);

        // when
        Response response = handler.toResponse(ex);

        // then
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());

        Object entity = response.getEntity();
        assertNotNull(entity);
        assertTrue(entity instanceof BaseResponse);

        @SuppressWarnings("unchecked")
        BaseResponse<?> body = (BaseResponse<?>) entity;

        // status code should be SESSION_NOT_FOUND
        assertEquals(ResponseCodeEnum.SESSION_NOT_FOUND.code(), body.getStatus());
        // message should be the original exception message
        assertEquals(message, body.getMessage());
    }

    @Test
    void toResponse_genericException_returns500WithInternalServerErrorBody() {
        // given
        String message = "Unexpected boom";
        Exception ex = new RuntimeException(message);

        // when
        Response response = handler.toResponse(ex);

        // then
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());

        Object entity = response.getEntity();
        assertNotNull(entity);
        assertTrue(entity instanceof BaseResponse);

        @SuppressWarnings("unchecked")
        BaseResponse<?> body = (BaseResponse<?>) entity;

        // status code should be the literal INTERNAL_SERVER_ERROR used in handler
        assertEquals("INTERNAL_SERVER_ERROR", body.getStatus());
        // message should be the original exception message
        assertEquals(message, body.getMessage());
    }
}
