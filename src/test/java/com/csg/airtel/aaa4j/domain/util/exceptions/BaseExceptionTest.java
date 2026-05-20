package com.csg.airtel.aaa4j.domain.util.exceptions;

import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BaseExceptionTest {

    @Test
    void shouldStoreAllFieldsAndExtendRuntimeException() {
        // given
        String message = "Something went wrong";
        String reason = "INVALID_REQUEST";
        int httpStatus = HttpStatus.SC_BAD_REQUEST;
        String resultCode = "40001";

        // when
        BaseException ex = new BaseException(message, reason, httpStatus, resultCode);

        // then
        // from RuntimeException
        assertEquals(message, ex.getMessage());
        assertTrue(ex instanceof RuntimeException);

        // own fields
        assertEquals(reason, ex.getReason());
        assertEquals(httpStatus, ex.getHttpStatus());
        assertEquals(resultCode, ex.getResultCode());
    }
}
