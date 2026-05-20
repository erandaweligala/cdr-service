package com.csg.airtel.aaa4j.domain.util.exceptions;

import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BaseExceptionMapperTest {

    @Test
    void shouldConvertBaseExceptionToBaseResponseWithCorrectFields() {
        // given
        String message = "Error occurred";
        String reason = "INVALID_DATA";
        int httpStatus = 400;
        String resultCode = "AAA-400-01";

        BaseException exception = new BaseException(
                message,
                reason,
                httpStatus,
                resultCode
        );

        BaseExceptionMapper mapper = new BaseExceptionMapper();

        // when
        Response response = mapper.toResponse(exception);

        // then
        assertNotNull(response);
        assertEquals(httpStatus, response.getStatus());

        Object entity = response.getEntity();
        assertNotNull(entity);
        assertTrue(entity instanceof BaseResponse);

        @SuppressWarnings("unchecked")
        BaseResponse<?> baseResponse = (BaseResponse<?>) entity;

        // BaseResponse.error(status, message) → status = resultCode, message = reason
        assertEquals(resultCode, baseResponse.getStatus());
        assertEquals(reason, baseResponse.getMessage());

        // For error(), data and pageDetails should be null
        assertNull(baseResponse.getData());
        assertNull(baseResponse.getPageDetails());
    }
}
