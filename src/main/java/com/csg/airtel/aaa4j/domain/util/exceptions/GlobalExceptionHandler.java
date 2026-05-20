package com.csg.airtel.aaa4j.domain.util.exceptions;

import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class GlobalExceptionHandler implements ExceptionMapper<Exception> {
    @Override
    public Response toResponse(Exception exception) {

        // 404
        if (exception instanceof SessionNotFoundException) {
            BaseResponse<?> error = BaseResponse.error(
                    ResponseCodeEnum.SESSION_NOT_FOUND.code(),
                    exception.getMessage()
            );
            return Response.status(Response.Status.NOT_FOUND).entity(error).build();
        }

        // 500 (fallback)
        BaseResponse<?> error = BaseResponse.error(
                "INTERNAL_SERVER_ERROR",
                exception.getMessage()
        );
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(error).build();
    }
}
