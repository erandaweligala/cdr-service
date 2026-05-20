package com.csg.airtel.aaa4j.domain.util.exceptions;

import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class BaseExceptionMapper implements ExceptionMapper<BaseException> {

    @Override
    public Response toResponse(BaseException ex) {

        BaseResponse<?> error = BaseResponse.error(
                ex.getResultCode(),
                ex.getReason()
        );

        return Response.status(ex.getHttpStatus()).entity(error).build();
    }
}
