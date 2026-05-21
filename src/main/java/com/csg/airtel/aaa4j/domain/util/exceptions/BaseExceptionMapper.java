package com.csg.airtel.aaa4j.domain.util.exceptions;

import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class BaseExceptionMapper implements ExceptionMapper<BaseException> {

    @Inject
    Instance<ExceptionMetricsService> metrics;

    @Override
    public Response toResponse(BaseException ex) {

        if (metrics != null && !metrics.isUnsatisfied()) {
            metrics.get().recordException(
                    ex,
                    ExceptionMetricsService.Layer.RESOURCE,
                    ExceptionMetricsService.Source.INTERNAL);
        }

        BaseResponse<?> error = BaseResponse.error(
                ex.getResultCode(),
                ex.getReason()
        );

        return Response.status(ex.getHttpStatus()).entity(error).build();
    }
}
