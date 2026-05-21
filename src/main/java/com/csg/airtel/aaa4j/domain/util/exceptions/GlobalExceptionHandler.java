package com.csg.airtel.aaa4j.domain.util.exceptions;

import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class GlobalExceptionHandler implements ExceptionMapper<Exception> {

    @Inject
    Instance<ExceptionMetricsService> metrics;

    @Override
    public Response toResponse(Exception exception) {

        recordMetric(exception);

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

    private void recordMetric(Throwable t) {
        if (metrics == null || metrics.isUnsatisfied()) {
            return;
        }
        metrics.get().recordException(
                t,
                ExceptionMetricsService.Layer.RESOURCE,
                ExceptionMetricsService.Source.INTERNAL);
    }
}
