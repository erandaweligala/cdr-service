package com.csg.airtel.aaa4j.domain.resource;

import com.csg.airtel.aaa4j.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionSearchCriteria;
import com.csg.airtel.aaa4j.domain.service.connectionhistory.ConnectionHistoryService;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

@Path("/api/aaa/admin-console/connection-history")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectionHistoryResource {

    private static final Logger log = Logger.getLogger(ConnectionHistoryResource.class);

    private final ConnectionHistoryService connectionHistoryService;

    public ConnectionHistoryResource(ConnectionHistoryService connectionHistoryService) {
        this.connectionHistoryService = connectionHistoryService;
    }

    @GET
    @Path("/summary/filter")
    public Uni<Response> getSessions(@BeanParam SessionSearchParams params) {
        LoggingUtil.logInfo(log, "getSessions", "Controller Request Received : ConnectionHistoryResource : fetchSessionDetails");

        SessionSearchCriteria criteria = new SessionSearchCriteria(
                params.username,
                params.connectionStatus,
                params.sessionId,
                params.groupId,
                params.startTime,
                params.endTime,
                params.pageSize,
                params.page
        );

        return connectionHistoryService.fetchSessionDetails(criteria)
                .map(sessions -> Response.ok(sessions).build());
    }

    @GET
    @Path("/detail/{sessionId}")
    public Uni<Response> getSessionInstances(@PathParam("sessionId") String sessionId) {

        LoggingUtil.logInfo(log, "getSessionInstances", "Controller Request Received : ConnectionHistoryResource : fetchSessionInstances");
        return connectionHistoryService.fetchSessionInstances(sessionId)
                .map(sessionInstances -> Response.ok(sessionInstances).build());
    }
}
