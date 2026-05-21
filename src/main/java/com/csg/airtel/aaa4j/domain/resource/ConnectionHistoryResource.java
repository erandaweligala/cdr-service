package com.csg.airtel.aaa4j.domain.resource;

import com.csg.airtel.aaa4j.domain.service.connectionhistory.ConnectionHistoryService;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/api/aaa/admin-console/connection-history")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectionHistoryResource {

    private static final Logger log = LoggerFactory.getLogger(ConnectionHistoryResource.class);

    private final ConnectionHistoryService connectionHistoryService;

    public ConnectionHistoryResource(ConnectionHistoryService connectionHistoryService) {
        this.connectionHistoryService = connectionHistoryService;
    }

    @GET
    @Path("/summary/filter")
    public Uni<Response> getSessions(
            @QueryParam("username") String username,
            @QueryParam("connectionStatus") String connectionStatus,
            @QueryParam("sessionId") String sessionId,
            @QueryParam("groupId") String groupId,
            @QueryParam("startTime") String startTime,
            @QueryParam("endTime") String endTime,
            @QueryParam("pageSize") @DefaultValue("10") int pageSize,
            @QueryParam("page") @DefaultValue("1") int page
    ) {
        log.info("Controller Request Received : ConnectionHistoryResource : fetchSessionDetails");

        return connectionHistoryService.fetchSessionDetails(
                        username,
                        connectionStatus,
                        sessionId,
                        groupId,
                        startTime,
                        endTime,
                        pageSize,
                        page
                )
                .map(sessions -> Response.ok(sessions).build());
    }

    @GET
    @Path("/detail/{sessionId}")
    public Uni<Response> getSessionInstances(@PathParam("sessionId") String sessionId) {

        log.info("Controller Request Received : ConnectionHistoryResource : fetchSessionInstances");
        return connectionHistoryService.fetchSessionInstances(sessionId)
                .map(sessionInstances -> Response.ok(sessionInstances).build());
    }
}
