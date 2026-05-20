package com.csg.airtel.aaa4j.domain.resource;

import com.csg.airtel.aaa4j.domain.model.BaseResponse;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.SessionInstanceInfo;
import com.csg.airtel.aaa4j.domain.service.connectionhistory.ConnectionHistoryService;
import com.csg.airtel.aaa4j.domain.util.exceptions.BaseException;
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
    public Response getSessions(
            @QueryParam("username") String username,
            @QueryParam("connectionStatus") String connectionStatus,
            @QueryParam("sessionId") String sessionId,
            @QueryParam("groupId") String groupId,
            @QueryParam("startTime") String startTime,
            @QueryParam("endTime") String endTime,
            @QueryParam("pageSize") @DefaultValue("10") int pageSize,
            @QueryParam("page") @DefaultValue("1") int page
    ) throws BaseException {
        log.info("Controller Request Received : ConnectionHistoryResource : fetchSessionDetails");
        BaseResponse<Session> sessions = connectionHistoryService.fetchSessionDetails(
                username,
                connectionStatus,
                sessionId,
                groupId,
                startTime,
                endTime,
                pageSize,
                page
        );

        return Response.ok(sessions).build();

    }

    @GET
    @Path("/detail/{sessionId}")
    public Response getSessionInstances(@PathParam("sessionId") String sessionId) throws BaseException {

        log.info("Controller Request Received : ConnectionHistoryResource : fetchSessionInstances");
        BaseResponse<SessionInstanceInfo> sessionInstances = connectionHistoryService.fetchSessionInstances(sessionId);

        return Response.ok(sessionInstances).build();
    }


}
