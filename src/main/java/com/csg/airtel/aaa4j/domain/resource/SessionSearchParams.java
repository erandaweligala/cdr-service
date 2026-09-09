package com.csg.airtel.aaa4j.domain.resource;

import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.QueryParam;

/** Query parameters accepted by {@code GET /summary/filter}, grouped into one bean. */
public class SessionSearchParams {

    @QueryParam("username")
    public String username;

    @QueryParam("connectionStatus")
    public String connectionStatus;

    @QueryParam("sessionId")
    public String sessionId;

    @QueryParam("groupId")
    public String groupId;

    @QueryParam("startTime")
    public String startTime;

    @QueryParam("endTime")
    public String endTime;

    @QueryParam("pageSize")
    @DefaultValue("10")
    public int pageSize;

    @QueryParam("page")
    @DefaultValue("1")
    public int page;
}
