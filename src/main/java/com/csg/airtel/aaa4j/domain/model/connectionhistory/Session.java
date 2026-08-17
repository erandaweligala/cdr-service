package com.csg.airtel.aaa4j.domain.model.connectionhistory;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Session {
    private String uniqueId;
    private String sessionId;
    private Date startTime;
    private Date endTime;
    private SessionStatus connectionStatus;
    private Long usage;
    private String userName;
    private String groupId;
    private Date updatedTime;
    private String indexName;
    @Builder.Default  // This ensures the list is initialized when using builder
    private List<SessionInstanceInfo> sessionInstances = new ArrayList<>();
}
