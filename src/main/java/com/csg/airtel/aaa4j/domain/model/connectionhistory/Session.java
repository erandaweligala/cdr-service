package com.csg.airtel.aaa4j.domain.model.connectionhistory;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Session {
    private String uniqueId;
    private String sessionId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private SessionStatus connectionStatus;
    private Long usage;
    private String userName;
    private String groupId;
    private LocalDateTime updatedTime;
    private String indexName;
    @Builder.Default
    private List<SessionInstanceInfo> sessionInstances = new ArrayList<>();
}
