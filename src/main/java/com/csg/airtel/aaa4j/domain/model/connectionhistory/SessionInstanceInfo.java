package com.csg.airtel.aaa4j.domain.model.connectionhistory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SessionInstanceInfo {
    private LocalDateTime dateTime;
    private String messageId;
    private String messageType;
    private String serviceId;
    private Long usage;
    private String bucketId;
}
