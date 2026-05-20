package com.csg.airtel.aaa4j.domain.model.connectionhistory;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AccountingEvent {
    @JsonProperty("eventId")
    private String eventId;

    @JsonProperty("eventType")
    private String eventType;

    @JsonProperty("eventVersion")
    private String eventVersion;

    @JsonProperty("eventTimestamp")
    private Instant eventTimestamp;

    @JsonProperty("source")
    private String source;

    @JsonProperty("partitionKey")
    private String partitionKey;

    @JsonProperty("payload")
    private Payload payload;
}