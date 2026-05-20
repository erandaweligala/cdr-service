package com.csg.airtel.aaa4j.domain.model.connectionhistory;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RadiusAttribute {

    @JsonProperty("type")
    private Integer type;

    @JsonProperty("name")
    private String name;

    @JsonProperty("value")
    private String value;
}
