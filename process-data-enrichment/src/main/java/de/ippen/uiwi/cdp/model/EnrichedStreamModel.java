package de.ippen.uiwi.cdp.model;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Setter
@Getter
public class EnrichedStreamModel {
    String userId;
    Event event;
    Experiment experiment;
}
