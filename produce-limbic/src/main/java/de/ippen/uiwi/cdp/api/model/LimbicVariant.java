package de.ippen.uiwi.cdp.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LimbicVariant {
    Integer limbicId;
    LimbicTrackingMetadata trackingMetadata;
    LimbicExternExecution externExecution;
}
