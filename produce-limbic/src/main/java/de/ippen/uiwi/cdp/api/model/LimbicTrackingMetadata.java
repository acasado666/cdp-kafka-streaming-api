package de.ippen.uiwi.cdp.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LimbicTrackingMetadata {
    String experimentName;
    String LimbicTypeFull;
    int LimbicTypePosition;

}
