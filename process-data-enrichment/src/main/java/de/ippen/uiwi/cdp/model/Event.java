package de.ippen.uiwi.cdp.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
@AllArgsConstructor
public class Event {
    Long timestamp;
    String eventType;
}
