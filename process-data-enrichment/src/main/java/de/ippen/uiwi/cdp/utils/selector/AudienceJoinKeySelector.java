package de.ippen.uiwi.cdp.utils.selector;

import lombok.SneakyThrows;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class AudienceJoinKeySelector implements KeyValueMapper<String, String, String> {
    JSONParser parser = new JSONParser();

    @SneakyThrows
    @Override
    public String apply(String key, String value) {
        JSONObject audienceMessage = (JSONObject) parser.parse(value);
        JSONObject payload = (JSONObject) audienceMessage.get("eventPayload");
        payload = (JSONObject) payload.get("de.id.dataflow.audience.analytics.model.enriched.event.EventPayload");
        payload = (JSONObject) payload.get("experiment");
        payload = (JSONObject) payload.get("de.id.dataflow.audience.analytics.model.raw.event.Experiment");

        JSONObject experimentId = (JSONObject) payload.get("experimentId");
        JSONObject experimentVariantId = (JSONObject) payload.get("experimentVariantId");
        return experimentId.get("string") + "-" + experimentVariantId.get("string");
    }
}
