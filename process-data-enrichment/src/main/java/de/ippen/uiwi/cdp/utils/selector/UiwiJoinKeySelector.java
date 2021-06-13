package de.ippen.uiwi.cdp.utils.selector;

import lombok.SneakyThrows;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class UiwiJoinKeySelector implements KeyValueMapper<String, String, String> {
    JSONParser parser = new JSONParser();

    @SneakyThrows
    @Override
    public String apply(String key, String value) {
        JSONObject limbicMessage = (JSONObject) parser.parse(value);
        JSONObject trackingMetadata = (JSONObject) limbicMessage.get("trackingMetadata");
        return trackingMetadata.get("experimentName") + "-" + trackingMetadata.get("limbicTypePosition");
    }
}
