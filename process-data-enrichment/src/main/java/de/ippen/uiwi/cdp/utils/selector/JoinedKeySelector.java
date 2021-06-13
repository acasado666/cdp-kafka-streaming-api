package de.ippen.uiwi.cdp.utils.selector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

@Slf4j
public class JoinedKeySelector implements KeyValueMapper<String, String, String> {
    JSONParser parser = new JSONParser();

    @Override
    public String apply(String oldKey, String value) {
        String newKey = "";

        try {
            JSONObject userProfile = (JSONObject) parser.parse(value);
            String userId = (String) userProfile.get("userId");
            JSONObject event = (JSONObject) userProfile.get("event");
            Long timestamp = (Long) event.get("timestamp"); //TODO to check


            JSONObject experiment = (JSONObject) userProfile.get("experiment");
            String experimentName = (String) experiment.get("experimentName");
            String limbicTypePattern = (String) experiment.get("limbicTypePattern");

//            newKey = userId + "-" + experimentName + "-" + limbicTypePattern + "-" + timestamp;
            newKey = userId + "-" + experimentName + "-" + limbicTypePattern;

        } catch (ParseException e) {
            log.error(e.getMessage());
        }

        return newKey;
    }
}
