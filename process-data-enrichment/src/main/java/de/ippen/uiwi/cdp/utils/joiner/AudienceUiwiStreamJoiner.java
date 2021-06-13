package de.ippen.uiwi.cdp.utils.joiner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.ippen.uiwi.cdp.model.EnrichedStreamModel;
import de.ippen.uiwi.cdp.model.Event;
import de.ippen.uiwi.cdp.model.Experiment;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class AudienceUiwiStreamJoiner implements ValueJoiner<String, String, String> {
    JSONParser parser = new JSONParser();

    private static final String STRING = "string";

    @Autowired
    ObjectMapper mapper = new ObjectMapper();

    @Override
    public String apply(String audienceMessage, String uiwiTableEntry) {
        EnrichedStreamModel output = new EnrichedStreamModel();
        String result = "";
        try {
            JSONObject audienceMessageJson = (JSONObject) parser.parse(audienceMessage);

            JSONObject evnetType = (JSONObject) audienceMessageJson.get("eventType");
            JSONObject eventGeneratedTimestamp = (JSONObject) audienceMessageJson.get("eventGeneratedTimestamp");
            JSONObject eventMetadata = (JSONObject) audienceMessageJson.get("eventMetadata");
            eventMetadata = (JSONObject) eventMetadata.get("de.id.dataflow.audience.analytics.model.enriched.metadata.EventMetadata");
            JSONObject eventPayload = (JSONObject) audienceMessageJson.get("eventPayload");
            eventPayload = (JSONObject) eventPayload.get("de.id.dataflow.audience.analytics.model.enriched.event.EventPayload");

            // Get user id from eventMetadata and set it in output
            eventMetadata = (JSONObject) eventMetadata.get("user");
            eventMetadata = (JSONObject) eventMetadata.get("de.id.dataflow.audience.analytics.model.enriched.metadata.User");
            eventMetadata = (JSONObject) eventMetadata.get("uid");
            output.setUserId((String) eventMetadata.get(STRING));

            // Get experiment
            eventPayload = (JSONObject) eventPayload.get("experiment");
            eventPayload = (JSONObject) eventPayload.get("de.id.dataflow.audience.analytics.model.raw.event.Experiment");

            JSONObject experimentId = (JSONObject) eventPayload.get("experimentId");
            JSONObject experimentVariantId = (JSONObject) eventPayload.get("experimentVariantId");

            String audienceExperiment = (String) experimentId.get(STRING);
            String audienceVariant = (String) experimentVariantId.get(STRING);

            // Get Experiment enrichment from uiwiTable
            JSONObject limbicMessage = (JSONObject) parser.parse(uiwiTableEntry);
            JSONObject trackingMetadata = (JSONObject) limbicMessage.get("trackingMetadata");

            String uiwiTypeFull = (String) trackingMetadata.get("limbicTypeFull");

            String audienceEventType = (String) evnetType.get("de.id.dataflow.audience.analytics.model.enriched.metadata.EventType\":\"elementActiveView");
            Long audienceTimestamp = (Long) eventGeneratedTimestamp.get("long");

            output.setEvent(new Event(audienceTimestamp, audienceEventType));
            output.setExperiment(new Experiment(audienceExperiment, audienceVariant, uiwiTypeFull));

            result = mapper.writeValueAsString(output);

        } catch (ParseException | JsonProcessingException e) {
            log.error(e.getMessage());
        }

        return result;
    }
}
