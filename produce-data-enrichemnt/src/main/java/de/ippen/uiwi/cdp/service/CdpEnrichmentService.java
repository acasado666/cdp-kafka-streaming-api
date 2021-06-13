package de.ippen.uiwi.cdp.service;

import de.ippen.uiwi.cdp.producer.CdpEnrichmentProducer;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

@Component
@AllArgsConstructor
public class CdpEnrichmentService {

    CdpEnrichmentProducer producer;

    @EventListener(ApplicationReadyEvent.class)
    public void getLimbicVariants() throws IOException {
        List<Pair<String, String>> messages = new LinkedList<>();
        List<String> names = Arrays.asList("ex1.json", "ex2.json", "ex3.json");

        for (String name : names) {
            messages.add(readFile(name));
        }

        for (Pair<String, String> pair : messages) {
            producer.sendMessage(pair.getLeft(), pair.getRight());
        }
    }

    private Pair<String, String> readFile(String jsonFile) throws IOException {
        Pair<String, String> pair = Pair.of("test", "test");
        JSONParser parser = new JSONParser();
        File file = ResourceUtils.getFile("classpath:"+ jsonFile);
        file.createNewFile();

        try (Reader reader = new FileReader(file)) {

            JSONObject jsonObject = (JSONObject) parser.parse(reader);
            var tmp = (JSONObject) jsonObject.get("eventMetadata");
            tmp = (JSONObject) tmp.get("de.id.dataflow.audience.analytics.model.enriched.metadata.EventMetadata");
            tmp = (JSONObject) tmp.get("pageViewId");

            String pageViewId = (String) tmp.get("string");

            pair = Pair.of(pageViewId, jsonObject.toJSONString());
            // eventMetadata": {
            //    "de.id.dataflow.audience.analytics.model.enriched.metadata.EventMetadata": {
            //      "pageViewId": {
            //        "string": "4f5ad055-d0e1-59f1-d268-1f9a654f4f70-1622487391-1059390916"
            //      },

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return pair;
    }
}
