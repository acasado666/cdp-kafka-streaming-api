package de.ippen.uiwi.cdp.api.utils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

@Component
public class LimbicReadConfiguration {

    public HashMap<String, String> readConfiguration(String jsonFile) throws IOException {

        HashMap<String, String> map = new HashMap<>();
        JSONParser parser = new JSONParser();
        File file = ResourceUtils.getFile("classpath:"+ jsonFile);
        file.createNewFile();

        try (Reader reader = new FileReader(file)) {

            JSONObject jsonObject = (JSONObject) parser.parse(reader);

            String experimentName = (String) jsonObject.get("experimentName");
            JSONObject variantsSearchSpaceObject = (JSONObject) jsonObject.get("variantsSearchSpace");
            JSONObject designObject = (JSONObject) variantsSearchSpaceObject.get("design");
            JSONObject externExecutionObject = (JSONObject) variantsSearchSpaceObject.get("externExecution");

            JSONArray designImageTypeArray = (JSONArray) designObject.get("designImageTypeAppearance");
            String listImage = getImageType(designImageTypeArray);

            JSONObject positionObject = (JSONObject) externExecutionObject.get("position");
            Long min = (Long) positionObject.get("min");

            createlimbicDataMap(map, experimentName, listImage, min);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return map;
    }

    private void createlimbicDataMap(HashMap<String, String> map, String experimentName, String listImage, Long min) {
        map.put("position", String.valueOf(min));
        map.put("experimentName", experimentName);
        map.put("limbicTypeFull", listImage);
    }

    private String getImageType(JSONArray designImageTypeArray) {
        List<String> listImageType = new ArrayList<>();
        Iterator<String> iterator = designImageTypeArray.iterator();
        while (iterator.hasNext()) {
            listImageType.add(iterator.next());
        }
        return listImageType.toString();
    }
}
