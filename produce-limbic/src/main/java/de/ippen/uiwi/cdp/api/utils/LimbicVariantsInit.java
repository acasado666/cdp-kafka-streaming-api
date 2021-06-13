package de.ippen.uiwi.cdp.api.utils;

import de.ippen.uiwi.cdp.api.model.LimbicExternExecution;
import de.ippen.uiwi.cdp.api.model.LimbicTrackingMetadata;
import de.ippen.uiwi.cdp.api.model.LimbicVariant;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Component
@AllArgsConstructor
public class LimbicVariantsInit {

    LimbicReadConfiguration readConfiguration;
    List<LimbicVariant> limbicVariants = new ArrayList<LimbicVariant>();
    HashMap<String, String> readConfigData;

    public List<LimbicVariant> getLimbicVariants() throws IOException {
        limbicVariants = readFile(limbicVariants, "data/experiment_1.json");
        limbicVariants = readFile(limbicVariants, "data/experiment_2.json");
        limbicVariants = readFile(limbicVariants, "data/experiment_3.json");
        limbicVariants = readFile(limbicVariants, "data/experiment_4.json");
        return limbicVariants;
    }

    private List<LimbicVariant> readFile(List<LimbicVariant> limbicVariants, String experiment) throws IOException {
        LimbicVariant limbicVariant;
        readConfigData = readConfiguration.readConfiguration(experiment);

        List<String> limbicTypeList = getLimbicTypeList(readConfigData);
        int limbicId = 0;
        while (limbicTypeList.size() > limbicId) {
            limbicVariant = new LimbicVariant(
                    limbicId,
                    new LimbicTrackingMetadata(readConfigData.get("experimentName"), limbicTypeList.get(limbicId++), limbicId),
                    new LimbicExternExecution(readConfigData.get("position")));
            limbicVariants.add(limbicVariant);
        }
        return limbicVariants;
    }

    private List<String> getLimbicTypeList(HashMap<String, String> readConfigData) {
        String limbicTypeFull = readConfigData.get("limbicTypeFull");
        limbicTypeFull = limbicTypeFull.substring(1, limbicTypeFull.length() - 1); //remove first and last char

        String str[] = limbicTypeFull.split(", ");
        List<String> limbicTypeList = new ArrayList<>();
        limbicTypeList = Arrays.asList(str);
        return limbicTypeList;
    }
}
