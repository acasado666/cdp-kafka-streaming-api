package de.ippen.uiwi.cdp;

import de.ippen.uiwi.cdp.utils.joiner.AudienceUiwiStreamJoiner;
import de.ippen.uiwi.cdp.utils.selector.AudienceJoinKeySelector;
import de.ippen.uiwi.cdp.utils.selector.JoinedKeySelector;
import de.ippen.uiwi.cdp.utils.selector.UiwiJoinKeySelector;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class DataEnrichmentTopology {
    private static final String AUDIENCE_TOPIC = "de.id.dataflow.audience.analytics.tracking-basic";
    private static final String UIWI_TOPIC = "uiwi.racim.limbic-experiments-out";
    private static final String OUTPUT_TOPIC = "bi.cdp.user-profile";
    private static final String USER_TOPIC_OUT = "bi.cdp.user-profile-out";

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration streamsConfiguration() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "myKafkaStreamsEnrichment");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return new KafkaStreamsConfiguration(properties);
    }

    @Bean
    public KStream<String, String> kafkaStream(StreamsBuilder builder) {
        KStream<String, String> audienceStream = builder.stream(AUDIENCE_TOPIC);
        KStream<String, String> uiwiStream = builder.stream(UIWI_TOPIC);

        UiwiJoinKeySelector uiwiSelector = new UiwiJoinKeySelector();
        AudienceJoinKeySelector audienceSelector = new AudienceJoinKeySelector();
        JoinedKeySelector joinedKeySelector = new JoinedKeySelector();

        AudienceUiwiStreamJoiner audienceUiwiStreamJoiner = new AudienceUiwiStreamJoiner();

        KTable<String, String> uiwiTable = uiwiStream.selectKey(uiwiSelector).toTable();

        audienceStream.filter((key, value) -> audienceFilter(value))
                .selectKey(audienceSelector)
                .join(uiwiTable, audienceUiwiStreamJoiner)
                .selectKey(joinedKeySelector)
                .to(OUTPUT_TOPIC);

        audienceStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, String> audienceStream1 = audienceStream;
        audienceStream1.print(Printed.toSysOut());

        return audienceStream;
    }

    private boolean audienceFilter(String value) { //looks for msgs with element activeview & elementclick
        final String elementActiveView = "elementActiveView";
        final String elementClick = "elementClick";

        boolean result = false;

        JSONParser parser = new JSONParser();
        try {
            JSONObject audienceMessage = (JSONObject) parser.parse(value);
            audienceMessage = (JSONObject) audienceMessage.get("eventType");
            switch ((String) audienceMessage.get("de.id.dataflow.audience.analytics.model.enriched.metadata.EventType")) {
                case elementActiveView:
                case elementClick:
                    result = true;
                    break;
                default:
                    result = false;
            }
        } catch (ParseException e) {
            log.error(e.getMessage());
        }

        return result;
    }

    @Bean
    public NewTopic cdpProfile() {
        return TopicBuilder.name(OUTPUT_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic cdpProfileOut() {
        return TopicBuilder.name(USER_TOPIC_OUT)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
