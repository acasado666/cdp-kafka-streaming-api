package de.ippen.uiwi.cdp;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
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
public class KafkaStreamsConfig {

    private static final String TOPIC_NAME = "uiwi.racim.limbic-experiments";
    private static final String TOPIC_NAME_OUT = "uiwi.racim.limbic-experiments-out";

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "myKafkaStreams");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<Integer, String> kStream(StreamsBuilder kStreamBuilder) {

        KStream<Integer, String> stream = kStreamBuilder.stream(TOPIC_NAME);
        stream.print(Printed.toSysOut());
        stream.selectKey((key, value) -> value.split(",")[0].toUpperCase())
                .mapValues(value -> value.split(",")[1].toLowerCase());

        stream.to(TOPIC_NAME_OUT, Produced.with(Serdes.Integer(), Serdes.String()));
        stream.print(Printed.toSysOut());
        return stream;
    }

    @Bean
    public NewTopic cdpEvents(){
        return TopicBuilder.name(TOPIC_NAME)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic cdpEventsOut(){
        return TopicBuilder.name(TOPIC_NAME_OUT)
                .partitions(1)
                .replicas(1)
                .build();
    }

}
