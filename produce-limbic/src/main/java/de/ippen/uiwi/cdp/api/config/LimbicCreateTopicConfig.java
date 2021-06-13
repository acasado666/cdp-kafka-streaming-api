package de.ippen.uiwi.cdp.api.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class LimbicCreateTopicConfig {

    private static final String TOPIC_NAME = "uiwi.racim.limbic-experiments";
    private static final String TOPIC_NAME_OUT = "uiwi.racim.limbic-experiments-out";

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
