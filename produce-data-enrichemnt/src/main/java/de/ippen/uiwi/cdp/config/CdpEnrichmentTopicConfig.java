package de.ippen.uiwi.cdp.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class CdpEnrichmentTopicConfig {
    @Bean
    public NewTopic cdpEvents(){
        return TopicBuilder.name("bi.cdp.user-profile")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic cdpEventsOut(){
        return TopicBuilder.name("de.id.dataflow.audience.analytics.tracking-basic")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
