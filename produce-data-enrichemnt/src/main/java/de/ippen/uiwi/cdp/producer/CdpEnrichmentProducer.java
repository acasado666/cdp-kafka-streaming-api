package de.ippen.uiwi.cdp.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
@Slf4j
public class CdpEnrichmentProducer {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    private static final String OUTPUT_TOPIC = "de.id.dataflow.audience.analytics.tracking-basic";

    public void sendMessage(String key, String value) {
        ProducerRecord<String, String> audienceRecord = new ProducerRecord<>(OUTPUT_TOPIC, null, key, value);
        ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send(audienceRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("Error Sending the Message and the exception is {}", ex.getMessage());
                try {
                    throw ex;
                } catch (Throwable throwable) {
                    log.error("Error in OnFailure: {}", throwable.getMessage());
                }
            }

            @Override
            public void onSuccess(SendResult<String, String> stringStringSendResult) {
                log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, stringStringSendResult.getRecordMetadata().partition());
            }
        });
    }

}
