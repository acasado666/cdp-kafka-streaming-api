package de.ippen.uiwi.cdp.api.service;

import de.ippen.uiwi.cdp.api.model.LimbicVariant;
import de.ippen.uiwi.cdp.api.producer.LimbicVariantEventProducer;
import de.ippen.uiwi.cdp.api.utils.LimbicVariantsInit;
import lombok.AllArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
@AllArgsConstructor
public class LimbicVariantsSend {

    LimbicVariantsInit limbicVariantsInit;
    LimbicVariantEventProducer limbicEventProducer;

    @EventListener(ApplicationReadyEvent.class)
    public void getLimbicVariants() throws IOException {
        List<LimbicVariant> limbicVariants = limbicVariantsInit.getLimbicVariants();

        for (LimbicVariant l: limbicVariants) {
            limbicEventProducer.sendLimbicEvent(l);
        }
    }
}
