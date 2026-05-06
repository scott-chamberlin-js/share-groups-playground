package uk.co.sainsburys;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShareMessageListener {

    private final MessageProcessor messageProcessor;

    @KafkaListener(
            topics = "${kafka.thing.topic}",
            containerFactory = "shareKafkaListenerContainerFactory",
            groupId = "${kafka.thing.group-id}",
            concurrency = "2"
    )
    public void listen(ConsumerRecord<String, String> consumerRecord) throws RetryableException {
        log.info("Received record with key={} and deliveryCount={}", consumerRecord.key(), consumerRecord.deliveryCount().orElse((short) 0));
        messageProcessor.processMessage(consumerRecord.value());
    }
}
