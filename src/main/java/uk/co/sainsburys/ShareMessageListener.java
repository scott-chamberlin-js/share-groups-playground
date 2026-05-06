package uk.co.sainsburys;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.ShareAcknowledgment;
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
    public void listen(ConsumerRecord<String, String> consumerRecord, ShareAcknowledgment acknowledgment) {
        try {
            log.info("Received record with key={} and deliveryCount={}", consumerRecord.key(), consumerRecord.deliveryCount().orElse((short) 0));
            messageProcessor.processMessage(consumerRecord.value());
            acknowledgment.acknowledge(); // ACCEPT
        } catch (RetryableException e) {
            log.warn("Retryable exception processing record with key={}. Releasing record so it can be retried. {}", consumerRecord.key(), e.getMessage());
            acknowledgment.release(); // RELEASE: will be redelivered
        } catch (Exception e) {
            log.error("Unknown error processing record. Rejecting record with key={}", consumerRecord.key(), e);
            acknowledgment.reject(); // REJECT: permanent failure
        }
    }
}
