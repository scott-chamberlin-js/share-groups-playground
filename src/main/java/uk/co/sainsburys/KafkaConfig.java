package uk.co.sainsburys;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.listener.ShareConsumerRecordRecoverer;

import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConfig {

    @Bean
    public ShareConsumerFactory<String, String> shareConsumerFactory(@Value("${kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        return new DefaultShareConsumerFactory<>(props);
    }

    @Bean
    public ShareKafkaListenerContainerFactory<String, String> shareKafkaListenerContainerFactory(
            ShareConsumerFactory<String, String> shareConsumerFactory,
            ShareConsumerRecordRecoverer recordRecoverer) {
        ShareKafkaListenerContainerFactory<String, String> containerFactory = new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
        containerFactory.setShareConsumerRecordRecoverer(recordRecoverer);
        return containerFactory;
    }

    /**
     * Creates a {@link ShareConsumerRecordRecoverer} bean that returns a
     * release ack on retryable exceptions and a reject ack on all other
     * exceptions.
     * <p>
     * Note, poll-level exceptions are not handled by this bean, so exceptions
     * like deserialisation exception will automatically be rejected by the
     * consumer.
     */
    @Bean
    public ShareConsumerRecordRecoverer shareConsumerRecordRecoverer() {
        return (consumerRecord, ex) -> {
            if (ex instanceof RetryableException || ex.getCause() instanceof RetryableException) {
                log.warn("Retryable exception processing record with key={}. Releasing record so it can be retried. {}", consumerRecord.key(), ex.getMessage());
                return AcknowledgeType.RELEASE; // will be redelivered
            } else {
                log.error("Unknown error processing record. Rejecting record with key={}", consumerRecord.key(), ex);
                return AcknowledgeType.REJECT; // permanent failure
            }
        };
    }
}
