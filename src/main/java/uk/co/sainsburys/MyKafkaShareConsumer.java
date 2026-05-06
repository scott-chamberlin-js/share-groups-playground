package uk.co.sainsburys;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class MyKafkaShareConsumer implements AutoCloseable {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch readyLatch = new CountDownLatch(1);

    private final Runnable consumerTask;

    public MyKafkaShareConsumer(MessageProcessor messageProcessor, String bootstrapServers, String shareGroupId, String topic) {
        Map<String, Object> consumerProperties = Map.of(
                ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, shareGroupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );

        consumerTask = () -> consume(messageProcessor, topic, consumerProperties);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        executor.submit(consumerTask);

        try {
            if (!readyLatch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for consumer thread to be ready");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting for consumer to be ready", e);
        }
    }

    @Override
    public void close() {
        running.set(false);
        executor.shutdown();

        try {
            if (!executor.awaitTermination(ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Timed out waiting for consumer to stop");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting for consumer to stop", e);
        }
    }

    private void consume(MessageProcessor messageProcessor, String topic, Map<String, Object> consumerProperties) {
        try (KafkaShareConsumer<String, String> consumer = new KafkaShareConsumer<>(consumerProperties)) {
            consumer.subscribe(List.of(topic));

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                readyLatch.countDown(); // Signal that the consumer is ready

                for (ConsumerRecord<String, String> consumerRecord : records) {
                    try {
                        log.info("Received record with key={} and deliveryCount={}", consumerRecord.key(), consumerRecord.deliveryCount().orElse((short) 0));
                        messageProcessor.processMessage(consumerRecord.value());
                        consumer.acknowledge(consumerRecord, AcknowledgeType.ACCEPT);
                    } catch (RetryableException e) {
                        log.warn("Retryable exception processing record with key={}. Releasing record so it can be retried. {}", consumerRecord.key(), e.getMessage());
                        consumer.acknowledge(consumerRecord, AcknowledgeType.RELEASE); // will be redelivered
                    } catch (Exception e) {
                        log.error("Unknown error processing record. Rejecting record with key={}", consumerRecord.key(), e);
                        consumer.acknowledge(consumerRecord, AcknowledgeType.REJECT); // permanent failure
                    }
                }
                consumer.commitSync();
            }
        }
    }
}
