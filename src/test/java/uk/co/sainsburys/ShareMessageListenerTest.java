package uk.co.sainsburys;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.io.IOException;
import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.co.sainsburys.TestUtils.configureShareGroupDLQ;
import static uk.co.sainsburys.TestUtils.createDlqConsumer;
import static uk.co.sainsburys.TestUtils.createKafkaTopic;
import static uk.co.sainsburys.TestUtils.createProducer;
import static uk.co.sainsburys.TestUtils.waitForShareGroupAssignment;

@Testcontainers
@SpringBootTest(classes = {KafkaConfig.class, ShareMessageListener.class})
class ShareMessageListenerTest {

    private static final String TOPIC = "my-topic";
    private static final String SHARE_GROUP_ID = "my-share-group";
    private static final String DLQ_TOPIC = "dlq.%s.%s".formatted(SHARE_GROUP_ID, TOPIC);

    @MockitoBean
    private MessageProcessor messageProcessor;

    private Producer<String, String> producer;
    private Consumer<String, String> dlqConsumer;

    @Container
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:8.4.0")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1");

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("kafka.thing.topic", () -> TOPIC);
        registry.add("kafka.thing.group-id", () -> SHARE_GROUP_ID);
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        // create topic with 4 partitions
        createKafkaTopic(KAFKA, TOPIC, 4);

        // create DLQ topic with 1 partition
        createKafkaTopic(KAFKA, DLQ_TOPIC, 1);

        // enable DLQ on DLQ topic
        configureShareGroupDLQ(KAFKA, DLQ_TOPIC, SHARE_GROUP_ID);
    }

    @BeforeEach
    void setUp() {
        producer = createProducer(KAFKA.getBootstrapServers());
        dlqConsumer = createDlqConsumer(KAFKA.getBootstrapServers(), DLQ_TOPIC);
        waitForShareGroupAssignment(KAFKA.getBootstrapServers(), TOPIC, SHARE_GROUP_ID, 2);
    }

    @AfterEach
    void tearDown() {
        producer.close();
        dlqConsumer.close();
    }

    @Test
    void produceMessage_callsMessageProcessorWithSameMessage() {
        String message = "Hello World!";

        producer.send(new ProducerRecord<>(TOPIC, "key", message));

        await().atMost(5, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .untilAsserted(() ->
                        verify(messageProcessor, times(1)).processMessage(message));
    }

    @Test
    void produceMessage_messageProcessorThrowsRetryableExceptionThreeTimes_shouldBeRetried() throws Exception {
        String message = "Goodbye cruel world!";
        doThrow(new RetryableException("Temporary failure"))
                .doThrow(new RetryableException("Temporary failure"))
                .doThrow(new RetryableException("Temporary failure"))
                .doNothing()
                .when(messageProcessor).processMessage(message);

        producer.send(new ProducerRecord<>(TOPIC, "key", message));

        await().atMost(5, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .untilAsserted(() ->
                        verify(messageProcessor, times(4)).processMessage(message));

    }

    @Test
    void produceMessage_messageProcessorThrowsRetryableExceptionAlways_shouldBeRetriedAndSentToDLQ() throws Exception {
        String message = "Goodbye cruel world!";
        doThrow(new RetryableException("Temporary failure"))
                .when(messageProcessor).processMessage(message);

        RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(TOPIC, "key1", message))
                .get(1, SECONDS);

        await().atMost(5, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .untilAsserted(() ->
                        verify(messageProcessor, times(5)).processMessage(message));

        ConsumerRecords<String, String> consumerRecords = dlqConsumer.poll(Duration.ofSeconds(2));
        assertThat(consumerRecords.count()).isEqualTo(1);

        ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
        assertThat(consumerRecord.key()).isEqualTo("key1");
        assertThat(consumerRecord.value()).isEqualTo(message);
        assertThat(consumerRecord.headers()).contains(
                new RecordHeader("__dlq.errors.topic", recordMetadata.topic().getBytes()),
                new RecordHeader("__dlq.errors.partition", Integer.toString(recordMetadata.partition()).getBytes()),
                new RecordHeader("__dlq.errors.offset", Long.toString(recordMetadata.offset()).getBytes()),
                new RecordHeader("__dlq.errors.group", SHARE_GROUP_ID.getBytes()),
                new RecordHeader("__dlq.errors.delivery.count", "5".getBytes()),
                new RecordHeader("__dlq.errors.message", "Offset delivery count exceeded the threshold.".getBytes())
        );
    }

    @Test
    void produceMessage_messageProcessorThrowsNonRetryableExceptionAlways_shouldBeArchivedAndSentToDLQ() throws Exception {
        String message = "Never recover";
        doThrow(new IOException("Permanent failure"))
                .when(messageProcessor).processMessage(message);

        RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(TOPIC, "key2", message))
                .get(1, SECONDS);

        await().atMost(5, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .untilAsserted(() ->
                        verify(messageProcessor, times(1)).processMessage(message));

        ConsumerRecords<String, String> consumerRecords = dlqConsumer.poll(Duration.ofSeconds(2));
        assertThat(consumerRecords.count()).isEqualTo(1);

        ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
        assertThat(consumerRecord.key()).isEqualTo("key2");
        assertThat(consumerRecord.value()).isEqualTo(message);
        assertThat(consumerRecord.headers()).contains(
                new RecordHeader("__dlq.errors.topic", recordMetadata.topic().getBytes()),
                new RecordHeader("__dlq.errors.partition", Integer.toString(recordMetadata.partition()).getBytes()),
                new RecordHeader("__dlq.errors.offset", Long.toString(recordMetadata.offset()).getBytes()),
                new RecordHeader("__dlq.errors.group", SHARE_GROUP_ID.getBytes()),
                new RecordHeader("__dlq.errors.delivery.count", "1".getBytes()),
                new RecordHeader("__dlq.errors.message", "Offset rejected by client.".getBytes())
        );
    }
}
