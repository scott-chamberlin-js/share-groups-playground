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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.io.IOException;
import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.co.sainsburys.TestUtils.createDlqConsumer;
import static uk.co.sainsburys.TestUtils.createKafkaTopic;
import static uk.co.sainsburys.TestUtils.createProducer;
import static uk.co.sainsburys.TestUtils.getLagForShareGroup;
import static uk.co.sainsburys.TestUtils.waitForShareGroupAssignment;

@Testcontainers
@MockitoSettings
class MyKafkaShareConsumerTest {

    private static final String TOPIC = "my-topic";
    private static final String SHARE_GROUP_ID = "my-share-group";
    private static final String DLQ_TOPIC = "dlq.%s.%s".formatted(SHARE_GROUP_ID, TOPIC);

    // KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR and KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR
    // must be specified to run Share Groups in a single broker cluster.
    @Container
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:8.4.0")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1");

    @Mock
    private MessageProcessor messageProcessor;

    private Producer<String, String> producer;
    private Consumer<String, String> dlqConsumer;

    // The Share Group Consumer we are testing
    private MyKafkaShareConsumer testObject;

    @BeforeAll
    static void beforeAll() throws Exception {
        createKafkaTopic(KAFKA.getBootstrapServers(), TOPIC, 4);
        createKafkaTopic(KAFKA.getBootstrapServers(), DLQ_TOPIC, 1);
    }

    @BeforeEach
    void setUp() throws Exception {
        testObject = new MyKafkaShareConsumer(messageProcessor, KAFKA.getBootstrapServers(), SHARE_GROUP_ID, TOPIC);
        testObject.start();

        waitForShareGroupAssignment(KAFKA.getBootstrapServers(), TOPIC, SHARE_GROUP_ID, 1);

        producer = createProducer(KAFKA.getBootstrapServers());
        dlqConsumer = createDlqConsumer(KAFKA.getBootstrapServers(), DLQ_TOPIC);
    }

    @AfterEach
    void tearDown() {
        testObject.close();
        producer.close();
        dlqConsumer.close();
    }

    @Test
    void produceMessage_callsMessageProcessorWithSameMessage() {
        String message = "Hello world!";

        producer.send(new ProducerRecord<>(TOPIC, "key", message));

        await().atMost(5, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .untilAsserted(() ->
                        verify(messageProcessor, atLeastOnce()).processMessage(message));
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

        await().atMost(10, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .untilAsserted(() ->
                        verify(messageProcessor, times(4)).processMessage(message));

    }

    @Test
    void produceMessage_messageProcessorThrowsRetryableExceptionAlways_shouldBeRetriedAndArchivedAndSentToDLQ() throws Exception {
        String message = "Goodbye cruel world!";
        doThrow(new RetryableException("Temporary failure"))
                .when(messageProcessor).processMessage(message);

        RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(TOPIC, "key3", message))
                .get(1, SECONDS);

        await().atMost(5, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .untilAsserted(() -> {
                    verify(messageProcessor, times(5)).processMessage(message);
                    assertThat(getLagForShareGroup(KAFKA.getBootstrapServers(), TOPIC, SHARE_GROUP_ID, recordMetadata.partition())).contains(0L);
                });

        ConsumerRecords<String, String> consumerRecords = dlqConsumer.poll(Duration.ofSeconds(2));
        assertThat(consumerRecords.count()).isEqualTo(1);

        ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
        assertThat(consumerRecord.key()).isEqualTo("key3");
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
        String message = "Goodbye cruel world!";
        doThrow(new IOException("Permanent failure"))
                .when(messageProcessor).processMessage(message);

        RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(TOPIC, "key4", message))
                .get(1, SECONDS);

        await().atMost(5, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .untilAsserted(() -> {
                    verify(messageProcessor, times(1)).processMessage(message);
                    assertThat(getLagForShareGroup(KAFKA.getBootstrapServers(), TOPIC, SHARE_GROUP_ID, recordMetadata.partition())).contains(0L);
                });

        ConsumerRecords<String, String> consumerRecords = dlqConsumer.poll(Duration.ofSeconds(2));
        assertThat(consumerRecords.count()).isEqualTo(1);

        ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
        assertThat(consumerRecord.key()).isEqualTo("key4");
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
