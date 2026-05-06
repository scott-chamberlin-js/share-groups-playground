package uk.co.sainsburys;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.co.sainsburys.TestUtils.waitForShareGroupAssignment;

@Testcontainers
@MockitoSettings
class MyKafkaShareConsumerTest {

    private static final String TOPIC = "my-topic";
    private static final String SHARE_GROUP_ID = "my-share-group";

    // KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR and KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR
    // must be specified to run Share Groups in a single broker cluster.
    @Container
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:8.2.0")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1");

    @Mock
    private MessageProcessor messageProcessor;
    private Producer<String, String> producer;

    // The Share Group Consumer we are testing
    private MyKafkaShareConsumer testObject;

    @BeforeAll
    static void beforeAll() throws Exception {
        // enable the Share Groups feature in the containerised Kafka instance
        var result = KAFKA.execInContainer("/usr/bin/kafka-features", "--bootstrap-server", "localhost:9093", "upgrade", "--feature", "share.version=1");
        System.out.printf("Enabling Share Groups %d - %s - %s%n", result.getExitCode(), result.getStdout(), result.getStderr());

        assertThat(result.getExitCode())
                .as("Failed to enable share groups")
                .isZero();
    }

    @BeforeEach
    void setUp() {
        testObject = new MyKafkaShareConsumer(messageProcessor, KAFKA.getBootstrapServers(), SHARE_GROUP_ID, TOPIC);
        testObject.start();

        waitForShareGroupAssignment(KAFKA.getBootstrapServers(), TOPIC, SHARE_GROUP_ID, 1);

        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"
        ));
    }

    @AfterEach
    void tearDown() {
        testObject.close();
        producer.close();
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
    void produceMessage_messageProcessorThrowsRetryableExceptionAlways_shouldBeRetriedAndArchived() throws Exception {
        String message = "Goodbye cruel world!";
        doThrow(new RetryableException("Temporary failure"))
                .when(messageProcessor).processMessage(message);

        producer.send(new ProducerRecord<>(TOPIC, "key", message));

        await().atMost(10, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .untilAsserted(() ->
                        verify(messageProcessor, times(5)).processMessage(message));
    }
}