package uk.co.sainsburys;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.co.sainsburys.TestUtils.waitForShareGroupAssignment;

@Testcontainers
@SpringBootTest(classes = {KafkaConfig.class, ShareMessageListener.class})
class ShareMessageListenerTest {

    private static final String TOPIC = "my-topic";
    private static final String SHARE_GROUP_ID = "my-share-group";

    @MockitoBean
    private MessageProcessor messageProcessor;

    private Producer<String, String> producer;

    @Container
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:8.2.0")
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
        var result = KAFKA.execInContainer("kafka-features", "--bootstrap-server", "localhost:9093", "upgrade", "--feature", "share.version=1");
        System.out.printf("Enabling Share Groups %d - %s - %s%n", result.getExitCode(), result.getStdout(), result.getStderr());

        assertThat(result.getExitCode())
                .as("Failed to enable share groups")
                .isZero();

        // create topic with 4 partitions
        var topicResult = KAFKA.execInContainer("kafka-topics", "--bootstrap-server", "localhost:9093", "--create", "--topic", "my-topic", "--replication-factor", "1", "--partitions", "4");
        System.out.printf("Topic creation %d - %s - %s%n", topicResult.getExitCode(), topicResult.getStdout(), topicResult.getStderr());

        assertThat(topicResult.getExitCode())
                .as("Failed to create topic")
                .isZero();
    }

    @BeforeEach
    void setUp() {
        producer = createProducer();
        waitForShareGroupAssignment(KAFKA.getBootstrapServers(), TOPIC, SHARE_GROUP_ID, 2);
    }

    @AfterEach
    void tearDown() {
        producer.close();
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
    void produceMessage_messageProcessorThrowsRetryableExceptionAlways_shouldBeRetriedAndArchived() throws Exception {
        String message = "Goodbye cruel world!";
        doThrow(new RetryableException("Temporary failure"))
                .when(messageProcessor).processMessage(message);

        producer.send(new ProducerRecord<>(TOPIC, "key", message));

        await().atMost(5, SECONDS)
                .pollDelay(100, MILLISECONDS)
                .untilAsserted(() ->
                        verify(messageProcessor, times(5)).processMessage(message));
    }

    private static Producer<String, String> createProducer() {
        Map<String, Object> props = Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "acks", "all"
        );

        return new KafkaProducer<>(props);
    }
}
