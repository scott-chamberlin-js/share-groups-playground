package uk.co.sainsburys;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberAssignment;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TestUtils {

    /**
     * Waits for all partitions in the given {@code topic} to be assigned
     * amongst all {@code shareGroupMembers} in the {@code shareGroupId}'s
     * share group.
     * <p>
     * After this method returns, the consumer will be completely ready to
     * consume messages.
     *
     * @param bootstrapServers  the bootstrap servers to connect to the cluster with
     * @param topic             the topic the share group is subscribed too
     * @param shareGroupId      the share group id to check for assignment
     * @param shareGroupMembers expected number of members in the share group for the given topic
     */
    public static void waitForShareGroupAssignment(String bootstrapServers, String topic, String shareGroupId, int shareGroupMembers) {
        try (Admin adminClient = Admin.create(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            await().atMost(30, SECONDS)
                    .pollDelay(1, SECONDS)
                    .untilAsserted(() -> {
                        // Find the topic and its partition count
                        TopicDescription topicDescription = adminClient.describeTopics(List.of(topic))
                                .allTopicNames()
                                .get(1, SECONDS)
                                .get(topic);
                        assertThat(topicDescription).as("Topic %s was not found", topic).isNotNull();

                        final long expectedPartitionCount = topicDescription.partitions().size();

                        // Find the share group
                        final ShareGroupDescription shareGroup = adminClient.describeShareGroups(List.of(shareGroupId))
                                .all()
                                .get(1, SECONDS)
                                .get(shareGroupId);

                        assertThat(shareGroup).as("Share Group %s was not found", shareGroupId).isNotNull();
                        assertThat(shareGroup.groupState())
                                .as("Share group should be in STABLE state")
                                .isEqualTo(GroupState.STABLE);

                        // Get all share member assignments that are assigned to the expected topic
                        List<ShareMemberAssignment> topicAssignments = shareGroup.members().stream()
                                .map(ShareMemberDescription::assignment)
                                .filter(assignment ->
                                        assignment.topicPartitions().stream().anyMatch(tp -> tp.topic().equals(topic)))
                                .toList();

                        assertThat(topicAssignments)
                                .as("Share group should have %d members with assigned Topic Partitions", shareGroupMembers)
                                .hasSize(shareGroupMembers)
                                .allSatisfy(assignment ->
                                        assertThat(assignment.topicPartitions()).isNotEmpty());

                        // Count all unique partitions that are assigned to the share group for the expected topic
                        long uniqueAssignedPartitions = topicAssignments.stream()
                                .map(ShareMemberAssignment::topicPartitions)
                                .flatMap(Collection::stream)
                                .filter(tp -> tp.topic().equals(topic))
                                .map(TopicPartition::partition)
                                .distinct()
                                .count();

                        assertThat(uniqueAssignedPartitions)
                                .as("All %d partitions in topic %s should be assigned to Share Group", expectedPartitionCount, topic)
                                .isEqualTo(expectedPartitionCount);
                    });
        }
    }

    public static Producer<String, String> createProducer(String bootstrapServers) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"
        );

        return new KafkaProducer<>(props);
    }

    public static Consumer<String, String> createDlqConsumer(String bootstrapServers, String dlqTopic) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.CLIENT_ID_CONFIG, "dlq-consumer",
                ConsumerConfig.GROUP_ID_CONFIG, "dlq-consumer-%s".formatted(UUID.randomUUID()),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase()
        );

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(dlqTopic));
        kafkaConsumer.poll(Duration.ofMillis(500));
        return kafkaConsumer;
    }

    public static void createKafkaTopic(ConfluentKafkaContainer kafkaContainer, String topic, int partitions) throws IOException, InterruptedException {
        runCommand(kafkaContainer, "Topic creation (%s)".formatted(topic),
                "kafka-topics", "--bootstrap-server", "localhost:9093", "--create", "--topic", topic, "--replication-factor", "1", "--partitions", String.valueOf(partitions));
    }

    public static void configureShareGroupDLQ(ConfluentKafkaContainer kafkaContainer, String dlqTopic, String shareGroupId) throws IOException, InterruptedException {
        // enable DLQ on DLQ topic
        runCommand(kafkaContainer, "DLQ Topic (%s) enable config".formatted(dlqTopic),
                "kafka-configs", "--bootstrap-server", "localhost:9093", "--entity-type", "topics", "--entity-name", dlqTopic, "--alter", "--add-config", "errors.deadletterqueue.group.enable=true");

        // config share group with DLQ topic
        runCommand(kafkaContainer, "Configure share group (%s) DLQ topic (%s)".formatted(shareGroupId, dlqTopic),
                "kafka-configs", "--bootstrap-server", "localhost:9093", "--entity-type", "groups", "--entity-name", shareGroupId, "--alter", "--add-config", "errors.deadletterqueue.topic.name=%s".formatted(dlqTopic));

        // enable DLQ full record copy
        runCommand(kafkaContainer, "Enable full record copy for share group (%s)".formatted(shareGroupId),
                "kafka-configs", "--bootstrap-server", "localhost:9093", "--entity-type", "groups", "--entity-name", shareGroupId, "--alter", "--add-config", "errors.deadletterqueue.copy.record.enable=true");
    }

    private static void runCommand(ConfluentKafkaContainer kafkaContainer, String commandMessage, String... command) throws IOException, InterruptedException {
        var result = kafkaContainer.execInContainer(command);

        System.out.printf("%s %d - %s - %s%n", commandMessage, result.getExitCode(), result.getStdout(), result.getStderr());

        assertThat(result.getExitCode())
                .as("%s failed: %s".formatted(commandMessage, result.getStdout()))
                .isZero();
    }
}
