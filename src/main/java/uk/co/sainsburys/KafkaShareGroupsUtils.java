package uk.co.sainsburys;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.List;
import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KafkaShareGroupsUtils {

    private static final String DLQ_TOPIC_TEMPLATE = "dlq.%s.%s";

    public static KafkaFuture<Void> configureShareGroupDLQ(String bootstrapServers, String originalTopic, String shareGroupId) {
        final String dlqTopic = DLQ_TOPIC_TEMPLATE.formatted(shareGroupId, originalTopic);

        try (Admin adminClient = Admin.create(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            return adminClient.incrementalAlterConfigs(Map.of(
                            new ConfigResource(ConfigResource.Type.TOPIC, dlqTopic), List.of(
                                    new AlterConfigOp(new ConfigEntry("errors.deadletterqueue.group.enable", "true"), AlterConfigOp.OpType.SET)),
                            new ConfigResource(ConfigResource.Type.GROUP, shareGroupId), List.of(
                                    new AlterConfigOp(new ConfigEntry("errors.deadletterqueue.topic.name", dlqTopic), AlterConfigOp.OpType.SET),
                                    new AlterConfigOp(new ConfigEntry("errors.deadletterqueue.copy.record.enable", "true"), AlterConfigOp.OpType.SET)
                            )
                    ))
                    .all();
        }
    }
}
