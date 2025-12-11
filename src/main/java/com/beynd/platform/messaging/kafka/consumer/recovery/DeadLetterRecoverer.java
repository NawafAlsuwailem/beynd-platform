package com.beynd.platform.messaging.kafka.consumer.recovery;

import com.beynd.platform.messaging.kafka.common.config.KafkaProperties;
import com.beynd.platform.messaging.kafka.common.serializer.EventPayloadSerializer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

import static com.beynd.platform.messaging.kafka.consumer.recovery.RecoveryUtils.*;

@Slf4j

public class DeadLetterRecoverer extends DeadLetterPublishingRecoverer {

    private final EventPayloadSerializer serializer;

    @Getter
    private final KafkaProperties properties;

    public DeadLetterRecoverer(KafkaTemplate<String, byte[]> template,
                               KafkaProperties properties,
                               EventPayloadSerializer serializer) {

        super(template, (record, exception) -> {
            KafkaProperties.Consumer c = properties.getConsumer();

            String retrySuffix = c.getRetryTopicSuffix();
            String dlqSuffix = c.getDlqTopicSuffix();

            String baseTopic = extractBaseTopic(record.topic(), retrySuffix);

            // If the message value is null, send directly to DLQ instead of retry topic
            // Null values cannot be retried and should be investigated
            if (record.value() == null) {
                log.error("[DLQ] Message value is null for topic={} key={}, sending directly to DLQ",
                        baseTopic, record.key());
                return new TopicPartition(baseTopic + dlqSuffix, 0);
            }

            int retry = getRetryCount(record);

            // Retry and DLQ topics typically have 1 partition, so use partition 0
            // instead of the original partition to avoid partition mismatch errors
            int targetPartition = 0;

            if (retry < c.getRetryMaxAttempts()) {
                return new TopicPartition(baseTopic + retrySuffix, targetPartition);
            }

            return new TopicPartition(baseTopic + dlqSuffix, targetPartition);
        });

        this.properties = properties;
        this.serializer = serializer;

        setHeadersFunction(this::buildHeaders);
    }

    @Override
    protected ProducerRecord<Object, Object> createProducerRecord(
            ConsumerRecord<?, ?> record,
            TopicPartition topicPartition,
            Headers headers,
            byte[] key,
            byte[] value) {

        // Handle null values - if value is null, serialize as null
        // This can happen if the original message had a null payload
        byte[] serialized = value != null ? serializer.serialize(topicPartition.topic(), value) : null;

        // Use null for partition if partition < 0 to let Kafka determine partition,
        // otherwise use the specified partition (typically 0 for retry/DLQ topics)
        Integer partition = topicPartition.partition() < 0 ? null : topicPartition.partition();

        return new ProducerRecord<>(
                topicPartition.topic(),
                partition,
                key != null ? key : record.key(),
                serialized,
                headers
        );
    }



    private Headers buildHeaders(ConsumerRecord<?, ?> record, Exception exception) {
        Headers headers = new RecordHeaders(record.headers().toArray());

        KafkaProperties.Consumer props = properties.getConsumer();

        int retry = getRetryCount(record);
        boolean willRetry = retry < props.getRetryMaxAttempts();

        headers.remove(RETRY_COUNT_HEADER);

        if (willRetry) {
            headers.add(RETRY_COUNT_HEADER, Integer.toString(retry + 1).getBytes(StandardCharsets.UTF_8));
        } else if (retry > 0) {
            headers.add(RETRY_COUNT_HEADER, Integer.toString(retry).getBytes(StandardCharsets.UTF_8));
        }

        return headers;
    }
}
