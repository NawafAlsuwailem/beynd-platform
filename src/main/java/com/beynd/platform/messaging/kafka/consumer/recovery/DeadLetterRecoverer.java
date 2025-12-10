package com.beynd.platform.messaging.kafka.consumer.recovery;

import com.beynd.platform.messaging.kafka.common.config.KafkaProperties;
import lombok.Getter;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

public class DeadLetterRecoverer extends DeadLetterPublishingRecoverer {

    @Getter
    private final KafkaProperties properties;

    public DeadLetterRecoverer(KafkaTemplate<String, byte[]> template,
                               KafkaProperties properties) {

        super(template, (record, exception) -> {
            String suffix = properties.getConsumer().getDlqTopicSuffix();
            String dlqTopic = record.topic() + suffix;
            return new TopicPartition(dlqTopic, record.partition());
        });

        this.properties = properties;
    }
}
