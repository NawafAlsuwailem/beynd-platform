package com.beynd.platform.messaging.kafka.consumer.recovery;

import com.beynd.platform.messaging.kafka.common.config.KafkaProperties;
import com.beynd.platform.messaging.kafka.common.serializer.EventPayloadSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;


public class DeadLetterRecoverer extends DeadLetterPublishingRecoverer {

    private final EventPayloadSerializer serializer;
    private final KafkaProperties properties;

    public DeadLetterRecoverer(
            KafkaTemplate<String, byte[]> template,
            KafkaProperties properties,
            EventPayloadSerializer serializer) {

        super(template, (record, ex) ->
                new TopicPartition(
                        record.topic() + properties.getConsumer().getDlqTopicSuffix(),
                        0
                )
        );

        this.serializer = serializer;
        this.properties = properties;
    }


    @Override
    protected ProducerRecord<Object, Object> createProducerRecord(
            ConsumerRecord<?, ?> record,
            TopicPartition topicPartition,
            Headers headers,
            byte[] key,
            byte[] ignored) {

        byte[] payload =
                record.headers().lastHeader("springDeserializerValue") != null
                        ? record.headers().lastHeader("springDeserializerValue").value()
                        : serializer.serialize(record.topic(), record.value());

        return new ProducerRecord<>(
                topicPartition.topic(),
                0,
                record.key(),
                payload,
                headers
        );
    }
}

