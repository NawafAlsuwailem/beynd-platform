package com.beynd.platform.messaging.serializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AvroBinaryEventPayloadSerializer implements EventPayloadSerializer {

    private final KafkaAvroSerializer kafkaAvroSerializer;

    @Override
    public byte[] serialize(String topic, Object event) {
        return kafkaAvroSerializer.serialize(topic, event);
    }
}
