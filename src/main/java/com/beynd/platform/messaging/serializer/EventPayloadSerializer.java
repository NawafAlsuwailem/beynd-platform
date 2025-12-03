package com.beynd.platform.messaging.serializer;


public interface EventPayloadSerializer {

    /**
     * Serialize an event object into a binary payload suitable for Kafka.
     */
    byte[] serialize(String topic, Object event);
}

