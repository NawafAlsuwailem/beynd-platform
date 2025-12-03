package com.beynd.platform.messaging.publisher;

public interface EventPublisher {

    /**
     * Publish a domain event to a Kafka topic via the Outbox pattern.
     *
     * @param topic the Kafka topic name
     * @param event the event payload (Avro-generated class, DTO, etc.)
     */
    void publish(String topic, Object event);
}
