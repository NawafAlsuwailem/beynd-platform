package com.beynd.platform.messaging.publisher;

public interface EventPublisher {

    /**
     * Publish a domain event to a Kafka topic via the Outbox pattern.
     *
     * @param partitionKey the Kafka partition key
     * @param topic the Kafka topic name
     * @param event the event payload (Avro-generated class, DTO, etc.)
     */
    void dispatch(String partitionKey, String topic, Object event);
}
