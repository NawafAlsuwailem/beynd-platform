package com.beynd.platform.messaging.kafka.consumer.idempotency;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {

    boolean existsByTopicAndPartitionAndOffset(String topic, int partition, long offset);

    Optional<ProcessedEvent> findByTopicAndPartitionAndOffset(String topic, int partition, long offset);
}

