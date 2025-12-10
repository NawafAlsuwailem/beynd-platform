package com.beynd.platform.messaging.kafka.producer.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query(
            value = """
            SELECT *
            FROM outbox_event
            WHERE status = CAST(:status AS TEXT)
            AND attempt_count < :maxAttempts
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT :batchSize
        """,
            nativeQuery = true
    )
    List<OutboxEvent> lockBatchForProcessing(
            @Param("status") String status,
            @Param("maxAttempts") int maxAttempts,
            @Param("batchSize") int batchSize
    );
}
