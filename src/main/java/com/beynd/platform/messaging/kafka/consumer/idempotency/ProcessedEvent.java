package com.beynd.platform.messaging.kafka.consumer.idempotency;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "processed_event",
        uniqueConstraints = {
                @UniqueConstraint(name = "uk_processed_topic_partition_offset",
                        columnNames = {"topic", "partition", "offset"})
        }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false)
    private int partition;

    @Column(name = "offset_value", nullable = false)
    private long offset;

    @Column(nullable = false)
    private Instant processedAt;
}
