package com.beynd.platform.messaging.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

import static com.beynd.platform.messaging.outbox.EventDispatchStatus.*;

@Slf4j
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final int batchSize;
    private final int maxAttempts;

    @Transactional
    @Scheduled(fixedDelayString = "${beynd.kafka.outbox.poll-interval-ms:500}")
    public void publishPendingEvents() {
        List<OutboxEvent> events = outboxEventRepository.lockBatchForProcessing(PENDING.name(), maxAttempts, batchSize);

        if (events.isEmpty()) {
            return;
        }

        log.debug("[BEYND KAFKA] OutboxRelay: processing {} events", events.size());

        for (OutboxEvent event : events) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getPartitionKey(), event.getPayload());

                event.setStatus(SENT);
                event.setProcessedAt(Instant.now());
                event.setAttemptCount(event.getAttemptCount() + 1);

            } catch (Exception ex) {
                log.error("[BEYND KAFKA] OutboxRelay: failed to publish event {}: {}",
                        event.getId(), ex.getMessage(), ex);

                event.setStatus(FAILED);
                event.setLastError(ex.getMessage());
                event.setAttemptCount(event.getAttemptCount() + 1);
            }
        }
    }
}
