package com.beynd.platform.messaging.publisher;

import com.beynd.platform.messaging.outbox.OutboxEvent;
import com.beynd.platform.messaging.outbox.OutboxEventRepository;
import com.beynd.platform.messaging.serializer.EventPayloadSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static com.beynd.platform.messaging.outbox.EventDispatchStatus.PENDING;

@Slf4j
@RequiredArgsConstructor
public class DefaultEventPublisher implements EventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final EventPayloadSerializer serializer;

    @Override
    @Transactional
    public void publish(String partitionKey, String topic, Object event) {
        if (event == null) {
            throw new IllegalArgumentException("[BEYND KAFKA] Event payload must not be null");
        }

        byte[] payload = serializer.serialize(topic, event);

        String eventType = event.getClass().getSimpleName();

        OutboxEvent outbox = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType(null)
                .aggregateId(null)
                .eventType(eventType)
                .payload(payload)
                .headers(null)
                .topic(topic)
                .partitionKey(partitionKey)
                .status(PENDING)
                .attemptCount(0)
                .build();

        outboxEventRepository.save(outbox);

        log.debug("[BEYND KAFKA] Outbox event saved: id={}, topic={}, type={}",
                outbox.getId(), topic, eventType);
    }
}
