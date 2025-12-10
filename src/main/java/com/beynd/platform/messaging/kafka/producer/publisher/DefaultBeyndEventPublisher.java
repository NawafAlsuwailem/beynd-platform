package com.beynd.platform.messaging.kafka.producer.publisher;

import com.beynd.platform.messaging.kafka.producer.outbox.OutboxEvent;
import com.beynd.platform.messaging.kafka.producer.outbox.OutboxEventRepository;
import com.beynd.platform.messaging.kafka.common.serializer.EventPayloadSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static com.beynd.platform.messaging.kafka.producer.outbox.EventDispatchStatus.PENDING;

@Slf4j
@RequiredArgsConstructor
public class DefaultBeyndEventPublisher implements BeyndEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final EventPayloadSerializer serializer;

    @Override
    @Transactional
    public void fire(String aggregateType, String aggregateId, String headers, String partitionKey, String topic, Object event) {
        if (event == null) {
            throw new IllegalArgumentException("[BEYND KAFKA] Event payload must not be null");
        }

        byte[] payload = serializer.serialize(topic, event);

        String eventType = event.getClass().getSimpleName();

        OutboxEvent outbox = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .eventType(eventType)
                .payload(payload)
                .headers(headers)
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
