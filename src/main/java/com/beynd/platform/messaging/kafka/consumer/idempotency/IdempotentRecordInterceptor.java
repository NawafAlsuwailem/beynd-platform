package com.beynd.platform.messaging.kafka.consumer.idempotency;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@RequiredArgsConstructor
public class IdempotentRecordInterceptor
        implements RecordInterceptor<String, Object> {

    private final ProcessedEventRepository processedEventRepository;

    @Override
    public ConsumerRecord<String, Object> intercept(
            ConsumerRecord<String, Object> record,
            Consumer<String, Object> consumer) {

        log.info("[Idempotency] Intercept topic={} partition={} offset={}", record.topic(), record.partition(), record.offset());

        if (processedEventRepository.existsByTopicAndPartitionAndOffset(
                record.topic(),
                record.partition(),
                record.offset())) {

            log.info("[Idempotency] Skipped duplicate topic={} partition={} offset={}", record.topic(), record.partition(), record.offset());
            return null;
        }

        return record;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void success(
            ConsumerRecord<String, Object> record,
            Consumer<String, Object> consumer) {

        processedEventRepository.save(
                ProcessedEvent.builder()
                        .topic(record.topic())
                        .partition(record.partition())
                        .offset(record.offset())
                        .processedAt(Instant.now())
                        .build()
        );

        log.info("[Idempotency] Persisted topic={} partition={} offset={}", record.topic(), record.partition(), record.offset());
    }
}
