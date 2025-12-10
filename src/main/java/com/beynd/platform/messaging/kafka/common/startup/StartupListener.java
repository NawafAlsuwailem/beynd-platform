package com.beynd.platform.messaging.kafka.common.startup;
import com.beynd.platform.messaging.kafka.common.startup.validation.KafkaStartupValidator;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public record StartupListener(KafkaStartupValidator validator) {

    @PostConstruct
    public void onStartup() {
        validator.validate();
    }
}
