package com.beynd.platform.messaging.startup;
import com.beynd.platform.messaging.startup.validation.BeyndKafkaStartupValidator;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public record StartupListener(BeyndKafkaStartupValidator validator) {

    @PostConstruct
    public void onStartup() {
        validator.validate();
    }
}
