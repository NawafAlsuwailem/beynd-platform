package com.beynd.platform.jpa;

import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@AutoConfigureBefore(HibernateJpaAutoConfiguration.class)
@EnableJpaRepositories(
        basePackageClasses = com.beynd.platform.messaging.outbox.OutboxEventRepository.class,
        considerNestedRepositories = true
)
@EntityScan(
        basePackageClasses = com.beynd.platform.messaging.outbox.OutboxEvent.class
)
public class PlatformJpaConfig {}
