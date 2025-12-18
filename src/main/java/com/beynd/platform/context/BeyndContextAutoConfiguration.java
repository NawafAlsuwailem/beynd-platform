package com.beynd.platform.context;

import jakarta.servlet.Filter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;

@AutoConfiguration
@ConditionalOnWebApplication
public class BeyndContextAutoConfiguration {

    @Bean
    public FilterRegistrationBean<Filter> beyndContextFilter(Environment environment) {
        FilterRegistrationBean<Filter> registration =
                new FilterRegistrationBean<>();

        registration.setFilter(new BeyndContextFilter(environment));
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        registration.addUrlPatterns("/*");
        registration.setName("beyndContextFilter");

        return registration;
    }
}
