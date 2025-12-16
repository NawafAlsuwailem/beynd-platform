package com.beynd.platform.context;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public class BeyndContextFilter extends OncePerRequestFilter {

    private final String source;

    public BeyndContextFilter(Environment environment) {
        this.source = environment.getProperty(
                "spring.application.name",
                "unknown-service"
        );
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {

        String correlationId = Optional
                .ofNullable(request.getHeader(BeyndHeaders.CORRELATION_ID))
                .orElse(UUID.randomUUID().toString());

        BeyndContext context = new BeyndContext(
                correlationId,
                request.getHeader(BeyndHeaders.USER_ID),
                request.getHeader(BeyndHeaders.CHANNEL),
                source,
                extractAccessToken(request),
                Instant.now()
        );

        BeyndContextHolder.set(context);

        MDC.put("correlationId", correlationId);

        try {
            response.setHeader(BeyndHeaders.CORRELATION_ID, correlationId);
            filterChain.doFilter(request, response);
        } finally {
            BeyndContextHolder.clear();
            MDC.clear();
        }
    }

    private String extractAccessToken(HttpServletRequest request) {
        String authorization = request.getHeader("Authorization");

        if (authorization == null || authorization.isBlank()) {
            return null;
        }

        if (authorization.startsWith("Bearer ")) {
            return authorization.substring(7);
        }

        return authorization;
    }
}

