package com.beynd.platform.context;

import java.time.Instant;

public record BeyndContext(
        String correlationId,
        String userId,
        String channel,
        String source,
        String accessToken,
        Instant createdAt
) {
    public static BeyndContext empty() {
        return new BeyndContext(null, null, null, null,null, Instant.now());
    }
}
