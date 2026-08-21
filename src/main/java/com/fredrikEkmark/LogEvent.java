package com.fredrikEkmark;

import java.time.Instant;

public record LogEvent(
        String serviceName,
        String level,
        String message,
        String timestamp
) {
    public LogEvent(String serviceName, String level, String message) {
        this(serviceName, level, message, Instant.now().toString());
    }
}
