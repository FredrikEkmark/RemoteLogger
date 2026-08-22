package com.fredrikEkmark;

import java.time.Instant;
import java.util.UUID;

public class Usage {

    private final Instant startTime;
    private final String traceId;
    private String endpoint;

    private int statusCode = 0;
    private boolean ended = false;

    public Usage() {
        this.traceId = UUID.randomUUID().toString();
        this.startTime = Instant.now();
    }

    public void log(String level, String message) {
        if (ended) {
            throw new IllegalStateException("Cannot log after usage has ended.");
        }
        RemoteLogger.log(traceId, level, message);
    }

    public void info(String message) {
        log("INFO", message);
    }

    public void error(String message) {
        log("ERROR", message);
    }

    public void warn(String message) {
        log("WARN", message);
    }

    public void debug(String message) {
        log("DEBUG", message);
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public String getTraceId() {
        return traceId;
    }

    public void end() {
        ended = true;
        RemoteLogger.endUsage(this);
    }
}
