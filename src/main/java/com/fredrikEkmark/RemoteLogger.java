package com.fredrikEkmark;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class RemoteLogger {

    private static String defaultServiceName = "default-service";
    private static BlockingQueue<Event> queue;
    private static ScheduledExecutorService memoryScheduler;
    private static final ThreadLocal<RequestContext> CONTEXT = new ThreadLocal<>();
    private static Thread workerThread;
    private static HttpClient httpClient;
    private static String apiEndpoint;
    private static volatile boolean running = false;
    private static volatile boolean initialized = false;

    private static final String LOG_ENDPOINT = "/v1/logs";
    private static final String MEMORY_ENDPOINT = "/v1/metrics/memory";
    private static final String USAGE_ENDPOINT = "/v1/metrics/usage";

    private static class RequestContext {
        final String traceId;
        final long startTimeNanos;

        RequestContext(String traceId) {
            this.traceId = traceId;
            this.startTimeNanos = System.nanoTime();
        }
    }

    public sealed interface Event permits LogEvent, MemoryStatusEvent, UsageEvent {
        String toJson();
        String getEndpointSuffix();
    }

    public record LogEvent(String serviceName, String traceId, String level, String message, Instant timestamp) implements Event {
        @Override
        public String toJson() {
            return "{\"serviceName\":\"" + escapeJson(serviceName) +
                    "\",\"traceId\":\"" + escapeJson(traceId != null ? traceId : "") +
                    "\",\"level\":\"" + escapeJson(level) +
                    "\",\"message\":\"" + escapeJson(message) +
                    "\",\"timestamp\":\"" + timestamp + "\"}";
        }

        @Override
        public String getEndpointSuffix() {
            return LOG_ENDPOINT;
        }
    }

    public record UsageEvent(String serviceName, String traceId, long latencyMs, int statusCode, Instant timestamp) implements Event {
        @Override
        public String toJson() {
            return "{\"serviceName\":\"" + escapeJson(serviceName) +
                    "\",\"traceId\":\"" + escapeJson(traceId != null ? traceId : "") +
                    "\",\"latencyMs\":" + latencyMs +
                    ",\"statusCode\":" + statusCode +
                    ",\"timestamp\":\"" + timestamp + "\"}";
        }

        @Override
        public String getEndpointSuffix() {
            return USAGE_ENDPOINT;
        }
    }

    public record MemoryStatusEvent(String serviceName, long totalMemory, long freeMemory, long usedMemory, long maxMemory, Instant timestamp) implements Event {
        @Override
        public String toJson() {
            return "{\"serviceName\":\"" + escapeJson(serviceName) +
                    "\",\"totalMemory\":" + totalMemory +
                    ",\"freeMemory\":" + freeMemory +
                    ",\"usedMemory\":" + usedMemory +
                    ",\"maxMemory\":" + maxMemory +
                    ",\"timestamp\":\"" + timestamp + "\"}";
        }

        @Override
        public String getEndpointSuffix() {
            return MEMORY_ENDPOINT;
        }
    }

    public static synchronized void init(String serviceName, String endpointUrl) {
        init(serviceName, endpointUrl, 2000, 60);
    }

    public static synchronized void init(String serviceName, String endpointUrl, int queueCapacity) {
        init(serviceName, endpointUrl, queueCapacity, 60);
    }

    public static synchronized void init(String serviceName, String endpointUrl, int queueCapacity, int memoryIntervalSeconds) {
        if (initialized) {
            System.err.println("[Logger] Warning: Logger is already initialized.");
            return;
        }

        defaultServiceName = serviceName;
        apiEndpoint = endpointUrl;
        queue = new ArrayBlockingQueue<>(queueCapacity);
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();

        running = true;
        workerThread = new Thread(RemoteLogger::processQueue, "static-logger-worker");
        workerThread.setDaemon(true);
        workerThread.start();

        if (memoryIntervalSeconds > 0) {
            memoryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "logger-memory-scheduler");
                t.setDaemon(true);
                return t;
            });

            memoryScheduler.scheduleAtFixedRate(
                    RemoteLogger::logMemoryStatus,
                    memoryIntervalSeconds,
                    memoryIntervalSeconds,
                    TimeUnit.SECONDS
            );
        }

        Runtime.getRuntime().addShutdownHook(new Thread(RemoteLogger::flushAndStop));
        initialized = true;
    }

    // --- Core Logging API ---

    public static void startUsage() {
        startUsage(UUID.randomUUID().toString());
    }

    public static void startUsage(String traceId) {
        if (!initialized) return;
        CONTEXT.set(new RequestContext(traceId));
    }

    public static void endUsage(int statusCode) {
        if (!initialized) return;

        RequestContext ctx = CONTEXT.get();
        if (ctx == null) {
            return;
        }

        try {
            long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ctx.startTimeNanos);

            UsageEvent event = new UsageEvent(
                    defaultServiceName,
                    ctx.traceId,
                    latencyMs,
                    statusCode,
                    Instant.now()
            );

            if (!queue.offer(event)) {
                System.err.println("[Logger] Queue full. Dropped usage event for traceId: " + ctx.traceId);
            }
        } finally {
            CONTEXT.remove();
        }
    }

    public static void logMemoryStatus() {
        if (!initialized) return;

        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();

        MemoryStatusEvent event = new MemoryStatusEvent(
                defaultServiceName,
                totalMemory,
                freeMemory,
                totalMemory - freeMemory,
                runtime.maxMemory(),
                Instant.now()
        );

        if (!queue.offer(event)) {
            System.err.println("[Logger] Queue full. Dropped memory status log.");
        }
    }

    public static void info(String message) {
        log("INFO", message);
    }

    public static void error(String message) {
        log("ERROR", message);
    }

    public static void warn(String message) {
        log("WARN", message);
    }

    public static void debug(String message) {
        log("DEBUG", message);
    }

    protected static void log(String level, String message) {
        if (!initialized) return;

        RequestContext ctx = CONTEXT.get();
        String traceId = (ctx != null) ? ctx.traceId : null;

        LogEvent event = new LogEvent(defaultServiceName, traceId, level, message, Instant.now());

        if (!queue.offer(event)) {
            System.err.println("[Logger] Queue full. Dropped [" + level + "]: " + message);
        }
    }

    // --- Dispatcher Worker ---

    private static void processQueue() {
        List<Event> batch = new ArrayList<>(50);

        while (running || !queue.isEmpty()) {
            try {
                Event event = queue.poll(500, TimeUnit.MILLISECONDS);
                if (event != null) {
                    batch.add(event);
                    queue.drainTo(batch, 49);

                    sendBatchGrouped(batch);
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[Logger] Worker error: " + e.getMessage());
            }
        }
    }

    private static void sendBatchGrouped(List<Event> batch) {
        if (batch.isEmpty()) return;

        Map<String, List<Event>> groupedEvents = new HashMap<>();
        for (int i = 0; i < batch.size(); i++) {
            Event event = batch.get(i);
            groupedEvents
                    .computeIfAbsent(event.getEndpointSuffix(), k -> new ArrayList<>())
                    .add(event);
        }

        for (Map.Entry<String, List<Event>> entry : groupedEvents.entrySet()) {
            String endpointSuffix = entry.getKey();
            List<Event> eventsForEndpoint = entry.getValue();

            StringBuilder json = new StringBuilder(eventsForEndpoint.size() * 128);
            json.append("[");
            for (int i = 0; i < eventsForEndpoint.size(); i++) {
                json.append(eventsForEndpoint.get(i).toJson());
                if (i < eventsForEndpoint.size() - 1) {
                    json.append(",");
                }
            }
            json.append("]");

            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(apiEndpoint + endpointSuffix))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json.toString()))
                        .timeout(Duration.ofSeconds(5))
                        .build();

                httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());
            } catch (Exception e) {
                System.err.println("[Logger] HTTP send failed for " + endpointSuffix + ": " + e.getMessage());
            }
        }
    }

    private static String escapeJson(String raw) {
        if (raw == null || raw.isEmpty()) return "";

        // Fast path for clean strings (avoids allocating new strings if no special chars)
        boolean needsEscaping = false;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == '\\' || c == '"' || c == '\n' || c == '\r' || c == '\t') {
                needsEscaping = true;
                break;
            }
        }
        if (!needsEscaping) return raw;

        StringBuilder sb = new StringBuilder(raw.length() + 8);
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"'  -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> sb.append(c);
            }
        }
        return sb.toString();
    }

    private static void flushAndStop() {
        running = false;

        if (memoryScheduler != null) {
            memoryScheduler.shutdown();
            try {
                if (!memoryScheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    memoryScheduler.shutdownNow();
                }
            } catch (InterruptedException ignored) {}
        }

        if (workerThread != null) {
            try {
                workerThread.join(2000);
            } catch (InterruptedException ignored) {}
        }
    }
}