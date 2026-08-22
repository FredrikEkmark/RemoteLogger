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
            return String.format("{\"serviceName\":\"%s\",\"traceId\":\"%s\",\"level\":\"%s\",\"message\":\"%s\",\"timestamp\":\"%s\"}",
                    escapeJson(serviceName),escapeJson(traceId), escapeJson(level), escapeJson(message), timestamp.toString());
        }

        @Override
        public String getEndpointSuffix() {
            return LOG_ENDPOINT;
        }
    }

    public record UsageEvent(String serviceName, String traceId, String endpoint, long latencyMs, int statusCode, Instant timestamp
    ) implements Event {

        @Override
        public String toJson() {
            return String.format(
                    "{\"serviceName\":\"%s\",\"traceId\":\"%s\",\"endpoint\":\"%s\",\"latencyMs\":%d,\"statusCode\":%d,\"timestamp\":\"%s\"}",
                    escapeJson(serviceName),
                    escapeJson(traceId != null ? traceId : ""),
                    escapeJson(endpoint),
                    latencyMs,
                    statusCode,
                    timestamp.toString()
            );
        }

        @Override
        public String getEndpointSuffix() {
            return USAGE_ENDPOINT;
        }
    }

    public record MemoryStatusEvent(String serviceName, long totalMemory, long freeMemory, long usedMemory, long maxMemory, Instant timestamp) implements Event {
        @Override
        public String toJson() {
            return String.format("{\"serviceName\":\"%s\",\"totalMemory\":\"%s\",\"freeMemory\":\"%s\",\"usedMemory\":\"%s\",\"maxMemory\":\"%s\",\"timestamp\":\"%s\"}",
                    escapeJson(serviceName), totalMemory, freeMemory, usedMemory, maxMemory, timestamp.toString());
        }

        @Override
        public String getEndpointSuffix() {
            return MEMORY_ENDPOINT;
        }
    }

    /**
     * Call this ONCE at application startup (e.g. main method or Spring @EventListener)
     */
    public static synchronized void init(String serviceName, String endpointUrl) {
        init(serviceName, endpointUrl, 5000, 60);
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

        // Start background scheduler if interval > 0
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

    public static void endUsage(String endpoint, int statusCode) {
        if (!initialized) return;

        RequestContext ctx = CONTEXT.get();
        if (ctx == null) {
            System.err.println("[Logger] Warning: endUsage called without startUsage on thread " + Thread.currentThread().getName());
            return;
        }

        try {
            long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ctx.startTimeNanos);

            UsageEvent event = new UsageEvent(
                    defaultServiceName,
                    ctx.traceId,
                    endpoint,
                    latencyMs,
                    statusCode,
                    Instant.now()
            );

            if (!queue.offer(event)) {
                System.err.println("[Logger] Queue full. Dropped usage event.");
            }
        } finally {
            // CRITICAL: Always remove to prevent memory leaks in thread pools!
            CONTEXT.remove();
        }
    }

    public static void logMemoryStatus() {
        if (!initialized) {
            System.err.println("[Logger] Uninitialized. Dropped memory status log.");
            return;
        }

        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();

        MemoryStatusEvent event = new MemoryStatusEvent(
                defaultServiceName,
                totalMemory,
                freeMemory,
                usedMemory,
                maxMemory,
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
        if (!initialized) {
            System.err.println("[Logger] Uninitialized log. Dropped: [" + level + "] " + message);
            return;
        }

        RequestContext ctx = CONTEXT.get();
        String traceId = (ctx != null) ? ctx.traceId : null;

        LogEvent event = new LogEvent(defaultServiceName, traceId, level, message, Instant.now());
        if (!queue.offer(event)) {
            System.err.println("[Logger] Queue full. Dropped: [" + level + "] " + message);
        }
    }

    // --- Dispatcher Worker ---

    private static void processQueue() {

        List<Event> batch = new ArrayList<>();

        while (running || !queue.isEmpty()) {
            try {
                Event event = queue.poll(500, TimeUnit.MILLISECONDS);
                if (event != null) {
                    batch.add(event);
                    queue.drainTo(batch, 49); // Polled up to 50 items per batch

                    // Changed: Pass the polymorphic batch to the grouping dispatcher
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
        for (Event event : batch) {
            groupedEvents
                    .computeIfAbsent(event.getEndpointSuffix(), k -> new ArrayList<>())
                    .add(event);
        }

        for (Map.Entry<String, List<Event>> entry : groupedEvents.entrySet())
        {
            String endpointSuffix = entry.getKey();
            List<Event> eventsForEndpoint = entry.getValue();

            StringBuilder json = new StringBuilder("[");
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
        if (raw == null) return "";
        return raw.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }

    private static void flushAndStop() {
        running = false;

        // Gracefully shutdown the scheduler
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