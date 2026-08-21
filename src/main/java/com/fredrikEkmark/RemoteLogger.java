package com.fredrikEkmark;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class RemoteLogger {

    private static String defaultServiceName;
    private static BlockingQueue<LogEvent> queue;
    private static Thread workerThread;
    private static HttpClient httpClient;
    private static String apiEndpoint;
    private static volatile boolean running = false;
    private static volatile boolean initialized = false;

    // Record for log items
    public record LogEvent(String serviceName, String level, String message, Instant timestamp) {}

    /**
     * Call this ONCE at application startup (e.g. main method or Spring @EventListener)
     */
    public static synchronized void init(String serviceName, String endpointUrl) {
        init(serviceName, endpointUrl, 5000);
    }

    public static synchronized void init(String serviceName, String endpointUrl, int queueCapacity) {
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

        Runtime.getRuntime().addShutdownHook(new Thread(RemoteLogger::flushAndStop));
        initialized = true;
    }

    // --- Core Logging API ---

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

    public static void log(String level, String message) {
        if (!initialized) {
            System.err.println("[Logger] Uninitialized log drop: [" + level + "] " + message);
            return;
        }

        LogEvent event = new LogEvent(defaultServiceName, level, message, Instant.now());
        if (!queue.offer(event)) {
            System.err.println("[Logger] Queue full. Dropped: " + message);
        }
    }

    // --- Dispatcher Worker ---

    private static void processQueue() {
        List<LogEvent> batch = new ArrayList<>();
        while (running || !queue.isEmpty()) {
            try {
                LogEvent event = queue.poll(500, TimeUnit.MILLISECONDS);
                if (event != null) {
                    batch.add(event);
                    queue.drainTo(batch, 49);
                    sendBatch(batch);
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

    private static void sendBatch(List<LogEvent> batch) {
        if (batch.isEmpty()) return;

        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < batch.size(); i++) {
            LogEvent e = batch.get(i);
            json.append(String.format(
                    "{\"serviceName\":\"%s\",\"level\":\"%s\",\"message\":\"%s\",\"timestamp\":\"%s\"}",
                    escapeJson(e.serviceName()), escapeJson(e.level()), escapeJson(e.message()), e.timestamp()
            ));
            if (i < batch.size() - 1) json.append(",");
        }
        json.append("]");

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiEndpoint))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json.toString()))
                    .timeout(Duration.ofSeconds(5))
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            System.err.println("[Logger] HTTP send failed: " + e.getMessage());
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
        if (workerThread != null) {
            try {
                workerThread.join(2000);
            } catch (InterruptedException ignored) {}
        }
    }
}