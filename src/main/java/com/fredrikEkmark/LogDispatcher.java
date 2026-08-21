package com.fredrikEkmark;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class LogDispatcher implements Runnable {

    private final BlockingQueue<LogEvent> queue;
    private final String apiEndpoint;
    private final HttpClient httpClient;
    private final Thread workerThread;
    private volatile boolean running = true;

    public LogDispatcher(BlockingQueue<LogEvent> queue, String apiEndpoint) {
        this.queue = queue;
        this.apiEndpoint = apiEndpoint;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        this.workerThread = new Thread(this, "async-logger-worker");
        this.workerThread.setDaemon(true); // Don't prevent JVM shutdown
    }

    public void start() {
        workerThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(this::flushAndStop));
    }

    @Override
    public void run() {
        List<LogEvent> batch = new ArrayList<>();
        while (running || !queue.isEmpty()) {
            try {
                // Poll with timeout so worker loops and checks 'running' flag
                LogEvent event = queue.poll(500, TimeUnit.MILLISECONDS);
                if (event != null) {
                    batch.add(event);
                    queue.drainTo(batch, 49); // Batch up to 50 logs at once
                    sendBatch(batch);
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[AsyncLogger] Error dispatching logs: " + e.getMessage());
            }
        }
    }

    private void sendBatch(List<LogEvent> batch) {
        if (batch.isEmpty()) return;

        // Manual JSON construction keeps consumer POM zero-dependency
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
            System.err.println("[AsyncLogger] HTTP send failed: " + e.getMessage());
        }
    }

    private String escapeJson(String raw) {
        return raw.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    public void stop() {
        this.running = false;
    }

    private void flushAndStop() {
        this.running = false;
        try {
            workerThread.join(2000); // Give 2 seconds to flush remaining queue
        } catch (InterruptedException ignored) {}
    }
}
