package com.fredrikEkmark;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class RemoteLogger {

    private final String serviceName;
    private final BlockingQueue<LogEvent> queue;
    private final LogDispatcher dispatcher;

    public RemoteLogger(String serviceName, String apiEndpoint, int queueCapacity) {
        this.serviceName = serviceName;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.dispatcher = new LogDispatcher(queue, apiEndpoint);
        this.dispatcher.start();
    }

    public void log(String level, String message) {
        LogEvent event = new LogEvent(serviceName, level, message);
        // offer() returns immediately false if full instead of blocking host thread
        boolean accepted = queue.offer(event);
        if (!accepted) {
            System.err.println("[AsyncLogger] Queue full. Dropping log event: " + message);
        }
    }

    public void info(String message) { log("INFO", message); }
    public void error(String message) { log("ERROR", message); }

    public void shutdown() {
        dispatcher.stop();
    }
}
