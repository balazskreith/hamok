package io.github.balazskreith.hamok.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

public class CompletablePromises<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(CompletablePromises.class);

    private final Map<K, CompletableFuture<Optional<V>>> requests = new ConcurrentHashMap<>();
    private final int timeoutInMs;
    private final String context;

    public CompletablePromises(int timeoutInMs) {
        this(timeoutInMs, "No context is given");
    }

    public CompletablePromises(int timeoutInMs, String context) {
        this.timeoutInMs = timeoutInMs;
        this.context = context;
    }

    public CompletableFuture<Optional<V>> create(K key) {
        if (this.requests.containsKey(key)) {
            logger.warn("Attempted to create a promise (context: {}) twice for key {}", this.context, key);
            return CompletableFuture.completedFuture(Optional.empty());
        }
        var request = new CompletableFuture<Optional<V>>();
        this.requests.put(key, request);
        return request;
    }

    public Optional<V> await(K key) {
        var request = this.requests.get(key);
        if (request == null) {
            logger.warn("No request has found for key: {} (context: {})", key, this.context);
            return Optional.empty();
        }
        try {
            if (this.timeoutInMs < 1) {
                return request.get();
            } else {
                return request.get(this.timeoutInMs, TimeUnit.MILLISECONDS);
            }
        } catch (ExecutionException e) {
            logger.warn("Exception occurred while awaiting for request {}, context: {}", key, context);
        } catch (InterruptedException e) {
            logger.warn("Interrupted exception occurred while awaiting for request {}, context: {}", key, context);
        } catch (TimeoutException e) {
            logger.warn("Timeout occurred while awaiting for request {}, context: {}", key, context);
        } finally {
            this.requests.remove(key);
        }
        return Optional.empty();
    }

    public void resolve(K key, V value) {
        var request = this.requests.get(key);
        if (request == null) {
            logger.warn("No request can be resolved, becasue key {} has not been found. Context: {}", key, this.context);
            return;
        }
        request.complete(Optional.of(value));
    }

    public void reject(K key) {
        var request = this.requests.get(key);
        if (request == null) {
            logger.warn("No request can be resolved, becasue key {} has not been found. Context: {}", key, this.context);
            return;
        }
        request.complete(Optional.empty());
    }
}
