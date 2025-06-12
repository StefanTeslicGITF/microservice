package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.rabbitmq.client.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class Main {
    private static AppConfig config;

    public static void main(String[] args) throws Exception {
        loadConfiguration();
        startProcessingService();
    }

    // Configuration classes
    private static class AppConfig {
        public RabbitMQConfig rabbitmq = new RabbitMQConfig();
        public ProcessingConfig processing = new ProcessingConfig();
        public DatasetConfig dataset = new DatasetConfig();
        public String aa = "default";
    }

    private static class RabbitMQConfig {
        public String host = "localhost";
        public int port = 5672;
        public String username = "guest";
        public String password = "guest";
        public String queue = "dataset-processing";
    }

    private static class ProcessingConfig {
        public int concurrency = 10;
        public int batchSize = 100;
        public int timeoutSeconds = 300;
        public RetryConfig retry = new RetryConfig();
    }

    private static class RetryConfig {
        public int attempts = 3;
        public int backoffSeconds = 1;
        public int maxBackoffSeconds = 10;
    }

    private static class DatasetConfig {
        public int chunkSize = 1000;
        public int maxMemoryMb = 512;
    }


    private static void loadConfiguration() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        InputStream yamlFile = Main.class.getResourceAsStream("/application.yml");

        if (yamlFile == null) {
            throw new IOException("application.yml not found in resources");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> yamlMap = mapper.readValue(yamlFile, Map.class);
        config = parseConfig(yamlMap);

        System.out.println("Configuration loaded - Queue: " + config.rabbitmq.queue +
                ", Concurrency: " + config.processing.concurrency);
        System.out.println("Test value 'aa': " + config.aa);
    }

    @SuppressWarnings("unchecked")
    private static AppConfig parseConfig(Map<String, Object> yamlMap) {
        AppConfig appConfig = new AppConfig();

        // Parse RabbitMQ config
        Map<String, Object> rabbitmqMap = (Map<String, Object>) yamlMap.get("rabbitmq");
        if (rabbitmqMap != null) {
            appConfig.rabbitmq.host = (String) rabbitmqMap.getOrDefault("host", "localhost");
            appConfig.rabbitmq.port = (Integer) rabbitmqMap.getOrDefault("port", 5672);
            appConfig.rabbitmq.queue = (String) rabbitmqMap.getOrDefault("queue", "dataset-processing");
            appConfig.rabbitmq.username = (String) rabbitmqMap.getOrDefault("username", "guest");
            appConfig.rabbitmq.password = (String) rabbitmqMap.getOrDefault("password", "guest");
        }

        // Parse Processing config
        Map<String, Object> processingMap = (Map<String, Object>) yamlMap.get("processing");
        if (processingMap != null) {
            appConfig.processing.concurrency = (Integer) processingMap.getOrDefault("concurrency", 10);
            appConfig.processing.batchSize = (Integer) processingMap.getOrDefault("batch-size", 100);
            appConfig.processing.timeoutSeconds = (Integer) processingMap.getOrDefault("timeout-seconds", 300);

            Map<String, Object> retryMap = (Map<String, Object>) processingMap.get("retry");
            if (retryMap != null) {
                appConfig.processing.retry.attempts = (Integer) retryMap.getOrDefault("attempts", 3);
                appConfig.processing.retry.backoffSeconds = (Integer) retryMap.getOrDefault("backoff-seconds", 1);
                appConfig.processing.retry.maxBackoffSeconds = (Integer) retryMap.getOrDefault("max-backoff-seconds", 10);
            }
        }

        // Parse Dataset config
        Map<String, Object> datasetMap = (Map<String, Object>) yamlMap.get("dataset");
        if (datasetMap != null) {
            appConfig.dataset.chunkSize = (Integer) datasetMap.getOrDefault("chunk-size", 1000);
            appConfig.dataset.maxMemoryMb = (Integer) datasetMap.getOrDefault("max-memory-mb", 512);
        }

        // Parse simple properties
        appConfig.aa = (String) yamlMap.getOrDefault("aa", "default");

        return appConfig;
    }

    private static void startProcessingService() throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.rabbitmq.host);
        factory.setPort(config.rabbitmq.port);
        factory.setUsername(config.rabbitmq.username);
        factory.setPassword(config.rabbitmq.password);
        factory.setAutomaticRecoveryEnabled(true);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Declare queue with durability for large dataset processing
        channel.queueDeclare(config.rabbitmq.queue, true, false, false, null);
        channel.basicQos(config.processing.concurrency); // Prefetch count for backpressure

        System.out.println("Starting reactive dataset processing service...");

        // Create reactive stream from RabbitMQ messages
        Flux.<DatasetMessage>create(sink -> {
            try {
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    try {
                        String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
                        DatasetMessage message = new DatasetMessage(
                                delivery.getEnvelope().getDeliveryTag(),
                                messageBody,
                                channel
                        );
                        sink.next(message);
                    } catch (Exception e) {
                        sink.error(e);
                    }
                };

                CancelCallback cancelCallback = consumerTag -> {
                    System.out.println("Consumer cancelled: " + consumerTag);
                };

                channel.basicConsume(config.rabbitmq.queue, false, deliverCallback, cancelCallback);

            } catch (IOException e) {
                sink.error(e);
            }
        });

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down processing service...");
            try {
                channel.close();
                connection.close();
            } catch (Exception e) {
                System.err.println("Error during shutdown: " + e.getMessage());
            }
        }));

        // Keep application running
        Thread.currentThread().join();
    }
}


// Data class for messages
class DatasetMessage {
    private final long deliveryTag;
    private final String body;
    private final Channel channel;

    public DatasetMessage(long deliveryTag, String body, Channel channel) {
        this.deliveryTag = deliveryTag;
        this.body = body;
        this.channel = channel;
    }

    public long getDeliveryTag() { return deliveryTag; }
    public String getBody() { return body; }
    public Channel getChannel() { return channel; }
}
