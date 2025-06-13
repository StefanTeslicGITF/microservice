package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.globalitfactory.api.v1.service.rabbitMQ.QueuePayload;
import com.rabbitmq.client.*;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@Slf4j
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
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
        public String queue = "test-queue";
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
    }

    @SuppressWarnings("unchecked")
    private static AppConfig parseConfig(Map<String, Object> yamlMap) {
        AppConfig appConfig = new AppConfig();

        Map<String, Object> rabbitmqMap = (Map<String, Object>) yamlMap.get("rabbitmq");
        if (rabbitmqMap != null) {
            appConfig.rabbitmq.host = (String) rabbitmqMap.getOrDefault("host", "localhost");
            appConfig.rabbitmq.port = (Integer) rabbitmqMap.getOrDefault("port", 5672);
            appConfig.rabbitmq.queue = (String) rabbitmqMap.getOrDefault("queue", "test-queue");
            appConfig.rabbitmq.username = (String) rabbitmqMap.getOrDefault("username", "guest");
            appConfig.rabbitmq.password = (String) rabbitmqMap.getOrDefault("password", "guest");
        }

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

        Map<String, Object> datasetMap = (Map<String, Object>) yamlMap.get("dataset");
        if (datasetMap != null) {
            appConfig.dataset.chunkSize = (Integer) datasetMap.getOrDefault("chunk-size", 1000);
            appConfig.dataset.maxMemoryMb = (Integer) datasetMap.getOrDefault("max-memory-mb", 512);
        }

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

        channel.queueDeclare(config.rabbitmq.queue, false, false, false, null);
        channel.basicQos(config.processing.concurrency);

        Flux.<DatasetMessage>create(sink -> {
            try {
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    try {
                        byte[] body = delivery.getBody();
                        QueuePayload dto = null;
                        try (ByteArrayInputStream bais = new ByteArrayInputStream(body);
                             ObjectInputStream ois = new ObjectInputStream(bais)) {

                            dto = (QueuePayload) ois.readObject();
                        } catch (IOException | ClassNotFoundException e) {
                            log.info("error ");
                        }

                        DatasetMessage message = new DatasetMessage(
                                delivery.getEnvelope().getDeliveryTag(),
                                dto,
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
        }).subscribeOn(Schedulers.boundedElastic())
                .subscribe(msg -> {
                    try {
                        log.info("Processing message: {}", msg.getBody());


                        msg.getChannel().basicAck(msg.getDeliveryTag(), false);
                    } catch (Exception e) {
                        log.error("Error processing message", e);
                        try {
                            msg.getChannel().basicNack(msg.getDeliveryTag(), false, true); // requeue = true
                        } catch (IOException ioException) {
                            log.error("Failed to nack message", ioException);
                        }
                    }
                });;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down processing service...");
            try {
                channel.close();
                connection.close();
            } catch (Exception e) {
                System.err.println("Error during shutdown: " + e.getMessage());
            }
        }));

        Thread.currentThread().join();
    }
}
class DatasetMessage {
    private final long deliveryTag;
    private final QueuePayload body;
    private final Channel channel;

    public DatasetMessage(long deliveryTag, QueuePayload body, Channel channel) {
        this.deliveryTag = deliveryTag;
        this.body = body;
        this.channel = channel;
    }

    public long getDeliveryTag() { return deliveryTag; }
    public QueuePayload getBody() { return body; }
    public Channel getChannel() { return channel; }
}
