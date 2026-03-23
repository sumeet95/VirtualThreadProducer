package demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleProducer {

    public static void main(String[] args) {
        // If you run the producer on your host machine: localhost:9092
        // If you run the producer as a container in the same docker-compose network: kafka:29092
        String bootstrapServers = envOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = envOrDefault("KAFKA_TOPIC", "test-topic");
        int messageNum = Integer.parseInt(envOrDefault("MESSAGE_NUM", "10000"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Reliability / safety defaults for a demo
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");

        String key = UUID.randomUUID().toString();
        String value = "test";

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            for(int i = 0; i < messageNum ; i++){
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("❌ Send failed: " + exception.getMessage());
                        exception.printStackTrace();
                    } else {
                        System.out.printf(
                                "✅ Sent to %s [partition=%d offset=%d] key=%s value=%s%n",
                                metadata.topic(), metadata.partition(), metadata.offset(), key, value
                        );
                    }
                });

                // Ensure the send completes before exiting
                producer.flush();

            }
        }

        while(true){

        }

    }

    private static String envOrDefault(String name, String defaultVal) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? defaultVal : v;
    }

    private static String hugeStructuredPayload(int recordCount, int textSizeEach) {
        String repeatedText = "x".repeat(Math.max(1, textSizeEach));

        String records = IntStream.range(0, recordCount)
                .mapToObj(i -> """
            {
              "index": %d,
              "recordId": "%s",
              "createdAt": "%s",
              "tags": ["alpha", "beta", "gamma", "delta", "epsilon"],
              "metrics": {
                "a": %d,
                "b": %d,
                "c": %.3f
              },
              "details": {
                "title": "Record %d",
                "description": "%s"
              }
            }
            """.formatted(
                        i,
                        UUID.randomUUID(),
                        Instant.now(),
                        i * 10,
                        i * 100,
                        i / 3.0,
                        i,
                        repeatedText
                ))
                .collect(Collectors.joining(","));

        return """
    {
      "eventType": "BULK_TEST",
      "eventId": "%s",
      "timestamp": "%s",
      "source": {
        "service": "java-kafka-producer",
        "env": "local"
      },
      "records": [%s]
    }
    """.formatted(UUID.randomUUID(), Instant.now(), records);
    }
}
