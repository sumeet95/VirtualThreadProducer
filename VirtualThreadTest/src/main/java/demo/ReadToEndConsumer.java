package demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ReadToEndConsumer {

    public static void main(String[] args) throws InterruptedException {
        String bootstrap = envOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = envOrDefault("KAFKA_TOPIC", "test-topic");
        String groupId = envOrDefault("KAFKA_GROUP", "read-to-end-consumer-virtual-thread");

        System.out.println("KAFKA_BOOTSTRAP = " + bootstrap);

        // Must stay caught up for this long before we declare "done"
        Duration idleWindow = Duration.ofSeconds(Long.parseLong(envOrDefault("IDLE_SECONDS", "3")));

        // In-flight cap (backpressure)
        int maxInFlight = Integer.parseInt(envOrDefault("MAX_IN_FLIGHT", "1200"));
        Semaphore inFlight = new Semaphore(maxInFlight);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        Instant startedAt = Instant.now();
        AtomicLong totalRecords = new AtomicLong(0);

        // Virtual threads: one virtual thread per task
        ExecutorService workerPool = Executors.newVirtualThreadPerTaskExecutor();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) { }
                @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("Assigned partitions: " + partitions);
                }
            });

            // Wait until partitions are assigned
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(200));
            }

            Set<TopicPartition> parts = consumer.assignment();

            // Snapshot the "end" offsets at start time (this defines “finish”)
            Map<TopicPartition, Long> targetEndOffsets = consumer.endOffsets(parts);
            System.out.println("Target end offsets (snapshot): " + targetEndOffsets);

            Instant caughtUpSince = null;

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));

                if (!records.isEmpty()) {
                    int batchCount = records.count();
                    totalRecords.addAndGet(batchCount);

                    List<Future<?>> futures = new ArrayList<>(batchCount);

                    for (ConsumerRecord<String, String> r : records) {
                        // Backpressure: block here if too many tasks are already running
                        inFlight.acquire();

                        futures.add(workerPool.submit(() -> {
                            try {
                                doWork(r);
                            } finally {
                                inFlight.release();
                            }
                        }));
                    }

                    // Wait for processing to finish for this batch
                    for (Future<?> f : futures) {
                        try {
                            f.get();
                        } catch (ExecutionException ee) {
                            throw new RuntimeException("Worker failed", ee.getCause());
                        }
                    }

                    // Commit after processing completes
                    consumer.commitAsync();
                }

                boolean caughtUpNow = isCaughtUp(consumer, targetEndOffsets);

                if (caughtUpNow) {
                    if (caughtUpSince == null) {
                        caughtUpSince = Instant.now();
                    }

                    if (Duration.between(caughtUpSince, Instant.now()).compareTo(idleWindow) >= 0) {
                        Instant finishedAt = Instant.now();
                        Duration elapsed = Duration.between(startedAt, finishedAt);

                        System.out.println("✅ Reached end (caught up to snapshot) at: " + finishedAt);
                        System.out.println("   Started at:  " + startedAt);
                        System.out.println("   Finished at: " + finishedAt);
                        System.out.println("   Elapsed:     " + formatDuration(elapsed));
                        System.out.println("   Total records read: " + totalRecords.get());
                        System.out.println("   Final positions: " + currentPositions(consumer, parts));
                        break;
                    }
                } else {
                    caughtUpSince = null;
                }
            }

            // Ensure commits are flushed before closing
            consumer.commitSync();

        } catch (WakeupException e) {
            // ignore on shutdown
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            workerPool.shutdown();
            try {
                workerPool.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        Thread.sleep(Integer.MAX_VALUE);
    }

    private static void doWork(ConsumerRecord<String, String> r) {
        // Replace with real work. Keep identical between platform vs virtual tests.
        try { Thread.sleep(20); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        int h = Objects.hash(r.key(), r.value());
        try { Thread.sleep(1000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static boolean isCaughtUp(KafkaConsumer<?, ?> consumer, Map<TopicPartition, Long> targetEndOffsets) {
        for (Map.Entry<TopicPartition, Long> e : targetEndOffsets.entrySet()) {
            TopicPartition tp = e.getKey();
            long end = e.getValue();            // next offset that would be written
            long pos = consumer.position(tp);   // next offset consumer will read
            if (pos < end) return false;
        }
        return true;
    }

    private static Map<TopicPartition, Long> currentPositions(KafkaConsumer<?, ?> consumer, Set<TopicPartition> parts) {
        Map<TopicPartition, Long> m = new LinkedHashMap<>();
        for (TopicPartition tp : parts) {
            m.put(tp, consumer.position(tp));
        }
        return m;
    }

    private static String formatDuration(Duration d) {
        long ms = d.toMillis();
        long minutes = ms / 60_000;
        long seconds = (ms % 60_000) / 1000;
        long millis = ms % 1000;
        if (minutes > 0) return String.format("%dm %ds %dms", minutes, seconds, millis);
        if (seconds > 0) return String.format("%ds %dms", seconds, millis);
        return String.format("%dms", millis);
    }

    private static String envOrDefault(String name, String def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : v;
    }
}
