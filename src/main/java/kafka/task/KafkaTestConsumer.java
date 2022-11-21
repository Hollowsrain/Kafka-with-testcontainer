package kafka.task;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class KafkaTestConsumer implements Runnable {

    private Consumer<String, String> consumer;
    private Long pollDuration;
    private List<String> messagesHistory;
    private boolean closed;

    public KafkaTestConsumer(Consumer<String, String> consumer, Long pollDuration) {
        this.consumer = consumer;
        this.pollDuration = pollDuration;
        messagesHistory = new ArrayList<>();
    }

    @Override
    public void run() {
        while (!closed) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollDuration));
            records.forEach(record -> {
                messagesHistory.add(record.value());
            });
        }
    }

    public List<String> getMessageHistory() {
        return messagesHistory;
    }

    public void close() {
        closed = true;
    }
}
