package kafka.task;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaTestProducer {

    private Producer<String, String> producer;
    private String topic;

    public KafkaTestProducer(Producer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public void sendMessage(String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);
        producer.send(producerRecord);
    }
}
