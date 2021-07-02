package it.uniroma2.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaProducer {
    private final Producer<Long, String> producer;

    public MyKafkaProducer() {
        producer = createProducer();
    }

    private static Producer<Long, String> createProducer() {
        // get the producer properties
        Properties props = KafkaHandler.getProperties("injector");
        return new KafkaProducer<>(props);
    }

    public void produce(Long key, String value, Long timestamp) {
        final ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaHandler.TOPIC_SOURCE, null,
                value);
        System.out.println(record.value());
        //send the records
        producer.send(record);
    }

    public void close() {
        producer.flush();
        producer.close();
    }
}