package it.uniroma2.query1;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class FlinkKafkaSerializer implements KafkaSerializationSchema<String> {

    private String topic;

    public FlinkKafkaSerializer( String topic ){
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
        return new ProducerRecord<>(topic, s.getBytes());
    }
}
