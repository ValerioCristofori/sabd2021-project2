package it.uniroma2.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaHandler {

    public static final String ADDRESS = "localhost:9092";

    public static final String TOPIC_SOURCE = "source";
    public static final String TOPIC_QUERY1 = "query1";
    public static final String TOPIC_QUERY2 = "query2";
    public static final String TOPIC_QUERY3 = "query3";

    public static Properties getProperties( String propCase ){
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ADDRESS); //broker
        if( propCase.equals("injector") ) {
            prop.put( ProducerConfig.CLIENT_ID_CONFIG, "producer-flink" ); // consumer group
            prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName()); //serializzazione key value
            prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        }else if( propCase.equals("consumer") ) {
            prop.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-flink"); // producer group id
            prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // per iniziare a leggere dall'inizio della partizione ( se no offset )
            prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName()); //deserializzazione key value
            prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }else if( propCase.equals("producer") ) {
            prop.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
            prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); //semantica exactly once
        }else if( propCase.equals("csv_output") ) {
            prop.put(ConsumerConfig.GROUP_ID_CONFIG, "csv-consumer");
            // start reading from beginning of partition if no offset was created
            prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // key and value deserializers
            prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
            prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }

        return prop;
    }

}
