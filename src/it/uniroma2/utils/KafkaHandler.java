package it.uniroma2.utils;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
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
        if( propCase.equals("injector") ) {
            prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ADDRESS); //broker
            prop.put( ProducerConfig.CLIENT_ID_CONFIG, "consumer-group-id" ); // consumer group
            prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        }else if( propCase.equals("consumer") ) {

        }else if( propCase.equals("producer") ) {

        }else if( propCase.equals("csv_output") ) {

        }

        return prop;
    }

}
