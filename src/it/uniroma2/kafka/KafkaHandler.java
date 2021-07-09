package it.uniroma2.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaHandler {

    // topics
    public static final String TOPIC_SOURCE = "flink-topic";
    public static final String TOPIC_QUERY1_WEEKLY = "query1weekly";
    public static final String TOPIC_QUERY1_MONTHLY = "query1monthly";
    public static final String TOPIC_QUERY2_WEEKLY = "query2weekly";
    public static final String TOPIC_QUERY2_MONTHLY = "query2monthly";
    public static final String TOPIC_QUERY3_ONEHOUR= "query3oneHour";
    public static final String TOPIC_QUERY3_TWOHOUR = "query3twoHour";

    // brokers
    public static final String KAFKA_BROKER_1 = "localhost:9092";
    public static final String KAFKA_BROKER_2 = "localhost:9093";
    public static final String KAFKA_BROKER_3 = "localhost:9094";

    // bootstrap servers
    public static final String BOOTSTRAP_SERVERS = KAFKA_BROKER_1 + "," + KAFKA_BROKER_2 + "," + KAFKA_BROKER_3;

    // insieme dei topic per iterare su di essi, per il consumo
    public static final String[] FLINK_TOPICS = {TOPIC_QUERY1_WEEKLY, TOPIC_QUERY1_MONTHLY,
            TOPIC_QUERY2_WEEKLY, TOPIC_QUERY2_MONTHLY, TOPIC_QUERY3_ONEHOUR,
            TOPIC_QUERY3_TWOHOUR};

    // Results
    public static final String QUERY1_WEEKLY_CSV = "Results/query1weekly.csv";
    public static final String QUERY1_MONTHLY_CSV = "Results/query1monthly.csv";
    public static final String QUERY2_WEEKLY_CSV = "Results/query2weekly.csv";
    public static final String QUERY2_MONTHLY_CSV = "Results/query2monthly.csv";
    public static final String QUERY3_ONEHOUR_CSV = "Results/query3oneHour.csv";
    public static final String QUERY3_TWOHOUR_CSV = "Results/query3twoHour.csv";

    // insieme dei file csv
    public static final String[] FLINK_OUTPUT_FILES = {QUERY1_WEEKLY_CSV, QUERY1_MONTHLY_CSV,
            QUERY2_WEEKLY_CSV, QUERY2_MONTHLY_CSV, QUERY3_ONEHOUR_CSV,
            QUERY3_TWOHOUR_CSV};

    public static Properties getProperties( String propCase ){
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // setup brokers nelle proprieta'

        if( propCase.equals("injector") ) {
            prop.put( ProducerConfig.CLIENT_ID_CONFIG, "producer-flink" ); // producer group id
            prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName()); //serializzazione key value
            prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        }else if( propCase.equals("consumer") ) {
            prop.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-flink"); // consumer group
            prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // per iniziare a leggere dall'inizio della partizione ( se no offset )
            prop.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"); //exactly once semantic
            prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName()); //deserializzazione key value
            prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }else if( propCase.equals("producer") ) {
            prop.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
            prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); //semantica exactly once
        }else if( propCase.equals("csv_output") ) {
            prop.put(ConsumerConfig.GROUP_ID_CONFIG, "csv-consumer");
            prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // deserializzazione key value
            prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
            prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }

        return prop;
    }

}
