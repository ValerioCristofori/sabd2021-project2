package it.uniroma2.main;

import it.uniroma2.utils.KafkaHandler;
import org.apache.kafka.clients.consumer.*;


import java.time.Duration;
import java.util.Properties;
import java.util.logging.Logger;

public class Writer {

    private static Logger log;
    private static String topicQuery;
    private static Properties prop;

    public static void main( String[] args ){

        if( args.length == 0 ){
            log.info("No arguments");
            System.exit(1);
        }else if( args.length > 0 ){
            topicQuery = args[0];
        }

        prop = KafkaHandler.getProperties("csv_output");
        Consumer<Long, String> consumer = new KafkaConsumer<>(prop);
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.count() == 0) {
                System.out.println("No record");
            } else {
                consumerRecords.forEach(longStringConsumerRecord -> System.out.println(longStringConsumerRecord.toString()));
            }
            consumer.commitAsync();
        }

    }
}
