package it.uniroma2.main;

import it.uniroma2.utils.KafkaHandler;
import org.apache.kafka.clients.consumer.*;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

public class ConsumerTopic{

    private static Logger log;
    private static String topicQuery;
    private static Properties prop;
    private static final String[] timeIntervalTypes = {"week","month"};

    public static void main( String[] args ){

        if( args.length == 0 ){
            log.info("No arguments");
            System.exit(1);
        }else if( args.length > 0 ){
            topicQuery = args[0];
        }


        // scelgo il topic da args
        if( topicQuery.equals("1") )topicQuery = KafkaHandler.TOPIC_QUERY1;
        else if( topicQuery.equals("2") )topicQuery = KafkaHandler.TOPIC_QUERY2;
        else if( topicQuery.equals("3") )topicQuery = KafkaHandler.TOPIC_QUERY3;


        // creo il consumer usando le prop
        prop = KafkaHandler.getProperties("csv_output");
        Consumer<Long, String> consumerWeek = new KafkaConsumer<>(prop);
        Consumer<Long, String> consumerMonth = new KafkaConsumer<>(prop);

        // sottoscrivo il consumer al topic
        consumerWeek.subscribe(Collections.singletonList(topicQuery + "week"));
        consumerMonth.subscribe(Collections.singletonList(topicQuery + "month"));
        // creo il csv writer per il risultato su Results
        CsvWriter writerWeek = new CsvWriter( topicQuery + "week" );
        CsvWriter writerMonth = new CsvWriter( topicQuery + "month" );

        while (true) {
            final ConsumerRecords<Long, String> consumerRecordsWeek = consumerWeek.poll(Duration.ofMillis(1000));
            final ConsumerRecords<Long, String> consumerRecordsMonth = consumerMonth.poll(Duration.ofMillis(1000));

            if (consumerRecordsWeek.count()==0) {
                System.out.println("There are no records in week topic");
            }else {
                consumerRecordsWeek.forEach(key -> {
                    System.out.println(key.toString());
                    writerWeek.appendRow( key.toString() );
                });
            }

            if (consumerRecordsMonth.count()==0) {
                System.out.println("There are no records in month topic");
            }else {
                consumerRecordsMonth.forEach(key -> {
                    System.out.println(key.toString());
                    writerMonth.appendRow( key.toString() );
                });
            }



            consumerWeek.commitAsync();
            consumerMonth.commitAsync();
        }

    }




}
