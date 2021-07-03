package it.uniroma2.main;

import it.uniroma2.kafka.KafkaHandler;
import org.apache.kafka.clients.consumer.*;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

public class ConsumerTopic{

    private static Logger log;
    private static String topicQuery;
    private static Properties prop;

    public static void main( String[] args ){

        Logger log = Logger.getLogger(ConsumerTopic.class.getSimpleName());

        log.info("Parsing arguments");

        if( args.length < 1 ){
            log.info("No arguments");
            System.exit(1);
        }else{
            log.info("argument " + args[0]);
            topicQuery = args[0];
        }


        // scelgo il topic da args
        if( topicQuery.equals("1") ){
            log.info("Topic query 1");
            topicQuery = KafkaHandler.TOPIC_QUERY1;
        }
        else if( topicQuery.equals("2") ){
            log.info("Topic query 2");
            topicQuery = KafkaHandler.TOPIC_QUERY2;
        }
        else if( topicQuery.equals("3") ){
            log.info("Topic query 3");
            topicQuery = KafkaHandler.TOPIC_QUERY3;
        }


        // creo il consumer usando le prop
        prop = KafkaHandler.getProperties("csv_output");
        final Consumer<Long, String> consumerWeek = new KafkaConsumer<>(prop);
        //final Consumer<Long, String> consumerMonth = new KafkaConsumer<>(prop);

        // sottoscrivo il consumer al topic
        consumerWeek.subscribe(Collections.singletonList(topicQuery + "weekly"));
        log.info("Consumer subscribed to " + topicQuery + "weekly");
        //consumerMonth.subscribe(Collections.singletonList(topicQuery + "monthly"));
        //log.info("Consumer subscribed to " + topicQuery + "monthly" );
        // creo il csv writer per il risultato su Results
        CsvWriter writerWeek = new CsvWriter( topicQuery + "weekly" );
        //CsvWriter writerMonth = new CsvWriter( topicQuery + "monthly" );

        while (true) {
            final ConsumerRecords<Long, String> consumerRecordsWeek = consumerWeek.poll(Duration.ofMillis(1000));
            //final ConsumerRecords<Long, String> consumerRecordsMonth = consumerMonth.poll(Duration.ofMillis(1000));
            final int giveUp[] = {1000,1000};   int[] noRecordsCount = {0,0};
            if ( consumerRecordsWeek.count()==0 ) {  //bugged****************************************
                System.out.println("There are no records in week topic");
                noRecordsCount[0]++;
                if (noRecordsCount[0] > giveUp[0]) break;
                else continue;
            }
            System.out.println("aaa");

            consumerRecordsWeek.forEach(key -> {
                System.out.println(key.toString());
                writerWeek.appendRow(key.toString());
            });


            consumerWeek.commitAsync();
        }

    }




}
