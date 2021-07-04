package it.uniroma2.main;

import it.uniroma2.kafka.KafkaHandler;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.*;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Logger;

public class ConsumerTopic{

    private static Logger log;
    private static final String RESULTS = "Results";

    public static void main( String[] args ){

        //cleaning result directory to store data results
        cleanResultsFolder();
        System.out.println("Result directory prepared");

        // create a consumer structure to allow stopping
        ArrayList<KafkaConsumerThread> consumers = new ArrayList<>();

        int id = 0;
        // launch Flink topics consumers
        for (int i = 0; i < KafkaHandler.FLINK_TOPICS.length; i++) {
            KafkaConsumerThread consumer = new KafkaConsumerThread(id,
                    KafkaHandler.FLINK_TOPICS[i],
                    KafkaHandler.FLINK_OUTPUT_FILES[i]);
            consumers.add(consumer);
            new Thread(consumer).start();
            id++;
        }

        System.out.println("Enter something to stop consumers");
        Scanner scanner = new Scanner(System.in);
        // wait for the user to digit something
        scanner.next();
        System.out.println("Sending shutdown signal to consumers");
        // stop consumers
        for (KafkaConsumerThread consumer : consumers) {
            consumer.stop();
        }
        System.out.flush();
        System.out.println("Signal sent, closing...");

    }

    public static void cleanResultsFolder() {
        try {
            FileUtils.cleanDirectory(new File(RESULTS));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Could not clean Results directory");
        }
    }




}
