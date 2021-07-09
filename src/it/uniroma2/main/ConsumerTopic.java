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

    private static final String RESULTS = "Results";

    public static void main( String[] args ){

        // clean della directory Results
        cleanResultsFolder();

        // lista contenente i riferimenti ai thread da stoppare
        ArrayList<KafkaConsumerThread> consumers = new ArrayList<>();

        int id = 0;
        // launch Flink consumer
        for (int i = 0; i < KafkaHandler.FLINK_TOPICS.length; i++) {
            KafkaConsumerThread consumer = new KafkaConsumerThread(id,
                    KafkaHandler.FLINK_TOPICS[i],
                    KafkaHandler.FLINK_OUTPUT_FILES[i]);
            consumers.add(consumer);
            new Thread(consumer).start();
            id++;
        }

        System.out.println("ENTER SOMETHING TO SHUTDOWN CONSUMERS...");
        Scanner scanner = new Scanner(System.in);
        scanner.next();
        System.out.println("Sending shutdown signal to consumers");
        // stop consumers
        for (KafkaConsumerThread consumer : consumers) {
            consumer.stop();
        }
        System.out.flush();
        System.out.println("Shutdown!!!");

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
