package it.uniroma2.main;

import it.uniroma2.kafka.KafkaHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerThread implements Runnable{

    private final static int POLL_WAIT_TIME = 1000;
    private final Consumer<Long, String> consumer;
    private final int id;
    private final String topic;
    private String path;
    private boolean running = true;


    private Consumer<Long, String> createConsumer() {
        Properties props = KafkaHandler.getProperties("csv_output");
        return new KafkaConsumer<>(props);
    }

    private static void subscribeToTopic(Consumer<Long, String> consumer, String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaConsumerThread(int id, String topic, String path) {
        this.id = id;
        this.topic = topic;
        this.path = path;
        // create the consumer
        consumer = createConsumer();

        // subscribe the consumer to the topic
        System.out.println("Subscribing at topic " + this.topic);
        subscribeToTopic(consumer, topic);
    }

    @Override
    public void run() {
        System.out.println("Flink Consumer " + id + " running");
        try {
            while (running) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(POLL_WAIT_TIME));

                if (!records.isEmpty()) {
                    File file = new File(path);
                    if (!file.exists()) {
                        // creates the file if it does not exist
                        file.createNewFile();
                    }

                    // append to existing version of the same file
                    FileWriter writer = new FileWriter(file, true);
                    BufferedWriter bw = new BufferedWriter(writer);

                    for (ConsumerRecord<Long, String> record : records) {
                        System.out.println("{TOPIC:"+this.topic+"} " + record.value());
                        bw.append(record.value());
                        bw.append("\n");
                    }

                    // close both buffered writer and file writer
                    bw.close();
                    writer.close();
                }
            }
        }catch (IOException e) {
        e.printStackTrace();
        System.err.println("Could not export result to " + path);

        } finally {
            // close consumer
            consumer.close();
            System.out.println("Flink Consumer stopped");
        }

    }


    public void stop() {
        this.running = false;
    }
}
