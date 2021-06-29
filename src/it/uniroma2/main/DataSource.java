package it.uniroma2.main;

import it.uniroma2.utils.KafkaHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class DataSource {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yy HH:mm");
    private static final String path = "data/prj2_dataset_imported.csv";
    private static final long timeRange = 5;
    private static Map<Long, List<String>> map;
    private static long min = Long.MIN_VALUE;
    private static long max = Long.MAX_VALUE;

    private static void sendAsStreaming( Properties prop) throws InterruptedException {
        Producer<Long, String> producer = new KafkaProducer<>(prop);

        Set<Map.Entry<Long, List<String>>> timeSet = map.entrySet();

        double proportion = timeRange / (double) (max - min);
        Long prev = null;

        for (Map.Entry<Long, List<String>> entry: timeSet) {
            Long timestamp = entry.getKey();

            if (prev != null) {
                TimeUnit.MILLISECONDS.sleep((long) ((timestamp - prev) * proportion));
            }


            for (String record : entry.getValue()) {

                ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(KafkaHandler.TOPIC_SOURCE, 0, timestamp, timestamp, record);
                System.out.println(record);
                producer.send(producerRecord, (recordMetadata, e) -> {e.printStackTrace();});
            }


            prev = timestamp;
        }
        producer.flush();
    }

    public static void main( String[] args ) throws ParseException, IOException, InterruptedException {

        map = new TreeMap<>();

        // prendo kafka prop
        Properties prop = KafkaHandler.getProperties("injector");


        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line;
        reader.readLine();
        while ((line = reader.readLine()) != null) {
            String[] values = line.split(",");
            String dateString = values[7];

            Long timestamp = sdf.parse(dateString).getTime();

            // prendo l'event time minore e il maggiore
            if( max < timestamp ) max = timestamp;
            if( min > timestamp ) min = timestamp;

            List<String> tuples = map.computeIfAbsent(timestamp, k -> new ArrayList<>());
            tuples.add(line);
        }
        reader.close();

        sendAsStreaming(prop);
    }


}
