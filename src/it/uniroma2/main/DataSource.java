package it.uniroma2.main;

import it.uniroma2.kafka.KafkaHandler;
import it.uniroma2.kafka.MyKafkaProducer;
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

@SuppressWarnings("BusyWait")
public class DataSource {

    private static final SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"),
            new SimpleDateFormat("dd-MM-yy HH:mm")} ;
    private static final String path = "data/prj2_dataset_imported.csv";
    private static final Long SLEEP = 10L;


    public static void main( String[] args ) {


        long timeRange = 5;
        // Converting timeRange from minutes to milliseconds
        timeRange = timeRange * 60 * 1000;

        Map<Long, List<String>> map = new TreeMap<>();
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;



        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                String dateString = values[7];

                Long timestamp = null;
                for (SimpleDateFormat dateFormat: dateFormats) {
                    try {
                        timestamp = dateFormat.parse(dateString).getTime();
                        break;
                    } catch (ParseException ignored) { }
                }

                if (timestamp == null) {
                    System.exit(-1);
                }

                min = (min < timestamp) ? min : timestamp;
                max = (max > timestamp) ? max : timestamp;
                List<String> records = map.computeIfAbsent(timestamp, k -> new ArrayList<>());
                records.add(line);
            }
            reader.close();
        } catch (IOException e) {
        }



        Producer producer = new KafkaProducer<>(KafkaHandler.getProperties("injector"));

        Set<Map.Entry<Long, List<String>>> timeSet = map.entrySet();

        double proportion = timeRange / (double) (max - min);
        Long previous = null;

        for (Map.Entry<Long, List<String>> entry: timeSet) {
            Long timestamp = entry.getKey();

            if (previous != null) {
                try {
                    long sleepTime =  (long) ((timestamp - previous) * proportion);
                    TimeUnit.MILLISECONDS.sleep(sleepTime);
                } catch (InterruptedException e) {
                }
            }

            for (String record : entry.getValue()) {

                ProducerRecord<Long, String> producerRecord = new ProducerRecord<>( KafkaHandler.TOPIC_SOURCE, 0, timestamp, timestamp, record);
                System.out.println(record);
                producer.send(producerRecord, (recordMetadata, e) -> {e.printStackTrace();});
            }

            previous = timestamp;
        }
        producer.flush();
        producer.close();

    }

 /*   public static void main(String[] args) {

        // create producer
        MyKafkaProducer producer = new MyKafkaProducer();

        String line;
        Long eventTime = null;

        try {
            // read from file
            FileReader file = new FileReader(path);
            BufferedReader bufferedReader = new BufferedReader(file);

            while ((line = bufferedReader.readLine()) != null) {
                // produce tuples simulating a data stream processing source
                String[] info = line.split(",");
                for (SimpleDateFormat dateFormat : dateFormats) {
                    try {
                        eventTime = dateFormat.parse(info[7]).getTime();
                        break;
                    } catch (ParseException ignored) {
                    }

                }
                if (eventTime == null)
                    throw new NullPointerException();
                producer.produce(eventTime, line, eventTime);
                // for real data stream processing source simulation
                Thread.sleep(SLEEP);


            }

            bufferedReader.close();
            file.close();
            producer.close();
            System.out.println("Producer process completed");
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }*/


}
