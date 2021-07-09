package it.uniroma2.main;

import it.uniroma2.kafka.KafkaHandler;
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
    //due tipi di date format, le entry del dataset non sono consistenti
    private static final SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"),
            new SimpleDateFormat("dd-MM-yy HH:mm")} ;
    private static final String path = "data/prj2_dataset_imported.csv";
    private static final long duration = 5*60*1000; // durata di 5 minuti in millisecondi
    private static Map<Long, List<String>> map = new TreeMap<>(); //mappa contenente: K e' l'event time, V e' la lista dei record del csv
    private static long min;
    private static long max;

    public static void main(String[] args) {

        buildTimeGap();

        // instanzio un producer per Kafka che andra' ad inserire
        //      i dati nel topic
        Producer producer = new KafkaProducer<>(KafkaHandler.getProperties("injector"));

        Set<Map.Entry<Long, List<String>>> timeSet = map.entrySet();
        double timeFrame = duration / (double) (max - min);
        Long prev = null;
        //visito l'insieme dei dati facendo una sleep per un tempo proporzionale al timestamp
        //     e inviando i dati a un topic di kafka
        for (Map.Entry<Long, List<String>> entry: timeSet) {
            Long timestamp = entry.getKey();

            if (prev != null) {
                try {
                    long sleepTime =  (long) ((timestamp - prev) * timeFrame);
                    TimeUnit.MILLISECONDS.sleep(sleepTime);
                } catch (InterruptedException e) {
                }
            }

            for (String record : entry.getValue()) {
                // manda i record al topic
                ProducerRecord<Long, String> producerRecord = new ProducerRecord<>( KafkaHandler.TOPIC_SOURCE, 0, timestamp, timestamp, record);
                System.out.println(record);
                producer.send(producerRecord, (recordMetadata, e) -> {e.printStackTrace();});
            }

            prev = timestamp;
        }
        producer.flush();
        producer.close();

    }

    private static void buildTimeGap() {
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;


        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                String dateString = values[7]; //prendo il timestamp
                Long timestamp = null;
                for (SimpleDateFormat dateFormat: dateFormats) {
                    // provo il parsing per entrambi i formati
                    try {
                        timestamp = dateFormat.parse(dateString).getTime();
                        break;
                    } catch (ParseException ignored) { }
                }

                if (timestamp == null) {
                    System.exit(-1);
                }

                if( min >= timestamp )  min = timestamp;
                if (max <= timestamp)  max = timestamp;
                List<String> records = map.computeIfAbsent(timestamp, k -> new ArrayList<>());
                records.add(line);
            }
            reader.close();
        } catch (IOException e) {
        }
    }

}

