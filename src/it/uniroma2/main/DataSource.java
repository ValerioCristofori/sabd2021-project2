package it.uniroma2.main;

import it.uniroma2.kafka.MyKafkaProducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class DataSource {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yy HH:mm");
    private static final String path = "data/prj2_dataset_imported.csv";
    private static final Long SLEEP = 1000L;

    public static void main(String[] args) {

        // create producer
        MyKafkaProducer producer = new MyKafkaProducer();

        String line;
        Long eventTime;

        try {
            // read from file
            FileReader file = new FileReader(path);
            BufferedReader bufferedReader = new BufferedReader(file);

            while ((line = bufferedReader.readLine()) != null) {
                try {
                    // produce tuples simulating a data stream processing source
                    String[] info = line.split(",", -1);
                    eventTime = sdf.parse(info[7]).getTime();
                    producer.produce(eventTime, line, eventTime);
                    // for real data stream processing source simulation
                    Thread.sleep(SLEEP);
                } catch (ParseException | InterruptedException ignored) {
                }
            }

            bufferedReader.close();
            file.close();
            producer.close();
            System.out.println("Producer process completed");
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }


}
