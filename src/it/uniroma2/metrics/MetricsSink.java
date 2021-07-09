package it.uniroma2.metrics;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class MetricsSink implements SinkFunction<String> {

    private static long counter = 0L;
    private static long initTime = 0L;
    private static final String path = "Results/metrics.csv";

    public static synchronized void increase() {
        // se il counter delle tuple e' nullo, assegno l'istante di inizio
        if (initTime == 0L) {
            //start
            initTime = System.currentTimeMillis();
            return;
        }
        counter ++;
        double currentTime = System.currentTimeMillis() - initTime;

        File file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // scrittura sul file da append su file esistente
        FileWriter writer = null;
        try {
            writer = new FileWriter(file, false);
            BufferedWriter bw = new BufferedWriter(writer);


            // mostro throughput medio e latenza media
            bw.write("Throughput," + (counter / currentTime) + "\n" +
                    "Latenza," + (currentTime / counter) + "\n");

            bw.close();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        increase();
    }
}
