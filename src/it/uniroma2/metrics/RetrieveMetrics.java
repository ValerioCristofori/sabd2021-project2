package it.uniroma2.metrics;

public class RetrieveMetrics {

    private static long counter = 0L;
    private static long initTime;

    public static synchronized void increase() {
        // se il counter delle tuple e' nullo, assegno l'istante di inizio
        if (counter == 0L) {
            initTime = System.currentTimeMillis();
            System.out.println("Start\n");
        }
        counter+=counter;
        double currentTime = System.currentTimeMillis() - initTime;
        // mostro throughput medio e latenza media
        System.out.println("Throughput = " + (counter/currentTime) + "\n" + "Latenza = " +
                (currentTime/counter) + "\n");
    }
}
