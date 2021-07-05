package it.uniroma2.query3;

import it.uniroma2.entity.Mappa;

import java.io.Serializable;

public class FirstAccumulatorQuery3 implements Serializable {


    private long ultimoTimestamp;
    private double distanzaTotale;
    private double lastLon;
    private double lastLat;

    public FirstAccumulatorQuery3(){
        this.distanzaTotale = 0;
        this.lastLon = Double.MAX_VALUE;
        this.lastLat = Double.MAX_VALUE;
    }

    public void add( double newLon, double newLat, long timestamp ){
        if( lastLon != Double.MAX_VALUE && lastLat != Double.MAX_VALUE ){
            // non e' il primo punto
            distanzaTotale = distanzaTotale + Mappa.distance(lastLat,lastLon,newLat,newLon,'K');
        }
        lastLon = newLon;
        lastLat = newLat;
        this.ultimoTimestamp = timestamp;
    }


    public double getDistanzaTotale() {
        return distanzaTotale;
    }

    public long getUltimoTimestamp() {
        return ultimoTimestamp;
    }
}
