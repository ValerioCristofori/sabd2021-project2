package it.uniroma2.query3;

import it.uniroma2.entity.Mappa;

import java.io.Serializable;

public class AccumulatorQuery3 implements Serializable {

    double distanzaTotale;
    double lastLon;
    double lastLat;

    public AccumulatorQuery3(){
        this.distanzaTotale = 0;
        this.lastLon = Double.MAX_VALUE;
        this.lastLat = Double.MAX_VALUE;
    }

    public void add( double newLon, double newLat ){
        if( lastLon != Double.MAX_VALUE && lastLat != Double.MAX_VALUE ){
            // non e' il primo punto nessuna distanza
            distanzaTotale = distanzaTotale + Mappa.distance(lastLat,lastLon,newLat,newLon,'K');
        }
        lastLon = newLon;
        lastLat = newLat;
    }


    public double getDistanzaTotale() {
        return distanzaTotale;
    }

    public void setDistanzaTotale(double distanzaTotale) {
        this.distanzaTotale = distanzaTotale;
    }
}
