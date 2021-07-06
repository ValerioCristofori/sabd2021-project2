package it.uniroma2.query3.ranking;

import java.io.Serializable;
import java.util.Date;

public class Trip implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tripId;
    private double distanza;

    public Trip( String tripId, double distanza){
        this.tripId = tripId;
        this.distanza = distanza;
    }

    public String getTripId() {
        return tripId;
    }

    public double getDistanza() {
        return distanza;
    }

}
