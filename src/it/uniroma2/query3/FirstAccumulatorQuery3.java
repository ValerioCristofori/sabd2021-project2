package it.uniroma2.query3;

import java.io.Serializable;

public class FirstAccumulatorQuery3 implements Serializable {


    private String tripId;
    private long lastTimestamp;
    private double lon;
    private double lat;
    private double distanzaTotale;


    public FirstAccumulatorQuery3(){
        this.distanzaTotale = 0;
        this.lat = 0;
        this.lon = 0;
        this.lastTimestamp = 0;
        this.tripId = null;
    }


    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getDistanzaTotale() {
        return distanzaTotale;
    }

    public void setDistanzaTotale(double distanzaTotale) {
        this.distanzaTotale = distanzaTotale;
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }
}
