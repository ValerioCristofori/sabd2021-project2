package it.uniroma2.query3;

import it.uniroma2.entity.Mappa;
import it.uniroma2.entity.Trip;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class FirstAccumulatorQuery3 implements Serializable {

    private final Map<String, Trip> tripList;
    private String tripId;
    private long lastTimestamp;
    private double lon;
    private double lat;
    private double distanzaTotale;


    public FirstAccumulatorQuery3(){
        this.tripList = new HashMap<>();
    }

    public Map<String, Trip> getTripList() {
        return tripList;
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
