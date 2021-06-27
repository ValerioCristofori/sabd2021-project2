package it.uniroma2.entity;

import it.uniroma2.utils.DifferentShipTypes;

import java.io.Serializable;

public class EntryData implements Serializable {

    private String tripId;
    private String shipId;
    private String shipType;
    private double lat;
    private double lon;
    private String cella;
    private long timestamp;

    public EntryData(String shipId, double lon, double lat,  Integer shipType, long timestamp,String tripId) {
        this.tripId = tripId;
        this.shipId = shipId;
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;

        this.shipType = DifferentShipTypes.getTypeShip(shipType);
        this.cella = Mappa.findRightCell(lon,lat);
    }

    public EntryData(String record, String record1, double lat, double parseDouble, int timestamp, Long f0, String record2) {
    }


    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getShipId() {
        return shipId;
    }

    public void setShipId(String shipId) {
        this.shipId = shipId;
    }

    public String getShipType() {
        return shipType;
    }

    public void setShipType(String shipType) {
        this.shipType = shipType;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public String getCella() {
        return cella;
    }

    public void setCella(String cella) {
        this.cella = cella;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
