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


    public String getTripId() {
        return tripId;
    }

    public String getShipId() {
        return shipId;
    }

    public String getShipType() {
        return shipType;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public String getCella() {
        return cella;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
