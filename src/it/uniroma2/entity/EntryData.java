package it.uniroma2.entity;

import java.io.Serializable;

public class EntryData implements Serializable {

    private String tripId;
    private String shipId;
    private String shipType;
    private double lat;
    private double lon;
    private String cella;
    private long timestamp;

    public EntryData(String shipId, double lon, double lat,  int shipType, long timestamp,String tripId) {
        this.tripId = tripId;
        this.shipId = shipId;
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;

        this.shipType = getTypeShip(shipType);
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

    private String getTypeShip(int shipType){
        if(shipType == 35){
            return String.valueOf(shipType);
        } else if (shipType >= 60 && shipType <= 69){
            return "60-69";
        } else if (shipType >= 70 && shipType <= 79){
            return "70-79";
        } else {
            return "others";
        }
    }
}
