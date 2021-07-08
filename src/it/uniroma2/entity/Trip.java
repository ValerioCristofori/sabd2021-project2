package it.uniroma2.entity;

import java.util.Date;

public class Trip {

    private double startLon;
    private double startLat;
    private long startDate;

    public Trip(double startLon, double startLat, long startDate) {
        this.startLon = startLon;
        this.startLat = startLat;
        this.startDate = startDate;
    }

    public double getStartLon() {
        return startLon;
    }

    public void setStartLon(double startLon) {
        this.startLon = startLon;
    }

    public double getStartLat() {
        return startLat;
    }

    public void setStartLat(double startLat) {
        this.startLat = startLat;
    }

    public long getStartDate() {
        return startDate;
    }

    public void setStartDate(long startDate) {
        this.startDate = startDate;
    }
}
