package it.uniroma2.entity;

import java.util.Date;

public class StartTrip {

    private double startLon;
    private double startLat;
    private long startDate;

    public StartTrip(double startLon, double startLat, long startDate) {
        this.startLon = startLon;
        this.startLat = startLat;
        this.startDate = startDate;
    }

    public double getStartLon() {
        return startLon;
    }

    public double getStartLat() {
        return startLat;
    }

    public long getStartDate() {
        return startDate;
    }

}
