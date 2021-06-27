package it.uniroma2.query3.ranking;

import java.io.Serializable;
import java.util.Date;

public class Trip implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tripId;
    private Date timestamp;
    private double distanza;

    public Trip( double distanza ){
        this.distanza = distanza;
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public double getDistanza() {
        return distanza;
    }

    public void setDistanza(double distanza) {
        this.distanza = distanza;
    }
}
