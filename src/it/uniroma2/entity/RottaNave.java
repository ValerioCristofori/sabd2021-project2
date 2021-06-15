package it.uniroma2.entity;

public class RottaNave {
    private double lat;
    private double lon;
    private double speed;
    //private String cella;
    private String viaggio_id;
    private long distanza;

    public RottaNave(double lat, double lon, double speed, String viaggio_id) {
        this.lat = lat;
        this.lon = lon;
        this.speed = speed;
        this.viaggio_id = viaggio_id;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public double getSpeed() {
        return speed;
    }

    /*public String getCella() {
        return cella;
    }*/

    public String getViaggio_id() {
        return viaggio_id;
    }

    public long getDistanza() {
        return distanza;
    }
}
