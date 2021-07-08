package it.uniroma2.entity;

import java.util.List;

public class Mappa {
    private static final double minLon = -6.0;
    private static final double maxLon = 37.0;
    private static final double minLat = 32.0;
    private static final double maxLat = 45.0;
    private static final double celle_x = 40;
    private static final double celle_y = 10;
    private static final double dim_cella_x = ( maxLon - minLon )/celle_x;
    private static final double dim_cella_y = ( maxLat - minLat )/celle_y;;
    private static final double canaleDiSiciliaLon = 12.0;
    private static final String[] shipTypes = {"35","60-69","70-79","others"};

    public static double getCanaleDiSiciliaLon() {
        return canaleDiSiciliaLon;
    }
    public static String[] getShipTypes() {
        return shipTypes;
    }

    public static double getLonByCella(String cella){
        int lonCella = Integer.parseInt(cella.substring(1));
        double lon = (lonCella * dim_cella_x) + minLon - (dim_cella_x/2);
        return lon;
    }

    public static String findRightCell( double lon, double lat ){
        char yId = 'A';
        int xId = 1;
        int y = (int)((lat-minLat)/dim_cella_y);
        yId += y;
        int x = (int)((lon-minLon)/dim_cella_x);
        xId += x;

        return "" + yId + xId;
    }

    public static double distance(double lat1, double lon1, double lat2, double lon2, char unit) {
        double theta = lon1 - lon2;
        double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        dist = dist * 60 * 1.1515;
        if (unit == 'K') { // kilometri
            dist = dist * 1.609344;
        } else if (unit == 'N') {  //miglie nautiche
            dist = dist * 0.8684;
        }
        return (dist); // else -> miglie
    }

    // funzione che converte decimal degrees to radians
    private static double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }

    // radians to decimal degrees
    private static double rad2deg(double rad) {
        return (rad * 180.0 / Math.PI);
    }


    public static double getDistanzaEuclidea(double lastLat, double lastLon, double newLat, double newLon) {
        double lon = newLon - lastLon;
        double lat = newLat - lastLat;
        return Math.sqrt(lon*lon + lat*lat);
    }
}
