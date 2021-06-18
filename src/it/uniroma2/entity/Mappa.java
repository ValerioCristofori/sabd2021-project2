package it.uniroma2.entity;

import java.util.ArrayList;
import java.util.List;

public class Mappa {
    private static final double minLon = -6;
    private static final double maxLon = 37;
    private static final double minLat = 32;
    private static final double maxLat = 45;
    private static final double celle_x = 40;
    private static final double celle_y = 10;
    private static final String[] list_y = {"A","B","C","D","E","F","G","H","I","J"};
    private static double dim_cella_x;
    private static double dim_cella_y;
    private static double dim_x;
    private static double dim_y;
    private static List<Nave> navi;

    public Mappa(){
        navi = new ArrayList<>();
    }

    public static void addShip(Nave nave){
        navi.add(nave);
    }

    public static Nave getNaveFromId( String id ){
        for( Nave nave : navi ){
            if( nave.getId().equals(id)) return nave;
        }
        return null;
    }

    private void setup(){
        dim_x = Math.abs( maxLon - minLon );
        dim_y = Math.abs( maxLat - minLat );
        dim_cella_x = dim_x/celle_x;
        dim_cella_y = dim_y/celle_y;
    }

    public String findRightCell( double lon, double lat ){
        String ret;
        int i;
        int j;
        for ( i = 1; i< celle_x; i++ ){
            double min_i_lon = (i-1)*dim_cella_x + minLon;
            double max_i_lon = i*dim_cella_x + minLon;
            if( lon > min_i_lon || lon <= max_i_lon ) break;
        }
        for ( j = 1; j< celle_y; j++ ){
            double min_j_lat = (j-1)*dim_cella_y + minLat;
            double max_j_lat = j*dim_cella_y + minLat;
            if( lat > min_j_lat || lat <= max_j_lat ) break;
        }
        ret = String.format( "%s-%d", list_y[j - 1], i);

        return ret;
    }


}
