package it.uniroma2.entity;

import java.util.ArrayList;
import java.util.List;

public class Mappa {
    private static final double minLon = -6;
    private static final double maxLon = 37;
    private static final double minLat = 32;
    private static final double maxLat = 45;

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

}
