package it.uniroma2.entity;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class Nave {
    private String id;
    private int type;
    private Map<String, RottaNave> viaggi; // K e' il timestamp, V e' la rotta che sta effettuando la nave

    public Nave( String id, int type){
        this.id = id;
        this.type = type;
        this.viaggi = new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public int getType() {
        return type;
    }

    public Map<String, RottaNave> getRoutes() {
        return viaggi;
    }


}
