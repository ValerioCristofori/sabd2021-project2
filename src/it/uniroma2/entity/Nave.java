package it.uniroma2.entity;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class Nave {
    private String id;
    private int type;
    private Map<LocalDateTime, Posizione> coordinate;
    private List<Viaggio> viaggi;

    public String getId() {
        return id;
    }

    public int getType() {
        return type;
    }

    public Map<LocalDateTime, Posizione> getCoordinate() {
        return coordinate;
    }

    public List<Viaggio> getViaggi() {
        return viaggi;
    }
}
