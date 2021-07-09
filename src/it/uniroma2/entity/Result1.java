package it.uniroma2.entity;

import java.io.Serializable;
import java.util.*;

public class Result1 implements Serializable{

    private Date timestamp;
    private String cella;
    private Map<String, Integer> resultMap; // K e' il tipo delle navi, V sono le navi contate per quel tipo

    public Result1(Map<String, Set<String>> mapTrips ) {
        resultMap = new HashMap<>();
        for (String type : mapTrips.keySet()) {
            resultMap.put(type, mapTrips.get(type).size()); // prendo il conteggio
        }
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getCella() {
        return cella;
    }

    public void setCella(String cella) {
        this.cella = cella;
    }

    public Map<String, Integer> getResultMap() {
        return resultMap;
    }

}
