package it.uniroma2.entity;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class Result3 {

    private Date timestamp;
    private Map<String, Integer> resultMap; // K e' l'id del viaggio, V e' la distanza totale

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Integer> getResultMap() {
        return resultMap;
    }

    public void setResultMap(Map<String, Integer> resultMap) {
        this.resultMap = resultMap;
    }
}
