package it.uniroma2.entity;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;

public class FirstResult2 {
    private Date timestamp;
    private String cella;
    private int frequentazione;     // navi diverse che attraversano una cella nella fascia oraria
    private String mare;            // mare orientale - occidentale

    public FirstResult2(int i) {
        this.frequentazione = i;
    }

    public void setMareByCella(String cella){
        // prendo valore della longitudine nell'id della cella
        if (Mappa.getLonByCella(cella) < Mappa.getCanaleDiSiciliaLon()){
            this.mare = "Occidentale";
        } else {
            this.mare = "Orientale";
        }
    }

    public String getMare(){
        return mare;
    }

    public String getCella(){
        return cella;
    }

    public void setCella(String cella){
        this.cella = cella;
        setMareByCella(cella);
    }

    public Date getTimestamp(){
        return timestamp;
    }

    public void setTimestamp(Date timestamp){
        this.timestamp = timestamp;
    }

    public int getFrequentazione() {
        return frequentazione;
    }

    public void setFrequentazione(int frequentazione) {
        this.frequentazione = frequentazione;
    }

    public int getHour(){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(timestamp);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    public FirstResult2(Set<String> frequentazione){
        this.frequentazione = frequentazione.size();
    }
}
