package it.uniroma2.entity;

import java.util.Date;

public class FirstResult2 {
    private Date timestamp;
    private String cella;
    private String marOrientale;    // invece di un enumeratore metto due stringhe diverse
    private String marOccidentale;  // devo fare i controlli sulla longitudine
    private int frequentazione;     // navi diverse che attraversano una cella nella fascia oraria

    public void setMare{
        // se il numero della cella (da 1 a 40) corrisponde a lon<12
            // set mar occidentale
        // altrimenti set mar orientale
    }

    public String getMarOrientale(){
        return marOrientale;
    }

    public String getMarOccidentale(){
        return marOccidentale;
    }

    public String getCella(){
        return cella;
    }

    public void setCella(String cella){
        this.cella = cella;
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

}
