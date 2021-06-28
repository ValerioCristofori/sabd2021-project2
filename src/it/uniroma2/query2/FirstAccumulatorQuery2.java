package it.uniroma2.query2;

import java.util.List;

public class FirstAccumulatorQuery2 {

    List<String> frequentazioni;

    public List<String> getFrequentazioni{
        return frequentazioni;
    }

    public void setFrequentazioni(List<String> frequentazioni){
        this.frequentazioni = frequentazioni;
    }

    public FirstAccumulatorQuery2(){
        frequentazioni = new List<>();
    }

    public void add(String trip){
        frequentazioni.add(trip);
    }
}
