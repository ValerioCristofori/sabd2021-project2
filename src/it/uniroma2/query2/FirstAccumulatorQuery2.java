package it.uniroma2.query2;

import java.util.List;
import java.util.Set;

public class FirstAccumulatorQuery2 {

    private Set<String> frequentazioni;

    public Set<String> getFrequentazioni(){
        return frequentazioni;
    }

    public void setFrequentazioni(Set<String> frequentazioni){
        this.frequentazioni = frequentazioni;
    }

//    public FirstAccumulatorQuery2(){
//        frequentazioni = new List<>();
//    }

    public void add(String trip){
        frequentazioni.add(trip);
    }
}
