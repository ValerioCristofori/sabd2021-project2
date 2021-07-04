package it.uniroma2.query2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FirstAccumulatorQuery2 implements Serializable {

    private Set<String> frequentazioni;

    public Set<String> getFrequentazioni(){
        return frequentazioni;
    }

    public FirstAccumulatorQuery2(){
        frequentazioni = new HashSet<>();
    }

    public void add(String trip){
        frequentazioni.add(trip);
    }
}
