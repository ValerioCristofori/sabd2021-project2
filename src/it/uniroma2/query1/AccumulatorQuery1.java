package it.uniroma2.query1;

import java.io.Serializable;
import java.util.*;

public class AccumulatorQuery1 implements Serializable {

    private Map<String, Set<String>> mapForResult; //K e' il tipo, V e' la lista di tutti trip

    public AccumulatorQuery1(){
        this.mapForResult = new HashMap<>();
    }

    public void add(String shipType, String tripId){
        Set<String> set = mapForResult.get(shipType);
        // se non vi sono trip per quella tipologia
        if(set == null){
            set = new HashSet<>();
        }
        // aggiungo il trip
        set.add(tripId);
        mapForResult.put(shipType, set);
    }

    public void add(String shipType, Set<String> trips ){
        for (String tripId : trips) {
            add(shipType, tripId);
        }
    }

    public Map<String, Set<String>> getMapForResult() {
        return mapForResult;
    }
}
