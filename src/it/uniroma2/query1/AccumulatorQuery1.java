package it.uniroma2.query1;

import java.io.Serializable;
import java.util.*;

public class AccumulatorQuery1 implements Serializable {

    private Map<String, Set<String>> mapForResult; //K e' il tipo, V e' la lista di tutti trip
    // trip per evitare duplicati dati da stesse navi con diverse rotte ( trip e' la chiave del dataset )

    public AccumulatorQuery1(){
        this.mapForResult = new HashMap<>();
    }

    public void add(String shipType, String tripId){
        Set<String> typeSet = mapForResult.get(shipType);
        // se non vi sono trip per quella tipologia
        if(typeSet == null){
            typeSet = new HashSet<>();
        }
        // aggiungo il trip
        typeSet.add(tripId);
        mapForResult.put(shipType, typeSet);
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
