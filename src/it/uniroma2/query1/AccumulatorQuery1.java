package it.uniroma2.query1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AccumulatorQuery1 implements Serializable {

    private Map<String, List<String>> mapForResult; //K e' il tipo, V e' la lista di tutti trip
    // trip per evitare duplicati dati da stesse navi con diverse rotte ( trip e' la chiave del dataset )

    public AccumulatorQuery1(){
        this.mapForResult = new HashMap<>();
    }

    public AccumulatorQuery1( Map<String, List<String>> mapForResult ){
        this.mapForResult = mapForResult;
    }

    public void add(String shipType, String tripId){
        List<String> typeSet = mapForResult.get(shipType);
        // se non vi sono trip per quella tipologia
        if(typeSet == null){
            typeSet = new ArrayList<>();
        }
        // aggiungo il trip
        typeSet.add(tripId);
        mapForResult.put(shipType, typeSet);
    }

    public void add(String shipType, List<String> trips ){
        for (String tripId : trips) {
            add(shipType, tripId);
        }
    }

    public Map<String, List<String>> getMapForResult() {
        return mapForResult;
    }
}
