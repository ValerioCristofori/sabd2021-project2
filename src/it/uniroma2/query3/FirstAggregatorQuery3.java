package it.uniroma2.query3;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.Mappa;
import it.uniroma2.entity.StartTrip;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FirstAggregatorQuery3 implements AggregateFunction<EntryData, FirstAccumulatorQuery3, Tuple2<Long, Double>> {

    private Map<String, StartTrip> tripList; // K e' il tripId, V e' il viaggio iniziale

    public FirstAggregatorQuery3() {
        this.tripList = new HashMap<>();
    }


    @Override
    public FirstAccumulatorQuery3 createAccumulator() {
        return new FirstAccumulatorQuery3();
    }


    private void newWindowTrip(EntryData entryData, FirstAccumulatorQuery3 firstAccumulatorQuery3){

        firstAccumulatorQuery3.setTripId(entryData.getTripId());
        firstAccumulatorQuery3.setLastTimestamp(entryData.getTimestamp());
        StartTrip startTrip = this.tripList.get(entryData.getTripId());

        //rimuovo tutti i vecchi trip rimasti nella lista
        Iterator iter = this.tripList.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry<String, StartTrip> trip = (Map.Entry)iter.next();
            if(trip.getValue().getStartDate() < entryData.getTimestamp()){
                iter.remove();
            }
        }

        if(startTrip == null){
            // Primo viaggio ( start_lon, start_lat )
            SimpleDateFormat formatter = new SimpleDateFormat("yy-MM-dd HH");
            Date trip_end = null;

            try {
                trip_end = formatter.parse(entryData.getTripId().split("_")[1].split(" - ")[1]);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            this.tripList.put(entryData.getTripId(), new StartTrip(entryData.getLon(), entryData.getLat(), trip_end.getTime()));
        }
        else{
            firstAccumulatorQuery3.setLon(startTrip.getStartLon());
            firstAccumulatorQuery3.setLat(startTrip.getStartLat());
            firstAccumulatorQuery3.setDistanzaTotale(Mappa.getDistanzaEuclidea( firstAccumulatorQuery3.getLon(), firstAccumulatorQuery3.getLat(), entryData.getLon(), entryData.getLat()));
        }
    }

    @Override
    public FirstAccumulatorQuery3 add(EntryData entryData, FirstAccumulatorQuery3 firstAccumulatorQuery3) {
        if( firstAccumulatorQuery3.getTripId() == null ) {
            this.newWindowTrip(entryData,firstAccumulatorQuery3);
        }
        else if(entryData.getTimestamp() > firstAccumulatorQuery3.getLastTimestamp()){
            //caso in cui il timestamp della nuova tupla e' successivo a quello nell'accumulatore
            firstAccumulatorQuery3.setDistanzaTotale(Mappa.getDistanzaEuclidea( firstAccumulatorQuery3.getLon(), firstAccumulatorQuery3.getLat(), entryData.getLon(), entryData.getLat()));
            firstAccumulatorQuery3.setLastTimestamp(entryData.getTimestamp());
        }
        return firstAccumulatorQuery3;
    }

    @Override
    public Tuple2<Long, Double> getResult(FirstAccumulatorQuery3 firstAccumulatorQuery3) {
        return new Tuple2<>( firstAccumulatorQuery3.getLastTimestamp(), firstAccumulatorQuery3.getDistanzaTotale() );
    }

    @Override
    public FirstAccumulatorQuery3 merge(FirstAccumulatorQuery3 acc1, FirstAccumulatorQuery3 acc2) {
        if( acc1.getLastTimestamp() > acc2.getLastTimestamp() ){
            return acc1;
        }
        else{
            return acc2;
        }
    }
}
