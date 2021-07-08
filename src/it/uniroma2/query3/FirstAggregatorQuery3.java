package it.uniroma2.query3;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.Mappa;
import it.uniroma2.entity.Trip;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class FirstAggregatorQuery3 implements AggregateFunction<EntryData, FirstAccumulatorQuery3, Tuple2<Long, Double>> {
    @Override
    public FirstAccumulatorQuery3 createAccumulator() {
        return new FirstAccumulatorQuery3();
    }

    public void cleanUpTripList(long date, FirstAccumulatorQuery3 acc){

        Iterator iter = acc.getTripList().entrySet().iterator();

        while(iter.hasNext()){
            Map.Entry<String, Trip> trip = (Map.Entry)iter.next();

            if(trip.getValue().getStartDate() < date){
                iter.remove();
            }
        }
    }

    @Override
    public FirstAccumulatorQuery3 add(EntryData entryData, FirstAccumulatorQuery3 firstAccumulatorQuery3) {
        if( firstAccumulatorQuery3.getTripList().isEmpty() ) {
            // new window
            String trip_id = entryData.getTripId();

            firstAccumulatorQuery3.setTripId(trip_id);
            firstAccumulatorQuery3.setLastTimestamp(entryData.getTimestamp());
            Trip trip = firstAccumulatorQuery3.getTripList().get(trip_id);

            cleanUpTripList(entryData.getTimestamp(), firstAccumulatorQuery3); // Remove finished trips

            if(trip == null){
                // First couple (lon,lat) found !!!
                SimpleDateFormat formatter = new SimpleDateFormat("yy-MM-dd HH");
                Date trip_end = null;

                try {
                    trip_end = formatter.parse(trip_id.split("_")[1].split(" - ")[1]); // Find end date from trip id
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                firstAccumulatorQuery3.getTripList().put(trip_id, new Trip(entryData.getLon(), entryData.getLat(), trip_end.getTime())); // Add trip to list

            }
            else{
                // Get initial (lon,lat)
                firstAccumulatorQuery3.setLon(trip.getStartLon());
                firstAccumulatorQuery3.setLat(trip.getStartLat());

                firstAccumulatorQuery3.setDistanzaTotale(Mappa.getDistanzaEuclidea( firstAccumulatorQuery3.getLon(), firstAccumulatorQuery3.getLat(), entryData.getLon(), entryData.getLat()));
            }

        }
        else if(entryData.getTimestamp() > firstAccumulatorQuery3.getLastTimestamp()){
            // Compute the distance only if timestamp > maxTs
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
        return null;
    }
}
