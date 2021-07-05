package it.uniroma2.query3;

import it.uniroma2.entity.EntryData;
import it.uniroma2.query3.ranking.Trip;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FirstAggregatorQuery3 implements AggregateFunction<EntryData, FirstAccumulatorQuery3, Tuple2<Long, Double>> {
    @Override
    public FirstAccumulatorQuery3 createAccumulator() {
        return new FirstAccumulatorQuery3();
    }

    @Override
    public FirstAccumulatorQuery3 add(EntryData entryData, FirstAccumulatorQuery3 firstAccumulatorQuery3) {
        firstAccumulatorQuery3.add( entryData.getLon(), entryData.getLat(), entryData.getTimestamp() );
        return firstAccumulatorQuery3;
    }

    @Override
    public Tuple2<Long, Double> getResult(FirstAccumulatorQuery3 firstAccumulatorQuery3) {
        return new Tuple2<>( firstAccumulatorQuery3.getUltimoTimestamp(), firstAccumulatorQuery3.getDistanzaTotale() );
    }

    @Override
    public FirstAccumulatorQuery3 merge(FirstAccumulatorQuery3 acc1, FirstAccumulatorQuery3 acc2) {
        return null;
    }
}
