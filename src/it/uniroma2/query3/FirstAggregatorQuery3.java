package it.uniroma2.query3;

import it.uniroma2.entity.EntryData;
import it.uniroma2.query3.ranking.Trip;
import org.apache.flink.api.common.functions.AggregateFunction;

public class FirstAggregatorQuery3 implements AggregateFunction<EntryData, FirstAccumulatorQuery3, Trip> {
    @Override
    public FirstAccumulatorQuery3 createAccumulator() {
        return new FirstAccumulatorQuery3();
    }

    @Override
    public FirstAccumulatorQuery3 add(EntryData entryData, FirstAccumulatorQuery3 firstAccumulatorQuery3) {
        firstAccumulatorQuery3.add( entryData.getLon(), entryData.getLat() );
        return firstAccumulatorQuery3;
    }

    @Override
    public Trip getResult(FirstAccumulatorQuery3 firstAccumulatorQuery3) {
        return new Trip( firstAccumulatorQuery3.getDistanzaTotale() );
    }

    @Override
    public FirstAccumulatorQuery3 merge(FirstAccumulatorQuery3 acc1, FirstAccumulatorQuery3 acc2) {
        acc1.setDistanzaTotale( acc1.getDistanzaTotale() + acc2.getDistanzaTotale());
        return acc1;
    }
}
