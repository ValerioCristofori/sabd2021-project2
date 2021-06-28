package it.uniroma2.query3;

import it.uniroma2.entity.EntryData;
import it.uniroma2.query3.ranking.Trip;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AggregatorQuery3 implements AggregateFunction<EntryData, AccumulatorQuery3, Trip> {
    @Override
    public AccumulatorQuery3 createAccumulator() {
        return new AccumulatorQuery3();
    }

    @Override
    public AccumulatorQuery3 add(EntryData entryData, AccumulatorQuery3 accumulatorQuery3) {
        accumulatorQuery3.add( entryData.getLon(), entryData.getLat() );
        return accumulatorQuery3;
    }

    @Override
    public Trip getResult(AccumulatorQuery3 accumulatorQuery3) {
        return new Trip( accumulatorQuery3.getDistanzaTotale() );
    }

    @Override
    public AccumulatorQuery3 merge(AccumulatorQuery3 acc1, AccumulatorQuery3 acc2) {
        acc1.setDistanzaTotale( acc1.getDistanzaTotale() + acc2.getDistanzaTotale());
        return acc1;
    }
}
