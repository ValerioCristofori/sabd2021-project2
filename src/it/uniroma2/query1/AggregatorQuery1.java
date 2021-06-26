package it.uniroma2.query1;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.Result1;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AggregatorQuery1 implements AggregateFunction<EntryData, AccumulatorQuery1, Result1> {
    @Override
    public AccumulatorQuery1 createAccumulator() {
        return new AccumulatorQuery1();

    }

    @Override
    public AccumulatorQuery1 add(EntryData entryData, AccumulatorQuery1 accumulatorQuery1) {
        accumulatorQuery1.add( entryData.getShipType(), entryData.getTripId() );
        return accumulatorQuery1;
    }

    @Override
    public Result1 getResult(AccumulatorQuery1 accumulatorQuery1) {
        return new Result1( accumulatorQuery1.getMapForResult());
    }

    @Override
    public AccumulatorQuery1 merge(AccumulatorQuery1 accumulator1, AccumulatorQuery1 accumulator2) {
        accumulator2.getMapForResult().forEach( accumulator1::add );
        return accumulator1;
    }
}
