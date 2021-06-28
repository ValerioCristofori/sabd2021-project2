package it.uniroma2.query2;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.FirstResult2;
import it.uniroma2.entity.Result1;
import it.uniroma2.query1.AccumulatorQuery1;
import org.apache.flink.api.common.functions.AggregateFunction;

public class FirstAggregatorQuery2 implements AggregateFunction<EntryData, FirstAccumulatorQuery2, FirstResult2> {

    @Override
    public FirstAccumulatorQuery2 createAccumulator() {
        return new FirstAccumulatorQuery2();
    }

    @Override
    public FirstAccumulatorQuery2 add(EntryData entryData, FirstAccumulatorQuery2 firstAccumulator2) {
        firstAccumulator2.add(entryData.getTripId());
        return firstAccumulator2;
    }

    @Override
    public FirstAccumulatorQuery2 merge(FirstAccumulatorQuery2 accumulator1, FirstAccumulatorQuery2 accumulator2) {
        accumulator2.getFrequentazioni().forEach(accumulator1::add);
        return accumulator1;
    }

    @Override
    public FirstResult2 getResult(FirstAccumulatorQuery2 accumulator) {
        return new FirstResult2(accumulator.getFrequentazioni());
    }



}

