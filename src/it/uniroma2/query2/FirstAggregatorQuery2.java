package it.uniroma2.query2;

public class FirstAggregatorQuery2 {

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
        return acc1;
    }

    @Override
    public FirstResult2 getResult(FirstAccumulatorQuery2 accumulator) {
        return new FirstResult2(accumulator.getFrequentazioni());
    }



}

