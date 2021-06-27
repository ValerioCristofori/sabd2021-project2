package it.uniroma2.query2;

public class AggregatorQuery2 implements AggregateFunction<Query2IntermediateOutcome, Query2Outcome, Query2Outcome> {

    @Override
    public Result2 createAccumulator() {
        return new Result2();
    }

    @Override
    public Result2 add(FirstResult2 firstResult2, Result2 result2) {
        result2.add(firstResult2);
        return result2;
    }

    @Override
    public Result2 merge(Result2 accumulator1, Result2 accumulator2) {
        accumulator2.getAm3().forEach(accumulator1::amAdd);
        accumulator2.getPm3().forEach(accumulator1::pmAdd);
        return accumulator1;
    }

    @Override
    public Result2 getResult(Result2 result2) {
        return result2;
    }
}
