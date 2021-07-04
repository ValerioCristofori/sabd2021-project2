package it.uniroma2.query2;

import it.uniroma2.entity.FirstResult2;
import it.uniroma2.entity.Result2;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AggregatorQuery2 implements AggregateFunction<FirstResult2, Result2, Result2> {

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
        accumulator2.getMattinaTop3().forEach(accumulator1::addMattina);
        accumulator2.getPomeriggioTop3().forEach(accumulator1::addPomeriggio);
        return accumulator1;
    }

    @Override
    public Result2 getResult(Result2 result2) {
        return result2;
    }
}
