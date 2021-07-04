package it.uniroma2.query2;

import it.uniroma2.entity.FirstResult2;
import it.uniroma2.entity.Result1;
import it.uniroma2.entity.Result2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.naming.Context;
import java.util.Date;

public class ProcessWindowFunctionQuery2 extends ProcessWindowFunction<Result2, Result2, String, TimeWindow>{
    @Override
    public void process(String mare, Context context, Iterable<Result2> iterable, Collector<Result2> collector) {
        Result2 query2Result = iterable.iterator().next();
        query2Result.setTimestamp(new Date(context.window().getStart()));
        query2Result.setMare(mare);

        collector.collect(query2Result);
    }

}

