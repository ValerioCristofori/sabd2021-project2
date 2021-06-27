package it.uniroma2.query2;

import it.uniroma2.entity.FirstResult2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class ProcessWindowFunctionQuery2  {
    @Override
    public void process(String cella, Context context, Iterable<Result2> iterable, Collector<Result2> collector) {
        Result2 query2Result = iterable.iterator().next();
        query2Result.setDate(new Date(context.window().getStart()));
        query2Result.setMare(cella);
        collector.collect(query2Result);
    }

}

