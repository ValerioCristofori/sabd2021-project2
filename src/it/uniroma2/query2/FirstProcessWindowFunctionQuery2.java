package it.uniroma2.query2;

import it.uniroma2.entity.FirstResult2;
import it.uniroma2.entity.Result1;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class FirstProcessWindowFunctionQuery2 extends ProcessWindowFunction<FirstResult2, FirstResult2, String, TimeWindow>{
    @Override
    public void process(String cella, Context context, Iterable<FirstResult2> iterable, Collector<FirstResult2> collector) throws Exception {
        FirstResult2 query2FirstResult = iterable.iterator().next();
        query2FirstResult.setTimestamp(new Date(context.window().getStart()));
        query2FirstResult.setCella(cella);

        collector.collect(query2FirstResult);
    }
}
