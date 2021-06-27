package it.uniroma2.query2;

import it.uniroma2.entity.FirstResult2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

public class FirstProcessWindowFunctionQuery2 {
    @Override
    public void process(String cella, ProcessWindowFunction.Context context, Iterable<FirstResult2> iterable, Collector<FirstResult2> collector) throws Exception {
        FirstResult2 query2FirstResult = iterable.iterator().next();
        query2FirstResult.setTimestamp(new Date(context.window().getStart()));
        query2FirstResult.setCella(cella);
        collector.collect(query2FirstResult);
    }
}
