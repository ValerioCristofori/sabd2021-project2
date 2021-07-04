package it.uniroma2.query1;

import it.uniroma2.entity.Result1;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class ProcessWindowFunctionQuery1 extends ProcessWindowFunction<Result1, Result1, String, TimeWindow> {

    @Override
    public void process(String cella, Context context, Iterable<Result1> iterable, Collector<Result1> collector){
        Result1 query1Result = iterable.iterator().next();
        query1Result.setTimestamp(new Date(context.window().getStart())); // prendo il timestamp di inizio della finestra per il risultato
        query1Result.setCella(cella);
        collector.collect(query1Result);
    }
}
