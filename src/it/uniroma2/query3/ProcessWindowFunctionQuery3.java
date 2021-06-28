package it.uniroma2.query3;

import it.uniroma2.entity.Result2;
import it.uniroma2.query3.ranking.RankingTrip;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class ProcessWindowFunctionQuery3 extends ProcessAllWindowFunction<RankingTrip, RankingTrip, TimeWindow> {


    @Override
    public void process(Context context, Iterable<RankingTrip> iterable, Collector<RankingTrip> collector) throws Exception {
        RankingTrip query3Result = iterable.iterator().next();
        query3Result.setTimestamp(new Date(context.window().getStart()));
        collector.collect(query3Result);
    }
}
