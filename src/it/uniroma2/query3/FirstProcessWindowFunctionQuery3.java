package it.uniroma2.query3;

import it.uniroma2.query3.ranking.Trip;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class FirstProcessWindowFunctionQuery3 extends ProcessWindowFunction<Tuple2<Long, Double> , Tuple3<String, Long, Double>, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Tuple2<Long, Double>> iterable, Collector<Tuple3<String, Long, Double>> collector) {
        Tuple2<Long, Double> result = iterable.iterator().next();
        collector.collect(new Tuple3<>(s, result.f0, result.f1));
    }
}
