package it.uniroma2.query3;

import it.uniroma2.query3.ranking.Trip;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class ProcessWindowFunctionQuery3 extends ProcessWindowFunction<Trip, Trip, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Trip> iterable, Collector<Trip> collector) throws Exception {
        Trip trip = iterable.iterator().next();
        trip.setTripId(s);
        trip.setTimestamp(new Date(context.window().getStart()));
        collector.collect(trip);
    }
}
