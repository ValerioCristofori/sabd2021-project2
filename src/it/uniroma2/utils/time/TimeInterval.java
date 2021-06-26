package it.uniroma2.utils.time;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

public interface TimeInterval {

    public String getTimeType();

    public TumblingEventTimeWindows getTimeIntervalClass();

}
