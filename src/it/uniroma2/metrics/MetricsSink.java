package it.uniroma2.metrics;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MetricsSink implements SinkFunction<String> {

    @Override
    public void invoke(String value, Context context) throws Exception {
        RetrieveMetrics.increase();
    }
}
