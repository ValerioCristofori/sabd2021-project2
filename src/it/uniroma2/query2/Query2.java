package it.uniroma2.query2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Query2 {

    private DataStream<Tuple2<Long, String>> dataStream;

    public Query2(DataStream<Tuple2<Long, String>> dataStream) {
        this.dataStream = dataStream;
        this.run();
    }

    private void run() {

    }
}
