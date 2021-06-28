package it.uniroma2.query3;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.Result3;
import it.uniroma2.query3.ranking.Trip;
import it.uniroma2.utils.KafkaHandler;
import it.uniroma2.utils.time.OneHourWindowAssigner;
import it.uniroma2.utils.time.TwoHourWindowAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Calendar;
import java.util.Properties;
import java.util.logging.Logger;

public class Query3 {
    private DataStream<Tuple2<Long, String>> dataStream;
    private String timeIntervalType;
    private Logger log;
    private Properties prop;

    public Query3(DataStream<Tuple2<Long, String>> dataStream, String timeIntervalType) {
        this.dataStream = dataStream;
        this.timeIntervalType = timeIntervalType;
        this.prop = KafkaHandler.getProperties("producer");
        this.run();
    }

    private void run() {

        DataStream<EntryData> stream = dataStream.map( (MapFunction<Tuple2<Long, String>, EntryData>) entry -> {
            String[] records = entry.f1.split(",");
            return new EntryData(records[0],Double.parseDouble(records[3]),
                    Double.parseDouble(records[4]), Integer.parseInt(records[1]), entry.f0, records[10]);
        });

        // keyed and windowed stream
        WindowedStream<EntryData,String, TimeWindow> windowedStream = null;
        if( timeIntervalType.equals("one-hour") ){
            windowedStream = stream.keyBy( EntryData::getTripId )
                    .window( new OneHourWindowAssigner() );
        }else if( timeIntervalType.equals("two-hour") ){
            windowedStream = stream.keyBy( EntryData::getTripId )
                    .window( new TwoHourWindowAssigner() );
        }else{
            log.warning("Time interval not valid");
            System.exit(1);
        }

        windowedStream.aggregate( new AggregatorQuery3(), new ProcessWindowFunctionQuery3())
                ;
    }
}
