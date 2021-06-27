package it.uniroma2.query2;

import it.uniroma2.entity.EntryData;
import it.uniroma2.utils.KafkaHandler;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Calendar;
import java.util.Properties;
import java.util.logging.Logger;

public class Query2 {

    private DataStream<Tuple2<Long, String>> dataStream;
    private String timeIntervalType;
    private int numDaysInterval;
    private Logger log;
    private static final double canaleDiSiciliaLon = 12.0;
    private Properties prop;

    public Query2(DataStream<Tuple2<Long, String>> dataStream) {
        this.dataStream = dataStream;
        this.timeIntervalType = timeIntervalType;
        if (timeIntervalType.equals("week")){
            this.numDaysInterval = Calendar.DAY_OF_WEEK;
        } else if (timeIntervalType.equals("month")){
            this.numDaysInterval = Calendar.DAY_OF_MONTH;
        }
        this.prop = KafkaHandler.getProperties("producer");

        this.run();
    }

    private void run() {
        final int numDaysInterval = this.numDaysInterval;

        DataStream<EntryData> stream = dataStream.map((MapFunction<Tuple2<Long, String>, EntryData>) entry -> {
            String[] records = entry.f1.split(",");
            return new EntryData(records[10],records[0], Double.parseDouble(records[3]),
                    Double.parseDouble(records[4]), Integer.parseInt(records[1]), entry.f0, records[10]);
        });

//        DataStream<EntryData> filteredMarOccidentaleStream = stream
//                .filter( (FilterFunction<EntryData>) entry -> entry.getLon() < canaleDiSiciliaLon)
//                .name("filtered-occidentale-stream");
//        DataStream<EntryData> filteredMarOrientaleStream = stream
//                .filter((FilterFunction<EntryData>) entry -> entry.getLon() >= canaleDiSiciliaLon)
//                .name("filtered-orientale-stream");







    }
}
