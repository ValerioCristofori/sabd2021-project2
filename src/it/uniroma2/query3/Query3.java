package it.uniroma2.query3;

import it.uniroma2.entity.EntryData;
import it.uniroma2.query3.ranking.RankingTrip;
import it.uniroma2.query3.ranking.Trip;
import it.uniroma2.utils.FlinkKafkaSerializer;
import it.uniroma2.kafka.KafkaHandler;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

public class Query3 {
    // trip id: 0xc35c9_10-03-15 12:xx - 10-03-15 13:26
    private static final SimpleDateFormat secondDateFormat = new SimpleDateFormat("dd-MM-yy HH:mm");
    private Logger log;

    public static void topology(DataStream<EntryData> dataStream) {

        Properties prop = KafkaHandler.getProperties("producer");
        // keyed stream
        KeyedStream<EntryData, String> keyedStream = dataStream.keyBy( EntryData::getTripId );

        //one hour
        keyedStream.window(EventTimeSessionWindows.withDynamicGap(new ExtractorSessionWindow()))
                    .trigger( WindowTrigger.createInstance() )
                    .aggregate( new FirstAggregatorQuery3(), new FirstProcessWindowFunctionQuery3()).name("Query3-oneHour-first")
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Double>>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.f1).withIdleness(Duration.ofMillis(1)))
                    .windowAll( TumblingEventTimeWindows.of(Time.hours(1)) )
                    // global rank
                    .aggregate( new AggregatorQuery3(), new ProcessWindowFunctionQuery3()).name("Query3-oneHour-second")
                    .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY3_ONEHOUR,
                            new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY3_ONEHOUR),
                            prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY3_ONEHOUR);

/*
        //two hour
        keyedStream.window( EventTimeSessionWindows.withDynamicGap(new ExtractorSessionWindow()))
                    .trigger( WindowTrigger.createInstance() )
                    .aggregate( new FirstAggregatorQuery3(), new FirstProcessWindowFunctionQuery3()).name("Query3-twoHour-first")
                    //.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Double>>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.f1).withIdleness(Duration.ofMillis(35)))
                    .windowAll( TumblingEventTimeWindows.of(Time.hours(2)) )
                    // global rank
                    .aggregate( new AggregatorQuery3(), new ProcessWindowFunctionQuery3()).name("Query3-twoHour-second")
                        .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY3_TWOHOUR,
                                new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY3_TWOHOUR),
                                prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY3_TWOHOUR);

*/


    }

    private static class ExtractorSessionWindow implements SessionWindowTimeGapExtractor<EntryData> {

        private static final long timeGap = 60*1000*10;


        @Override
        public long extract(EntryData entryData) {
            String tripId = entryData.getTripId();
            String secondDate;
            if (tripId.contains("_parking")) {
                secondDate = tripId.substring(tripId.indexOf(" - ")+3, tripId.indexOf("_parking"));
            } else {
                secondDate = tripId.substring(tripId.indexOf(" - ")+3);
            }

            long secondTime = 0;
            long actualTime = entryData.getTimestamp();
            try {
                secondTime = secondDateFormat.parse(secondDate).getTime();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            return Math.max(secondTime - actualTime + timeGap, timeGap);
        }
    }



}
