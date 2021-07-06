package it.uniroma2.query1;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.Mappa;
import it.uniroma2.entity.Result1;
import it.uniroma2.kafka.KafkaHandler;
import it.uniroma2.utils.FlinkKafkaSerializer;
import it.uniroma2.utils.time.MonthWindowAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

public class Query1 {

    private Logger log;

    public static void topology(DataStream<EntryData> dataStream) {

        Properties prop = KafkaHandler.getProperties("producer");

        //DataStream<EntryData> filteredMarOccidentaleStream = dataStream
         //       .filter( entry -> entry.getLon() < Mappa.getCanaleDiSiciliaLon())
          //      .name("filtered-stream");

        //week
        dataStream.assignTimestampsAndWatermarks( WatermarkStrategy.<EntryData>forBoundedOutOfOrderness(Duration.ofMinutes(1)).withTimestampAssigner( (entry,timestamp) -> entry.getTimestamp()))
                .keyBy( EntryData::getCella )
                .window( TumblingEventTimeWindows.of(Time.days(7)) )
                .aggregate( new AggregatorQuery1(), new ProcessWindowFunctionQuery1()).name("Query1-weekly")
                .map( resultQuery1 -> resultMap(Calendar.DAY_OF_WEEK,resultQuery1))
                .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY1_WEEKLY,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY1_WEEKLY),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY1_WEEKLY);

        //month
        dataStream.assignTimestampsAndWatermarks( WatermarkStrategy.<EntryData>forBoundedOutOfOrderness(Duration.ofMinutes(1)).withTimestampAssigner( (entry,timestamp) -> entry.getTimestamp()))
                .keyBy( EntryData::getCella )
                .window( new MonthWindowAssigner() )
                .aggregate( new AggregatorQuery1(), new ProcessWindowFunctionQuery1()).name("Query1-monthly")
                .map( resultQuery1 -> resultMap(Calendar.DAY_OF_MONTH,resultQuery1))
                .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY1_MONTHLY,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY1_MONTHLY),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY1_MONTHLY);



    }



    private static String resultMap(int days, Result1 resultQuery1) {
        StringBuilder entryResultBld = new StringBuilder();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date timestampInit = resultQuery1.getTimestamp();
        entryResultBld.append(simpleDateFormat.format(timestampInit))
                .append(",")
                .append(resultQuery1.getCella())
                .append(",");
        resultQuery1.getResultMap().forEach( (key,value) -> {
            entryResultBld.append(key).append(",").append(String.format( "%.2f", (double) value/days));

        });
        System.out.println(entryResultBld);
        return entryResultBld.toString();
    }


}
