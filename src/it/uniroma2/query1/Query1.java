package it.uniroma2.query1;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.Mappa;
import it.uniroma2.entity.Result1;
import it.uniroma2.utils.FlinkKafkaSerializer;
import it.uniroma2.kafka.KafkaHandler;
import it.uniroma2.utils.time.MonthWindowAssigner;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

public class Query1 {

    private final DataStream<Tuple2<Long, String>> dataStream;
    private Logger log;
    private final Properties prop;

    public Query1(DataStream<Tuple2<Long, String>> dataStream) {
        this.dataStream = dataStream;
        this.prop = KafkaHandler.getProperties("producer");
        this.run();
    }

    private void run() {

        DataStream<EntryData> stream = dataStream.map( (MapFunction<Tuple2<Long, String>, EntryData>) entry -> {
            String[] records = entry.f1.split(",");
            return new EntryData(records[0],Double.parseDouble(records[3]),
                    Double.parseDouble(records[4]), Integer.parseInt(records[1]), entry.f0, records[10]);
        });

        DataStream<EntryData> filteredMarOccidentaleStream = stream
                .filter( (FilterFunction<EntryData>) entry -> entry.getLon() < Mappa.getCanaleDiSiciliaLon())
                .name("filtered-stream");

        //week
        filteredMarOccidentaleStream.keyBy( EntryData::getCella )
                .window( TumblingEventTimeWindows.of(Time.days(7)) )
                .aggregate( new AggregatorQuery1(), new ProcessWindowFunctionQuery1())
                .map( (MapFunction<Result1, String>) resultQuery1 -> resultMap(Calendar.DAY_OF_WEEK,resultQuery1)).name( "query1-weekly")
                .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY1_WEEKLY,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY1_WEEKLY),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+ KafkaHandler.TOPIC_QUERY1_WEEKLY);

        //month
        filteredMarOccidentaleStream.keyBy( EntryData::getCella )
                .window( new MonthWindowAssigner() )
                .aggregate( new AggregatorQuery1(), new ProcessWindowFunctionQuery1())
                        .map( (MapFunction<Result1, String>) resultQuery1 -> resultMap(Calendar.DAY_OF_MONTH,resultQuery1)).name( "query1-monthly")
                        .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY1_MONTHLY,
                                new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY1_MONTHLY),
                                prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+ KafkaHandler.TOPIC_QUERY1_MONTHLY);



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
