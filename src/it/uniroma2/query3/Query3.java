package it.uniroma2.query3;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.FirstResult2;
import it.uniroma2.query3.ranking.RankingTrip;
import it.uniroma2.utils.FlinkKafkaSerializer;
import it.uniroma2.kafka.KafkaHandler;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

public class Query3 {
    private final DataStream<Tuple2<Long, String>> dataStream;
    private Logger log;
    private final Properties prop;

    public Query3(DataStream<Tuple2<Long, String>> dataStream) {
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

        // keyed stream
        KeyedStream<EntryData, String> keyedStream = stream.keyBy( EntryData::getTripId );

        //one hour
        keyedStream.window( TumblingEventTimeWindows.of(Time.hours(1)) )
                    // stream di trip aggregati sommando le distanze
                    .aggregate( new FirstAggregatorQuery3(), new FirstProcessWindowFunctionQuery3())
                    .windowAll( TumblingEventTimeWindows.of(Time.hours(1)) )
                    // global rank
                    .aggregate( new AggregatorQuery3(), new ProcessWindowFunctionQuery3())
                    .map( (MapFunction<RankingTrip, String>) rank -> resultMap(rank)).name( "query3-oneHour")
                        .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY3_ONEHOUR,
                                new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY3_ONEHOUR),
                                prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY3_ONEHOUR);;
        //two hour
        keyedStream.window( TumblingEventTimeWindows.of(Time.hours(2)) )
                    // stream di trip aggregati sommando le distanze
                    .aggregate( new FirstAggregatorQuery3(), new FirstProcessWindowFunctionQuery3())
                    .windowAll( TumblingEventTimeWindows.of(Time.hours(2)) )
                    // global rank
                    .aggregate( new AggregatorQuery3(), new ProcessWindowFunctionQuery3())
                    .map( (MapFunction<RankingTrip, String>) rank -> resultMap(rank)).name( "query3-twoHour")
                        .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY3_TWOHOUR,
                                new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY3_TWOHOUR),
                                prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY3_TWOHOUR);




    }

    private static String resultMap(RankingTrip rank) {
        StringBuilder entryResultBld = new StringBuilder();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm");
        Date timestampInit = rank.getTimestamp();
        entryResultBld.append(simpleDateFormat.format(timestampInit));

        for( int i=0; i<rank.getRanking().size();i++ ){
            entryResultBld.append(",").append( rank.getRanking().get(i).getTripId() ).append(",").append(i+1);
        }
        return entryResultBld.toString();
    }

}
