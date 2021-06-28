package it.uniroma2.query3;

import it.uniroma2.entity.EntryData;
import it.uniroma2.query3.ranking.RankingTrip;
import it.uniroma2.query3.ranking.Trip;
import it.uniroma2.utils.FlinkKafkaSerializer;
import it.uniroma2.utils.KafkaHandler;
import it.uniroma2.utils.time.OneHourWindowAssigner;
import it.uniroma2.utils.time.TwoHourWindowAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Date;
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
        // stream di trip aggregati sommando le distanze
        SingleOutputStreamOperator<Trip> windowedTrip = windowedStream
                .aggregate( new FirstAggregatorQuery3(), new FirstProcessWindowFunctionQuery3());

        AllWindowedStream<Trip, TimeWindow> windowedStreamGlobalRank = null;
        if( timeIntervalType.equals("one-hour") ){
            windowedStreamGlobalRank = windowedTrip.windowAll( new OneHourWindowAssigner() );
        }else if( timeIntervalType.equals("two-hour") ){
            windowedStreamGlobalRank = windowedTrip.windowAll( new TwoHourWindowAssigner() );
        }else{
            log.warning("Time interval not valid");
            System.exit(1);
        }
        // global rank
        windowedStreamGlobalRank.aggregate( new AggregatorQuery3(), new ProcessWindowFunctionQuery3())
            .map( (MapFunction<RankingTrip, String>) rank -> {
                StringBuilder entryResultBld = new StringBuilder();
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm");
                Date timestampInit = rank.getTimestamp();
                entryResultBld.append(simpleDateFormat.format(timestampInit));

                for( int i=0; i<rank.getRanking().size();i++ ){
                    entryResultBld.append(",").append( rank.getRanking().get(i).getTripId() ).append(",").append(i+1);
                }
                return entryResultBld.toString();
        }).name( "query3-"+this.timeIntervalType)
                .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY3 + this.timeIntervalType,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY3 + this.timeIntervalType),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
    }
}
