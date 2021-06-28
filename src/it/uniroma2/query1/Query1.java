package it.uniroma2.query1;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.Mappa;
import it.uniroma2.entity.Result1;
import it.uniroma2.utils.KafkaHandler;
import it.uniroma2.utils.time.MonthWindowAssigner;
import it.uniroma2.utils.time.WeekWindowAssigner;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

public class Query1 {

    private DataStream<Tuple2<Long, String>> dataStream;
    private String timeIntervalType;
    private int numDaysInteval;
    private Logger log;
    private Properties prop;

    public Query1(DataStream<Tuple2<Long, String>> dataStream, String timeIntervalType) {
        this.dataStream = dataStream;
        this.timeIntervalType = timeIntervalType;
        if( timeIntervalType.equals("week") ){
            this.numDaysInteval = Calendar.DAY_OF_WEEK;
        }else if( timeIntervalType.equals("month") ){
            this.numDaysInteval = Calendar.DAY_OF_MONTH;
        }
        this.prop = KafkaHandler.getProperties("producer");
        this.run();
    }

    private void run() {

        final int numDaysInterval = this.numDaysInteval;

        DataStream<EntryData> stream = dataStream.map( (MapFunction<Tuple2<Long, String>, EntryData>) entry -> {
            String[] records = entry.f1.split(",");
            return new EntryData(records[0],Double.parseDouble(records[3]),
                    Double.parseDouble(records[4]), Integer.parseInt(records[1]), entry.f0, records[10]);
        });

        DataStream<EntryData> filteredMarOccidentaleStream = stream
                .filter( (FilterFunction<EntryData>) entry -> entry.getLon() < Mappa.getCanaleDiSiciliaLon())
                .name("filtered-stream");

        // keyed and windowed stream
        WindowedStream<EntryData,String, TimeWindow> windowedStream = null;
        if( timeIntervalType.equals("week") ){
            windowedStream = filteredMarOccidentaleStream.keyBy( EntryData::getCella )
                    .window( new WeekWindowAssigner() );
        }else if( timeIntervalType.equals("month") ){
            windowedStream = filteredMarOccidentaleStream.keyBy( EntryData::getCella )
                    .window( new MonthWindowAssigner() );
        }else{
            log.warning("Time interval not valid");
            System.exit(1);
        }
        if( windowedStream == null ){
            log.warning("Null error on windowed stream");
            System.exit(1);
        }

        windowedStream.aggregate( new AggregatorQuery1(), new ProcessWindowFunctionQuery1())
                .map( (MapFunction<Result1, String>) resultQuery1 -> {
                    StringBuilder entryResultBld = new StringBuilder();
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    Date timestampInit = resultQuery1.getTimestamp();
                    entryResultBld.append(simpleDateFormat.format(timestampInit))
                            .append(",")
                            .append(resultQuery1.getCella())
                            .append(",");
                    resultQuery1.getResultMap().forEach( (key,value) -> {
                        entryResultBld.append(key).append(",").append(String.format( "%.2f", (double) value/numDaysInterval));

                    });
                    return entryResultBld.toString();
                } ).name( "query1-"+this.timeIntervalType)
                .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY1 + this.timeIntervalType,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY1 + this.timeIntervalType),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));



    }


}
