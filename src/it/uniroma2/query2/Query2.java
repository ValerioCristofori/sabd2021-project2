package it.uniroma2.query2;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.FirstResult2;
import it.uniroma2.entity.Result2;
import it.uniroma2.utils.FlinkKafkaSerializer;
import it.uniroma2.kafka.KafkaHandler;
import it.uniroma2.utils.time.MonthWindowAssigner;
import it.uniroma2.utils.time.WeekWindowAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

public class Query2 {

    private final DataStream<Tuple2<Long, String>> dataStream;
    private final String timeIntervalType;
    private int numDaysInterval;
    private String topic;
    private Logger log;
    private final Properties prop;

    public Query2(DataStream<Tuple2<Long, String>> dataStream, String timeIntervalType) {
        this.dataStream = dataStream;
        this.timeIntervalType = timeIntervalType;
        if (timeIntervalType.equals("weekly")){
            this.topic = KafkaHandler.TOPIC_QUERY2_WEEKLY;
        } else if (timeIntervalType.equals("monthly")){
            this.topic = KafkaHandler.TOPIC_QUERY2_MONTHLY;
        }
        this.prop = KafkaHandler.getProperties("producer");

        this.run();
    }

    private void run() {

        DataStream<EntryData> stream = dataStream.map((MapFunction<Tuple2<Long, String>, EntryData>) entry -> {
            String[] records = entry.f1.split(",");
            return new EntryData(records[10],records[0], Double.parseDouble(records[3]),
                    Double.parseDouble(records[4]), Integer.parseInt(records[1]), entry.f0, records[10]);
        });

        KeyedStream<FirstResult2, Tuple2<String, Integer>> firstKeyedStream = stream.keyBy(EntryData::getCella)
                .window(TumblingEventTimeWindows.of(Time.hours(12)))
                // lista dei tripId per ogni cella e fascia oraria
                .aggregate(new FirstAggregatorQuery2(), new FirstProcessWindowFunctionQuery2())
                // divido il flusso attraverso coppie dove Stringa = cella e Integer = ora del giorno
                .keyBy(new KeySelector<FirstResult2, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(FirstResult2 value) throws Exception {
                        return new Tuple2<String, Integer>(value.getCella(), value.getHour());
                    }
                });

        // per le due finestre temporali di una settimana e un mese
        WindowedStream<FirstResult2, Tuple2<String, Integer>, TimeWindow> firstWindowedStream = null;
        if( timeIntervalType.equals("weekly") ){
            firstWindowedStream = firstKeyedStream.window( new WeekWindowAssigner() );
        }else if( timeIntervalType.equals("monthly") ){
            firstWindowedStream = firstKeyedStream.window( new MonthWindowAssigner() );
        }else{
            log.warning("Time interval not valid");
            System.exit(1);
        }
        if( firstWindowedStream == null ){
            log.warning("Null error on windowed stream");
            System.exit(1);
        }

        // per ogni cella e ogni ora trovo il grado di frequentazione totale
        KeyedStream<FirstResult2, String> keyedStream = firstWindowedStream.reduce(new ReduceFunction<FirstResult2>() {
            @Override
            public FirstResult2 reduce(FirstResult2 value1, FirstResult2 value2) throws Exception {
                value1.setFrequentazione(value1.getFrequentazione()+value2.getFrequentazione());
                return value1;
            }
        })
                // divido il flusso in base al tipo di mare (orientale occidentale)
        .keyBy(FirstResult2::getMare);

        WindowedStream<FirstResult2, String, TimeWindow> windowedStream = null;
        if( timeIntervalType.equals("weekly") ){
            windowedStream = keyedStream.window( new WeekWindowAssigner() );
        }else if( timeIntervalType.equals("monthly") ){
            windowedStream = keyedStream.window( new MonthWindowAssigner() );
        }else{
            log.warning("Time interval not valid");
            System.exit(1);
        }
        if( windowedStream == null ){
            log.warning("Null error on windowed stream");
            System.exit(1);
        }

        // aggregazione finale per il ranking
        windowedStream.aggregate(new AggregatorQuery2(), new ProcessWindowFunctionQuery2())
                .map((MapFunction<Result2, String>) resultQuery2 -> {
            StringBuilder entryResultBld = new StringBuilder();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date timestampInit = resultQuery2.getTimestamp();
            entryResultBld.append(simpleDateFormat.format(timestampInit))
                .append(",")
                    .append(resultQuery2.getMare()).append(",")
                    .append("AM");

            for(FirstResult2 res : resultQuery2.getAm3()){
                entryResultBld.append(",").append(res.getCella());
            }

            entryResultBld.append(",").append("PM");

            for(FirstResult2 res : resultQuery2.getPm3()){
                entryResultBld.append(",").append(res.getCella());
            }

            return entryResultBld.toString();

            }).name( "query2-"+this.timeIntervalType)
            .addSink(new FlinkKafkaProducer<>(this.topic,
                    new FlinkKafkaSerializer(this.topic),
                    prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+this.topic);

    }

}
