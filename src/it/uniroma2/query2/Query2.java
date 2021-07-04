package it.uniroma2.query2;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.FirstResult2;
import it.uniroma2.entity.Result2;
import it.uniroma2.utils.FlinkKafkaSerializer;
import it.uniroma2.kafka.KafkaHandler;
import it.uniroma2.utils.time.MonthWindowAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
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

public class Query2 {

    private final DataStream<Tuple2<Long, String>> dataStream;
    private Logger log;
    private final Properties prop;

    public Query2(DataStream<Tuple2<Long, String>> dataStream) {
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

        KeyedStream<FirstResult2, Tuple2<String, Integer>> firstKeyedStream = stream.keyBy(EntryData::getCella)
                .window(TumblingEventTimeWindows.of(Time.hours(12)))
                // lista dei tripId per ogni cella e fascia oraria
                .aggregate(new FirstAggregatorQuery2(), new FirstProcessWindowFunctionQuery2())
                // divido il flusso attraverso coppie dove Stringa = cella e Integer = ora del giorno
                .keyBy(new KeySelector<FirstResult2, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(FirstResult2 value) throws Exception {
                        return new Tuple2<>(value.getCella(), value.getHour());
                    }
                });

        // week
        firstKeyedStream.window( TumblingEventTimeWindows.of(Time.days(7)) )
                // per ogni cella e ogni ora trovo il grado di frequentazione totale
                .reduce(new ReduceFunction<FirstResult2>() {
                    @Override
                    public FirstResult2 reduce(FirstResult2 value1, FirstResult2 value2) {
                        value1.setFrequentazione(value1.getFrequentazione()+value2.getFrequentazione());
                        return value1;
                    }
                })
                .keyBy(FirstResult2::getMare)
                .window( TumblingEventTimeWindows.of(Time.days(7)) )
                .aggregate(new AggregatorQuery2(), new ProcessWindowFunctionQuery2())
                .map((MapFunction<Result2, String>) resultQuery2 -> resultMap(resultQuery2)).name( "query2-weekly")
                .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY2_WEEKLY,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY2_WEEKLY),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY2_WEEKLY);

        // month
        firstKeyedStream.window( new MonthWindowAssigner() )
                // per ogni cella e ogni ora trovo il grado di frequentazione totale
                .reduce(new ReduceFunction<FirstResult2>() {
                    @Override
                    public FirstResult2 reduce(FirstResult2 value1, FirstResult2 value2) {
                        value1.setFrequentazione(value1.getFrequentazione()+value2.getFrequentazione());
                        return value1;
                    }
                })
                .keyBy(FirstResult2::getMare)
                .window( new MonthWindowAssigner() )
                .aggregate(new AggregatorQuery2(), new ProcessWindowFunctionQuery2())
                .map((MapFunction<Result2, String>) resultQuery2 -> resultMap(resultQuery2)).name( "query2-monthly")
                .addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY2_MONTHLY,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY2_MONTHLY),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY2_MONTHLY);

    }

    private static String resultMap(Result2 resultQuery2) {
        StringBuilder entryResultBld = new StringBuilder();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date timestampInit = resultQuery2.getTimestamp();
        entryResultBld.append(simpleDateFormat.format(timestampInit))
                .append(",")
                .append(resultQuery2.getMare()).append(",")
                .append("00:00-11:59");

        for(FirstResult2 res : resultQuery2.getMattinaTop3()){
            entryResultBld.append(",").append(res.getCella());
        }

        entryResultBld.append(",").append("12:00-23:59");

        for(FirstResult2 res : resultQuery2.getPomeriggioTop3()){
            entryResultBld.append(",").append(res.getCella());
        }

        return entryResultBld.toString();
    }

}
