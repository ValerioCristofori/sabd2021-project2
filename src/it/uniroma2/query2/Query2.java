package it.uniroma2.query2;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.FirstResult2;
import it.uniroma2.entity.Result2;
import it.uniroma2.metrics.MetricsSink;
import it.uniroma2.utils.FlinkKafkaSerializer;
import it.uniroma2.kafka.KafkaHandler;
import it.uniroma2.utils.time.MonthWindowAssigner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Query2:
 *      Dopo aver suddiviso il flusso di dati attraverso la chiave della cella,
 *      si creano finestre di 12 ore di timestramp e aggrego per trovare il grado di frequentazione
 *      per quella cella nella fascia oraria.
 *      Successivamente divido il flusso secondo la coppia < cella,ora> e, per la finestra da 1 settimana e da 1 mese,
 *      aggrego andando a ricavare il grado di frequentazione totale per una cella in un'ora.
 *      Successivamente stilo una classifica delle 3 celle piu' frequentate nelle fascie orarie.
 */

public class Query2 {

    public static void topology(DataStream<EntryData> dataStream) {

        Properties prop = KafkaHandler.getProperties("producer");

        KeyedStream<FirstResult2, Tuple2<String, Integer>> firstKeyedStream = dataStream
                                    .keyBy(EntryData::getCella)
                                    .window(TumblingEventTimeWindows.of(Time.hours(12)))
                                    // lista dei tripId per ogni cella e fascia oraria
                                    .aggregate(new FirstAggregatorQuery2(), new FirstProcessWindowFunctionQuery2()).name("Query2-AM/PM")
                                    // divido il flusso attraverso coppie  < cella , ora del giorno >
                                    .keyBy(new KeySelector<FirstResult2, Tuple2<String, Integer>>() {
                                        @Override
                                        public Tuple2<String, Integer> getKey(FirstResult2 value) throws Exception {
                                            return new Tuple2<>(value.getCella(), value.getHourOfTimestamp());
                                        }
                                    });

        // week
        SingleOutputStreamOperator<String> resultStreamWeek = firstKeyedStream.window( TumblingEventTimeWindows.of(Time.days(7)) )
                // faccio la somma di tutte le frequentazioni per quella cella e fascia oraria
                .reduce(Query2::reduce)
                .keyBy(FirstResult2::getMare) // divido il flusso per differenti mari
                .window( TumblingEventTimeWindows.of(Time.days(7)) )
                // aggrego il flusso per ogni finestra stilando una classifica delle celle piu' frequentate
                .aggregate(new AggregatorQuery2(), new ProcessWindowFunctionQuery2()).name("Query2-weekly")
                .map( Query2::resultMap); // formatto il risultato come output


        resultStreamWeek.addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY2_WEEKLY,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY2_WEEKLY),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY2_WEEKLY);
        //resultStreamWeek.addSink(new MetricsSink());


        // month
        SingleOutputStreamOperator<String> resultStreamMonth = firstKeyedStream.window( new MonthWindowAssigner() )
                // faccio la somma di tutte le frequentazioni per quella cella e fascia oraria
                .reduce(Query2::reduce)
                .keyBy(FirstResult2::getMare) // divido il flusso per differenti mari
                .window( new MonthWindowAssigner() )
                // aggrego il flusso per ogni finestra stilando una classifica delle celle piu' frequentate
                .aggregate(new AggregatorQuery2(), new ProcessWindowFunctionQuery2()).name("Query2-monthly")
                .map( Query2::resultMap ); // formatto il risultato come output


        resultStreamMonth.addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY2_MONTHLY,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY2_MONTHLY),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY2_MONTHLY);
        //resultStreamMonth.addSink(new MetricsSink());

    }

    private static String resultMap(Result2 resultQuery2) {
        StringBuilder entryResultBld = new StringBuilder();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date timestampInit = resultQuery2.getTimestamp();
        entryResultBld.append(simpleDateFormat.format(timestampInit))
                .append(",")
                .append(resultQuery2.getMare()).append(",")
                .append("00:00-11:59");

        for(int i=resultQuery2.getMattinaTop3().size()-1; i>=0; i--){
            entryResultBld.append(",").append(resultQuery2.getMattinaTop3().get(i).getCella());
        }

        entryResultBld.append(",").append("12:00-23:59");

        for(int i=resultQuery2.getPomeriggioTop3().size()-1; i>=0; i--){
            entryResultBld.append(",").append(resultQuery2.getPomeriggioTop3().get(i).getCella());
        }

        return entryResultBld.toString();
    }

    private static FirstResult2 reduce(FirstResult2 value1, FirstResult2 value2) {
        value1.setFrequentazione(value1.getFrequentazione() + value2.getFrequentazione());
        return value1;
    }

}
