package it.uniroma2.query1;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.Mappa;
import it.uniroma2.entity.Result1;
import it.uniroma2.kafka.KafkaHandler;
import it.uniroma2.metrics.MetricsSink;
import it.uniroma2.utils.FlinkKafkaSerializer;
import it.uniroma2.utils.time.MonthWindowAssigner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Query1:
 *      Filtro per il mar Occidentale
 *      Sia per finestre da 1 settimana che finestre da un mese
 *      tengo traccia per ogni cella di una mappa contenente shipType e set di tripId
 *      Per ricavare il numero di navi con viaggi diversi che attraversano la stessa cella restituisco la lunghezza del set.
 */

public class Query1 {

    public static void topology(DataStream<EntryData> dataStream) {

        Properties prop = KafkaHandler.getProperties("producer");

        // filtro per le celle del mar Occidentale
        DataStream<EntryData> filteredStream = dataStream
                .filter( entry -> entry.getLon() < Mappa.getCanaleDiSiciliaLon())
                .name("filtered-stream");

        //week
        SingleOutputStreamOperator<String> resultStreamWeek = filteredStream
                .keyBy( EntryData::getCella ) // keyed stream attraverso la cella
                .window( TumblingEventTimeWindows.of(Time.days(7)) ) // faccio finestre di 1 settimana
                .aggregate( new AggregatorQuery1(), new ProcessWindowFunctionQuery1()).name("Query1-weekly") // aggrego: map<shipType,count>
                .map( resultQuery1 -> resultMap(Calendar.DAY_OF_WEEK,resultQuery1)); // restituisco una stringa ben formattata


        resultStreamWeek.addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY1_WEEKLY,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY1_WEEKLY),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY1_WEEKLY);
        //resultStreamWeek.addSink(new MetricsSink());

        //month
        SingleOutputStreamOperator<String> resultStreamMonth = filteredStream
                .keyBy( EntryData::getCella ) // keyed stream attraverso la cella
                .window( new MonthWindowAssigner() ) // faccio finestre di 1 mese attraverso l'assigner
                .aggregate( new AggregatorQuery1(), new ProcessWindowFunctionQuery1()).name("Query1-monthly") // aggrego: map<shipType,count>
                .map( resultQuery1 -> resultMap(Calendar.DAY_OF_MONTH,resultQuery1)); // restituisco una stringa ben formattata


        resultStreamMonth.addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY1_MONTHLY,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY1_MONTHLY),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY1_MONTHLY);
        //resultStreamMonth.addSink(new MetricsSink());

    }



    private static String resultMap(int days, Result1 resultQuery1) {
        StringBuilder entryResultBld = new StringBuilder();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date timestampInit = resultQuery1.getTimestamp();
        double d;
        entryResultBld.append(simpleDateFormat.format(timestampInit))
                .append(",")
                .append(resultQuery1.getCella());
        for(int i=0; i< Mappa.getShipTypes().length ; i++){
            String type = Mappa.getShipTypes()[i];
            Integer v = resultQuery1.getResultMap().get(type);
            if(v == null){
                entryResultBld.append(",").append(type).append(",");
            } else {
                d = ((double)v)  / days;
                entryResultBld.append(",").append(type).append(",").append(String.format(Locale.ENGLISH,"%.2f",d));
            }
        }
        return entryResultBld.toString();
    }


}
