package it.uniroma2.query3;

import it.uniroma2.entity.EntryData;
import it.uniroma2.metrics.MetricsSink;
import it.uniroma2.utils.FlinkKafkaSerializer;
import it.uniroma2.kafka.KafkaHandler;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Query3:
 *      Si divide il flusso per la chiave id del viaggio,
 *      successivamente sia per finestre da 1 ora che da 2 ore,
 *      si aggregano i dati per le finestre calcolando per ogni viaggio la distanza con il suo punto di partenza
 *      Infine, usando un operatore globale, stilo una classifica dei 5 viaggi con maggiore distanza
 *      sia per 1 che per 2 ore.
 */

public class Query3 {
    // trip id: 0xc35c9_10-03-15 12:xx - 10-03-15 13:26

    public static void topology(DataStream<EntryData> dataStream) {

        Properties prop = KafkaHandler.getProperties("producer");
        // keyed stream
        KeyedStream<EntryData, String> keyedStream = dataStream.keyBy( EntryData::getTripId );

        //one hour
        SingleOutputStreamOperator<String> resultStreamOneHour = keyedStream.window( TumblingEventTimeWindows.of( Time.hours(1)))
                .aggregate( new FirstAggregatorQuery3(), new FirstProcessWindowFunctionQuery3()) // calcolo distanza con la partenza per ogni viaggio
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate( new AggregatorQuery3(), new ProcessWindowFunctionQuery3()).name("Query3-oneHour-second"); // stilo una classifica globale


        resultStreamOneHour.addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY3_ONEHOUR,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY3_ONEHOUR),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY3_ONEHOUR);
        //resultStreamOneHour.addSink(new MetricsSink());

        //two hour
        SingleOutputStreamOperator<String> resultStreamTwoHour = keyedStream.window( TumblingEventTimeWindows.of( Time.hours(2)))
                .aggregate( new FirstAggregatorQuery3(), new FirstProcessWindowFunctionQuery3()) // calcolo distanza con la partenza per ogni viaggio
                .windowAll(TumblingEventTimeWindows.of(Time.hours(2)))
                .aggregate( new AggregatorQuery3(), new ProcessWindowFunctionQuery3()).name("Query3-twoHour-second"); // stilo una classifica globale


        resultStreamTwoHour.addSink(new FlinkKafkaProducer<>(KafkaHandler.TOPIC_QUERY3_TWOHOUR,
                        new FlinkKafkaSerializer(KafkaHandler.TOPIC_QUERY3_TWOHOUR),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Sink-"+KafkaHandler.TOPIC_QUERY3_TWOHOUR);
        //resultStreamTwoHour.addSink(new MetricsSink());




    }


}
