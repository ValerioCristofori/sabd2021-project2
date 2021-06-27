package it.uniroma2.main;

import it.uniroma2.entity.Mappa;
import it.uniroma2.query1.Query1;
import it.uniroma2.query2.Query2;
import it.uniroma2.query3.Query3;
import it.uniroma2.utils.KafkaHandler;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class FlinkMain {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yy HH:mm");
    private static StreamExecutionEnvironment env;


    private static DataStream<Tuple2<Long,String>> setup(){

        // faccio setup ambiente flink
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // creo Flink consumer per kafka
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(KafkaHandler.TOPIC_SOURCE, new SimpleStringSchema(), KafkaHandler.getProperties("consumer"));
        // assegno i watermarks per la granularita' del minuto
        consumer.assignTimestampsAndWatermarks( WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)) );

        return env.addSource(consumer).flatMap( new Splitter() ).name("source");

    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<Long, String>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<Long,String>> out) throws Exception {
            String[] records = sentence.split(",");
            Long timestamp = sdf.parse(records[7]).getTime();
            out.collect(new Tuple2<>(timestamp,sentence));
        }
    }

    public static void main( String[] args ){

        DataStream<Tuple2<Long,String>> dataStream = setup(); // tupla: timestamp, list record
        Mappa.setup(); //setup della mappa per il calcolo della dimensione delle celle

        new Query1(dataStream, "week");
        new Query1(dataStream, "month");

        //new Query2(dataStream);

        //new Query3(dataStream,"one-hour");
        //new Query3(dataStream,"two-hour");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
