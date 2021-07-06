package it.uniroma2.main;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.Mappa;
import it.uniroma2.query1.Query1;
import it.uniroma2.query2.Query2;
import it.uniroma2.query3.Query3;
import it.uniroma2.kafka.KafkaHandler;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class FlinkMain {

    private static FlinkJedisPoolConfig conf;
    private static final SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"),
            new SimpleDateFormat("dd-MM-yy HH:mm")} ;


    public static void main( String[] args ){

        // faccio setup ambiente flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // creo Flink consumer per kafka
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(KafkaHandler.TOPIC_SOURCE, new SimpleStringSchema(), KafkaHandler.getProperties("consumer"));

        DataStream<EntryData> stream = env.addSource(consumer).map( new MapFunction<String, EntryData>() {
            @Override
            public EntryData map(String s) throws Exception {

                System.out.println(s);
                String[] records = s.split(",");
                Long timestamp = null;
                for (SimpleDateFormat dateFormat : dateFormats) {
                    try {
                        timestamp = dateFormat.parse(records[7]).getTime();
                        break;
                    } catch (ParseException ignored) {
                    }
                }
                if (timestamp == null)
                    throw new NullPointerException();
                return new EntryData(records[0],Double.parseDouble(records[3]),
                        Double.parseDouble(records[4]), Integer.parseInt(records[1]), timestamp, records[10]);

            }
        }).name("source");

        //Query1.topology(stream);

        //Query2.topology(stream);

        Query3.topology(stream);

        try {
            env.execute("sabd2021-project2");
        } catch (Exception e) {
            e.printStackTrace();
        }

        /* Metriche: ci aspettiamo throughput alto e bassa latenza
        *
        * per fare tracking della latenza devo settare latencyTrackingInterval a un numero positivo nella Flink configuration o ExecutionConfig
        * al latencyTrackingInterval, le sorgenti emettono periodicamente il record LatencyMarker
        * che contiene un timestamp di quando il record è stato emesso
        *
        * https://www.programmersought.com/article/32946124698/
        *
        * */


    }

    public static FlinkJedisPoolConfig getConf() {
        return conf;
    }
}
