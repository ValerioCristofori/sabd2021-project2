package it.uniroma2.main;

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
        // assegno i watermarks con la granularita' del minuto
        consumer.assignTimestampsAndWatermarks( WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)) );

        // Redis sink
         conf = new FlinkJedisPoolConfig.Builder().setHost("redis").setPort(6379).build();

        DataStream<Tuple2<Long,String>> dataStream = env.addSource(consumer).map( new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String s) throws Exception {

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
                return new Tuple2<>(timestamp,s);

            }
        }).name("source");

        new Query1(dataStream);

        new Query2(dataStream);

        new Query3(dataStream);

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
