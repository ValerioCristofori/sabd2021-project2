package it.uniroma2.query2;

import it.uniroma2.entity.EntryData;
import it.uniroma2.entity.FirstResult2;
import it.uniroma2.utils.KafkaHandler;
import it.uniroma2.utils.time.MonthTimeInterval;
import it.uniroma2.utils.time.WeekTimeInterval;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Calendar;
import java.util.Properties;
import java.util.logging.Logger;

public class Query2 {

    private DataStream<Tuple2<Long, String>> dataStream;
    private String timeIntervalType;
    private int numDaysInterval;
    private Logger log;
    private static final double canaleDiSiciliaLon = 12.0;
    private Properties prop;

    public Query2(DataStream<Tuple2<Long, String>> dataStream) {
        this.dataStream = dataStream;
        this.timeIntervalType = timeIntervalType;
        if (timeIntervalType.equals("week")){
            this.numDaysInterval = Calendar.DAY_OF_WEEK;
        } else if (timeIntervalType.equals("month")){
            this.numDaysInterval = Calendar.DAY_OF_MONTH;
        }
        this.prop = KafkaHandler.getProperties("producer");

        this.run();
    }

    private void run() {
        final int numDaysInterval = this.numDaysInterval;

        DataStream<EntryData> stream = dataStream.map((MapFunction<Tuple2<Long, String>, EntryData>) entry -> {
            String[] records = entry.f1.split(",");
            return new EntryData(records[10],records[0], Double.parseDouble(records[3]),
                    Double.parseDouble(records[4]), Integer.parseInt(records[1]), entry.f0, records[10]);
        });

//        DataStream<EntryData> filteredMarOccidentaleStream = stream
//                .filter( (FilterFunction<EntryData>) entry -> entry.getLon() < canaleDiSiciliaLon)
//                .name("filtered-occidentale-stream");
//        DataStream<EntryData> filteredMarOrientaleStream = stream
//                .filter((FilterFunction<EntryData>) entry -> entry.getLon() >= canaleDiSiciliaLon)
//                .name("filtered-orientale-stream");

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
        if( timeIntervalType.equals("week") ){
            firstWindowedStream = firstKeyedStream.window( new WeekTimeInterval.WeekWindowAssigner() );
        }else if( timeIntervalType.equals("month") ){
            firstWindowedStream = firstKeyedStream.window( new MonthTimeInterval.MonthWindowAssigner() );
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
        if( timeIntervalType.equals("week") ){
            windowedStream = keyedStream.window( new WeekTimeInterval.WeekWindowAssigner() );
        }else if( timeIntervalType.equals("month") ){
            windowedStream = keyedStream.window( new MonthTimeInterval.MonthWindowAssigner() );
        }else{
            log.warning("Time interval not valid");
            System.exit(1);
        }
        if( windowedStream == null ){
            log.warning("Null error on windowed stream");
            System.exit(1);
        }

        



    }




}
