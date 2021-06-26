package it.uniroma2.query1;

import it.uniroma2.utils.time.MonthTimeInterval;
import it.uniroma2.utils.time.TimeInterval;
import it.uniroma2.utils.time.WeekTimeInterval;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.logging.Logger;

public class Query1 {

    private DataStream<Tuple2<Long, String>> dataStream;
    private TimeInterval timeInterval;
    private Logger log;

    public Query1(DataStream<Tuple2<Long, String>> dataStream, String timeIntervalType) {
        this.dataStream = dataStream;
        if( timeIntervalType.equals("week") ){
            this.timeInterval = new WeekTimeInterval();
        }else if( timeIntervalType.equals("month") ){
            this.timeInterval = new MonthTimeInterval();
        }else{
            log.warning("Time interval not valid");
            System.exit(1);
        }
        this.run();
    }

    private void run() {

    }


}
