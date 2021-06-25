package it.uniroma2.query1.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

public class MetronomeBolt extends BaseRichBolt {

    public static final String S_METRONOME 			    = "metronome";
    public static final String TIME				        = "time";
    public static final String MSGID				    = "MSGID";
    public static final String TIMESTAMP 				= "TIMESTAMP";
    public static final String TIME_TYPE 				= "TIME_TYPE";


    private static final long serialVersionUID = 1L;
    private OutputCollector _collector;

    private long currentTimeMonth = 0;
    private long currentTimeWeek = 0;
    private long latestMsgId = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        /* Emit message every (simulated) minute */
        String tMsgId 		= tuple.getStringByField(ParserAndFilterBolt.MSGID);
        String timestamp	= tuple.getStringByField(ParserAndFilterBolt.TIMESTAMP);

        long msgId = Long.valueOf(tMsgId);
        long timeMonth = roundToCompletedMonth(timestamp);
        long timeWeek = roundToCompletedWeek(timestamp);


        if (this.latestMsgId < msgId ){
            this.latestMsgId = msgId;

            if( this.currentTimeMonth < timeMonth ){
                this.currentTimeMonth = timeMonth;

                Values values = new Values();
                values.add(tMsgId);
                values.add(String.valueOf(timeMonth));
                values.add(timestamp);
                values.add("month");
                _collector.emit(S_METRONOME, values);   // To observe: event time

            }
            if( this.currentTimeWeek < timeWeek ){
                this.currentTimeWeek = timeWeek;

                Values values = new Values();
                values.add(tMsgId);
                values.add(String.valueOf(timeWeek));
                values.add(timestamp);
                values.add("week");
                _collector.emit(S_METRONOME, values);   // To observe: event time

            }





        } else {
            /* time did not go forward */

        }

        _collector.ack(tuple);
    }

    private long roundToCompletedMonth(String timestamp) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(Long.valueOf(timestamp)));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);

        return calendar.getTime().getTime();

    }


    private long roundToCompletedWeek(String timestamp) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(Long.valueOf(timestamp)));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.DAY_OF_WEEK, 1);

        return calendar.getTime().getTime();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(S_METRONOME, new Fields(MSGID, TIME, TIMESTAMP, TIME_TYPE));
    }
}
