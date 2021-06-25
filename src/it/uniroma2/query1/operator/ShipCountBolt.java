package it.uniroma2.query1.operator;

import it.uniroma2.entity.Nave;
import it.uniroma2.utils.DifferentShipTypes;
import it.uniroma2.utils.Window;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class ShipCountBolt extends BaseRichBolt {

    public static final String MSGID				    = "MSGID";
    public static final String CELLA 				    = "CELLA";
    public static final String TIMESTAMP 				= "TIMESTAMP";
    public static final String COUNT 				    = "COUNT";
    public static final String TYPE 				    = "SHIPTYPE";
    private static final int  WINDOW_SIZE_MONTH 		= 30 * 24 * 60;
    private static final int  WINDOW_SIZE_WEEK 		    = 7 * 24 * 60;
    private static final double MIN_IN_MS 		= 60 * 1000;


    private static final long serialVersionUID = 1L;
    private long latestCompletedTimeframeMonth;
    private long latestCompletedTimeframeWeek;

    Map<String, Window> windowPerCellaMonth; // K e' la cella, V e' la finestra dell'ultimo mese
    Map<String, Window> windowPerCellaWeek;  // K e' la cella, V e' la finestra dell'ultima settimana
    private OutputCollector _collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        this.latestCompletedTimeframeMonth = 0;
        this.latestCompletedTimeframeWeek = 0;
        this.windowPerCellaMonth = new HashMap<>();
        this.windowPerCellaWeek = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(MetronomeBolt.S_METRONOME)){
            String timeType = tuple.getStringByField( MetronomeBolt.TIME_TYPE );
            if( timeType.equals("month") ){
                handleMetronomeMessageMonth(tuple);  //sliding window based on event time
            }else if( timeType.equals("week") ){
                handleMetronomeMessageWeek(tuple);  //sliding window based on event time
            }else{
                System.exit(1);
            }
        } else {

            handleCountMonth(tuple);
            handleCountWeek(tuple);

        }


    }


    private void handleMetronomeMessageMonth(Tuple tuple) {
        String msgId 			= tuple.getStringByField(MetronomeBolt.MSGID);
        String time		 		= tuple.getStringByField(MetronomeBolt.TIME);
        String timestamp 		= tuple.getStringByField(MetronomeBolt.TIMESTAMP);

        long latestTimeframe = roundToCompletedMonth(time);


        if ( this.latestCompletedTimeframeMonth < latestTimeframe){

            int elapsedMinutes = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeMonth) / (MIN_IN_MS));
            List<String> expiredTuples = new ArrayList<String>();

            for (String r : windowPerCellaMonth.keySet()){

                Window w = windowPerCellaMonth.get(r);
                if (w == null){
                    continue;
                }

                w.moveForward(elapsedMinutes);
                String rCount = String.valueOf(w.getEstimatedTotal());

                /* Reduce memory by removing windows with no data */
                if (w.getEstimatedTotal() == 0)
                    expiredTuples.add(r);

                Values v = new Values();
                v.add(msgId);
                v.add(r);
                v.add(rCount);
                v.add(timestamp);
                _collector.emit(v);
            }

            /* Reduce memory by removing windows with no data */
            for (String r : expiredTuples){
                windowPerCellaMonth.remove(r);
            }

            this.latestCompletedTimeframeMonth = latestTimeframe;

        }

        _collector.ack(tuple);
    }


    private void handleMetronomeMessageWeek(Tuple tuple) {
        String msgId 			= tuple.getStringByField(MetronomeBolt.MSGID);
        String time		 		= tuple.getStringByField(MetronomeBolt.TIME);
        String timestamp 		= tuple.getStringByField(MetronomeBolt.TIMESTAMP);

        long latestTimeframe = roundToCompletedWeek(time);


        if ( this.latestCompletedTimeframeWeek < latestTimeframe){

            int elapsedMinutes = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeWeek) / (MIN_IN_MS));
            List<String> expiredTuples = new ArrayList<String>();

            for (String r : windowPerCellaWeek.keySet()){

                Window w = windowPerCellaWeek.get(r);
                if (w == null){
                    continue;
                }

                w.moveForward(elapsedMinutes);
                String rCount = String.valueOf(w.getEstimatedTotal());

                /* Reduce memory by removing windows with no data */
                if (w.getEstimatedTotal() == 0)
                    expiredTuples.add(r);

                Values v = new Values();
                v.add(msgId);
                v.add(r);
                v.add(rCount);
                v.add(timestamp);
                _collector.emit(v);
            }

            /* Reduce memory by removing windows with no data */
            for (String r : expiredTuples){
                windowPerCellaWeek.remove(r);
            }

            this.latestCompletedTimeframeWeek = latestTimeframe;

        }

        _collector.ack(tuple);
    }



    private void handleCountMonth(Tuple tuple) {

        String msgId 			= tuple.getStringByField(ParserAndFilterBolt.MSGID);
        String shipId 			= tuple.getStringByField(ParserAndFilterBolt.SHIP_ID);
        String type 			= tuple.getStringByField(ParserAndFilterBolt.SHIPTYPE);
        String cella 			= tuple.getStringByField(ParserAndFilterBolt.CELLA);
        String timestamp 		= tuple.getStringByField(ParserAndFilterBolt.TIMESTAMP);

        long latestTimeframeMonth = roundToCompletedMonth(timestamp); //utile(?)*****************

        //MONTH
        if (this.latestCompletedTimeframeMonth < latestTimeframeMonth){

            int elapsedMinutes = (int) Math.ceil((latestTimeframeMonth - this.latestCompletedTimeframeMonth) / (MIN_IN_MS));
            List<String> expiredRoutes = new ArrayList<>();

            for (String r : windowPerCellaMonth.keySet()){

                Window w = windowPerCellaMonth.get(r);
                if (w == null){
                    continue;
                }

                w.moveForward(elapsedMinutes);
                String rCount = String.valueOf(w.getEstimatedTotal());

                /* Emit the count of the current route after the update*/
                if (r.equals(cella))
                    continue;

                /* Reduce memory by removing windows with no data */
                if (w.getEstimatedTotal() == 0)
                    expiredRoutes.add(r);

                Values v = new Values();
                v.add(msgId);
                v.add(cella);
                v.add(rCount);
                v.add(type);
                v.add(timestamp);

                _collector.emit(v);

            }

            /* Reduce memory by removing windows with no data */
            for (String r : expiredRoutes){
                windowPerCellaMonth.remove(r);
            }

            this.latestCompletedTimeframeMonth = latestTimeframeMonth;

        }


        /* Time has not moved forward. Update and emit count */
        Window w = windowPerCellaMonth.get(cella);
        if (w == null){
            w = new Window(WINDOW_SIZE_MONTH);
            windowPerCellaMonth.put(cella, w);
        }

        w.increment();

        /* Retrieve route frequency in the last 30 mins */
        String count = String.valueOf(w.getEstimatedTotal());

        Values v = new Values();
        v.add(msgId);
        v.add(cella);
        v.add(count);
        v.add(type);
        v.add(timestamp);

        _collector.emit(v);
        _collector.ack(tuple);
    }

    private void handleCountWeek(Tuple tuple) {
        String msgId 			= tuple.getStringByField(ParserAndFilterBolt.MSGID);
        String shipId 			= tuple.getStringByField(ParserAndFilterBolt.SHIP_ID);
        String type 			= tuple.getStringByField(ParserAndFilterBolt.SHIPTYPE);
        String cella 			= tuple.getStringByField(ParserAndFilterBolt.CELLA);
        String timestamp 		= tuple.getStringByField(ParserAndFilterBolt.TIMESTAMP);

        long latestTimeframeWeek = roundToCompletedWeek(timestamp);

        //MONTH
        if (this.latestCompletedTimeframeWeek < latestTimeframeWeek){

            int elapsedMinutes = (int) Math.ceil((latestTimeframeWeek - this.latestCompletedTimeframeWeek) / (MIN_IN_MS));
            List<String> expiredRoutes = new ArrayList<>();

            for (String r : windowPerCellaWeek.keySet()){

                Window w = windowPerCellaWeek.get(r);
                if (w == null){
                    continue;
                }

                w.moveForward(elapsedMinutes);
                String rCount = String.valueOf(w.getEstimatedTotal());

                /* Emit the count of the current route after the update*/
                if (r.equals(cella))
                    continue;

                /* Reduce memory by removing windows with no data */
                if (w.getEstimatedTotal() == 0)
                    expiredRoutes.add(r);

                Values v = new Values();
                v.add(msgId);
                v.add(cella);
                v.add(rCount);
                v.add(type);
                v.add(timestamp);

                _collector.emit(v);

            }

            /* Reduce memory by removing windows with no data */
            for (String r : expiredRoutes){
                windowPerCellaWeek.remove(r);
            }

            this.latestCompletedTimeframeWeek = latestTimeframeWeek;

        }


        /* Time has not moved forward. Update and emit count */
        Window w = windowPerCellaWeek.get(cella);
        if (w == null){
            w = new Window(WINDOW_SIZE_MONTH);
            windowPerCellaWeek.put(cella, w);
        }

        w.increment();

        /* Retrieve route frequency in the last 30 mins */
        String count = String.valueOf(w.getEstimatedTotal());

        Values v = new Values();
        v.add(msgId);
        v.add(cella);
        v.add(count);
        v.add(type);
        v.add(timestamp);

        _collector.emit(v);
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

        declarer.declare(new Fields( MSGID, TIMESTAMP, CELLA, COUNT));

    }
}
