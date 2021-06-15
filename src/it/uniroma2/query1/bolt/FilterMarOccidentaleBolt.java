package it.uniroma2.query1.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Map;

public class FilterMarOccidentaleBolt extends BaseRichBolt {

    public static final String SHIP_ID 				    = "SHIP_ID";
    public static final String SHIPTYPE				    = "SHIPTYPE";
    public static final String SPEED	                = "SPEED";
    public static final String LON 	                    = "LON";
    public static final String LAT 				        = "LAT";
    public static final String TIMESTAMP 				= "TIMESTAMP";
    public static final String TRIP_ID			        = "TRIP_ID";

    private double initialLat 	= 32.0;
    private double finalLat 	= 45.0; 	// 40.125224422
    private double initialLon = -6.0;
    private double finalLon 	= 37.0;

    private static final long serialVersionUID = 1L;
    private OutputCollector _collector;
    private SimpleDateFormat sdf;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector=collector;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void execute(Tuple tuple) {
        String id 		    = tuple.getStringByField(SHIP_ID);
        String typeString 	= tuple.getStringByField(SHIPTYPE);
        String speedString 	= tuple.getStringByField(SPEED);
        String lonString  	= tuple.getStringByField(LON);
        String latString 	= tuple.getStringByField(LAT);
        String timestamp 	= tuple.getStringByField(TIMESTAMP);
        String trip_id  	= tuple.getStringByField(TRIP_ID);

        Double lon = null;
        Double lat = null;

        try{

            lon = Double.parseDouble(lonString);
            lat  = Double.parseDouble(latString);
            if (isOutsideGrid(lat, lon)){
                _collector.ack(tuple);
                return;
            }

        } catch(NumberFormatException e){
            _collector.ack(tuple);
            return;
        }


        Values values = new Values();
        values.add(id);
        values.add(typeString);
        values.add(speedString);
        values.add(lonString);
        values.add(latString);
        values.add(timestamp);
        values.add(trip_id);

        _collector.emit(values);
        _collector.ack(tuple);
    }

    private boolean isOutsideGrid(double latitude, double longitude){

        if (latitude > initialLat ||
                latitude < finalLat ||
                longitude < initialLon ||
                longitude > finalLon)

            return true;

        return false;

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(SHIP_ID, SHIPTYPE, SPEED, LON, LAT, TIMESTAMP,TRIP_ID));
    }
}
