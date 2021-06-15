package it.uniroma2.query1.bolt;

import it.uniroma2.entity.Mappa;
import it.uniroma2.entity.Nave;
import it.uniroma2.entity.RottaNave;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ParserCellaBolt extends BaseRichBolt {

    public static final String SHIP_ID 				    = "SHIP_ID";
    public static final String SHIPTYPE				    = "SHIPTYPE";
    public static final String SPEED	                = "SPEED";
    public static final String LON 	                    = "LON";
    public static final String LAT 				        = "LAT";
    public static final String TIMESTAMP 				= "TIMESTAMP";
    public static final String TRIP_ID			        = "TRIP_ID";
    private OutputCollector _collector;
    private SimpleDateFormat sdf;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void execute(Tuple tuple) {
        String id 		= tuple.getStringByField(SHIP_ID);
        String typeString 	= tuple.getStringByField(SHIPTYPE);
        String speedString 	= tuple.getStringByField(SPEED);
        String lonString  	= tuple.getStringByField(LON);
        String latString 	= tuple.getStringByField(LAT);
        String timestamp 	= tuple.getStringByField(TIMESTAMP);
        String trip_id  	= tuple.getStringByField(TRIP_ID);


        Double lon = null;
        Double lat = null;
        Integer type = null;
        Double speed = null;

        try{

            lon = Double.parseDouble(lonString);
            lat  = Double.parseDouble(latString);
            type = Integer.valueOf(typeString);
            speed  = Double.parseDouble(speedString);

        } catch(NumberFormatException e){
            _collector.ack(tuple);
            return;
        }
        String dTimestamp = null;
        try {
            Date dDate = sdf.parse(timestamp);
            dTimestamp = String.valueOf(dDate.getTime());
        } catch (ParseException e) {
            _collector.ack(tuple);
            return;
        }

        RottaNave rotta = new RottaNave(lat,lon,speed,trip_id);
        Nave nave = Mappa.getNaveFromId(id);
        if( nave == null ){
            nave = new Nave( id, type);
            nave.getRoutes().put( dTimestamp, rotta);
            Mappa.addShip(nave);
        }else{
            nave.getRoutes().put( dTimestamp, rotta);
        }

        Values values = new Values();
        values.add(nave);

        _collector.emit(values);
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FilterMarOccidentaleBolt.SHIP_ID,
                FilterMarOccidentaleBolt.SHIPTYPE,
                FilterMarOccidentaleBolt.SPEED,
                FilterMarOccidentaleBolt.LON,
                FilterMarOccidentaleBolt.LAT,
                FilterMarOccidentaleBolt.TIMESTAMP,
                FilterMarOccidentaleBolt.TRIP_ID));

    }
}
