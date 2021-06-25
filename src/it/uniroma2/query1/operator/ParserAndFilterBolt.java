package it.uniroma2.query1.operator;

import it.uniroma2.entity.Mappa;
import it.uniroma2.entity.Nave;
import it.uniroma2.entity.RottaNave;
import it.uniroma2.query.operator.EntrySpout;
import it.uniroma2.utils.Constants;
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

public class ParserAndFilterBolt extends BaseRichBolt {

    public static final String MSGID				    = "MSGID";
    public static final String SHIP_ID 				    = "SHIP_ID";
    public static final String SHIPTYPE				    = "SHIPTYPE";
    //public static final String SPEED	                = "SPEED";
    //public static final String LON 	                    = "LON";
    //public static final String LAT 				        = "LAT";
    public static final String CELLA 				    = "CELLA";
    public static final String TIMESTAMP 				= "TIMESTAMP";
    //public static final String TRIP_ID			        = "TRIP_ID";

    private SimpleDateFormat sdf;

    private double initialLat 	= 32.0;
    private double finalLat 	= 45.0;
    private double initialLon   = -6.0;
    private double finalLon 	= 12.0; //canale di sicilia

    private static final long serialVersionUID = 1L;
    private OutputCollector _collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector=collector;
        this.sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");
    }

    @Override
    public void execute(Tuple tuple) {
        String rawData 	= tuple.getStringByField(EntrySpout.F_DATA);
        String msgId 	= tuple.getStringByField(EntrySpout.F_MSGID);
        String timestamp = tuple.getStringByField(EntrySpout.F_TIMESTAMP);

        /* Do NOT emit if the EOF has been reached */
        if (rawData == null || rawData.equals(Constants.REDIS_EOF)){
            _collector.ack(tuple);
            return;
        }

        /* Do NOT emit if the EOF has been reached */
        String[] data = rawData.split(",");
        if (data == null){
            _collector.ack(tuple);
            return;
        }

        String shipId 		= data[0];
        String typeString 	= data[1];
        String speedString 	= data[2];
        String lonString  	= data[3];
        String latString 	= data[4];
        String trip_id  	= data[10];

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

        /* Do NOT emit if coordinates are outside the monitored grid */
        if (isOutsideGrid(lat, lon)){
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

        String cella = Mappa.findRightCell(lon, lat);

        Values values = new Values();
        values.add(msgId);        // tuple id
        values.add(shipId);      // ship id
        values.add(type);      // ship type
        values.add(cella);      // lon
        values.add(dTimestamp);      // timestamp


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
        outputFieldsDeclarer.declare(new Fields(MSGID, SHIP_ID, SHIPTYPE, CELLA, TIMESTAMP));
    }
}
