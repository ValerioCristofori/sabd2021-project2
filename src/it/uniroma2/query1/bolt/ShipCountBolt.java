package it.uniroma2.query1.bolt;

import it.uniroma2.entity.Nave;
import it.uniroma2.utils.DifferentShipTypes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class ShipCountBolt extends BaseRichBolt {

    Map<String, Integer> counts = new HashMap<>(); //K e' il tipo della barca, V sono il numero di navi di quella tipologia
    private OutputCollector _collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        Nave nave = (Nave) tuple.getValue(0);
        Integer type = nave.getType();
        String tipoNave = DifferentShipTypes.getTypeShip(type);
        Integer count = counts.get(tipoNave);
        if (count == null)
            count = 0;
        count++;
        counts.put(tipoNave, count);
        _collector.emit(new Values(tipoNave, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("type", "count"));

    }
}
