package it.uniroma2.query1.bolt;

import it.uniroma2.utils.RabbitMQManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class RabbitMQExporterBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector _collector;

    private RabbitMQManager rabbitmq;
    private String rabbitMqHost;
    private String rabbitMqUsername;
    private String rabbitMqPassword;
    private String defaultQueue;

    public RabbitMQExporterBolt(String rabbitMqHost, String rabbitMqUsername, String rabbitMqPassword,
                                String defaultQueue) {
        super();
        this.rabbitMqHost = rabbitMqHost;
        this.rabbitMqUsername = rabbitMqUsername;
        this.rabbitMqPassword = rabbitMqPassword;
        this.defaultQueue = defaultQueue;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        this.rabbitmq = new RabbitMQManager(rabbitMqHost, rabbitMqUsername, rabbitMqPassword, defaultQueue);
    }

    @Override
    public void execute(Tuple tuple) {
        String type = tuple.getString(0);
        Integer count = tuple.getInteger(1);

        String output = type + " " + count;
        rabbitmq.send(output);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("type"));
    }
}
