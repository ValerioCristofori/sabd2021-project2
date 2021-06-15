package it.uniroma2.query1;

import java.io.IOException;

import it.uniroma2.query1.bolt.FilterMarOccidentaleBolt;
import it.uniroma2.query1.bolt.ParserCellaBolt;
import it.uniroma2.query1.bolt.RabbitMQExporterBolt;
import it.uniroma2.query1.bolt.ShipCountBolt;
import it.uniroma2.query1.spout.EntrySpout;
import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;

import it.uniroma2.utils.LogController;
import org.apache.storm.tuple.Fields;

public class Query1 {

	private static final String RABBITMQ_HOST = "rabbitmq";
	private static final String RABBITMQ_USER = "rabbitmq";
	private static final String RABBITMQ_PASS = "rabbitmq";
	private static final String RABBITMQ_QUEUE = "query1_queue";
	private TopologyBuilder builder;
	
	public Query1(String[] args) throws SecurityException, IOException {
		if( args != null && args.length > 0 ) {
			this.builder = new TopologyBuilder();
			builder.setSpout("spout", new EntrySpout(), 5);

			builder.setBolt("filterMarOccidentale", new FilterMarOccidentaleBolt(), 5)
					.shuffleGrouping("spout");

	        builder.setBolt("parser", new ParserCellaBolt(), 4)
	                .shuffleGrouping("filterMarOccidentale");
	        //metronome


	        builder.setBolt("count", new ShipCountBolt(), 12)
	               .fieldsGrouping("parser", new Fields("type"));
	
	        Config conf = new Config();
	        conf.setDebug(true);
	        
	        

            builder.setBolt("exporter",
                  new RabbitMQExporterBolt(
                           RABBITMQ_HOST, RABBITMQ_USER,
                            RABBITMQ_PASS, RABBITMQ_QUEUE ),
					3).shuffleGrouping("count");

            conf.setNumWorkers(3);

           // StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());

        } else {

            LogController.getSingletonInstance().saveMess("Error: invalid number of arguments");
            System.exit(1);
        }
	}
	
}
