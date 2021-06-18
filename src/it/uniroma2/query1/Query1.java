package it.uniroma2.query1;

import java.io.IOException;

import it.uniroma2.query1.bolt.FilterMarOccidentaleBolt;
import it.uniroma2.query1.bolt.ParserCellaBolt;
import it.uniroma2.query1.bolt.RabbitMQExporterBolt;
import it.uniroma2.query1.bolt.ShipCountBolt;
import it.uniroma2.query.spout.EntrySpout;
import it.uniroma2.query.Query;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import it.uniroma2.utils.LogController;
import org.apache.storm.tuple.Fields;

public class Query1 extends Query {

	private static final String RABBITMQ_QUEUE = "query1_queue";
	
	public Query1(String[] args) throws SecurityException, IOException, AuthorizationException, InvalidTopologyException, AlreadyAliveException {
		if( args != null && args.length > 0 ) {

			builder.setSpout("spout", new EntrySpout(), 5);

			builder.setBolt("filterMarOccidentale", new FilterMarOccidentaleBolt(), 5)
					.shuffleGrouping("spout");

	        builder.setBolt("parser", new ParserCellaBolt(), 4)
	                .shuffleGrouping("filterMarOccidentale");
	        //metronome


	        builder.setBolt("count", new ShipCountBolt(), 12)
	               .fieldsGrouping("parser", new Fields("type"));

			builder.setBolt("exporter",
					new RabbitMQExporterBolt(
							RABBITMQ_HOST, RABBITMQ_USER,
							RABBITMQ_PASS, RABBITMQ_QUEUE ),
					3).shuffleGrouping("count");

			super.submitTopology(args);

        } else {

            LogController.getSingletonInstance().saveMess("Error: invalid number of arguments");
            System.exit(1);
        }
	}
	
}
