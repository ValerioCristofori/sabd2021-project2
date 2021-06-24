package it.uniroma2.query1;

import java.io.IOException;

import it.uniroma2.query1.operator.FilterMarOccidentaleBolt;
import it.uniroma2.query1.operator.ParserCellaBolt;
import it.uniroma2.query1.operator.RabbitMQExporterBolt;
import it.uniroma2.query1.operator.ShipCountBolt;
import it.uniroma2.query.operator.EntrySpout;
import it.uniroma2.query.Query;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import it.uniroma2.utils.LogController;
import org.apache.storm.tuple.Fields;

public class Query1 extends Query {

	private static final String RABBITMQ_QUEUE = "query1_queue";
	
	public Query1(String[] args) throws SecurityException, IOException, AuthorizationException, InvalidTopologyException, AlreadyAliveException {
		if( args != null && args.length > 0 ) {

			builder.setSpout("spout", new EntrySpout(getRedisUrl(),getRedisPort()), 5);

			builder.setBolt("filterMarOccidentale", new FilterMarOccidentaleBolt(), 5)
					.shuffleGrouping("spout");

	        builder.setBolt("parser", new ParserCellaBolt(), 4)
	                .shuffleGrouping("filterMarOccidentale");
	        //metronome


	        builder.setBolt("count", new ShipCountBolt(), 12)
	               .fieldsGrouping("parser", new Fields(ParserCellaBolt.SHIPTYPE));

			builder.setBolt("exporter",
					new RabbitMQExporterBolt(
							RABBITMQ_HOST, RABBITMQ_USER,
							RABBITMQ_PASS, RABBITMQ_QUEUE ),
					3).shuffleGrouping("count");

			if( args.length > 0 ){
				//change topology name
				args[0] += "-query1";
			}
			super.submitTopology(args);

        } else {

            LogController.getSingletonInstance().saveMess("Error: invalid number of arguments");
            System.exit(1);
        }
	}
	
}
