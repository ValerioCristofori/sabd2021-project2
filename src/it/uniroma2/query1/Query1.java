package it.uniroma2.query1;

import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;

import it.uniroma2.utils.LogController;

public class Query1 {
	
	private TopologyBuilder builder;
	
	public Query1(String[] args) throws SecurityException, IOException {
		if( args != null && args.length > 0 ) {
			this.builder = new TopologyBuilder();
			//builder.setSpout("spout", new RandomSentenceSpout(), 5);
	
	        //builder.setBolt("split", new SplitSentenceBolt(), 8)
	        //        .shuffleGrouping("spout");
	
	        //builder.setBolt("count", new WordCountBolt(), 12)
	        //       .fieldsGrouping("split", new Fields("word"));
	
	        Config conf = new Config();
	        conf.setDebug(true);
	        
	        

            //builder.setBolt("exporter",
            //       new RabbitMQExporterBolt(
            //               RABBITMQ_HOST, RABBITMQ_USER,
            //                RABBITMQ_PASS, RABBITMQ_QUEUE ),
             //       3)
             //  .shuffleGrouping("count");

            conf.setNumWorkers(3);

           // StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());

        } else {

            LogController.getSingletonInstance().saveMess("Error: invalid number of arguments");
            System.exit(1);
        }
	}
	
}
