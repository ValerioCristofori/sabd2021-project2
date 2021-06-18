package it.uniroma2.query2;

import it.uniroma2.query1.bolt.FilterMarOccidentaleBolt;
import it.uniroma2.query1.bolt.ParserCellaBolt;
import it.uniroma2.query1.bolt.RabbitMQExporterBolt;
import it.uniroma2.query1.bolt.ShipCountBolt;
import it.uniroma2.query.spout.EntrySpout;
import it.uniroma2.query2.bolt.CountGradoFreqBolt;
import it.uniroma2.query2.bolt.FilterMarOccidentaleOrientaleBolt;
import it.uniroma2.query2.bolt.PartialRankBolt;
import it.uniroma2.query2.bolt.RankBolt;
import it.uniroma2.utils.LogController;
import it.uniroma2.query.Query;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.tuple.Fields;

import java.io.IOException;

public class Query2 extends Query {

    private static final String RABBITMQ_QUEUE = "query2_queue";

    public Query2(String[] args) throws SecurityException, IOException, AuthorizationException, InvalidTopologyException, AlreadyAliveException {
        if( args != null && args.length > 0 ) {

            builder.setSpout("spout", new EntrySpout(), 5);

            builder.setBolt("filterMarOccidentaleOrientale", new FilterMarOccidentaleOrientaleBolt(), 5)
                    .shuffleGrouping("spout");

            builder.setBolt("parser", new ParserCellaBolt(), 4)
                    .shuffleGrouping("filterMarOccidentaleOrientale");
            //metronome


            builder.setBolt("count", new CountGradoFreqBolt(), 12)
                    .fieldsGrouping("parser", new Fields("type"));

            builder.setBolt("partialRank", new PartialRankBolt(), 8)
                    .fieldsGrouping("count", new Fields("type"));

            builder.setBolt("rank", new RankBolt(), 1)
                    .fieldsGrouping("partialRank", new Fields("type"));


            builder.setBolt("exporter",
                    new RabbitMQExporterBolt(
                            RABBITMQ_HOST, RABBITMQ_USER,
                            RABBITMQ_PASS, RABBITMQ_QUEUE ),
                    3).shuffleGrouping("rank");

            super.submitTopology(args);

        } else {

            LogController.getSingletonInstance().saveMess("Error: invalid number of arguments");
            System.exit(1);
        }
    }
}
