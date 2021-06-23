package it.uniroma2.query2;

import it.uniroma2.query1.operator.ParserCellaBolt;
import it.uniroma2.query1.operator.RabbitMQExporterBolt;
import it.uniroma2.query.operator.EntrySpout;
import it.uniroma2.query2.operator.CountGradoFreqBolt;
import it.uniroma2.query2.operator.FilterMarOccidentaleOrientaleBolt;
import it.uniroma2.query2.operator.PartialRankBolt;
import it.uniroma2.query2.operator.RankBolt;
import it.uniroma2.utils.LogController;
import it.uniroma2.query.Query;
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
