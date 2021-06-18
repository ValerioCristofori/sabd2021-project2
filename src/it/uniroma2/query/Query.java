package it.uniroma2.query;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public abstract class Query {

    protected static final String RABBITMQ_HOST = "rabbitmq";
    protected static final String RABBITMQ_USER = "rabbitmq";
    protected static final String RABBITMQ_PASS = "rabbitmq";
    protected TopologyBuilder builder;
    protected Config conf;

    protected Query(){
        this.builder = new TopologyBuilder();
    }

    protected void submitTopology( String[] args ) throws AuthorizationException, InvalidTopologyException, AlreadyAliveException {

        StormTopology stormTopology = builder.createTopology();
        /* Create configurations */
        conf = new Config();
        conf.setDebug(true);
        /* number of workers to create for current topology */
        conf.setNumWorkers(3);


        /* Update numWorkers using command-line received parameters */
        if (args.length == 2){
            try{
                if (args[1] != null){
                    int numWorkers = Integer.parseInt(args[1]);
                    conf.setNumWorkers(numWorkers);
                    System.out.println("Number of workers to generate for current topology set to: " + numWorkers);
                }
            } catch (NumberFormatException nf){}
        }

        // cluster
        StormSubmitter.submitTopology(args[0], conf, stormTopology);
    }
}
