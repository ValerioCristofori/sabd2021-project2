package it.uniroma2.query;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public abstract class Query {

    public static String REDIS_URL;
    public static int REDIS_PORT;
    public static String RABBITMQ_HOST;
    public static String RABBITMQ_USER;
    public static String RABBITMQ_PASS;
    protected TopologyBuilder builder;
    protected Config conf;

    protected Query(){
        this.builder = new TopologyBuilder();
    }

    protected void submitTopology( String[] args ) throws AuthorizationException, InvalidTopologyException, AlreadyAliveException {

        StormTopology stormTopology = builder.createTopology();
        /* Create configurations */
        conf = new Config();
        conf.setDebug(false);
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

    public static void setRedisUrl(String redisUrl) {
        REDIS_URL = redisUrl;
    }

    public static void setRedisPort(int redisPort) {
        REDIS_PORT = redisPort;
    }

    public static void setRabbitmqHost(String rabbitmqHost) {
        RABBITMQ_HOST = rabbitmqHost;
    }

    public static void setRabbitmqUser(String rabbitmqUser) {
        RABBITMQ_USER = rabbitmqUser;
    }

    public static void setRabbitmqPass(String rabbitmqPass) {
        RABBITMQ_PASS = rabbitmqPass;
    }

    public static String getRedisUrl() {
        return REDIS_URL;
    }

    public static int getRedisPort() {
        return REDIS_PORT;
    }

    public static String getRabbitmqHost() {
        return RABBITMQ_HOST;
    }

    public static String getRabbitmqUser() {
        return RABBITMQ_USER;
    }
}
