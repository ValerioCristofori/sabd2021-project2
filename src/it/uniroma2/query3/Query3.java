package it.uniroma2.query3;

import it.uniroma2.query.Query;
import it.uniroma2.utils.LogController;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import java.io.IOException;

public class Query3 extends Query {

    private static final String RABBITMQ_QUEUE = "query3_queue";

    public Query3(String[] args) throws SecurityException, IOException, AuthorizationException, InvalidTopologyException, AlreadyAliveException {
        if( args != null && args.length > 0 ) {


        } else {

            LogController.getSingletonInstance().saveMess("Error: invalid number of arguments");
            System.exit(1);
        }
    }

}
