package it.uniroma2.main;

import java.io.IOException;

import it.uniroma2.entity.Mappa;
import it.uniroma2.query.Query;
import it.uniroma2.query1.Query1;
import it.uniroma2.query2.Query2;
import it.uniroma2.query3.Query3;
import it.uniroma2.utils.TConf;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

public class QueryTopology {

	public static void main(String[] args) throws SecurityException, IOException, AuthorizationException, InvalidTopologyException, AlreadyAliveException {

		TConf config = new TConf();
		String redisUrl			= config.getString(TConf.REDIS_URL);
		int redisPort 			= config.getInteger(TConf.REDIS_PORT);
		int numTasks 			= config.getInteger(TConf.NUM_TASKS);
		int numTasksMetronome   = 1;  // each task of the metronome generate a flood of messages
		int numTasksGlobalRank  = 1;
		String rabbitMqHost 	= config.getString(TConf.RABBITMQ_HOST);
		String rabbitMqUsername = config.getString(TConf.RABBITMQ_USERNAME);
		String rabbitMqPassword	= config.getString(TConf.RABBITMQ_PASSWORD);

		Query.setRedisPort(redisPort);
		Query.setRedisUrl(redisUrl);
		Query.setRabbitmqHost(rabbitMqHost);
		Query.setRabbitmqUser(rabbitMqUsername);
		Query.setRabbitmqPass(rabbitMqPassword);

		System.out.println("===================================================== ");
		System.out.println("Configuration:");
		System.out.println("Redis: " + redisUrl + ":" + redisPort);
		System.out.println("RabbitMQ: " + rabbitMqHost + " (user: " + rabbitMqUsername + ", " + rabbitMqPassword + ")");
		System.out.println("Tasks:" + numTasks);
		System.out.println("===================================================== ");



		Mappa.setup();
		new Query1(args);

		//Mappa.cleanUp();
		//new Query2(args);

		//Mappa.cleanUp();
		//new Query3(args);

	}
	
	

}
