package it.uniroma2.main;

import java.io.IOException;

import it.uniroma2.entity.Mappa;
import it.uniroma2.query1.Query1;
import it.uniroma2.query2.Query2;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

public class Main {

	public static void main(String[] args) throws SecurityException, IOException, AuthorizationException, InvalidTopologyException, AlreadyAliveException {
		Mappa.setup();
		new Query1(args);

		Mappa.cleanUp();
		new Query2(args);
	}
	
	

}
