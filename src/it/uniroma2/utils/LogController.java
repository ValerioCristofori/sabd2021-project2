package it.uniroma2.utils;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/* Classe Singleton per logging di dati */

public class LogController {

	private static final String PATH = "./logFile.log";
	
	private static LogController instance = null;
	private Logger logger = Logger.getLogger( "mLogger" );
	private FileHandler fh;
	
	private LogController() throws SecurityException, IOException {
		this.fh = new FileHandler( PATH );
		this.logger.addHandler(fh);
		SimpleFormatter formatter = new SimpleFormatter();  
        fh.setFormatter(formatter);
	}
	
	public static LogController getSingletonInstance() throws SecurityException, IOException {
		if( instance == null )
			instance = new LogController();
		return instance;
	}
	
	public void saveMess( String message ) {
		this.logger.info(message);
	}

	public void queryOutput( String... args ){
		StringBuilder sb = new StringBuilder();
		sb.append( String.format("%n+-----------------------------------+%n"));
		for( String arg : args ){
			sb.append( String.format("|  " +arg+ "%n") );
		}
		sb.append( String.format("+-----------------------------------+%n%n"));
		this.logger.info(sb.toString());
	}
	
	
}
