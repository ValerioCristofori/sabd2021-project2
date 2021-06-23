package it.uniroma2.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class TConf {

    public static String configFilename = "conf.properties";

    public static String NUM_TASKS  = "topology.tasks.num";
    private static int 	 DEFAULT_NUM_TASKS = 32;

    public static String REDIS_URL  = "redis.url";
    private static String DEFAULT_REDIS_URL 	= "localhost";

    public static String REDIS_PORT = "redis.port";
    private static int 	 DEFAULT_REDIS_PORT = 6379;

    public static String  RABBITMQ_HOST = "rabbitmq.host";
    private static String DEFAULT_RABBITMQ_HOST = "localhost";

    public static String  RABBITMQ_USERNAME = "rabbitmq.username";
    private static String DEFAULT_RABBITMQ_USERNAME = "rabbitmq";

    public static String  RABBITMQ_PASSWORD = "rabbitmq.password";
    private static String DEFAULT_RABBITMQ_PASSWORD = "rabbitmq";

    Properties p;

    public TConf() {

        this.p = new Properties();

        loadDefaults();

        readFromFile();

    }

    private void readFromFile(){

        Path currentRelativePath = Paths.get("");
        String path = currentRelativePath.toAbsolutePath().toString();
        path += "/" + configFilename;

        File f = new File(path);

        System.out.println("Reading: " + path);
        if (f.exists()){

            System.out.println("Exists");
            try {
                InputStream input = new FileInputStream(path);
                p.load(input);
            } catch (IOException e) {
                e.printStackTrace();
            }

        } else {

            OutputStream output;
            try {

                output = new FileOutputStream(path + ".defaults");
                p.store(output, "");

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    private void loadDefaults(){

        p.setProperty(NUM_TASKS, String.valueOf(DEFAULT_NUM_TASKS));
        p.setProperty(REDIS_URL, DEFAULT_REDIS_URL);
        p.setProperty(REDIS_PORT,String.valueOf(DEFAULT_REDIS_PORT));
        p.setProperty(RABBITMQ_HOST, DEFAULT_RABBITMQ_HOST);
        p.setProperty(RABBITMQ_USERNAME, DEFAULT_RABBITMQ_USERNAME);
        p.setProperty(RABBITMQ_PASSWORD, DEFAULT_RABBITMQ_PASSWORD);

    }

    public Integer getInteger(String key){

        String raw = p.getProperty(key);
        if (raw == null)
            return null;

        Integer value = null;
        try{
            value = Integer.valueOf(raw);
        } catch(NumberFormatException nfe){
            return null;
        }

        return value;
    }


    public String getString(String key){

        return p.getProperty(key);

    }
}
