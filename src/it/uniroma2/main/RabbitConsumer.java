package it.uniroma2.main;

import it.uniroma2.utils.Constants;
import it.uniroma2.utils.RabbitMQManager;

public class RabbitConsumer {
    public static void main(String[] args) throws InterruptedException {

        String rabbitMQ = "rabbitmq";
        String rabbitMQUsername = "rabbitmq";
        String rabbitMQPassword = "rabbitmq";
        String rabbitMQQueue = Constants.RABBITMQ_RESULTS;

        if (args.length > 0) { rabbitMQ = args[0]; }
        if (args.length > 1) { rabbitMQUsername	 = args[1]; }
        if (args.length > 2) { rabbitMQPassword = args[2]; }
        if (args.length > 3) { rabbitMQQueue = args[2]; }

        RabbitMQManager rmq = new RabbitMQManager(rabbitMQ, rabbitMQUsername, rabbitMQPassword, rabbitMQQueue);

        rmq.createDetachedReader(rabbitMQQueue); //RabbitMQ reader

        while(true){
            Thread.sleep(1);
        }

    }
}
