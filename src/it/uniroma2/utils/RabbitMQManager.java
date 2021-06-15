package it.uniroma2.utils;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

// RabbitMQ manager

public class RabbitMQManager {

    private String host;
    private String username;
    private String password;
    private ConnectionFactory factory;
    private Connection connection;

    private String defaultQueue;

    public RabbitMQManager(String host, String username, String password, String queue) {
        super();
        this.host = host;
        this.username = username;
        this.password = password;

        this.factory = null;
        this.connection = null;
        this.defaultQueue = queue;

        this.initialize();
        this.initializeQueue(defaultQueue);

    }

    private void initializeQueue(String queue){

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        Connection connection;
        try {
            connection = factory.newConnection();
            Channel channel = connection.createChannel();

            boolean durable = false;
            boolean exclusive = false;
            boolean autoDelete = false;

            channel.queueDeclare(queue, durable, exclusive, autoDelete, null);

            channel.close();
            connection.close();

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }

    }

    private void initialize(){

        factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        try {

            connection = factory.newConnection();

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public void terminate(){

        if (connection != null && connection.isOpen()){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private boolean reopenConnectionIfNeeded(){

        try {

            if (connection == null){
                connection = factory.newConnection();
                return true;
            }

            if (!connection.isOpen()){
                connection = factory.newConnection();
            }

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            return false;
        }

        return true;

    }

    public boolean send(String message){
        return this.send(defaultQueue, message);
    }

    public boolean send(String queue, String message){

        try {

            reopenConnectionIfNeeded();
            Channel channel = connection.createChannel();
            channel.basicPublish("", queue, null, message.getBytes());
            channel.close();

            return true;

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }

        return false;

    }

    public boolean createDetachedReader(String queue) {

        try {

            reopenConnectionIfNeeded();

            Channel channel = connection.createChannel();

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    message = System.currentTimeMillis() + "," + message;
                    System.out.println(message);
                }
            };
            channel.basicConsume(queue, true, consumer);

            return true;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;

    }

}
