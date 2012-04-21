package com.plant42.log4j.appenders;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RabbitMQAppender extends AppenderSkeleton {
    
    private ConnectionFactory factory = new ConnectionFactory();
    private Connection connection = null;
    private Channel channel = null;
    private String identifier = null;
    private String host = "localhost";
    private int port = 5762;
    private String username = "guest";
    private String password = "guest";
    private String virtualHost = "/";
    private String exchange = "amqp-exchange";
    private String type = "direct";
    private boolean durable = false;
    private String queue = "amqp-queue";
    private String routingKey = "";

    private ExecutorService threadPool = Executors.newSingleThreadExecutor();

    private void setFactoryConfiguration() {
        factory.setHost(this.host);
        factory.setPort(this.port);
        factory.setVirtualHost(this.virtualHost);
        factory.setUsername(this.username);
        factory.setPassword(this.password);
    }

    @Override
    public void activateOptions() {
        super.activateOptions();

        //== creating connection
        try {
            this.createConnection();
        } catch (IOException ioe) {
            errorHandler.error(ioe.getMessage(), ioe, ErrorCode.GENERIC_FAILURE);
        }

        //== creating channel
        try {
            this.createChannel();
        } catch (IOException ioe) {
            errorHandler.error(ioe.getMessage(), ioe, ErrorCode.GENERIC_FAILURE);
        }
        
        //== create exchange
        try {
            this.createExchange();
        } catch (Exception ioe) {
            errorHandler.error(ioe.getMessage(), ioe, ErrorCode.GENERIC_FAILURE);
        }

        //== create queue
        try {
            this.createQueue();
        } catch (Exception ioe) {
            errorHandler.error(ioe.getMessage(), ioe, ErrorCode.GENERIC_FAILURE);
        }
    }


    @Override
    protected void append(LoggingEvent loggingEvent) {
        if ( isAsSevereAsThreshold(loggingEvent.getLevel())) {
            threadPool.submit( new AppenderTask(loggingEvent) );
        }
    }


    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    private void createExchange() throws IOException {
        if (this.channel != null && this.channel.isOpen()) {
            synchronized (this.channel) {
                this.channel.exchangeDeclare(this.exchange, this.type, this.durable);
            }
        }
    }


    private void createQueue() throws IOException {
        if (this.channel != null && this.channel.isOpen()) {
            synchronized (this.channel) {
                this.channel.queueDeclare(this.queue, false, false, false, null);
                this.channel.queueBind(this.queue, this.exchange, this.routingKey);
            }
        }
    }

    private Channel createChannel() throws IOException {
        if (this.channel == null || !this.channel.isOpen() && (this.connection != null && this.connection.isOpen()) ) {
            this.channel = this.connection.createChannel();
        }
        return this.channel;
    }


    private Connection createConnection() throws IOException {
        setFactoryConfiguration();
        if (this.connection == null || !this.connection.isOpen()) {
            this.connection = factory.newConnection();
        }

        return this.connection;
    }

    @Override
    public void close() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException ioe) {
                errorHandler.error(ioe.getMessage(), ioe, ErrorCode.CLOSE_FAILURE);
            }
        }

        if (connection != null && connection.isOpen()) {
            try {
                this.connection.close();
            } catch (IOException ioe) {
                errorHandler.error(ioe.getMessage(), ioe, ErrorCode.CLOSE_FAILURE);
            }
        }
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    class AppenderTask implements Callable<LoggingEvent> {
        LoggingEvent loggingEvent;

        AppenderTask(LoggingEvent loggingEvent) {
            this.loggingEvent = loggingEvent;
        }

        @Override
        public LoggingEvent call() throws Exception {
            String payload = layout.format(loggingEvent);
            String id = String.format("%s:%s", identifier, System.currentTimeMillis());

            
            AMQP.BasicProperties.Builder b = new AMQP.BasicProperties().builder();
            b.appId(identifier)
                    .type(loggingEvent.getLevel().toString())
                    .correlationId(id)
                    .contentType("text/json");

            createChannel().basicPublish(exchange, routingKey, b.build(), payload.toString().getBytes());

            return loggingEvent;
        }
    }
}
