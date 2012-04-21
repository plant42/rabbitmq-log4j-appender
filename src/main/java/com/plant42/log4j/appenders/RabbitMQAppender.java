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


/**
 Copyright (c) 2011 Stuart Clark, http://www.plant42.com/

 Permission is hereby granted, free of charge, to any person obtaining
 a copy of this software and associated documentation files (the
 "Software"), to deal in the Software without restriction, including
 without limitation the rights to use, copy, modify, merge, publish,
 distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to
 the following conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 A Log4j appender that publishes messages to a RabbitMQ exchange/queue.

 */
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


    /**
     * Submits LoggingEvent for publishing if it reaches severity threshold.
     * @param loggingEvent
     */
    @Override
    protected void append(LoggingEvent loggingEvent) {
        if ( isAsSevereAsThreshold(loggingEvent.getLevel())) {
            threadPool.submit( new AppenderTask(loggingEvent) );
        }
    }

    /**
     * Creates the connection, channel to RabbitMQ. Declares exchange and queue
     * @see AppenderSkeleton
     */
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

    /**
     * Sets the ConnectionFactory parameters
     */
    private void setFactoryConfiguration() {
        factory.setHost(this.host);
        factory.setPort(this.port);
        factory.setVirtualHost(this.virtualHost);
        factory.setUsername(this.username);
        factory.setPassword(this.password);
    }

    /**
     * Returns identifier property as set in appender configuration
     * @return
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Sets identifier property from parameter in appender configuration
     * @param identifier
     */
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    /**
     * Returns host property as set in appender configuration
     * @return
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets host property from parameter in appender configuration
     * @param host
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Returns port property as set in appender configuration
     * @return
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets port property from parameter in appender configuration
     * @param port
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Returns username property as set in appender configuration
     * @return
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets username property from parameter in appender configuration
     * @param username
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Returns password property as set in appender configuration
     * @return
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets password property from parameter in appender configuration
     * @param password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Returns virtualHost property as set in appender configuration
     * @return
     */
    public String getVirtualHost() {
        return virtualHost;
    }

    /**
     * Sets virtualHost property from parameter in appender configuration
     * @param virtualHost
     */
    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    /**
     * Returns exchange property as set in appender configuration
     * @return
     */
    public String getExchange() {
        return exchange;
    }

    /**
     * Sets exchange property from parameter in appender configuration
     * @param exchange
     */
    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    /**
     * Returns type property as set in appender configuration
     * @return
     */
    public String getType() {
        return type;
    }

    /**
     * Sets type property from parameter in appender configuration
     * @param type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Returns queue property as set in appender configuration
     * @return
     */
    public String getQueue() {
        return queue;
    }

    /**
     * Sets host property from parameter in appender configuration
     * @param queue
     */
    public void setQueue(String queue) {
        this.queue = queue;
    }

    public boolean isDurable() {
        return durable;
    }

    /**
     * Sets durable property from parameter in appender configuration
     * @param durable
     */
    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    /**
     * Returns routingKey property as set in appender configuration
     * @return
     */
    public String getRoutingKey() {
        return routingKey;
    }

    /**
     * Sets routingKey property from parameter in appender configuration
     * @param routingKey
     */
    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    /**
     * Declares the exchange on RabbitMQ server according to properties set
     * @throws IOException
     */
    private void createExchange() throws IOException {
        if (this.channel != null && this.channel.isOpen()) {
            synchronized (this.channel) {
                this.channel.exchangeDeclare(this.exchange, this.type, this.durable);
            }
        }
    }


    /**
     * Declares and binds queue on rabbitMQ server according to properties
     * @throws IOException
     */
    private void createQueue() throws IOException {
        if (this.channel != null && this.channel.isOpen()) {
            synchronized (this.channel) {
                this.channel.queueDeclare(this.queue, false, false, false, null);
                this.channel.queueBind(this.queue, this.exchange, this.routingKey);
            }
        }
    }

    /**
     * Creates channel on RabbitMQ server
     * @return
     * @throws IOException
     */
    private Channel createChannel() throws IOException {
        if (this.channel == null || !this.channel.isOpen() && (this.connection != null && this.connection.isOpen()) ) {
            this.channel = this.connection.createChannel();
        }
        return this.channel;
    }

    /**
     * Creates connection to RabbitMQ server according to properties
     * @return
     * @throws IOException
     */
    private Connection createConnection() throws IOException {
        setFactoryConfiguration();
        if (this.connection == null || !this.connection.isOpen()) {
            this.connection = factory.newConnection();
        }

        return this.connection;
    }


    /**
     * Closes the channel and connection to RabbitMQ when shutting down the appender
     */
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


    /**
     * Ensures that a Layout property is required
     * @return
     */
    @Override
    public boolean requiresLayout() {
        return true;
    }


    /**
     * Simple Callable class that publishes messages to RabbitMQ server
     */
    class AppenderTask implements Callable<LoggingEvent> {
        LoggingEvent loggingEvent;

        AppenderTask(LoggingEvent loggingEvent) {
            this.loggingEvent = loggingEvent;
        }

        /**
         * Method is called by ExecutorService and publishes message on RabbitMQ
         * @return
         * @throws Exception
         */
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
