package com.plant42.log4j.appenders;

import com.rabbitmq.client.*;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.*;
import java.util.concurrent.*;


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
public class RabbitMQAppender extends AppenderSkeleton implements ShutdownListener {
    
    private ConnectionFactory factory = new ConnectionFactory();
    private Connection connection = null;
    private Channel channel = null;
    private String identifier = null;
    private String host = "localhost";
    private int port = 5762;
    private boolean ssl = false;
    private boolean verifySsl = false;
    private String username = "guest";
    private String password = "guest";
    private String virtualHost = "/";
    private String exchange = "amqp-exchange";
    private String type = "direct";
    private boolean durable = false;
    private String queue = "amqp-queue";
    private String routingKey = "";
    private long droppedEvents = 0;
    private long reconnections = 0;
    private int queueLimit = 1024;

    // We will shutdown the threadPool if there is a problem activating the options.
    private ExecutorService threadPool;

    /**
     * Submits LoggingEvent for publishing if it reaches severity threshold.
     * @param loggingEvent
     */
    @Override
    protected void append(LoggingEvent loggingEvent) {
        if ( isAsSevereAsThreshold(loggingEvent.getLevel())) {
            try {
                threadPool.submit(new AppenderTask(loggingEvent));
            } catch (RejectedExecutionException ree) {
                droppedEvents++;
                // This is expected if the server goes down, we have network problems, or we
                // failed to create the connection in the first place.
                errorHandler.error("Dropping logging events.", ree, ErrorCode.GENERIC_FAILURE);
            }
        }
    }

    /**
     * Build a string containing the details of the connection, this is useful in debugging.
     * @return
     */
    protected String getConnectionDetails() {
        return
                "Host: "+ host+ ", "+
                "Port: "+ port + ", "+
                "Virtual Host:"+ virtualHost+ ", "+
                "Username: "+ username+ ", "+
                "Password: "+ ((password == null || password.isEmpty())?"":"******")+ ", "+
                "SSL: "+ ssl+ ", ";
    }

    /**
     * Creates the connection, channel to RabbitMQ. Declares exchange and queue
     * @see AppenderSkeleton
     */
    @Override
    public void activateOptions() {
        super.activateOptions();
        threadPool = new ThreadPoolExecutor(0, 1,
                1000L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(queueLimit));

        // Use hostname if identifier isn't there.
        if (identifier == null || identifier.isEmpty()) {
            try {
                identifier = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                // Ignore.
            }
        }

        //== creating connection
        try {
            setFactoryConfiguration();
            this.createConnection();
        } catch (IOException ioe) {
            errorHandler.error("Failed to connect to: " + getConnectionDetails(), ioe, ErrorCode.GENERIC_FAILURE);
            threadPool.shutdown();
        } catch (GeneralSecurityException gse) { // thrown when SSL problems happen.
            errorHandler.error(gse.getMessage(), gse, ErrorCode.GENERIC_FAILURE);
            threadPool.shutdown();
        }

        //== creating channel
        try {
            this.createChannel();
        } catch (IOException ioe) {
            errorHandler.error("Failed to create channel", ioe, ErrorCode.GENERIC_FAILURE);
            threadPool.shutdown();
        }

        //== create exchange
        try {
            this.createExchange();
        } catch (Exception ioe) {
            errorHandler.error("Failed to create exchange: "+ getExchange(), ioe, ErrorCode.GENERIC_FAILURE);
            threadPool.shutdown();
        }

        //== create queue
        try {
            this.createQueue();
        } catch (Exception ioe) {
            errorHandler.error("Failed to create queue: "+ getQueue(), ioe, ErrorCode.GENERIC_FAILURE);
            threadPool.shutdown();
        }
    }

    /**
     * Sets the ConnectionFactory parameters
     */
    private void setFactoryConfiguration() throws KeyManagementException, NoSuchAlgorithmException, NoSuchProviderException, KeyStoreException {
        factory.setHost(this.host);
        factory.setPort(this.port);
        if (ssl) {
            if (verifySsl) {
                factory.useSslProtocol("TLSv1", getTrustManager());
            } else {
                factory.useSslProtocol("TLSv1");
            }
        }
        factory.setVirtualHost(this.virtualHost);
        factory.setUsername(this.username);
        factory.setPassword(this.password);
    }

    /**
     * This finds a trust manager from the JVM.
     * @return
     * @throws NoSuchProviderException
     * @throws NoSuchAlgorithmException
     */
    private TrustManager getTrustManager() throws NoSuchProviderException, NoSuchAlgorithmException, KeyStoreException {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("PKIX", "SunJSSE");
        trustManagerFactory.init((KeyStore)null);
        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        for (int i = 0; i < trustManagers.length; i++) {
            if (trustManagers[i] instanceof X509TrustManager) {
                return trustManagers[i];
            }
        }
        // We don't want to ignore this.
        throw new NoSuchProviderException("Unable to find a trust manager to use.");
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
     * Returns <code>true</code> if SSL is being used to connect.
     * @return
     */
    public boolean isSsl() {
        return ssl;
    }

    /**
     * Configures SSL on the connection.
     * @param ssl
     */
    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    /**
     * Returns <code>true</code> if the server certificate should be verified.
     * @return
     */
    public boolean isVerifySsl() {
        return verifySsl;
    }

    /**
     * Sets if we should verify the server certificate.
     * @param verifySsl
     */
    public void setVerifySsl(boolean verifySsl) {
        this.verifySsl = verifySsl;
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
     * Returns the number of events that can be queued up before we start dropping them.
     * @return
     */
    public int getQueueLimit() {
        return queueLimit;
    }

    public void setQueueLimit(int queueLimit) {
        this.queueLimit = queueLimit;
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
        if ((this.channel == null || !this.channel.isOpen()) && (this.connection != null && this.connection.isOpen()) ) {
            this.channel = this.connection.createChannel();
            this.channel.addShutdownListener(this);
        }
        return this.channel;
    }

    /**
     * Creates connection to RabbitMQ server according to properties
     * @return
     * @throws IOException
     */
    private Connection createConnection() throws IOException, NoSuchAlgorithmException, KeyManagementException, NoSuchProviderException, KeyStoreException {
        if (this.connection == null || !this.connection.isOpen()) {
            reconnections++;
            this.connection = factory.newConnection();
            this.connection.addShutdownListener(this);
        }
        return this.connection;
    }


    /**
     * Closes the channel and connection to RabbitMQ when shutting down the appender
     */
    @Override
    public void close() {
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            errorHandler.error("Didn't manage to shutdown in time.", e, ErrorCode.CLOSE_FAILURE);
        }

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

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        Object item = cause.getReference();
        if (item instanceof Connection) {
            connection = null;
        }
        if (item instanceof Channel) {
            channel = null;
        }
    }


    /**
     * Simple Callable class that publishes messages to RabbitMQ server
     */
    class AppenderTask implements Callable<Object> {

        String payload;
        String id;
        String level;

        AppenderTask(LoggingEvent loggingEvent) {
            // We throw away the loggingEvent here so that things like the thread name
            // are correct and any delays in logging don't affect the timestamp
            payload = layout.format(loggingEvent);
            id = String.format("%s:%s", identifier, System.currentTimeMillis());
            level = loggingEvent.getLevel().toString();
        }

        /**
         * Method is called by ExecutorService and publishes message on RabbitMQ
         * @return
         * @throws Exception
         */
        @Override
        public Object call() throws Exception {

            
            AMQP.BasicProperties.Builder b = new AMQP.BasicProperties().builder();
            b.appId(identifier)
                    .type(level)
                    .correlationId(id)
                    .contentType("text/json");

            boolean success = false;
            int backOff = 1; // Don't want to hammer remote server.
            do {
                try {
                    createConnection();
                    createChannel().basicPublish(exchange, routingKey, b.build(), payload.toString().getBytes());
                    success = true;
                } catch (IOException e) {
                    Thread.sleep(backOff);
                    backOff *= 2;
                    backOff = Math.min(backOff, 1024); // Limit to 1 second
                }
            } while(!success); // If the queue gets stuck it's best to reset the logging framework
            return null;
        }
    }
}
