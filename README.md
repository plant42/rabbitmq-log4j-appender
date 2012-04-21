rabbitmq-log4j-appender
=======================

A simple log4j appender to publish messages to a RabbitMQ queue.  

The configuration is simple:

### XML Configuration ###
<pre><code>&lt;?xml version="1.0" encoding="UTF-8" ?&gt;
&lt;?xml version="1.0" encoding="UTF-8" ?&gt;
&lt;!DOCTYPE log4j:configuration SYSTEM "log4j.dtd"&gt;
&lt;log4j:configuration xmlns:log4j="http://jakarta.apache.org/log4j/"&gt;
    &lt;appender name="rabbitmq" class="com.plant42.log4j.appenders.RabbitMQAppender"&gt;
        &lt;param name="identifier" value="identifier"/&gt;
        &lt;param name="host" value="rabbitmq.whateveryourhostis.com"/&gt;
        &lt;param name="port" value="5672"/&gt;
        &lt;param name="username" value="guest"/&gt;
        &lt;param name="password" value="guest"/&gt;
        &lt;param name="virtualHost" value="/"/&gt;
        &lt;param name="exchange" value="log4j-exchange"/&gt;
        &lt;param name="type" value="direct"/&gt;
        &lt;param name="durable" value="false"/&gt;
        &lt;param name="queue" value="log4j-queue"/&gt;
        &lt;param name="routingKey" value=""/&gt;
        &lt;layout class="com.plant42.log4j.layouts.JSONLayout" /&gt;
    &lt;/appender&gt;
    &lt;root&gt;
        &lt;level value="ERROR"/&gt;
        &lt;appender-ref ref="rabbitmq"/&gt;
    &lt;/root&gt;
</code></pre>


### Properties Configuration ###
<pre><code>log4j.appender.rabbitmq=com.plant42.log4j.appenders.RabbitMQAppender
log4j.appender.rabbitmq.identifier=identifier
log4j.appender.rabbitmq.host=rabbitmq.whateveryourhostis.com
log4j.appender.rabbitmq.port=5672
log4j.appender.rabbitmq.username=guest
log4j.appender.rabbitmq.password=guest
log4j.appender.rabbitmq.virtualHost=/
log4j.appender.rabbitmq.exchange=log4j-exchange
log4j.appender.rabbitmq.type=direct
log4j.appender.rabbitmq.durable=false
log4j.appender.rabbitmq.queue=log4j-queue
log4j.appender.rabbitmq.layout=com.plant42.log4j.layouts.JSONLayout
</code></pre>



