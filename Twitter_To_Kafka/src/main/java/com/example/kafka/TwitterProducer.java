package com.example.kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private String consumerKey ="";
    private String consumerSecret ="";
    private String token ="";
    private String secret ="";
    private List<String> terms = Lists.newArrayList("Turkey");

    public TwitterProducer(){}

    public static void main(String[] args) {
        new TwitterProducer().run();

    }

    public void run(){
        logger.info("Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        //create twitter client
        // Attempts to establish a connection.
        Client client = createTwitterClient(msgQueue);
        client.connect();

        //create kafka producer
        KafkaProducer<String ,String> producer = createKafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping application");
            client.stop();
            logger.info("shutting down client from twitter");
            producer.close();
            logger.info("Closing Producer ");
        }));

        //loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("tweets", null, msg), (recordMetadata, e) -> {
                    if(e!=null){
                        logger.error("Something bad happened",e);
                    }
                });
            }

        }
        logger.info("End of application");
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue){


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        System.out.println(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some track terms

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

    public KafkaProducer<String,String> createKafkaProducer(){
        String bootstrapServers = "localhost:9092"; //my kafka server is running
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers); //kafka server connect
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //key serializer to send kafka
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //value serializer to send kafka

        //safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true"); //if the same messages is sent multiple times is written only 1 time in kafka.
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //takes acks from all brokers to make sure data is written and replicated.
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE)); //number of retries if the messages is not send
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5"); // makes 5 parallel requests to kafka cluster

        //high throughput Producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy"); //compresses the messages in the buffer
        // with snappy before sending it to kafka cluster
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20"); //waits 20 ms before sending messages in order to
        // send them together instead of sending incoming messages right away
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); //32KB batch size,
        // Increased batch size in order to send more messages together

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        return producer;
    }
}
