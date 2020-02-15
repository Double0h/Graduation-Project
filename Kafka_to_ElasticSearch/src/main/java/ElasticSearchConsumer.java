import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    //creating elasticsearch client
    public static RestHighLevelClient createClient(){

        //credentials for bonsai

        String hostname="";
        String username="";
        String password="";

        //this part for bonsai secure mode, if it is local elastic search connection we don't need this part
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));
        //
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,443,"https")) //connect over http to the hostname over port 443
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String,String> createKafkaConsumer(String topic){
        String bootstrapServer = "185.86.4.155:9092";
        String groupId="Elastic-Consumer2";

        //consumer configs
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer); //where my kafka server is running
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());//to deserialize key of content of topic
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());//to deserialize value of content of topic
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId); //consumer group id
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); //disable autocommit of offsets
        prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"5");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList(topic)); //subscribe consumer to topic so that it will read from that topic.
        return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName()); //application logger
        //create client
        RestHighLevelClient elasticClient = createClient();


        //create consumer
        KafkaConsumer<String ,String> elasticConsumer = createKafkaConsumer("important-tweets");
        //loop to poll records from kafka and save into elastic search cluster
        while(true){
            ConsumerRecords<String,String> records = elasticConsumer.poll(Duration.ofMillis(100)); // polling records from kafka topic "twitter-tweets"
            Integer recordCount = records.count();
            //logger.info("Received " +recordCount+" records.");

            BulkRequest bulkRequest=new BulkRequest(); // instead of making request to elastic for every record, we make one request for every 100 records
            for(ConsumerRecord<String ,String> record:records){
                try{
                    String id = extractIdfromTweet(record.value()); //to make consumer idempotent we use twitter id to index elastic search id.
                    IndexRequest indexRequest= new IndexRequest(
                            "twitter"
                    ).id(id).source(record.value(), XContentType.JSON); //saving as Json

                    bulkRequest.add(indexRequest); //add request to make all together.
                    //logger.info(id);
                }catch (NullPointerException e){
                    logger.warn("Skipping bad data: " +record.value()); //in case of bad tweets.
                }
            }
            if(recordCount>0){
                BulkResponse bulkItemResponses=elasticClient.bulk(bulkRequest,RequestOptions.DEFAULT);
                logger.info("All records are saved to Elastic Search");
                logger.info("Committing offsets...");
                elasticConsumer.commitSync(); // committing offsets after we write all 100 records to elastic so that we will not lose data.
                logger.info("Offsets have been committed.");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //elasticClient.close();
    }

    private static JsonParser jsonParser = new JsonParser();
    //to parse tweet id from json string in record.
    private static String extractIdfromTweet(String tweetJson) {
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

}
