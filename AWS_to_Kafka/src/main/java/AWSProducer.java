import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.util.Properties;

public class AWSProducer {

    private Logger logger = LoggerFactory.getLogger(AWSProducer.class.getName());

    public static void main(String[] args) throws IOException {
        new AWSProducer().run();
    }

    public void run() throws IOException {
        logger.info("Setup");


        //Create Kafka producer
        KafkaProducer<String ,String> producer = createKafkaProducer();

        //Invoke API Get endpoint via proxy continuously
        URL url = new URL("https://streamdata.motwin.net/https://sfiuihrtdb.execute-api.us-east-2.amazonaws.com/sensor/postdata/all?X-Sd-Token=MDA3NTM5YTEtZjMzMC00MWJiLWJhMDQtMzgzYjg5NDNkY2I4");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping application");
            logger.info("Disconnecting http-url");
            logger.info("Closing Producer");
            producer.close();
            con.disconnect();

        }));

        //To read result from proxy server
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));


        String inputLine;
        int i=6;
        while (!((inputLine = in.readLine()) == null)) {
            if(!inputLine.equals(":") && !inputLine.equals("")){ //pass if no new data
                i--;
                if(i==0){ //to check the line that contains new data value
                    String[] words = inputLine.split(":",2); //split the line -> data:[{"op":"add","path":"/2","value":{"STT_TEMPERATURE":31.42475825208712,"STT_HUMIDITY":15.18512874167391}}]
                    String s=words[1];
                    JsonParser parser = new JsonParser();
                    String op = parser.parse(s)
                            .getAsJsonArray().get(0)
                            .getAsJsonObject()
                            .get("op")
                            .getAsString();
                    if(op.equals("add")){
                        JsonObject sensorData = parser.parse(s)
                                .getAsJsonArray().get(0)//convert to json from -> {"op":"add","path":"/2","value":{"STT_TEMPERATURE":31.42475825208712,"STT_HUMIDITY":15.18512874167391}}
                                .getAsJsonObject()
                                .get("value")
                                .getAsJsonObject(); //get this only -> {"STT_TEMPERATURE":31.42475825208712,"STT_HUMIDITY":15.18512874167391}
                        long t = System.currentTimeMillis();
                        Timestamp ts = new Timestamp(t);
                        sensorData.addProperty("created at",ts.toString());
                        logger.info(sensorData.toString());

                        //send to Kafka topic "sensor-data"
                        producer.send(new ProducerRecord<>("sensor-data", "1", sensorData.toString()), (recordMetadata, e) -> {
                            if(e!=null){
                                logger.error("Something bad happened",e);
                            }
                        });


                        }
                    i = 3;
                }
            }
        }
        //in.close();
        //logger.info("End of application");
    }

    public static KafkaProducer<String,String> createKafkaProducer(){
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

        return new KafkaProducer<>(properties);
    }

}

