import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import org.json.simple.JSONObject;

import java.util.Random;

public class PostSensorData {
    public static void main(String[] args) {

        //AWS credentials
        String accessKey = "";
        String secretKey = "";
        BasicAWSCredentials credentials = new
                BasicAWSCredentials(accessKey,secretKey);

        Regions region = Regions.fromName("us-east-2"); //Region where our AWS

        //Building AWS Lambda Client
        AWSLambdaClientBuilder builder = AWSLambdaClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(region);
        AWSLambda client = builder.build();

        //Loop for generating sensor data
        while(true){
            Random t = new Random();
            double temperature = 20 + (50 - 20) * t.nextDouble();
            double humidity = 10 + (30 - 20) * t.nextDouble();
            int dataID = t.nextInt(Integer.MAX_VALUE);
            JSONObject json = new JSONObject();
            json.put("dataID",String.valueOf(dataID));
            json.put("STT_TEMPERATURE", String.valueOf(temperature));
            json.put("STT_HUMIDITY", String.valueOf(humidity));


            //invoke Request to our AWS Lambda function called "sensorFuntion" (The function that saves data in DynamoDB)
            InvokeRequest req = new InvokeRequest()
                    .withFunctionName("sensorFunction") //function name
                    .withPayload(json.toString());  //sensor data
            client.invoke(req);
            System.out.println(json);

            //sleep for 5 minutes (to send data every 5 minutes)
            try {
                Thread.sleep(20_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
