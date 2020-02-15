import org.apache.http.*;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import javax.swing.text.html.parser.Entity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Posting {

        // one instance, reuse
        private final CloseableHttpClient httpClient = HttpClients.createDefault();

        public static void main(String[] args) throws Exception {

            Posting obj = new Posting();

            try {
                //System.out.println("Testing 1 - Send Http GET request");
                //obj.sendGet();

                System.out.println("Testing 2 - Send Http POST request");
                obj.sendPost();
            } finally {
                obj.close();
            }
        }

        private void close() throws IOException {
            httpClient.close();
        }

        private void sendGet() throws Exception {

            HttpGet request = new HttpGet("https://www.google.com/search?q=mkyong");

            // add request headers
            request.addHeader("custom-key", "mkyong");
            request.addHeader(HttpHeaders.USER_AGENT, "Googlebot");

            try (CloseableHttpResponse response = httpClient.execute(request)) {

                // Get HttpResponse Status
                System.out.println(response.getStatusLine().toString());

                HttpEntity entity = response.getEntity();
                Header headers = entity.getContentType();
                System.out.println(headers);

                // return it as a String
                String result = EntityUtils.toString(entity);
                System.out.println(result);

            }

        }

        private void sendPost() throws Exception {

            HttpPost post = new HttpPost("http://185.141.33.98:8082/topics/sensorData");

            // add request parameter, form parameters
            Header header1 = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/vnd.kafka.json.v1+json");
            Header header2 = new BasicHeader(HttpHeaders.ACCEPT, "application/vnd.kafka.v1+json");

            post.setHeader(header1);
            post.setHeader(header2);
            String data = "{\"records\":[{\"key\":35,\"value\":{\"foo\":\"bar\"}}]}";
            StringEntity entity = new StringEntity(data);
            post.setEntity(entity);


            try (CloseableHttpClient httpClient = HttpClients.createDefault();
                 CloseableHttpResponse response = httpClient.execute(post)) {

                System.out.println(EntityUtils.toString(response.getEntity()));
            }

        }


}
