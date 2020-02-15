package com.example;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.*;

public class CalculateHourlyAverageData {
    public static void main(String[] args) {

        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092"); //our server that kafka broker runs
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"hourly-average-data"); //Kafka Stream application ID
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()); //Key serializer and deserializer
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName()); //Value serializer and deserializer

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,String> sensorStream = streamsBuilder.stream("sensor-data",Consumed.with(Serdes.String(), Serdes.String())); //sensor data will be read from this topic

        //Table that contains sum of Temperature incoming data with the window of 1 hour, and sum of humidity as well
        KTable<Windowed<String>, String> summedTable = sensorStream
                .groupByKey(Grouped.with(Serdes.String(),Serdes.String()))  //Group by key so that data with different keys will be calculated differently
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ofSeconds(5))) // Time window of 1 hour
                .reduce(                                                //reducing to calculate sum of data in the same interval
                        (aggVal,newVal)-> "{\"STT_TEMPERATURE\":"+
                                (extractTemperature(aggVal) + extractTemperature(newVal)) +
                                ",\"STT_HUMIDITY\":"+
                                (extractHumidity(aggVal) + extractHumidity(newVal)) +"}"
                ).suppress(untilWindowCloses(BufferConfig.unbounded())  // suppress to get only last sum of data in the same interval
                );

        //Table that contains start and end time of the window
        KTable<Windowed<String>, String> timeIntervalTable = sensorStream
                .groupByKey(Grouped.with(Serdes.String(),Serdes.String()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ofSeconds(5)))
                .count()
                .suppress(untilWindowCloses(BufferConfig.unbounded()))
                .mapValues(
                        (key,value)->extractStartTime(key) +"/"+ extractEndTime(key)        //map values to get start and end time of the Window
                );

        //Table that contains number of incoming data with the window of 1 hour
        KTable<Windowed<String>, Long> countedTable = sensorStream
                .groupByKey(Grouped.with(Serdes.String(),Serdes.String()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ofSeconds(5)))
                .count()                //To count number of data that comes in the same time interval. Later this will be used to calculate average -> sum/count
                .suppress(untilWindowCloses(BufferConfig.unbounded()));

        //Stream that contains average value of temperature with window of 1 hour, and average value of humidity as well
        KTable<Windowed<String>, String> averageTable = summedTable
                .join(countedTable,                     //Join sum and count table to calculate sum/count (average)
                        (summedData,count)-> "{\"STT_TEMPERATURE\":"+
                        extractTemperature(summedData)/count +
                        ",\"STT_HUMIDITY\":"+
                        extractHumidity(summedData)/count +"}"
                );

        //Stream that constructs data with additional "timeInterval" and "Created at" values
        KStream<String, String> reconstructedStream = averageTable
                .join(timeIntervalTable,                     //Join average and time interval table to reconstruct json with additional interval's start and end times
                        (averageVal,timeValue)-> "{\"STT_TEMPERATURE\":"+
                                extractTemperature(averageVal) +
                                ",\"STT_HUMIDITY\":"+
                                extractHumidity(averageVal) +",\"timeInterval\":" + "\""+extractHour(timeValue)+"\""+",\"created at\":" +"\""+extractCreated(timeValue)+"\""+"}"
    ).toStream().map(
                        (key,value)-> KeyValue.pair("1",value)
                );

        //forward stream to another topic in Kafka
        reconstructedStream.to("average-data-per-hour");  // average hourly data will be written to this topic

        //construct app
        KafkaStreams kafkaStreams=new KafkaStreams(streamsBuilder.build(),properties);

        //start app
        kafkaStreams.start();
    }
    private static JsonParser jsonParser = new JsonParser(); //jSon parser to parse json

    //Function to get "STT_TEMPERATURE" value from Json data that are read from "sensor-data" topic.
    private static Double extractTemperature(String sensorData) {
        try{
        return jsonParser.parse(sensorData)
                .getAsJsonObject()
                .get("STT_TEMPERATURE")
                .getAsDouble();

        }
        catch (NullPointerException e){ //if it does not exist return 0;
            return 0d;
        }
    }

    //Function to get "STT_HUMIDITY" value from Json data that are read from "sensor-data" topic.
    private static Double extractHumidity(String sensorData) {
        try{
            return jsonParser.parse(sensorData)
                    .getAsJsonObject()
                    .get("STT_HUMIDITY")
                    .getAsDouble();

        }
        catch (NullPointerException e){ //if it does not exist return 0;
            return 0d;
        }
    }

    //Function to get Start Time of hourly window
    private static String extractStartTime(Windowed<String> timeInterval){
        String time = timeInterval.toString();
        String[] word = time.replace("[1@","").replace("]","").split("/");
        Timestamp date = new Timestamp(Long.parseLong(word[0]));
        return date.toString();
    }

    //Function to get End time of hourly window
    private static String extractEndTime(Windowed<String> timeInterval){
        String time = timeInterval.toString();
        String[] word = time.replace("[1@","").replace("]","").split("/");
        Timestamp date = new Timestamp(Long.parseLong(word[1]));
        return date.toString();
    }

    //Function to get Created Time of the data
    private static String extractCreated(String s){
        String[] a = s.split("/");
        return a[1];
    }

    //Function to add Time interval values to sensor-data
    private static String extractHour(String s){
        String[] interval = s.split("/");
        String starthour = interval[0].split(" ")[1];
        String endhour = interval[1].split(" ")[1];
        return starthour+" - "+endhour;
    }

}
