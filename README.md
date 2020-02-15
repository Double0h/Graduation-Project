# Graduation-Project

Includes source codes of my parts for graduation project.

# 1)AWS_to_Kafka

Aws producer,
Gets data from our AWS Gateway API and writes them to Apache Kafka.

# 2)Generate_SensorData

Posts data to our AWS Gateway API.

# 3)Kafka Streams application.

Calculates average temperature and humidity values windowed by 1 hour.

# 4)Twitter_to_Kafka.

Twitter producer,
Gets tweets that include word "Turkey" from twitter API and writes them to Kafka.

# 5)Kafka_to_ElasticSearch

ElasticSearch consumer,
Gets filtered tweets and saves them into elasticsearch.

# 6)Poster

A producer,
Makes post requests to write data to Kafka over Kafka-Rest API.
