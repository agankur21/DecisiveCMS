package com.scoopwhoop.reporting

import kafka.serializer.StringDecoder
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import java.io.FileInputStream
import java.util.Properties

object CaptureEventLog {
    var props = new Properties()

    def main(args: Array[String]): Unit = {
        //Read properties file
        props.load(new FileInputStream("config/spark-job.properties"));
        val projectTopics = props.getProperty("kafka.topics.list", "test")
        val topics = projectTopics.split(",")
        val kafkaBrokers: String = props.getProperty("metadata.broker.list", "localhost:9092")

        // Create context batch interval
        val conf = new SparkConf(true).set("spark.cassandra.connection.host", "10.2.3.10")
            .setAppName("CaptureEventLogs")
        conf.registerKryoClasses(Array(classOf[StatisticalProcessing], classOf[UpdateCassandraData]))
        conf.set("spark.cassandra.input.split.size_in_mb", "1500")

        val batchInterval = Integer.parseInt(props.getProperty("spark.batchInterval", "120"))
        val ssc = new StreamingContext(conf, Seconds(batchInterval))

        // Create direct kafka stream with brokers and topics
        val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)

        //Create Direct Stream per topic
        val kafkaDStreams = topics.map { topic =>
            KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
                ssc, kafkaParams, Set(topic))
        }

        // Get the lines, split them into words, count the words and print
        SparkProcessingFunctions.saveDataToCassandra(topics, kafkaDStreams)

        // Start the computation
        ssc.start()
        ssc.awaitTermination()
    }


}



