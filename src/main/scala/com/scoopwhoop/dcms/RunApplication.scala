package com.scoopwhoop.dcms
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext,DataFrame}
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

object RunApplication {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf(true).set("spark.cassandra.connection.host", "10.2.3.10")
            .setAppName("RunApplication")
        val sparkContext = new SparkContext(conf)
        val eventsData = UpdateCassandraData.getData(sparkContext,"/home/cassandra/ScoopWhoop_2015-06-01")
        UpdateCassandraData.updateEventsData(eventsData,"dcms","events")
        UpdateCassandraData.updateUsersData(sparkContext,"dcms","users")
        UpdateCassandraData.updatePageData(sparkContext,"dcms","pages")
        sparkContext.stop()
    }
}
