package com.scoopwhoop.dcms

import com.scoopwhoop.dcms.Logger
import com.scoopwhoop.dcms.UpdateCassandraData
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext,DataFrame}
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

object RunApplication {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf(true)
            .set("spark.cassandra.connection.host", "10.2.3.10")
            .setAppName("RunApplication")
        val sparkContext = new SparkContext(conf)
        val eventsData = UpdateCassandraData.getData(sparkContext,"/home/cassandra/ScoopWhoop_2015-06-01")
        UpdateCassandraData.updateUsersData(eventsData,"dcms","users")
        UpdateCassandraData.updatePageData(eventsData,"dcms","pages")
        sparkContext.stop()
    }
}
