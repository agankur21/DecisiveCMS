package com.scoopwhoop.dcms

import org.apache.spark.{SparkContext, SparkConf}

object RunApplication {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf(true).set("spark.cassandra.connection.host", "10.2.3.10")
            .setAppName("RunApplication")
        val sparkContext = new SparkContext(conf)
        val updateData = new UpdateCassandraData()
        val eventsData = updateData.getEventsData(sparkContext,"/home/cassandra/ScoopWhoop_2015-06-01")
        val googleAnalyticsData =updateData.getGoogleAnalyticsData(sparkContext,"/home/cassandra/google_data")
        //updateData.updateEventsData(eventsData,"dcms","events")
        //updateData.updateUsersData(eventsData,"dcms","users")
        //updateData.updatePageData(eventsData,"dcms","pages")
        updateData.updateGoogleAnalyticsData(googleAnalyticsData,"dcms","google_analytics_data")
        val statisticalProcessing  = new StatisticalProcessing()
        statisticalProcessing.mergeEventGoogleData(sparkContext,"dcms","events","google_analytics_data","google_category_statistics")
        sparkContext.stop()
    }
}
