package com.scoopwhoop.reporting

import org.apache.spark.{SparkContext, SparkConf}

object RunApplication {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf(true).set("spark.cassandra.connection.host", "10.2.3.10")
            .setAppName("RunApplicationReporting").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.registerKryoClasses(Array(classOf[StatisticalProcessing], classOf[UpdateCassandraData]))
        conf.set("spark.cassandra.input.split.size_in_mb", "1500")
        val sparkContext = new SparkContext(conf)
        val updateData = new UpdateCassandraData()
        //val eventsData = updateData.getEventsData(sparkContext, "/home/cassandra/ScoopWhoop_2015-06-01")
        //val googleAnalyticsData = updateData.getGoogleAnalyticsData(sparkContext, "/home/cassandra/google_data")
        //updateData.updateEventsData(eventsData, "dcms", "events")
        //updateData.updateUsersData(eventsData, "dcms", "users")
        //updateData.getAndUpdatePagesData(sparkContext, "dcms", "pages")
        //updateData.updateGoogleAnalyticsData(googleAnalyticsData, "dcms", "google_analytics_data")
        //val statisticalProcessing = new StatisticalProcessing()
        //statisticalProcessing.mergeEventGoogleData(sparkContext, "dcms", "events", "google_analytics_data", "google_category_statistics")
        //statisticalProcessing.mergeEventsPageData(sparkContext, "dcms", "events", "pages", "tag_statistics")
        sparkContext.stop()
    }

}
