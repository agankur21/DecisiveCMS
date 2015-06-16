package com.scoopwhoop.dcms
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext,DataFrame}
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

object TestCassandra {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf(true)
            .set("spark.cassandra.connection.host", "127.0.0.1")
            .setAppName("TestCassandra")
        val sparkContext = new SparkContext(conf)
        val rdd = sparkContext.cassandraTable("test", "kv")
        Logger.logInfo(rdd.count.toString)
        Logger.logInfo(rdd.first.toString)
        Logger.logInfo(rdd.map(_.getInt("value")).sum.toString)
        sparkContext.stop()
    }
}