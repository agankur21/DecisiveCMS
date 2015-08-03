package com.scoopwhoop.dcms

import com.scoopwhoop.logger.Logger
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

object TestCassandra {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf(true)
            .set("spark.cassandra.connection.host", "10.2.3.10")
            .setAppName("TestCassandra")
        val sparkContext = new SparkContext(conf)
        val rdd = sparkContext.cassandraTable("test", "kv")
        Logger.logInfo(rdd.count.toString)
        Logger.logInfo(rdd.first.toString)
        Logger.logInfo(rdd.map(_.getInt("value")).sum.toString)
	sparkContext.stop()
    }
}
