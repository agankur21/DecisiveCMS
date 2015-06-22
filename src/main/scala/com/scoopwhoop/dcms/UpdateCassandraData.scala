package com.scoopwhoop.dcms
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext,DataFrame}
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import scala.io.Source


object UpdateCassandraData {
    def getData(sparkContext: SparkContext, path: String): DataFrame = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
        return  sqlContext.read.json(path)
    }
    
    def updateUsersData(eventData: DataFrame,keySpace:String,table:String):Unit = {
        val users = eventData.select("properties.distinct_id","properties.$browser","properties.$browser_version",
                                    "properties.$Region,properties.$city,properties.mp_country_code,properties.$os",
                                        "properties.device")
        users.rdd.saveToCassandra(keySpace, table, SomeColumns("user_id","browser","browser_version","region",
                                    "city","country_code","os","device"))
    }

    def updatePageData(eventData: DataFrame,keySpace:String,table:String):Unit = {
        val pages = eventData.select("properties.url","properties.title","properties.category","properties.author",
                                        "properties.$screen_height","properties.$screen_width")
        pages.rdd.saveToCassandra(keySpace, table, SomeColumns("url","title","category","author",
            "screen_height","screen_width"))
    }


}
