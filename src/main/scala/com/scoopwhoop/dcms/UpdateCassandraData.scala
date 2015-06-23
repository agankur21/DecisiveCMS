package com.scoopwhoop.dcms
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext,DataFrame,Row}
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import scala.io.Source


object UpdateCassandraData {
    def getData(sparkContext: SparkContext, path: String): DataFrame = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
        return  sqlContext.read.json(path)
    }
    
    case class User(user_id :String,browser:String,browser_version:String,region:String,city:String,country_code:String,os:String,device:String)
    
    def updateUsersData(eventData: DataFrame,keySpace:String,table:String):Unit = {
        val users = eventData.select("properties.distinct_id","properties.$browser","properties.$browser_version",
                                    "properties.$region","properties.$city","properties.mp_country_code","properties.$os",
                                        "properties.device")
        users.map {case(x:Row) => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)) }.saveToCassandra(keySpace, table, SomeColumns("user_id","browser","browser_version","region","city","country_code","os","device"))
    }

    def updatePageData(eventData: DataFrame,keySpace:String,table:String):Unit = {
        val pages = eventData.select("properties.url","properties.title","properties.category","properties.author",
                                        "properties.$screen_height","properties.$screen_width")
        pages.map {case(x:Row) => (x(0),x(1),x(2),x(3),x(4),x(5)) }.saveToCassandra(keySpace, table, SomeColumns("url","title","category","author",
            "screen_height","screen_width"))
    }
    


}
