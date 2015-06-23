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

    
    def updateEventsData(eventData: DataFrame,keySpace:String,table:String):Unit = {
        val events = eventData.select("properties.url","properties.distinct_id","event","properties.time",
            "properties.$browser","properties.$browser_version","properties.$region","properties.$city",
            "properties.mp_country_code","properties.$os","properties.device","properties.$device","properties.title",
            "properties.category","properties.author","properties.$screen_height","properties.$screen_width",
            "properties.from","properties.which","properties.where","properties.$initial_referrer",
            "properties.$initial_referring_domain","properties.$referrer","properties.$search_engine",
            "properties.mp_keyword","properties.$mp_lib","properties.$lib_version","properties.$referring_domain",
            "properties.utm_campaign","properties.utm_content","properties.utm_medium",	"properties.utm_source")
        events.map {case(x:Row) => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),
            x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22),x(23),x(24),x(25),x(26),x(27),x(28),x(29),x(30),x(31)) }
            .saveToCassandra(keySpace, table, SomeColumns("url","user_id","event","time","browser","browser_version",
            "region","city","country_code","os", "device","device_type","title","category","author","screen_height",
            "screen_width","from_url","event_destination","screen_location","initial_referrer","initial_referring_domain",
            "referrer","search_engine","mp_keyword","mp_lib","lib_version","referring_domain","utm_campaign","utm_content",
            "utm_medium","utm_source"))
    }

    def updateUsersData(sparkContext: SparkContext,keySpace:String,table:String):Unit = {
        val userTable = sparkContext.cassandraTable[CommonFunctions.Users](keySpace,"events").select("user_id","browser",
            "browser_version","region", "city","country_code","os","device")
        userTable.map {case(x:CassandraRow) => (x.getString("user_id"),x.getString("browser"),x.getString("browser_version"),
            x.getString("region"),x.getString("city"),x.getString("country_code"),x.getString("os"),x.getString("device")) }
            .saveToCassandra(keySpace, table, SomeColumns("user_id","browser","browser_version","region", "city",
            "country_code","os","device"))

    }

    def updatePageData(sparkContext: SparkContext,keySpace:String,table:String):Unit = {
        val pageTable = sparkContext.cassandraTable[CommonFunctions.Pages](keySpace,"events").select("url","title","category",
            "author","screen_height","screen_width")
        pageTable.map {case(x:CassandraRow) => (x.getString("url"),x.getString("title"),x.getString("category"),
            x.getString("author"),x.getString("screen_height"),x.getString("screen_width")) }.saveToCassandra(keySpace, table,
            SomeColumns("url","title","category","author","screen_height","screen_width"))
    }


}
