package com.scoopwhoop.dcms
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext,DataFrame,Row}
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import scala.io.Source
import com.datastax.spark.connector.UDTValue

object UpdateCassandraData  {

    val eventFields = CommonFunctions.readResourceFile("event_fields")
    val jsonFields = eventFields.map(x =>x.split(",")(0))
    val tableEventFieldsIndexMap  = eventFields.map(x =>x.split(",")(1)).zipWithIndex.toMap
    
    def getData(sparkContext: SparkContext, path: String): DataFrame = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
        return  sqlContext.read.json(path)
    }

    
    def eventMapper(row:Row): CommonFunctions.Events={
        val user_data = UDTValue.fromMap(Map("browser" -> row(tableEventFieldsIndexMap("browser")),
            "browser_version" -> row(tableEventFieldsIndexMap("browser_version")),
            "region" -> row(tableEventFieldsIndexMap("region")),
            "city" -> row(tableEventFieldsIndexMap("city")),
            "country_code" -> row(tableEventFieldsIndexMap("country_code")),
            "os" -> row(tableEventFieldsIndexMap("os")),
            "device" -> row(tableEventFieldsIndexMap("device")),
            "device_type" -> row(tableEventFieldsIndexMap("device_type"))))
        val referrer_data = UDTValue.fromMap(Map("initial_referrer" -> row(tableEventFieldsIndexMap("initial_referrer")),
            "initial_referring_domain" -> row(tableEventFieldsIndexMap("initial_referring_domain")),
            "referrer" -> row(tableEventFieldsIndexMap("referrer")),
            "referring_domain" -> row(tableEventFieldsIndexMap("referring_domain"))))
        val utm_data = UDTValue.fromMap(Map("utm_campaign" -> row(tableEventFieldsIndexMap("utm_campaign")),
            "utm_content" -> row(tableEventFieldsIndexMap("utm_content")),
            "utm_medium" -> row(tableEventFieldsIndexMap("utm_medium")),
            "utm_source" -> row(tableEventFieldsIndexMap("utm_medium"))))
        val eventRow = CommonFunctions.Events(row(tableEventFieldsIndexMap("url")).toString,row(tableEventFieldsIndexMap("user_id")).toString,row(tableEventFieldsIndexMap("event")).toString,
            row(tableEventFieldsIndexMap("time")).toString,row(tableEventFieldsIndexMap("title")).toString,row(tableEventFieldsIndexMap("category")).toString,
            row(tableEventFieldsIndexMap("author")).toString,row(tableEventFieldsIndexMap("screen_height")).toString,row(tableEventFieldsIndexMap("screen_width")).toString,
            row(tableEventFieldsIndexMap("from_url")).toString,row(tableEventFieldsIndexMap("event_destination")).toString,row(tableEventFieldsIndexMap("screen_location")).toString,
            row(tableEventFieldsIndexMap("search_engine")).toString,row(tableEventFieldsIndexMap("mp_keyword")).toString,row(tableEventFieldsIndexMap("mp_lib")).toString,
            row(tableEventFieldsIndexMap("lib_version")).toString,user_data,referrer_data,utm_data)
        return eventRow;
    }
    
    def updateEventsData(eventData: DataFrame,keySpace:String,table:String):Unit = {
       
        val events = eventData.select(jsonFields(0),jsonFields(1),jsonFields(2),jsonFields(3),jsonFields(4),jsonFields(5),
            jsonFields(6),jsonFields(7),jsonFields(8),jsonFields(9),jsonFields(10),jsonFields(11),jsonFields(12),
            jsonFields(13),jsonFields(14),jsonFields(15),jsonFields(16),jsonFields(17),jsonFields(18),jsonFields(19),
            jsonFields(20),jsonFields(21),jsonFields(22),jsonFields(23),jsonFields(24),jsonFields(25),jsonFields(26),
            jsonFields(27),jsonFields(28),jsonFields(29),jsonFields(30),jsonFields(31))
        events.map(eventMapper).saveToCassandra(keySpace, table)
    }

    def updateUsersData(sparkContext: SparkContext,keySpace:String,table:String):Unit = {
        val userTable = sparkContext.cassandraTable[CommonFunctions.Users](keySpace,"events").select("user_id","browser",
            "browser_version","region", "city","country_code","os","device")
        userTable.saveToCassandra(keySpace, table, SomeColumns("user_id","browser","browser_version","region", "city",
            "country_code","os","device"))

    }

    def updatePageData(sparkContext: SparkContext,keySpace:String,table:String):Unit = {
        val pageTable = sparkContext.cassandraTable[CommonFunctions.Pages](keySpace,"events").select("url","title","category",
            "author","screen_height","screen_width")
        pageTable.saveToCassandra(keySpace, table, SomeColumns("url","title","category","author","screen_height","screen_width"))
    }


}
