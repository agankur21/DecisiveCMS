package com.scoopwhoop.dcms
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext,DataFrame,Row}
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import scala.io.Source
import com.datastax.spark.connector.UDTValue

class UpdateCassandraData extends Serializable  {
    val eventFields = CommonFunctions.readResourceFile("event_fields")
    val jsonFields = eventFields.map(x =>x.split(",")(0))
    val tableEventFieldsIndexMap  = eventFields.map(x =>x.split(",")(1)).zipWithIndex.toMap
    
    def getData(sparkContext: SparkContext, path: String): DataFrame = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
        return  sqlContext.read.json(path)
    }


    def eventMapper(row:Row): CommonFunctions.Events={
        val tableEventFieldsIndexMap_ = this.tableEventFieldsIndexMap
        val user_data = UDTValue.fromMap(Map("browser" -> row(tableEventFieldsIndexMap_("browser")).toString,
            "browser_version" -> row(tableEventFieldsIndexMap_("browser_version")).toString,
            "region" -> row(tableEventFieldsIndexMap_("region")).toString,
            "city" -> row(tableEventFieldsIndexMap_("city")).toString,
            "country_code" -> row(tableEventFieldsIndexMap_("country_code")).toString,
            "os" -> row(tableEventFieldsIndexMap_("os")).toString,
            "device" -> row(tableEventFieldsIndexMap_("device")).toString,
            "device_type" -> row(tableEventFieldsIndexMap_("device_type")).toString))
        val referrer_data = UDTValue.fromMap(Map("initial_referrer" -> row(tableEventFieldsIndexMap_("initial_referrer")).toString,
            "initial_referring_domain" -> row(tableEventFieldsIndexMap_("initial_referring_domain")).toString,
            "referrer" -> row(tableEventFieldsIndexMap_("referrer")).toString,
            "referring_domain" -> row(tableEventFieldsIndexMap_("referring_domain")).toString))
        val utm_data = UDTValue.fromMap(Map("utm_campaign" -> row(tableEventFieldsIndexMap_("utm_campaign")).toString,
            "utm_content" -> row(tableEventFieldsIndexMap_("utm_content")).toString,
            "utm_medium" -> row(tableEventFieldsIndexMap_("utm_medium")).toString,
            "utm_source" -> row(tableEventFieldsIndexMap_("utm_medium")).toString))
        val eventRow = CommonFunctions.Events(row(tableEventFieldsIndexMap_("url")).toString,row(tableEventFieldsIndexMap_("user_id")).toString,row(tableEventFieldsIndexMap_("event")).toString,
            row(tableEventFieldsIndexMap_("time")).toString,row(tableEventFieldsIndexMap_("title")).toString,row(tableEventFieldsIndexMap_("category")).toString,
            row(tableEventFieldsIndexMap_("author")).toString,row(tableEventFieldsIndexMap_("screen_height")).toString,row(tableEventFieldsIndexMap_("screen_width")).toString,
            row(tableEventFieldsIndexMap_("from_url")).toString,row(tableEventFieldsIndexMap_("event_destination")).toString,row(tableEventFieldsIndexMap_("screen_location")).toString,
            row(tableEventFieldsIndexMap_("search_engine")).toString,row(tableEventFieldsIndexMap_("mp_keyword")).toString,row(tableEventFieldsIndexMap("mp_lib")).toString,
            row(tableEventFieldsIndexMap_("lib_version")).toString,user_data,referrer_data,utm_data)
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
