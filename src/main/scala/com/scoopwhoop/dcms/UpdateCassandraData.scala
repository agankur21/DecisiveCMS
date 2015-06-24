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

    case class Users(user_id:Any,browser : Any,browser_version:Any,region:Any,city:Any,country_code:Any,
                     os: Any,device:Any,device_type:Any) extends Serializable

    case class Pages(url:Any,title:Any,category:Any,author:Any,screen_height:Any, screen_width:Any) extends Serializable

    case class Events(url:Any,user_id:Any,event:Any,time:Any,title:Any,category:Any,author:Any,
                      screen_height:Any,screen_width:Any,from_url:Any,event_destination:Any,screen_location:Any,
                      search_engine:Any,mp_keyword:Any,mp_lib:Any,lib_version:Any,user_data:UDTValue,
                      referrer_data:UDTValue,utm_data:UDTValue) extends Serializable


    def getData(sparkContext: SparkContext, path: String): DataFrame = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
        return  sqlContext.read.json(path)
    }
  
    def eventMapper(row:Row):Events={
        val tableEventFieldsIndexMap_ = this.tableEventFieldsIndexMap
        val user_data = UDTValue.fromMap(Map("browser" -> row(tableEventFieldsIndexMap_("browser")),
            "browser_version" -> row(tableEventFieldsIndexMap_("browser_version")),
            "region" -> row(tableEventFieldsIndexMap_("region")),
            "city" -> row(tableEventFieldsIndexMap_("city")),
            "country_code" -> row(tableEventFieldsIndexMap_("country_code")),
            "os" -> row(tableEventFieldsIndexMap_("os")),
            "device" -> row(tableEventFieldsIndexMap_("device")),
            "device_type" -> row(tableEventFieldsIndexMap_("device_type"))))
        val referrer_data = UDTValue.fromMap(Map("initial_referrer" -> row(tableEventFieldsIndexMap_("initial_referrer")),
            "initial_referring_domain" -> row(tableEventFieldsIndexMap_("initial_referring_domain")),
            "referrer" -> row(tableEventFieldsIndexMap_("referrer")),
            "referring_domain" -> row(tableEventFieldsIndexMap_("referring_domain"))))
        val utm_data = UDTValue.fromMap(Map("utm_campaign" -> row(tableEventFieldsIndexMap_("utm_campaign")),
            "utm_content" -> row(tableEventFieldsIndexMap_("utm_content")),
            "utm_medium" -> row(tableEventFieldsIndexMap_("utm_medium")),
            "utm_source" -> row(tableEventFieldsIndexMap_("utm_medium"))))
        val eventRow = Events(row(tableEventFieldsIndexMap_("url")),row(tableEventFieldsIndexMap_("user_id")),row(tableEventFieldsIndexMap_("event")),
            row(tableEventFieldsIndexMap_("time")),row(tableEventFieldsIndexMap_("title")),row(tableEventFieldsIndexMap_("category")),
            row(tableEventFieldsIndexMap_("author")),row(tableEventFieldsIndexMap_("screen_height")),row(tableEventFieldsIndexMap_("screen_width")),
            row(tableEventFieldsIndexMap_("from_url")),row(tableEventFieldsIndexMap_("event_destination")),row(tableEventFieldsIndexMap_("screen_location")),
            row(tableEventFieldsIndexMap_("search_engine")),row(tableEventFieldsIndexMap_("mp_keyword")),row(tableEventFieldsIndexMap("mp_lib")),
            row(tableEventFieldsIndexMap_("lib_version")),user_data,referrer_data,utm_data)
        return eventRow;
    }

    def userMapper(row:CassandraRow):Users = {
        val user_info: UDTValue = row.getUDTValue("user_data")
        val user = Users(row.get[Any]("user_id"),user_info.get[Any]("browser"),user_info.get[Any]("browser_version"),user_info.get[Any]("region"),
            user_info.get[Any]("city"),user_info.get[Any]("country_code"),user_info.get[Any]("os"),user_info.get[Any]("device"),user_info.get[Any]("device_type"))
        return user
    }
    
    def updateEventsData(eventData: DataFrame,keySpace:String,table:String):Unit = {
        val events = eventData.select(jsonFields(0),jsonFields(1),jsonFields(2),jsonFields(3),jsonFields(4),jsonFields(5),
            jsonFields(6),jsonFields(7),jsonFields(8),jsonFields(9),jsonFields(10),jsonFields(11),jsonFields(12),
            jsonFields(13),jsonFields(14),jsonFields(15),jsonFields(16),jsonFields(17),jsonFields(18),jsonFields(19),
            jsonFields(20),jsonFields(21),jsonFields(22),jsonFields(23),jsonFields(24),jsonFields(25),jsonFields(26),
            jsonFields(27),jsonFields(28),jsonFields(29),jsonFields(30),jsonFields(31))
        events.map(eventMapper).saveToCassandra(keySpace, table,SomeColumns("url","user_id","event","time","title","category",
            "author", "screen_height","screen_width","from_url","event_destination","screen_location","search_engine",
            "mp_keyword","mp_lib","lib_version","user_data","referrer_data","utm_data"))
    }


    def updateUsersData(sparkContext: SparkContext,keySpace:String,table:String):Unit = {
        val userTable = sparkContext.cassandraTable(keySpace,"events").select("user_id","user_data")
        userTable.map(userMapper).saveToCassandra(keySpace, table, SomeColumns("user_id","browser","browser_version","region", "city",
            "country_code","os","device","device_type"))

    }

    def updatePageData(sparkContext: SparkContext,keySpace:String,table:String):Unit = {
        val pageTable = sparkContext.cassandraTable[Pages](keySpace,"events").select("url","title","category",
            "author","screen_height","screen_width")
        pageTable.saveToCassandra(keySpace, table, SomeColumns("url","title","category","author","screen_height","screen_width"))
    }

}
