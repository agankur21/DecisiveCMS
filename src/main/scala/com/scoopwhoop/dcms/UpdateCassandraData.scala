package com.scoopwhoop.dcms
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext,DataFrame,Row}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import scala.io.Source
import com.datastax.spark.connector.UDTValue
import org.joda.time.format.{DateTimeFormat}
import org.joda.time.DateTime

class UpdateCassandraData extends Serializable  {
    val eventFields = CommonFunctions.readResourceFile("event_fields")
    val jsonFields = eventFields.map(x =>x.split(",")(0))
    val tableEventFieldsIndexMap  = eventFields.map(x =>x.split(",")(1)).zipWithIndex.toMap


    case class GoogleData(url:String,start_date:String,end_date:String,page_views:Int,unique_page_views:Int,
                          avg_time_per_page:Double,entrances:Int,bounce_rate:Double,exit:Double,page_value:Double) extends Serializable

    def getEventsData(sparkContext: SparkContext, path: String): DataFrame = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
        return  sqlContext.read.json(path)
    }
    
    def getGoogleAnalyticsData(sparkContext: SparkContext, path: String):DataFrame = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
        import sqlContext.implicits._
        val inputFile = sparkContext.textFile(path)
        val startEndDate = inputFile.zipWithIndex.filter(_._2 == 3).map(_._1.replaceAll("#","").trim.split("-")).collect
        val Array(startDate,endDate) = startEndDate(0).map(CommonFunctions.convertDateStringFormat)
        val output = inputFile.zipWithIndex.filter(_._2 > 6).map(_._1).filter(x => (x.startsWith("/") || x.startsWith("\"/")))
            .map(CommonFunctions.cleanString)
            .map(_.split(",")).map(p => GoogleData("http://www.scoopwhoop.com" +p(0).stripPrefix("\"").stripSuffix("\"").trim,
            startDate,endDate,p(1).toInt,p(2).toInt,CommonFunctions.getTimeInMinutes(p(3)),p(4).toInt,
            CommonFunctions.getNumberFromPercentage(p(5)),CommonFunctions.getNumberFromPercentage(p(6)),
            CommonFunctions.getNumberFromCurrency(p(7)))).toDF()
        return output;
    }


    def updateGoogleAnalyticsData(gaData:DataFrame,keySpace:String,table:String):Unit= {
        Logger.logInfo(s"Updating the Cassandra Table $keySpace.$table............. ")
        gaData.map {case(x:Row) => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)) }.saveToCassandra(keySpace, table,
            SomeColumns("url","start_date","end_date","page_views","unique_page_views","avg_time_per_page",
                "entrances","bounce_rate","exit","page_value"))
        Logger.logInfo(s"Cassandra Table $keySpace.$table Updated !!")
    }
    
    def updateEventsData(eventData: DataFrame,keySpace:String,table:String):Unit = {
        Logger.logInfo(s"Updating the Cassandra Table $keySpace.$table............. ")
        val events = eventData.select(jsonFields(0),jsonFields(1),jsonFields(2),jsonFields(3),jsonFields(4),jsonFields(5),
            jsonFields(6),jsonFields(7),jsonFields(8))
        events.map {case(x:Row) => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)) }.saveToCassandra(keySpace, table,
            SomeColumns("url","user_id","event","time","category","from_url","event_destination","screen_location","referring_domain"))
        Logger.logInfo(s"Cassandra Table $keySpace.$table Updated !!")
    }

    def updateUsersData(eventData: DataFrame,keySpace:String,table:String):Unit = {
        Logger.logInfo(s"Updating the Cassandra Table $keySpace.$table............. ")
        val users = eventData.select("properties.distinct_id","properties.$browser","properties.$browser_version",
            "properties.$region","properties.$city","properties.mp_country_code","properties.$os", "properties.device","properties.$device")
        users.map {case(x:Row) => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)) }.saveToCassandra(keySpace, table,
            SomeColumns("user_id","browser","browser_version","region","city","country_code","os","device","device_type"))
        Logger.logInfo(s"Cassandra Table $keySpace.$table Updated !!")
    }

    def updatePageData(eventData: DataFrame,keySpace:String,table:String):Unit = {
        Logger.logInfo(s"Updating the Cassandra Table $keySpace.$table............. ")
        val pages = eventData.select("properties.url","properties.title","properties.category","properties.author",
            "properties.$screen_height","properties.$screen_width")
        pages.map {case(x:Row) => (x(0),x(1),x(2),x(3),x(4),x(5)) }.saveToCassandra(keySpace, table, 
            SomeColumns("url","title","category","author", "screen_height","screen_width"))
        Logger.logInfo(s"Cassandra Table $keySpace.$table Updated !!")
    }


}
