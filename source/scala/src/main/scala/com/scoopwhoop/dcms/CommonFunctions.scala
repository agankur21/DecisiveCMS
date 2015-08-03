package com.scoopwhoop.dcms

import java.text.NumberFormat
import net.liftweb.json.JsonParser.ParseException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Days, DateTime}
import com.datastax.spark.connector.{CassandraRow, UDTValue}
import scala.io.Source
import net.liftweb.json._
import org.apache.commons.lang3.StringEscapeUtils
import java.util.Arrays

object CommonFunctions extends Serializable {

    case class Page(title:String,link:String,author:String,pubon:String,s_heading:String,category:List[String],tags:List[String],content:String)
    
    def getDayWeek(timestamp: Long): String = {
        if (timestamp == 0)  return ""
        val date_time = new DateTime(timestamp*1000L);
        val day = date_time.dayOfWeek().getAsText;
        day
    }
    
    def getDate(timestamp:Long):String = {
        if (timestamp == 0)  return ""
        val date_time = new DateTime(timestamp*1000L);
        return date_time.toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
    }
    
    def isGreaterOrEqual(timestamp:Long,date:String):Boolean ={
        return (getDate(timestamp) >= date)
    }

    def isLowerOrEqual(timestamp:Long,date:String):Boolean ={
        return (getDate(timestamp) <= date)
    }

    def getTimeSlot(timestamp: Long): Int = {
        if (timestamp == 0)  return 0
        val date_time = new DateTime(timestamp*1000L);
        val time_minutes = date_time.getMinuteOfDay;
        time_minutes / 2
    }

    def getDayHour(timestamp: Long): Int = {
        if (timestamp == 0)  return 0
        val date_time = new DateTime(timestamp*1000L);
        date_time.getHourOfDay
    }

    def getDayDiff(timestamp: Long): Int = {
        return Days.daysBetween(new DateTime(timestamp*1000L), new DateTime()).getDays;
    }

    def isInteger(text:String):Boolean = {
        text.forall(_.isDigit)
    }

    def isFloat(text: String) :Boolean= {
        try {
            text.toFloat;
            return true;
        } catch {case _:Throwable => return false}
    }

    def isBoolean(text:String): Boolean ={
        text match{
            case ("true" | "1" | "false"| "0") => true
            case _ => false
        }

    }

    def getDataType(text:String):String = {
        if (isBoolean(text)) "Boolean"
        else if (isInteger(text)) "Integer"
        else if (isFloat(text)) "Float"
        else "String"
    }
    
    def readResourceFile(fileName:String): List[String] ={
        val lines = Source.fromURL(getClass.getResource("/"+fileName)).getLines()
        lines.toList
        
    }
    
    def convertDateStringFormat(input:String): String ={
        val pattern = "yyyyMMdd";
        val dateTime  = DateTime.parse(input, DateTimeFormat.forPattern(pattern));
        dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
    }
    
    def cleanString(input:String):String = {
        val pattern = """\"(\d+),(\d+)\"""".r
        return pattern.replaceAllIn (input,m => m.group(1).toString + m.group(2).toString )
    }
    
    def getNumberFromPercentage(input:String):Double = {
       val format = NumberFormat.getPercentInstance();
       val number = format.parse(input) ;
       number.doubleValue() ;
    }

    def getNumberFromCurrency(input:String):Double = {
        val format = NumberFormat.getCurrencyInstance();
        val number = format.parse(input) ;
        number.doubleValue() ;
    }
    
    def getTimeInMinutes(input:String) :Double = {
        val timeArray = input.split(":")
        return ((timeArray(0).toDouble*60) + timeArray(1).toDouble + (timeArray(2).toDouble/60))
        
    }

    def getTopElementsByFrequency(listElement: List[String], numTopElement:Int) :List[String] ={
        val elementWithFrequencies =   listElement.map(x => x -> 1).groupBy(_._1).mapValues{ case (list:List[(String,Int)]) => 
                                        list.map(x => x._2).foldLeft(0)((a,b)=> a+b)}
                                        .toList.sortWith(_._2 > _._2)
        val out  = elementWithFrequencies.slice(0,numTopElement+1).map(_._1)
        out
    }

    
    def getStringFromCassandraRow(row:CassandraRow,element:String):String = {
        if(row.isNullAt(element)) ""  else row.getString(element)
    }

    def getLongFromCassandraRow(row:CassandraRow,element:String):Long = {
        if(row.isNullAt(element)) 0  else row.getLong(element)
    }

    def getIntFromCassandraRow(row:CassandraRow,element:String):Int = {
        if(row.isNullAt(element)) 0  else row.getInt(element)
    }

    def getDoubleFromCassandraRow(row:CassandraRow,element:String):Double = {
        if(row.isNullAt(element)) 0  else row.getDouble(element)
    }
    
    def putData(sparkContext: SparkContext, data: RDD[String], path: String): Unit = {
        val hadoopConf = sparkContext.hadoopConfiguration
        val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), hadoopConf)
        try {
            hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
        } catch {
            case _: Throwable => {}
        }
        data.saveAsTextFile(path);
    }

    def getEventCount(event: String): (Int, Int, Int, Int) = {
        event match {
            case "Desktop PVs" => (1, 0, 0, 0)
            case "Mobile PVs" => (0, 1, 0, 0)
            case "itemClick" => (0, 0, 1, 0)
            case "shareClick" => (0, 0, 0, 1)
            case _ => (0, 0, 0, 0)
        }
    }

    def processJoinedRowEventGoogle(data: (CassandraRow, CassandraRow)): (String, String, String, Int, Int, Int, Int, Int,Double, Int, Double, Int) = {
        val (eventRow, gaRow) = data
        val eventCount: (Int, Int, Int, Int) = getEventCount(CommonFunctions.getStringFromCassandraRow(eventRow,"event"))
        val output = (CommonFunctions.getStringFromCassandraRow(gaRow,"category"), CommonFunctions.getStringFromCassandraRow(gaRow,"start_date"),
            CommonFunctions.getStringFromCassandraRow(gaRow,"end_date"),eventCount._1, eventCount._2, eventCount._3, eventCount._4, gaRow.getInt("page_views"),
            gaRow.getDouble("avg_time_per_page"), gaRow.getInt("entrances"), gaRow.getDouble("bounce_rate") * gaRow.getInt("entrances"),
            1)
        output
    }

}
