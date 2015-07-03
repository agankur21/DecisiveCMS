package com.scoopwhoop.dcms

import java.text.NumberFormat

import org.apache.spark.sql.Row
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Days, DateTime}
import com.datastax.spark.connector.{CassandraRow, UDTValue}

import scala.io.Source

object CommonFunctions extends Serializable {


    def getDayWeek(timestamp: Long): String = {
        val date_time = new DateTime(timestamp*1000L);
        val day = date_time.dayOfWeek().getAsText;
        day
    }
    
    def getDate(timestamp:Long):String = {
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
        val date_time = new DateTime(timestamp*1000L);
        val time_minutes = date_time.getMinuteOfDay;
        time_minutes / 2
    }

    def getDayHour(timestamp: Long): Int = {
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

    case class GoogleEventData(category: String, start_date: String, end_date: String, desktop_views: Int, mobile_views: Int,
                               clicks: Int, shares: Int, ga_page_views: Int, ga_unique_page_views: Int, ga_avg_time: Double,
                               ga_entrances: Int, ga_bounce_rate: Double)

    def getEventCount(event: String): (Int, Int, Int, Int) = {
        event match {
            case "Desktop PVs" => (1, 0, 0, 0)
            case "Mobile PVs" => (0, 1, 0, 0)
            case "itemClick" => (0, 0, 1, 0)
            case "shareClick" => (0, 0, 0, 1)
            case _ => (0, 0, 0, 0)
        }
    }

    def processJoinedRow(data:(CassandraRow,CassandraRow)):(String,String,String,Int,Int,Int,Int,Int,Int,Double,Int,Double,Int) ={
        val (eventRow,gaRow) = data
        val eventCount :(Int,Int,Int,Int) = getEventCount(eventRow.getString("event"))
        val output = (eventRow.getString("category"),gaRow.getString("start_date"),gaRow.getString("end_date"),
            eventCount._1,eventCount._2,eventCount._3,eventCount._4,gaRow.getInt("page_views"),gaRow.getInt("unique_page_views"),
            gaRow.getDouble("avg_time_per_page"),gaRow.getInt("entrances"),gaRow.getDouble("bounce_rate")*gaRow.getInt("entrances"),
            1)
        output
    }
    

}
