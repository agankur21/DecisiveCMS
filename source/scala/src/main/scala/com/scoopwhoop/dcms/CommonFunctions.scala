package com.scoopwhoop.dcms

import java.text.NumberFormat
import net.liftweb.json.JsonParser.ParseException
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Days, DateTime}
import com.datastax.spark.connector.{CassandraRow, UDTValue}
import scala.io.Source
import net.liftweb.json._
import org.apache.commons.lang3.StringEscapeUtils
import java.util.Arrays

object CommonFunctions extends Serializable {

    case class Page(title:String,link:String,author:String,pubon:String,s_heading:String,category:List[String],tags:List[String])
    
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

    def getTopElementsByFrequency(listElement: List[String], numTopElement:Int) :List[String] ={
        val elementWithFrequencies =   listElement.map(x => x -> 1).groupBy(_._1).mapValues{ case (list:List[(String,Int)]) => 
                                        list.map(x => x._2).foldLeft(0)((a,b)=> a+b)}
                                        .toList.sortWith(_._2 > _._2)
        val out  = elementWithFrequencies.slice(0,numTopElement).map(_._1)
        out
    }


    def processJoinedRowPagesEvent(data: (CassandraRow, CassandraRow)) :(List[String],Long,String) = {
        val (pagesRow, eventRow) = data
        val tags : List[String] = pagesRow.get[List[String]]("tags")
        val time = eventRow.getLong("time")
        val region = if(eventRow.isNullAt("city")) ""  else eventRow.getString("city")
        return (tags,time,region)
    }


}
