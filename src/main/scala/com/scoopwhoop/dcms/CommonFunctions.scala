package com.scoopwhoop.dcms
import org.joda.time.{Days, DateTime}
import com.datastax.spark.connector.UDTValue

import scala.io.Source

object CommonFunctions {
    
    case class Users(user_id:String,browser : String,browser_version:String,region:String,city:String,country_code:String,
                     os: String,device:String)

    case class Pages(url:String,title:String,category:String,author:String,screen_height:String, screen_width:String)

    case class Events(url:String,user_id:String,event:String,time:String,title:String,category:String,author:String,
                 screen_height:String,screen_width:String,from_url:String,event_destination:String,screen_location:String,
                 search_engine:String,mp_keyword:String,mp_lib:String,lib_version:String,user_data:UDTValue,
                 referrer_data:UDTValue,utm_data:UDTValue)
    
    def getDayWeek(timestamp: Long): String = {
        val date_time = new DateTime(timestamp);
        val day = date_time.dayOfWeek().getAsText;
        day
    }

    def getTimeSlot(timestamp: Long): Int = {
        val date_time = new DateTime(timestamp);
        val time_minutes = date_time.getMinuteOfDay;
        time_minutes / 2
    }

    def getDayHour(timestamp: Long): Int = {
        val date_time = new DateTime(timestamp);
        date_time.getHourOfDay
    }

    def getDayDiff(timestamp: Long): Int = {
        return Days.daysBetween(new DateTime(timestamp), new DateTime()).getDays;
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
    

    
    
    
}
