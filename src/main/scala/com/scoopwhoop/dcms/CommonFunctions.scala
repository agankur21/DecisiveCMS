package com.scoopwhoop.dcms

import java.text.NumberFormat

import org.apache.spark.sql.Row
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Days, DateTime}
import com.datastax.spark.connector.UDTValue

import scala.io.Source

object CommonFunctions extends Serializable {


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
    
    def getDateString(input:String): String ={
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
}
