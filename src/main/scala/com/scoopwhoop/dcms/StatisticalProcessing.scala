package com.scoopwhoop.dcms

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import com.datastax.spark.connector._
import org.apache.spark.sql.{SQLContext,DataFrame,Row}

class StatisticalProcessing extends Serializable {

    case class EventData(url: String, time: Long, category: String, desktop_views: Int, mobile_views: Int, clicks: Int, shares: Int);
    case class GoogleData(url: String, start_date: String, end_date: String, page_views: Int, unique_page_views: Int,
                          avg_time_per_page: Double, entrances: Int, bounce_rate: Double,exit:Double,page_value:Double);
    case class GoogleEventData(category: String, start_date: String, end_date: String, desktop_views: Int, mobile_views: Int,
                               clicks: Int, shares: Int, ga_page_views: Int, ga_unique_page_views: Int, ga_avg_time: Double,
                               ga_entrants: Int, ga_bounce_rate: Double)

    def mergeEventGoogleData(sparkContext: SparkContext, keySpace: String, eventTable: String, gaTable: String, outTable: String): Unit = {
        
        
        val rawEvents = sparkContext.cassandraTable(keySpace, eventTable).select("url", "event", "time", "category")

        def getEventCount(event: String): (Int, Int, Int, Int) = {
            event match {
                case "Desktop PVs" => (1, 0, 0, 0)
                case "Mobile PVs" => (0, 1, 0, 0)
                case "itemClick" => (0, 0, 1, 0)
                case "shareClick" => (0, 0, 0, 1)
                case _ => (0, 0, 0, 0)
            }
        }
        val events = rawEvents.map { case (x: CassandraRow) =>
            val eventCountTuple = getEventCount(x.getString("event"))
            EventData(x.getString("url"), x.getLong("time"), x.getString("category"), eventCountTuple._1, eventCountTuple._2,
                eventCountTuple._3, eventCountTuple._4)
        }.cache
        val googleData = sparkContext.cassandraTable[GoogleData](keySpace, gaTable).cache
        val eventsByUrl = events.keyBy(f => f.url)
        val googleDataByUrl = googleData.keyBy(f => f.url)
        //Join the tables by the url
        val joinedUsers = eventsByUrl.join(googleDataByUrl).cache
        val categoryData = joinedUsers
            .filter(f => (CommonFunctions.isGreaterOrEqual(f._2._1.time, f._2._2.start_date) &&
            CommonFunctions.isLowerOrEqual(f._2._1.time, f._2._2.end_date)))
            .map(f => (f._2._1.category, f._2._2.start_date, f._2._2.end_date) ->(f._2._1.desktop_views,
            f._2._1.mobile_views, f._2._1.clicks, f._2._1.shares, f._2._2.page_views, f._2._2.unique_page_views, f._2._2.avg_time_per_page,
            f._2._2.entrances, f._2._2.bounce_rate * f._2._2.entrances,1))
            .reduceByKey((x, y) => (x._1 + y._1,x._2 + y._2,x._3 + y._3,x._4 + y._4,x._5 + y._5,x._6 + y._6,x._7 + y._7,x._8 + y._8,x._9 + y._9,x._10+y._10))
            .map {case((category:String,start_date:String,end_date:String),(desktop_views: Int, mobile_views: Int,
        clicks: Int, shares: Int, ga_page_views: Int, ga_unique_page_views: Int, ga_avg_time: Double,
        ga_entrants: Int, ga_bounce_number: Double,count:Int)) => GoogleEventData(category,start_date,end_date,desktop_views,
            mobile_views,clicks,shares,ga_page_views,ga_unique_page_views,ga_avg_time/count,ga_entrants,ga_bounce_number/ga_entrants) }

        categoryData.saveToCassandra(keySpace,outTable,SomeColumns("category","start_date","end_date","desktop_views",
            "mobile_views","clicks","shares","ga_page_views","ga_unique_page_views","ga_avg_time","ga_entrances","ga_bounce_rate"))

    }
}
