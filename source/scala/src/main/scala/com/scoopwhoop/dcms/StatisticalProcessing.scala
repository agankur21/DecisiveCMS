package com.scoopwhoop.dcms
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel

class StatisticalProcessing extends Serializable {

    case class GoogleEventData(category: String, start_date: String, end_date: String, desktop_views: Int, mobile_views: Int,
                               clicks: Int, shares: Int, avg_time: Double,bounce_rate: Double)

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
        val (gaRow, eventRow) = data
        val eventCount: (Int, Int, Int, Int) = getEventCount(CommonFunctions.getStringFromCassandraRow(eventRow,"event"))
        val output = (CommonFunctions.getStringFromCassandraRow(gaRow,"category"), CommonFunctions.getStringFromCassandraRow(gaRow,"start_date"),
            CommonFunctions.getStringFromCassandraRow(gaRow,"end_date"),eventCount._1, eventCount._2, eventCount._3, eventCount._4,CommonFunctions.getIntFromCassandraRow(gaRow,"page_views"),
            CommonFunctions.getDoubleFromCassandraRow(gaRow,"avg_time_per_page"), CommonFunctions.getIntFromCassandraRow(gaRow,"entrances"), CommonFunctions.getDoubleFromCassandraRow(gaRow,"bounce_rate") * CommonFunctions.getIntFromCassandraRow(gaRow,"entrances"),
            1)
        output
    }
    
    
    def mergeEventGoogleData(sparkContext: SparkContext, keySpace: String, eventTable: String, gaTable: String, outTable: String): Unit = {
        val googleData = sparkContext.cassandraTable(keySpace, gaTable).select("title","start_date","end_date",
            "category","page_views","avg_time_per_page","entrances","bounce_rate")
        val joinedTable = googleData.joinWithCassandraTable(keySpace, eventTable).select("title", "event", "time").on(SomeColumns("title"))
        val categoryData = joinedTable.filter(f => (CommonFunctions.isGreaterOrEqual(f._2.getLong("time"), f._1.getString("start_date"))) && (CommonFunctions.isLowerOrEqual(f._2.getLong("time"), f._1.getString("end_date"))))
            .map(processJoinedRowEventGoogle).filter(f => (f._1 != "") && (f._1 != null))
        val aggregateCategoryData=  categoryData.map(f => (f._1, f._2, f._3) ->(f._4, f._5, f._6, f._7, f._8, f._9, f._10, f._11, f._12))
            .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7, x._8 + y._8, x._9 + y._9)).cache()
        val googleEventData =  aggregateCategoryData.map { case ((category: String, start_date: String, end_date: String), (desktop_views: Int, mobile_views: Int,
        clicks: Int, shares: Int, ga_page_views: Int,ga_avg_time: Double,ga_entrances: Int, ga_bounce_number: Double, count: Int)) => GoogleEventData(category, start_date, end_date, desktop_views,
            mobile_views, clicks, shares,ga_avg_time / count, ga_bounce_number / ga_entrances)
        }
        googleEventData.saveToCassandra(keySpace, outTable, SomeColumns("category", "start_date", "end_date", "desktop_views",
            "mobile_views", "clicks", "shares", "avg_time", "bounce_rate"))
    }
    

    
    def processJoinedRowPagesEvent(data: (CassandraRow, CassandraRow)) :(List[String],Long,String) = {
        val (pagesRow, eventRow) = data
        val tags : List[String] = pagesRow.get[List[String]]("tags")
        val time = eventRow.getLong("time")
        val region = if(eventRow.isNullAt("city")) ""  else eventRow.getString("city")
        return (tags,time,region)
    }
    

    def mergeEventsPageData(sparkContext: SparkContext, keySpace: String, eventTable: String, pagesTable: String, outTable: String): Unit = {
        val pagesData = sparkContext.cassandraTable(keySpace, pagesTable).select("title", "tags")
        val joinedTable = pagesData.joinWithCassandraTable(keySpace, eventTable).select("title","time","city")
        val combinedData = joinedTable.map(processJoinedRowPagesEvent).flatMap{case(x,y,z) => x.map((_,y,z))}.cache()
        val tagData = combinedData.filter(_._1 != "").spanBy(_._1).reduceByKey(_ ++ _).mapValues(_.toList).map{case (tag:String,data:List[(String,Long,String)]) => (tag,
                CommonFunctions.getTopElementsByFrequency(data.map(x => CommonFunctions.getDayHour(x._2).toString), 6),
                CommonFunctions.getTopElementsByFrequency(data.map(x => CommonFunctions.getDayWeek(x._2)), 3),
                CommonFunctions.getTopElementsByFrequency(data.map(_._3).filter(_ != ""), 20))}
        tagData.saveToCassandra(keySpace, outTable, SomeColumns("tag", "time","day","region"))
    }

}
