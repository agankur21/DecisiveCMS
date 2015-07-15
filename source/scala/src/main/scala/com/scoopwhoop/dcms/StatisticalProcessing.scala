package com.scoopwhoop.dcms
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel

class StatisticalProcessing extends Serializable {

    case class GoogleEventData(category: String, start_date: String, end_date: String, desktop_views: Int, mobile_views: Int,
                               clicks: Int, shares: Int, ga_page_views: Int, ga_unique_page_views: Int, ga_avg_time: Double,
                               ga_entrances: Int, ga_bounce_rate: Double)

    def mergeEventGoogleData(sparkContext: SparkContext, keySpace: String, eventTable: String, gaTable: String, outTable: String): Unit = {
        val rawEvents = sparkContext.cassandraTable(keySpace, eventTable).select("title", "event", "time")
        val joinedTable = rawEvents.joinWithCassandraTable("dcms", "google_analytics_data").on(SomeColumns("title")).coalesce(10000)

        val categoryData = joinedTable.filter(f => (CommonFunctions.isGreaterOrEqual(f._1.getLong("time"), f._2.getString("start_date"))) && (CommonFunctions.isLowerOrEqual(f._1.getLong("time"), f._2.getString("end_date"))))
            .map(processJoinedRowEventGoogle)
            .filter(f => (f._1 != "") && (f._1 != null))
            .map(f => (f._1, f._2, f._3) ->(f._4, f._5, f._6, f._7, f._8, f._9, f._10, f._11, f._12, f._13))
            .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7, x._8 + y._8, x._9 + y._9, x._10 + y._10))
            .map { case ((category: String, start_date: String, end_date: String), (desktop_views: Int, mobile_views: Int,
        clicks: Int, shares: Int, ga_page_views: Int, ga_unique_page_views: Int, ga_avg_time: Double,
        ga_entrances: Int, ga_bounce_number: Double, count: Int)) => GoogleEventData(category, start_date, end_date, desktop_views,
            mobile_views, clicks, shares, ga_page_views, ga_unique_page_views, ga_avg_time / count, ga_entrances, ga_bounce_number / ga_entrances)
        }

        val outputTextData = categoryData.map { case (x: GoogleEventData) => (x.category, x.start_date, x.end_date, x.desktop_views, x.mobile_views, x.clicks,
            x.shares, x.ga_page_views, x.ga_unique_page_views, x.ga_avg_time, x.ga_entrances, x.ga_bounce_rate).productIterator.toList.mkString("\t")
        }

        putData(sparkContext, outputTextData, "/opt/scoopwhoop/category_stats/out")

        categoryData.saveToCassandra(keySpace, outTable, SomeColumns("category", "start_date", "end_date", "desktop_views",
            "mobile_views", "clicks", "shares", "ga_page_views", "ga_unique_page_views", "ga_avg_time", "ga_entrances", "ga_bounce_rate"))
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

    def processJoinedRowEventGoogle(data: (CassandraRow, CassandraRow)): (String, String, String, Int, Int, Int, Int, Int, Int, Double, Int, Double, Int) = {
        val (eventRow, gaRow) = data
        val eventCount: (Int, Int, Int, Int) = getEventCount(eventRow.getString("event"))
        val output = (gaRow.getString("category"), gaRow.getString("start_date"), gaRow.getString("end_date"),
            eventCount._1, eventCount._2, eventCount._3, eventCount._4, gaRow.getInt("page_views"), gaRow.getInt("unique_page_views"),
            gaRow.getDouble("avg_time_per_page"), gaRow.getInt("entrances"), gaRow.getDouble("bounce_rate") * gaRow.getInt("entrances"),
            1)
        output
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
        val combinedData = joinedTable.map(processJoinedRowPagesEvent).flatMap{case(x,y,z) => x.map((_,y,z))}.persist(StorageLevel.MEMORY_ONLY_SER)
        val tagTimeData = combinedData.map(x => x._1 -> x._2).groupByKey.mapValues{ case (timeList: Iterable[Long]) => (
            CommonFunctions.getTopElementsByFrequency(timeList.toList.map(x => CommonFunctions.getDayHour(x).toString), 6),
            CommonFunctions.getTopElementsByFrequency(timeList.toList.map(x => CommonFunctions.getDayWeek(x)), 3))
        }
        tagTimeData.saveToCassandra(keySpace, outTable, SomeColumns("tag", "time", "day"))

        val tagRegionData = combinedData.map(x => x._1 -> x._3).filter(_._2 != "").groupByKey.mapValues{ case (regionList: Iterable[String]) =>
            CommonFunctions.getTopElementsByFrequency(regionList.toList, 20)
        }
        tagRegionData.saveToCassandra(keySpace, outTable, SomeColumns("tag", "region"))
    }

}
