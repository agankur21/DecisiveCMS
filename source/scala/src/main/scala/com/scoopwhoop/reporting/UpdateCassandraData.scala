package com.scoopwhoop.reporting

import com.scoopwhoop.logger.Logger
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import java.net.URLDecoder
import scala.xml.XML

class UpdateCassandraData extends Serializable {
    val eventFields = CommonFunctions.readResourceFile("event_fields")
    val jsonFields = eventFields.map(x => x.split(",")(0))
    val tableEventFieldsIndexMap = eventFields.map(x => x.split(",")(1)).zipWithIndex.toMap


    case class GoogleData(title: String, start_date: String, end_date: String, category: String, page_views: Int, unique_page_views: Int,
                          avg_time_per_page: Double, entrances: Int, bounce_rate: Double, exit: Double, page_value: Double) extends Serializable

    def getEventsData(sparkContext: SparkContext, path: String): DataFrame = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
        return sqlContext.read.json(path)
    }

    def completeURL(path: String): String = {
        val shortPath = path.split("/").slice(0, 3).mkString("/")
        "http://www.scoopwhoop.com" + shortPath.split(" ")(0).stripPrefix("\"").stripSuffix("\"").trim
    }

    def getGoogleAnalyticsData(sparkContext: SparkContext, path: String): DataFrame = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
        import sqlContext.implicits._
        val inputFile = sparkContext.textFile(path)
        val startEndDate = inputFile.zipWithIndex.filter(_._2 == 3).map(_._1.replaceAll("#", "").trim.split("-")).collect
        val Array(startDate, endDate) = startEndDate(0).map(CommonFunctions.convertDateStringFormat)
        val output = inputFile.zipWithIndex.filter(_._2 > 6).map(_._1).filter(x => (x.startsWith("/") || x.startsWith("\"/"))).map(CommonFunctions.cleanString)
            .map(_.split(",")).map(p => (ParseDataFromAPI.getTitleFromURL(completeURL(p(0))), startDate, endDate,
            ParseDataFromAPI.getCategoryFromURL(completeURL(p(0)))) ->(p(1).toInt, p(2).toInt,
            CommonFunctions.getTimeInMinutes(p(3)), p(4).toInt, CommonFunctions.getNumberFromPercentage(p(5)),
            CommonFunctions.getNumberFromPercentage(p(6)), CommonFunctions.getNumberFromCurrency(p(7)), 1))
            .mapValues { case (page_views: Int, unique_page_views: Int, avg_time_per_page: Double, entrances: Int, bounce_rate: Double, exit: Double, page_value: Double, count: Int) =>
            (page_views, unique_page_views, avg_time_per_page, entrances, entrances * bounce_rate, exit * page_views, page_value, count)
        }
            .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7, x._8 + y._8))
            .map(x => GoogleData(x._1._1, x._1._2, x._1._3, x._1._4, x._2._1, x._2._2, if (x._2._8 == 0.0) 0.0 else x._2._3 / x._2._8, x._2._4, if (x._2._4 == 0.0) 0.0 else x._2._5 / x._2._4, x._2._6 / x._2._1, x._2._7))
            .toDF()
        return output;
    }


    def updateGoogleAnalyticsData(gaData: DataFrame, keySpace: String, table: String): Unit = {
        Logger.logInfo(s"Updating the Cassandra Table $keySpace.$table............. ")
        gaData.map { case (x: Row) => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10))}.filter(_._1 != "").saveToCassandra(keySpace, table,
            SomeColumns("title", "start_date", "end_date", "category", "page_views", "unique_page_views", "avg_time_per_page", "entrances", "bounce_rate", "exit", "page_value"))
        Logger.logInfo(s"Cassandra Table $keySpace.$table Updated !!")
    }

    def updateEventsData(eventData: DataFrame, keySpace: String, table: String): Unit = {
        Logger.logInfo(s"Updating the Cassandra Table $keySpace.$table............. ")
        val events = eventData.select(jsonFields(0), jsonFields(1), jsonFields(2), jsonFields(3), jsonFields(4), jsonFields(5),
            jsonFields(6), jsonFields(7), jsonFields(8), jsonFields(9), jsonFields(10), jsonFields(11)).distinct
        events.map { case (x: Row) => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11))}.filter(_._1 != "").saveToCassandra(keySpace, table,
            SomeColumns("title", "user_id", "event", "time", "region", "city", "country_code", "category", "from_url", "event_destination", "screen_location", "referring_domain"))
        Logger.logInfo(s"Cassandra Table $keySpace.$table Updated !!")
    }

    def updateUsersData(eventData: DataFrame, keySpace: String, table: String): Unit = {
        Logger.logInfo(s"Updating the Cassandra Table $keySpace.$table............. ")
        val users = eventData.select("properties.distinct_id", "properties.$browser", "properties.$browser_version",
            "properties.$region", "properties.$city", "properties.mp_country_code", "properties.$os", "properties.device", "properties.$device").distinct
        users.map { case (x: Row) => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8))}.saveToCassandra(keySpace, table,
            SomeColumns("user_id", "browser", "browser_version", "region", "city", "country_code", "os", "device", "device_type"))
        Logger.logInfo(s"Cassandra Table $keySpace.$table Updated !!")
    }

    def updatePageData(data: RDD[CommonFunctions.Page], keySpace: String, table: String): Unit = {
        data.map { case (x: CommonFunctions.Page) => (StringEscapeUtils.unescapeHtml4(x.title).replaceAll("\\p{C}", ""), URLDecoder.decode(x.link, "UTF-8"), x.author, x.pubon, x.s_heading, x.category, x.tags)}.filter(_._1 != "").saveToCassandra(keySpace, table,
            SomeColumns("title", "url", "author", "published_date", "super_heading", "category", "tags"))

    }


    def getAndUpdatePagesData(sparkContext: SparkContext, keySpace: String, table: String): Unit = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
        import sqlContext.implicits._
        var listPages: List[CommonFunctions.Page] = List()
        var offset: Int = 0
        val limit: Int = 100
        var pageDataFrame: RDD[CommonFunctions.Page] = null
        Logger.logInfo(s"Updating the Cassandra Table $keySpace.$table............. ")
        do {
            listPages = ParseDataFromAPI.getPagesFromAPI(offset, limit)
            pageDataFrame = sparkContext.parallelize(listPages)
            updatePageData(pageDataFrame, keySpace, table)
            Logger.logInfo(s"Updated pages from $offset to next $limit")
            offset += 100
        }
        while (listPages.length > 0)
        Logger.logInfo(s"Cassandra Table $keySpace.$table Updated !!")
    }


}
