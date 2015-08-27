package com.scoopwhoop.reporting

import java.util.Properties
import com.datastax.spark.connector.CassandraRow
import com.scoopwhoop.logger.Logger
import net.liftweb.json.JsonAST.{JString, JField}
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.spark.streaming.dstream.InputDStream

object SparkProcessingFunctions {
        def parseEventLog(jsonString:String):Models.EventLog = {
                implicit val formats = DefaultFormats
                try {
                        val jsonObj = parse(jsonString)
                        return jsonObj.extract[Models.EventLog]

                }
                catch {
                        case pe: ParseException => {
                                Logger.logError(pe.getMessage)
                                return null
                        }
                }
                
        }

        def saveDataToCassandra(topics: Array[String], kafkaDStreams: Array[InputDStream[(String, String)]]): Unit = {
                for (i <- 0 until topics.length) {
                        kafkaDStreams(i).foreachRDD((rdd, time) => {

                        })
                }
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

        
        def processJoinedRowEventGoogle(data: (CassandraRow, CassandraRow)): (String, String, String, Int, Int, Int, Int, Int, Double, Int, Double, Int) = {
                val (eventRow, gaRow) = data
                val eventCount: (Int, Int, Int, Int) = getEventCount(CommonFunctions.getStringFromCassandraRow(eventRow, "event"))
                val output = (CommonFunctions.getStringFromCassandraRow(gaRow, "category"), CommonFunctions.getStringFromCassandraRow(gaRow, "start_date"),
                    CommonFunctions.getStringFromCassandraRow(gaRow, "end_date"), eventCount._1, eventCount._2, eventCount._3, eventCount._4, gaRow.getInt("page_views"),
                    gaRow.getDouble("avg_time_per_page"), gaRow.getInt("entrances"), gaRow.getDouble("bounce_rate") * gaRow.getInt("entrances"),
                    1)
                output
        }
        
        

}
