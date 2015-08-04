package com.scoopwhoop.recommender

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

class SimilarItemProcessing extends ApplicationProcesses {

    private def getEventsData(sparkContext: SparkContext, tableName: String, startDate: String, endDate: String): RDD[Array[String]] = {
        val events = sparkContext.cassandraTable("dcms", "events").select("user_id", "title", "event", "time")
        val filteredEvents = events.map { case (row: CassandraRow) => (row.getString("user_id"),
            row.getString("title"), row.getString("event"), row.getString("time"))
        }
            .filter { case (user_id, title, event, time) => ((user_id != "") && (title != ""))}
            .filter { case (user_id, title, event, time) => (CommonFunctions.isGreaterOrEqual(time.toLong, startDate)
            && CommonFunctions.isLowerOrEqual(time.toLong, endDate))
        }
            .map { case (user_id, title, event, time) => Array(user_id, title, event, time)}
        return filteredEvents
    }

    def getRecommendations(sparkContext: SparkContext, tableName: String, startDate: String, endDate: String): RDD[(String, String, Float)] = {
        val ratingsModule = new NormalizedUserItemRating
        val recommenderModule = new ItemItemSimilarity
        val eventsData = getEventsData(sparkContext, tableName, startDate, endDate)
        val ratingsData = calculateNormalisedRating(eventsData, ratingsModule)
        val similarityRatings = calculateRecommendedRatings(ratingsData, recommenderModule)
        return similarityRatings
    }

    def calculateNormalisedRating(inputData: RDD[Array[String]], ratingModule: RatingModule): RDD[(String, String, Float)] = {
        val ratings = ratingModule.getNormalisedUserItemRating(inputData, CommonFunctions.eventScore, 3, 0, 1, 2)
            .map { case ((user_id: String, title: String), rating: Float) => (user_id, title, rating)}
        ratings
    }

    def calculateRecommendedRatings(ratingsData: RDD[(String, String, Float)], recommenderAlgorithmModule: RecommenderAlgorithmModule): RDD[(String, String, Float)] = {
        return recommenderAlgorithmModule.calculatePredictedRating(ratingsData);
    }


    def updateCassandraTable(keySpace: String, tableName: String, similarityRatings: RDD[(String, String, Float)], startDate: String, endDate: String): Unit = {
        val modifiedData = similarityRatings.spanBy(_._1).reduceByKey(_ ++ _).mapValues(_.toList)
            .map { case (title: String, data: List[(String, String, Float)]) =>
            (title, startDate, endDate, data.filter(x => x != title).sortBy(-_._3).map(_._2).slice(0, 10))
        }
        modifiedData.saveToCassandra(keySpace, tableName, SomeColumns("title", "start_date", "end_date", "similar_pages"))
    }


}
