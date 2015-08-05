package com.scoopwhoop.recommender

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.joda.time._
import org.joda.time.format._
import org.joda.time.Days
import org.joda.time.DateTime
import com.scoopwhoop.logger._

class NormalizedUserItemRating extends RatingModule {

    private def clean(args: String): String = {
        args.replaceAll( """(?m)\s+$""", "")
    }

    private def getDayDiff(timestamp: String): Int = {
        return Days.daysBetween(new DateTime(timestamp.toLong), new DateTime()).getDays;
    }

    override def getNormalisedUserItemRating(listOfActivityData: RDD[Array[String]], ratingsData: Map[String, Int], timeIndex: Int, userIndex: Int, itemIndex: Int, activityIndex: Int): RDD[((String, String), Float)] = {
        Logger.logInfo("Getting Information on the data")
        val numPartitions = listOfActivityData.partitions.size
        val sparkContext = listOfActivityData.sparkContext
        val ratingsBroad = sparkContext.broadcast(ratingsData)
        Logger.logInfo("Number of Partitions : " + numPartitions.toString)
        Logger.logInfo("Updating Ratings Value")
        val userItemRatingWeights = listOfActivityData.map { elements =>
            (clean(elements(userIndex)), clean(elements(itemIndex))) ->(ratingsBroad.value.getOrElse(clean(elements(activityIndex)), 0), 1 / (1.0 + getDayDiff(clean(elements(timeIndex)))))
        }
        val userItemRatings = userItemRatingWeights
            .combineByKey((v) => (v._1 * v._2, v._2), (acc: (Double, Double), v) => (acc._1 + (v._1 * v._2), acc._2 + v._2), (acc1: (Double, Double), acc2: (Double, Double)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
            .mapValues { case (totalWeightedRatings, totalWeights) => if (totalWeights > 0) (totalWeightedRatings / totalWeights).toFloat else 0}
        return userItemRatings;
    }

}
