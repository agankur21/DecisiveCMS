package com.scoopwhoop.recommender

import org.apache.spark.{SparkContext, SparkConf}

object RunApplication {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf(true).set("spark.cassandra.connection.host", "10.2.3.10")
            .setAppName("RunApplicationRecommender").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.registerKryoClasses(Array(classOf[SimilarItemProcessing], classOf[ItemItemSimilarity], classOf[NormalizedUserItemRating]))
        conf.set("spark.cassandra.input.split.size_in_mb", "50000")
        val sparkContext = new SparkContext(conf)
        val similarItemProcessing  = new SimilarItemProcessing
        val similarityRatings = similarItemProcessing.getRecommendations(sparkContext,"events","2015-06-01","2015-06-01")
        similarItemProcessing.updateCassandraTable("dcms","pages_preferred_together",similarityRatings,"2015-06-01","2015-06-01")
        sparkContext.stop()
    }
}

