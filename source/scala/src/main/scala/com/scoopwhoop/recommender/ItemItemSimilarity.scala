package com.scoopwhoop.recommender

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}


class ItemItemSimilarity extends RecommenderAlgorithmModule{
    
    private def convertStringToID(stringElement:String,map: scala.collection.Map[String,Long]) :Long = {
        map.getOrElse(stringElement,-1)
    }

    private def convertIDToString(id:Long,map: scala.collection.Map[Long,String]) :String = {
        map.getOrElse(id,"")
    }

    /** Calculates the cosine similarity of each item with other item based on user preferences
      * @param sparkContext
      * @param inputData
      * @return An RDD for (String,String,Float)
      */
    def calculatePredictedRating(sparkContext: SparkContext,ratings:RDD[(String,String,Float)]): RDD[(String,String,Float)] = {
        val userIdMapBroad = sparkContext.broadcast(ratings.map(_._1).distinct.zipWithIndex.collectAsMap())
        val itemIdMapBroad = sparkContext.broadcast(ratings.map(_._2).distinct.zipWithIndex.collectAsMap())
        val idItemMapBroad = sparkContext.broadcast(itemIdMapBroad.value.map(x => x._2 -> x._1))
        val userItemRating = ratings.map{case(userId:String,itemId:String,rating:Float) =>
            new MatrixEntry(convertStringToID(userId,userIdMapBroad.value),
                convertStringToID(itemId,itemIdMapBroad.value),rating.toDouble)}
        userItemRating.cache()
        val ratingCoordinateMatrix = new CoordinateMatrix(userItemRating)
        val ratingRowMatrix = ratingCoordinateMatrix.toRowMatrix()
        val itemItemCoordinateMatrix = ratingRowMatrix.columnSimilarities(0.3)
        val similarityEntries = itemItemCoordinateMatrix.entries.distinct.map{ case(x : MatrixEntry) =>
            (convertIDToString(x.i,idItemMapBroad.value),convertIDToString(x.j,idItemMapBroad.value),x.value.toFloat)}
        val sortedEntries = similarityEntries.sortBy(_._3,ascending = false)
        return sortedEntries;
    }

}
