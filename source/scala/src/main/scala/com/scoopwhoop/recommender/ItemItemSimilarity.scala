package com.scoopwhoop.recommender

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.{RowMatrix,MatrixEntry}
import org.apache.spark.mllib.linalg.{Vectors}
import com.scoopwhoop.logger.Logger


class ItemItemSimilarity extends RecommenderAlgorithmModule {
    
    var userIdMap = scala.collection.Map[String, Long]()
    var itemIdMap  = scala.collection.Map[String, Long]()
    var idItemMap = scala.collection.Map[Long, String]()
    def convertStringToID(stringElement: String, map: scala.collection.Map[String, Long]): Long = {
        map.getOrElse(stringElement, -1)
    }

    def convertIDToString(id: Long, map: scala.collection.Map[Long, String]): String = {
        map.getOrElse(id, "")
    }
    
    def encodeUserItemToId(ratings: RDD[(String, String, Float)]):RDD[(Long,Long,Double)] = {
        this.userIdMap = ratings.map(_._1).distinct.zipWithIndex.collectAsMap()
        this.itemIdMap = ratings.map(_._2).distinct.zipWithIndex.collectAsMap()
        this.idItemMap = this.itemIdMap.map(x => x._2 -> x._1)
        val userIdItemIdRating = ratings.map { case (userId: String, itemId: String, rating: Float) =>
            (convertStringToID(userId, this.userIdMap),
                convertStringToID(itemId, this.itemIdMap), rating.toDouble)
        }
        userIdItemIdRating
    }

    /** Calculates the cosine similarity of each item with other item based on user preferences
      * @param sparkContext
      * @param inputData
      * @return An RDD for (String,String,Float)
      */
    def calculatePredictedRating(ratings: RDD[(String, String, Float)]): RDD[(String, String, Float)] = {
        ratings.persist()
        val userIdItemIdRating = encodeUserItemToId(ratings)
        userIdItemIdRating.persist()
        ratings.unpersist()
        userIdItemIdRating.persist()
        val numItems = this.itemIdMap.size
        val numUsers = this.userIdMap.size
        val userVectors = userIdItemIdRating.groupBy(_._1)
                        .filter {case(userId:Long,itemSeq : Iterable[(Long,Long,Double)]) => itemSeq.size > 0 }
                        .map{ case(userId:Long,itemSeq : Iterable[(Long,Long,Double)]) => Vectors.sparse(numItems,itemSeq.map(x => (x._2.toInt,x._3)).toSeq)  }
        userVectors.persist()
        userIdItemIdRating.unpersist()
        val numberOfEntriesInFirstRow = userVectors.first().size
        val numberOfRows = userVectors.count()
        Logger.logInfo(s"Columns in first row : $numberOfEntriesInFirstRow")
        Logger.logInfo(s"Number of entries in the user vectors: $numberOfRows")
        Logger.logInfo(s"Total number of users : $numUsers")
        Logger.logInfo(s"Total number of items : $numItems")
        val ratingRowMatrix = new RowMatrix(userVectors,numUsers,numItems)
        val itemItemCoordinateMatrix = ratingRowMatrix.columnSimilarities(0.3)
        val similarityEntries = itemItemCoordinateMatrix.entries.distinct.map { case (x: MatrixEntry) =>
            (convertIDToString(x.i, this.idItemMap), convertIDToString(x.j, this.idItemMap), x.value.toFloat)
        }
        return similarityEntries;
    }

}
