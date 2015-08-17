package com.scoopwhoop.recommender

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.{RowMatrix,MatrixEntry}
import org.apache.spark.mllib.linalg.{Matrix, Vectors,Vector}
import com.scoopwhoop.logger.Logger


class LatentFactorItemItemSimilarity extends RecommenderAlgorithmModule {

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

    def getRowMatrix(userIdItemIdRating: RDD[(Long,Long,Double)],numRows:Int,numColumns:Int) : RowMatrix = {
        userIdItemIdRating.persist()
        val userVectors = userIdItemIdRating.groupBy(_._1)
            .filter {case(userId:Long,itemSeq : Iterable[(Long,Long,Double)]) => itemSeq.size > 0 }
            .map{ case(userId:Long,itemSeq : Iterable[(Long,Long,Double)]) => Vectors.sparse(numColumns,itemSeq.map(x => (x._2.toInt,x._3)).toSeq)  }
        userVectors.persist()
        userIdItemIdRating.unpersist()
        val numberOfEntriesInFirstRow = userVectors.first().size
        val numberOfRows = userVectors.count()
        Logger.logInfo(s"Columns in first row : $numberOfEntriesInFirstRow")
        Logger.logInfo(s"Number of entries in the user vectors: $numberOfRows")
        val ratingRowMatrix = new RowMatrix(userVectors,numRows,numColumns)
        return ratingRowMatrix
    }

    
    /** Calculates the cosine similarity of each item with other item based on user preferences
      * @param sparkContext
      * @param inputData
      * @return An RDD for (String,String,Float)
      */
    def calculatePredictedRating(ratings: RDD[(String, String, Float)]): RDD[(String, String, Float)] = {
        val sparkContext = ratings.sparkContext
        ratings.persist()
        val userIdItemIdRating = encodeUserItemToId(ratings)
        ratings.unpersist()
        val numItems = this.itemIdMap.size
        val numUsers = this.userIdMap.size
        Logger.logInfo(s"Total number of users : $numUsers")
        Logger.logInfo(s"Total number of items : $numItems")
        val ratingRowMatrix = getRowMatrix(userIdItemIdRating,numUsers,numItems)
        val svd = ratingRowMatrix.computeSVD(100,computeU = false)
        val itemMatrix :Matrix= svd.V
        val eigValues: Vector = svd.s
        val normalisedWeightedItemMatrix = CommonFunctions.rowsNormalized(CommonFunctions.multiplyByDiagonalMatrix(itemMatrix,eigValues))
        val rowNormVector = CommonFunctions.getRowNorms(normalisedWeightedItemMatrix)
        var similarityIndices : List[(Int,Int)] = List[(Int,Int)]()
        for(i <- 0 until rowNormVector.length){
            for(j <- 0 until i){
                similarityIndices = (i,j) :: similarityIndices
            }
        }
        val distributedIndices = sparkContext.parallelize(similarityIndices)
        val cosineSimilarityValues = distributedIndices.map{ case(i:Int,j:Int) => (i,j,CommonFunctions.dotProduct(normalisedWeightedItemMatrix(i,::).t,normalisedWeightedItemMatrix(j,::).t)/(rowNormVector(i) * rowNormVector(j)) ) }
        val similarityEntries = cosineSimilarityValues.map { case (i:Int,j:Int,value:Double) =>
            (convertIDToString(i, this.idItemMap), convertIDToString(j, this.idItemMap), value.toFloat)
        }
        return similarityEntries;
    }

}

