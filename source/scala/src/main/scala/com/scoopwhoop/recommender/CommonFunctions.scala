package com.scoopwhoop.recommender

import com.datastax.spark.connector.CassandraRow
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.distributed.{RowMatrix}
import org.apache.spark.mllib.linalg.{Matrix,Vector}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import breeze.linalg.{DenseMatrix,DenseVector}


object CommonFunctions {

    val eventScore = Map("Desktop PVs" -> 1, "Mobile PVs" -> 1, "itemClick" -> 4, "shareClick" -> 8)

    def nnHash(tag: String): Int = (tag.hashCode & 0x7FFFFF).toInt

    def getDate(timestamp: Long): String = {
        if (timestamp == 0) return ""
        val date_time = new DateTime(timestamp * 1000L);
        return date_time.toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
    }
    
    def isGreaterOrEqual(timestamp: Long, date: String): Boolean = {
        return (getDate(timestamp) >= date)
    }

    def isLowerOrEqual(timestamp: Long, date: String): Boolean = {
        return (getDate(timestamp) <= date)
    }

    def getStringFromCassandraRow(row: CassandraRow, element: String): String = {
        if (row.isNullAt(element)) "" else row.getString(element)
    }


    def getSVD(rows: RDD[Vector],numConcepts:Int):SingularValueDecomposition[RowMatrix, Matrix] = {
        rows.cache()
        val mat = new RowMatrix(rows)
        val svd = mat.computeSVD(numConcepts, computeU = true)
        svd
    }

    def multiplyByDiagonalMatrix(mat: Matrix, diag: Vector): DenseMatrix[Double] = {
        val sArr = diag.toArray
        new DenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
            .mapPairs{case ((r, c), v) => v * sArr(c)}
    }

    def rowsNormalized(mat: DenseMatrix[Double]): DenseMatrix[Double] = {
        val newMat = new DenseMatrix[Double](mat.rows, mat.cols)
        for (r <- 0 until mat.rows) {
            val length = math.sqrt((0 until mat.cols).map(c => mat(r, c) * mat(r, c)).sum)
            (0 until mat.cols).map(c => newMat.update(r, c, mat(r, c) / length))
        }
        newMat
    }
    
    def dotProduct(vector1:DenseVector[Double],vector2:DenseVector[Double]):Double = {
        val numElements = vector1.length
        if (numElements !=  vector2.length) return 0.0
        var sum  = 0.0
        for (i <- 0 until numElements) {
            sum += vector1(i) *  vector2(i)
        }
        return sum
    }
    
    def getRowNorms(mat: DenseMatrix[Double]) : DenseVector[Double] = {
        val numRows :Int = mat.rows
        val normVector = DenseVector.zeros[Double](numRows)
        for (i <- 0 until numRows) {
            normVector(i) = math.sqrt(dotProduct(mat(i,::).t,mat(i,::).t))
        }
        return normVector
    }
  
    
}
