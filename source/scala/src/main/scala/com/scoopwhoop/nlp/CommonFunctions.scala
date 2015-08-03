package com.scoopwhoop.nlp

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import scala.io.Source

object CommonFunctions {
    
    def readResourceFile(fileName:String): Set[String] ={
        val lines = Source.fromURL(getClass.getResource("/"+fileName)).getLines()
        lines.toSet
    }

    def isOnlyLetters(str:String):Boolean = {
        str.forall(c => Character.isLetter(c))
    }

    def multiplyByDiagonalMatrix(mat: RowMatrix, diag: Vector): RowMatrix = {
        val sArr = diag.toArray
        new RowMatrix(mat.rows.map(vec => {
            val vecArr = vec.toArray
            val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
            Vectors.dense(newArr)
        }))
    }

    def row(mat: RowMatrix, id: Long): Array[Double] = {
        mat.rows.zipWithUniqueId.map(_.swap).lookup(id).head.toArray
    }

    def rowsNormalized(mat: RowMatrix): RowMatrix = {
        new RowMatrix(mat.rows.map(vec => {
            val length = math.sqrt(vec.toArray.map(x => x * x).sum)
            Vectors.dense(vec.toArray.map(_ / length))
        }))
    }

}
