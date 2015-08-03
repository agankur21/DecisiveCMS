package com.scoopwhoop.nlp
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer


object LSAImplementation {
    def getSVD(rows: RDD[Vector],numConcepts:Int):SingularValueDecomposition[RowMatrix, Matrix] = {
        rows.cache()
        val mat = new RowMatrix(rows)
        val svd = mat.computeSVD(numConcepts, computeU = true)
        svd
    }
    
    def getTopTermsForEachConcept(svd:SingularValueDecomposition[RowMatrix, Matrix],numConcepts:Int,numTerms :Int,termIds:Map[Long,String]):Seq[Seq[(String, Double)]] = {
        val v = svd.V
        val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
        val arr = v.toArray
        for (i <- 0 until numConcepts) {
            val offs = i * v.numRows
            val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex 
            val sorted = termWeights.sortBy(-_._1)
            topTerms += sorted.take(numTerms).map{ case (score, id) => (termIds(id), score)}}
        topTerms
    }

    def getTopDocsForEachConcept(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int, numDocs: Int, docIds: Map[Long, String])
    : Seq[Seq[(String, Double)]] = {
        val u = svd.U
        val topDocs = new ArrayBuffer[Seq[(String, Double)]]() 
        for (i <- 0 until numConcepts) {
            val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
            topDocs += docWeights.top(numDocs).map{
                case (score, id) => (docIds(id), score) }
        }
        topDocs
    }

    def rowsNormalized(mat: RowMatrix): RowMatrix = {
        new RowMatrix(mat.rows.map(vec => {
            val length = math.sqrt(vec.toArray.map(x => x * x).sum)
            Vectors.dense(vec.toArray.map(_ / length))
        }))
    }

    def getTopDocsForDoc(normalizedUS: RowMatrix, docId: Long): Seq[(Double, Long)] = {
        val docRowArr = CommonFunctions.row(normalizedUS, docId)
        val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
        val docScores = normalizedUS.multiply(docRowVec)
        val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
        allDocWeights.filter(!_._1.isNaN).top(10)
    }
    
    
    








}
