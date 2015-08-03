package com.scoopwhoop.nlp

import java.util.Properties
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, TokensAnnotation, SentencesAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ArrayBuffer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.feature.IDF

object FeatureVectorExtraction {

    private def termDocWeight(termFrequencyInDoc: Int, totalTermsInDoc: Int, termFreqInCorpus: Int, totalDocs: Int): Double = {
        val tf = termFrequencyInDoc.toDouble / totalTermsInDoc
        val docFreq = totalDocs.toDouble / termFreqInCorpus
        val idf = math.log(docFreq)
        tf*idf
    }

    def plainTextToLemmas(text: String, stopWords: Set[String]) : Seq[String] = {
        val props = new Properties()
        props.put("annotators", "tokenize, ssplit, pos, lemma")
        val pipeline = new StanfordCoreNLP(props)
        val doc = new Annotation(text)
        pipeline.annotate(doc)
        val lemmas = new ArrayBuffer[String]()
        val sentences = doc.get(classOf[SentencesAnnotation])
        for (sentence <- sentences;token <- sentence.get(classOf[TokensAnnotation])) {
            val lemma = token.get(classOf[LemmaAnnotation])
            if (lemma.length > 2 && !stopWords.contains(lemma) && CommonFunctions.isOnlyLetters(lemma)) {
                lemmas += lemma.toLowerCase
            }
        }
        lemmas
    }

    def getTermFrequency(textRDD: RDD[String],stopWordsBroad:Broadcast[Set[String]]):RDD[HashMap[String,Int]] = {
        val stopWords = stopWordsBroad.value
        val lemmatized = textRDD.map(plainTextToLemmas(_, stopWords))
        val docTermFreq = lemmatized.map(terms => {
            val termFreq = terms.foldLeft(new HashMap[String, Int]()) {
                (map, term) => {
                    map += term -> (map.getOrElse(term, 0) + 1)
                    map
                } }
            termFreq
        })
        docTermFreq
    }

    def getDocFrequencies(docTermFreq :RDD[HashMap[String,Int]]) : RDD[(String,Int)] = {
        docTermFreq.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _)
    }

    
    
    def getTFIDF(documentText:RDD[String],stopWordFile :String):RDD[Vector] = {
        val sparkContext = documentText.sparkContext
        val stopWordsBroad = sparkContext.broadcast(CommonFunctions.readResourceFile("stopwords.txt"))
        val docTermFrequency = getTermFrequency(documentText,stopWordsBroad)
        docTermFrequency.cache()
        val docFrequency = getDocFrequencies(docTermFrequency)
        val numDocs = documentText.count()
        val idf = docFrequency.map{ case (term, count) => term -> math.log(numDocs.toDouble / count)}.collectAsMap()
        val bIdfs = sparkContext.broadcast(idf).value
        val termIds = idf.keys.zipWithIndex.toMap
        val bTermIds = sparkContext.broadcast(termIds).value
        
        val vecs = docTermFrequency.map(termFrequency => { 
            val docTotalTerms = termFrequency.values.sum
            val termScores = termFrequency.filter{ case (term, freq) => bTermIds.containsKey(term) }
                                        .map{ case (term, freq) => (bTermIds(term), bIdfs(term) * termFrequency(term) / docTotalTerms)}
                                        .toSeq
            Vectors.sparse(bTermIds.size, termScores) 
        })
        vecs
    }
    

    def getTFIDFShort(documentText:RDD[String],stopWordFile :String):RDD[Vector] = {
        val hashingTF = new HashingTF()
        val sparkContext = documentText.sparkContext
        val stopWordsBroad = sparkContext.broadcast(CommonFunctions.readResourceFile("stopwords.txt"))
        val stopWords = stopWordsBroad.value
        val textTokens = documentText.map(plainTextToLemmas(_,stopWords))
        val tf: RDD[Vector] = hashingTF.transform(textTokens)
        tf.cache()
        val idf = new IDF(minDocFreq = 2).fit(tf)
        val tfidf: RDD[Vector] = idf.transform(tf)
        tfidf
    }


}
