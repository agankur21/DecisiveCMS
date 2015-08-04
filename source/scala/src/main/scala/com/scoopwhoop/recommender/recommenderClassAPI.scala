import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.scoopwhoop.logger.Logger
package com.scoopwhoop.recommender {



/** class RatingModule for calculating normalised rating/preference score of a user for each Tag
  *
  * @constructor create a  object using activity data
  * @param listOfActivityData  : an RDD list of string representing activity data
  * @return actualUserItemRating : a list of Tuple3 of (user,item,actualRating)
  */
trait RatingModule extends Serializable {

    /** Parse input data
      * @param listOfActivityData  : an RDD of JSON string representing activity data
      * @return An RDD  : (user_id,tag) -> normalisedEventRating
      */
    def getNormalisedUserItemRating(listOfActivityData: RDD[Array[String]], ratingsData: Map[String, Int], timeIndex: Int, userIndex: Int, itemIndex: Int, activityIndex: Int): RDD[((String, String), Float)];


}


/** ********************************************************************************************************************/
/** ********************************************************************************************************************/
/** ********************************************************************************************************************/


/** base class RecommenderAlgorithmModule for determining the preference score/rating(actual and predicted) of each user, tag
  * @constructor create a  object using rating data
  * @param inputDataBaseHandler  : a database handler for input ratings
  * @return predictedUserItemRating : a list of Tuple3 of (user,item,predictedRating)
  */
trait RecommenderAlgorithmModule extends Serializable {

    /** Calculates the preferred rating for each user and each item using different algorithms
      * @param sparkContext
      * @param inputData
      * @return An RDD for (String,String,Float)
      */
    def calculatePredictedRating(inputData: RDD[(String, String, Float)]): RDD[(String, String, Float)];
    

}


/** ********************************************************************************************************************/
/** ********************************************************************************************************************/
/** ********************************************************************************************************************/

/** base class BackgroundProcesses for updating the recommendation data in the background
  */
trait ApplicationProcesses extends Serializable {

    /** Calculate implied ratings  and updates the output Database
      * @param sparkContext
      * @param inputData : an RDD list of string representing activity data
      * @param ratingModule : An object of RatingsModule
      **/
    def calculateNormalisedRating(inputData: RDD[Array[String]], ratingModule: RatingModule): RDD[(String, String, Float)];


    /** Calculate ratings for those items which are not rated by user
      * @param ratingsData : data for normalised ratings
      * @param recommenderAlgorithmModule : The module implementing specific recommendation algorithm
      **/
    def calculateRecommendedRatings(ratingsData: RDD[(String, String, Float)], recommenderAlgorithmModule: RecommenderAlgorithmModule): RDD[(String, String, Float)];

    /** Update the cassandra table with the recommended ratings
      * @param sparkContext
      * @param tableName
      * @param startDate
      * @param  endDate
      * @return RDD[(String, String, Float)]
      */
    def getRecommendations(sparkContext:SparkContext,tableName:String,startDate:String,endDate:String):RDD[(String, String, Float)];
    
    /** Update the cassandra table with the recommended ratings
      * @param keySpace
      * @param tableName
      * @param similarityRatings
      * @param startDate
      * @param  endDate
      */
    def updateCassandraTable(keySpace:String,tableName:String,similarityRatings : RDD[(String, String, Float)],startDate:String,endDate:String) :Unit
}

}