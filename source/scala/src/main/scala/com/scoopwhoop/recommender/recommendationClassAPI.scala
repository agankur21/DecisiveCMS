import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.scoopwhoop.logger.Logger
package com.scoopwhoop.recommender {

/** A base class AccessExternal database for reading and  writing data
  *
  * @constructor create an object
  */
trait AccessExternalDataSource extends Serializable {

    /** Connect with the external data source
      * @return Type Any
      */
    def makeConnectionToDataSource(): Any;

    /** Parse data from external Data source
      * @param arg : Path from which to read Data
      * @return An RDD of input data as String
      */
    def getData(sparkContext: SparkContext, arg: String): RDD[Array[String]];

    /** Puts data in external Data source
      * @param data : An RDD of data in the form of string to be saved in the input path
      * @param arg : Path from which to read Data
      */
    def putData(sparkContext: SparkContext, data: RDD[(String, String, String)], arg: String): Unit;

    /** Close all associated connections */
    def closeConnection(): Unit = {
        Logger.logInfo("All Connections Safely Closed !!!!")
    };
}


/** ********************************************************************************************************************/
/** ********************************************************************************************************************/
/** ********************************************************************************************************************/


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
    def getNormalisedUserItemRating(listOfActivityData: RDD[Array[String]], ratingsData: Map[String,Int], timeIndex: Int, userIndex: Int, itemIndex: Int, activityIndex: Int): RDD[((String, String), Float)];


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
    def calculatePredictedRating(sparkContext: SparkContext, inputData: RDD[(String, String, Float)]): RDD[(String, String, Float)];

}


/** ********************************************************************************************************************/
/** ********************************************************************************************************************/
/** ********************************************************************************************************************/

/** base class BackgroundProcesses for updating the recommendation data in the background
  */
trait ApplicationProcesses extends Serializable {
    /** Get a connector object to an external database
      * @param dataSourceType : Type of the database to be connected eg: S3,Redis,HBase
      * @return an object of type AccessExternalDataSource
      * */
    def getDataHandler(dataSourceType: String): AccessExternalDataSource;

    /** Calculate implied ratings  and updates the output Database
      * @param sparkContext
      * @param inputData : an RDD list of string representing activity data
      * @param ratingModule : An object of RatingsModule
      * */
    def calculateNormalisedRating(sparkContext: SparkContext, inputData: RDD[Array[String]], ratingModule: RatingModule, ratingParameters: RDD[Array[String]]): RDD[(String, String, Float)];


    /** Calculate ratings for those items which are not rated by user
      * @param ratingsData : data for normalised ratings
      * @param recommenderAlgorithmModule : The module implementing specific recommendation algorithm
      * */
    def calculateRecommendedRatings(sparkContext: SparkContext, ratingsData: RDD[(String, String, Float)], recommenderAlgorithmModule: RecommenderAlgorithmModule): RDD[(String, String, Float)];


}

}