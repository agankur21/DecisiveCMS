package com.scoopwhoop.database

import com.scoopwhoop.logger.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait AccessExternalDatabase extends Serializable {

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
    }

}
