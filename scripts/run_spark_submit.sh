#!/bin/bash
#set -x

##############################################################################
#
# Script to run Spark Scala job.
#
##############################################################################
export PROJECT_HOME=/mnt/git-repo/DecisiveCMS/source/scala
source "${PROJECT_HOME}/scripts/common_func.sh";
SPARK_HOME="/usr/local/spark";
JOB_CLASS="com.scoopwhoop.recommender.RunApplication";
JOB_JAR="${PROJECT_HOME}/target/scala-2.10/dcms-assembly.jar"
JOB_DEP_JARS="${PROJECT_HOME}/lib/joda-time-2.7.jar,${PROJECT_HOME}/lib/joda-convert-1.7.jar"
MASTER="spark://10.2.3.10:7077"
##############################################################################


##############################################################################
#
# Function MAIN()
#
##############################################################################
function MAIN()
{
   loggerInfo "***      Script: $0   START  ***";
  
   loggerInfo "Running Job -  ${JOB_CLASS} "
   ${SPARK_HOME}/bin/spark-submit --master ${MASTER} --class ${JOB_CLASS} --jars ${JOB_DEP_JARS} --conf spark.shuffle.spill=false --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseCompressedOops" --executor-memory 3g --driver-memory 3g ${JOB_JAR}  ;
   loggerInfo "***      Script: $0    END   ***";
}
##############################################################################


##############################################################################
#
# Script Entry
#
##############################################################################
MAIN $@;


