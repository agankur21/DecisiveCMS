#!/bin/bash
#set -x

##############################################################################
#
# Script to run Spark Scala job.
#
##############################################################################
export PROJECT_HOME=/mnt/git-repo/DecisiveCMS
source "${PROJECT_HOME}/scripts/common_func.sh";
SPARK_HOME="/usr/local/spark";
JOB_CLASS="com.scoopwhoop.dcms.RunApplication";
JOB_JAR="${PROJECT_HOME}/target/scala-2.10/dcms-assembly.jar"
JOB_DEP_JARS="${PROJECT_HOME}/lib/joda-time-2.7.jar,${PROJECT_HOME}/lib/joda-convert-1.7.jar"
MASTER="local[2]"
##############################################################################


##############################################################################
#
# Function MAIN()
#
##############################################################################
function MAIN()
{
   loggerInfo "***      Script: $0   START  ***";
  
   loggerInfo "Running Spark shell with Jar : ${JOB_JAR} "
   ${SPARK_HOME}/bin/spark-shell --master ${MASTER}  --jars ${JOB_JAR}  ;
   loggerInfo "***      Script: $0    END   ***";
}
##############################################################################


##############################################################################
#
# Script Entry
#
##############################################################################
MAIN $@;


