#!/bin/bash
#set -x

##############################################################################
#
# Script to run Spark Scala job.
#
##############################################################################
source "/mnt/git-repo/DecisiveCMS/scripts/common_func.sh";
SPARK_HOME="/usr/local/spark";
JOB_CLASS="com.scoopwhoop.dcms.TestCassandra";
JOB_JAR="/mnt/git-repo/user-DecisiveCMS/target/scala-2.11/rdcms-assembly.jar"
JOB_DEP_JARS="/mnt/git-repo/DecisiveCMS/lib/joda-time-2.7.jar,/mnt/git-repo/DecisiveCMS/lib/joda-convert-1.7.jar"
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
   ${SPARK_HOME}/bin/spark-submit --master ${MASTER} --class ${JOB_CLASS} --jars ${JOB_DEP_JARS} --num-executors 5 --executor-memory 5g --driver-memory 4g  ${JOB_JAR}  ;
   loggerInfo "***      Script: $0    END   ***";
}
##############################################################################


##############################################################################
#
# Script Entry
#
##############################################################################
MAIN $@;


