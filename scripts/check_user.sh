#!/bin/bash

export CASSANDRA_HOME=/usr/local/cassandra

USERID=`whoami`;

if [ "${USERID}" != "cassandra" ]
then
   echo "Permission denied : Current User: ${USERID}       Permitted User : hdfs";
   echo "Script Aborted";
   exit;
fi