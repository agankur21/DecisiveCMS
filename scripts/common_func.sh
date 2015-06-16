#!/bin/bash

TODAYSTR=`date '+%Y-%m-%d'`;

function error_msg_exit()
{
  ERR_MSG=$1;
  echo "[`date`] [FATAL - ERROR] [$ERR_MSG]";
  exit;
}

function loggerInfo()
{
  INFO_MSG=$1;
  echo "[`date`] [INFO] [${INFO_MSG}]";
}

function loggerWarn()
{
  WARN_MSG=$1;
  echo "[`date`] [WARN] [${WARN_MSG}]";
}

function loggerError()
{
  ERR_MSG=$1;
  echo "[`date`] [ERROR] [${ERR_MSG}]";
}

function checkStatusANDErrMsgExit()
{
   ERR_MSG=$1;
   if [ $? -ne 0 ]
   then
     error_msg_exit ${ERR_MSG};
   fi
}

function emailto()
{
  E_TO=$1;
  E_FROM=$2;
  E_SUBJECT=$3;
  E_BODY=$4;

  echo "From: ${E_FROM}
To: ${E_TO}
Subject: ${E_SUBJECT}" > /tmp/emailAttrib

  if [ -f  ${E_BODY} ]
  then
    cat ${E_BODY} >> /tmp/emailAttrib;
  else
    echo ${E_BODY} >> /tmp/emailAttrib;
  fi

  /usr/sbin/sendmail -t <  /tmp/emailAttrib;
  rm  /tmp/emailAttrib;
}

##############################################################################
#
# Function verifyInputDate()
#
##############################################################################
verifyInputDate()
{
    DATESTR=$1
	date "+%Y-%m-%d" -d ${DATESTR} 2>1 > /dev/null
    checkStatusANDErrMsgExit "Incorrect Date format : ${DATESTR}";
}
verifyInputDateHour()
{
    DATESTR=$1
	date "+%Y-%m-%d-%H" -d ${DATESTR} 2>1 > /dev/null
    checkStatusANDErrMsgExit "Incorrect Date-Hour format : ${DATESTR}";
}
##############################################################################


