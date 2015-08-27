#!/bin/bash
export DATASTORE_HOME=/usr/local/apache-cassandra-2.1.6
export DCMS_HOME=/mnt/git-repo/DecisiveCMS

source "${DCMS_HOME}/scripts/common_func.sh";
source "${DCMS_HOME}/scripts/check_user.sh";

loggerInfo "Creating KeySpace: dcms";

${DATASTORE_HOME}/bin/cqlsh -e "
    CREATE KEYSPACE IF NOT EXISTS dcms
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
"
checkStatusANDErrMsgExit "ERROR : Creating Keyspace";


loggerInfo "Creating Tables in Keyspace dcms";
${DATASTORE_HOME}/bin/cqlsh -e "
    USE dcms;

    CREATE TABLE IF NOT EXISTS events(
        product_id                  text,
        event_id                    uuid,
        event_type                  text,
        time                        bigint,
        post_id                     text,
        cookie_id                   text,
        event_where                 text,
        event_desc                  text,
        user_agent                  text,
        referrer_url                text,
        referrer_domain             text,
        author                      text,
        category                    text,
        ip                          text,
        PRIMARY KEY (product_id,time,event_id,event_type,post_id,cookie_id)
    )
    WITH CLUSTERING ORDER BY (time DESC);


        


"