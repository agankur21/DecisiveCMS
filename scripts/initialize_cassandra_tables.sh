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
    
    CREATE TABLE IF NOT EXISTS users(
        user_id                     text PRIMARY KEY,
        browser                     text,
        browser_version             text,
        region                      text,
        city                        text,
        country_code                text,
        os                          text,
        device                      text
    );
    
    CREATE TABLE IF NOT EXISTS pages(
        url                         text PRIMARY KEY,
        title                       text,
        category                    text,
        author                      text,
        screen_height               text, 
        screen_width                text
    );
    
    CREATE TABLE IF NOT EXISTS events(
        url                         text,
        user_id                     text,
        event                       text,
        time                        text,
        browser                     text,
        browser_version             text,
        region                      text,
        city                        text,
        country_code                text,
        os                          text,
        device                      text,
        device_type                 text,
        title                       text,
        category                    text,
        author                      text,
        screen_height               text, 
        screen_width                text,
        from_url                    text,
        event_destination           text,
        screen_location             text,
        initial_referrer            text,
        initial_referring_domain    text,
        referrer                    text,
        search_engine               text,  
        mp_keyword                  text,
        mp_lib                      text,
        lib_version                 text,
        referring_domain            text,
        utm_campaign                text,
        utm_content                 text,
        utm_medium                  text,
        utm_source                  text,
        PRIMARY KEY (url,user_id,event,time)
     )

"