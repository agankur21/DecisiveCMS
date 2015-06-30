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
        device                      text,
        device_type                 text
    );
    

    CREATE TABLE IF NOT EXISTS pages(
        url                         text PRIMARY KEY,
        title                       text,
        category                    text,
        author                      text,
        screen_height               text, 
        screen_width                text
    );
    
    CREATE TYPE IF NOT EXISTS utm(
        utm_campaign                text,
        utm_content                 text,
        utm_medium                  text,
        utm_source                  text,
    );
    
    CREATE TYPE IF NOT EXISTS referrer(
        initial_referrer            text,
        initial_referring_domain    text,
        referrer                    text,
        referring_domain            text,
    );
    
    CREATE TYPE IF NOT EXISTS user_info(
        browser                     text,
        browser_version             text,
        region                      text,
        city                        text,
        country_code                text,
        os                          text,
        device                      text,
        device_type                 text
    );

    CREATE TABLE IF NOT EXISTS google_analytics_data(
        url                         text,
        start_date                  text,
        end_date                    text,
        page_views                  int,
        unique_page_views           int,
        avg_time_per_page           double,
        entrances                   int,
        bounce_rate                 double,
        exit                        double,
        page_value                  double,
        PRIMARY KEY (url,start_date,end_date)
    );

        
    CREATE TABLE IF NOT EXISTS events(
        url                         text,
        user_id                     text,
        event                       text,
        time                        text,
        title                       text,
        category                    text,
        author                      text,
        screen_height               text, 
        screen_width                text,
        from_url                    text,
        event_destination           text,
        screen_location             text,
        search_engine               text,  
        mp_keyword                  text,
        mp_lib                      text,
        lib_version                 text,
        user_data                   frozen <user_info>,
        referrer_data               frozen <referrer>,
        utm_data                    frozen <utm>,
        PRIMARY KEY ((url,user_id,event),time))
        WITH CLUSTERING ORDER BY (time DESC)
        


"