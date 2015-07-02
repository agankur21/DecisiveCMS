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
        time                        bigint,
        category                    text,
        from_url                    text,
        event_destination           text,
        screen_location             text,
        referring_domain            text,
        PRIMARY KEY ((url,user_id,event),time)
        WITH CLUSTERING ORDER BY (time DESC)
    );
        
    CREATE TABLE IF NOT EXISTS google_category_statistics(
        category                    text,
        start_date                  text,
        end_date                    text,
        desktop_views               int,
        mobile_views                int,
        clicks                      int,
        shares                      int,
        ga_page_views               int,
        ga_unique_page_views        int,
        ga_avg_time                 double,
        ga_entrances                 int,
        ga_bounce_rate              double,
        PRIMARY KEY (category,start_date,end_date)
    )

"