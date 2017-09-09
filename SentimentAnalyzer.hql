
# Upload the file to the root of DFS

-- Create the directory structure

hadoop fs -mkdir /user/adi/tweets_raw
hadoop fs -mkdir /user/adi/dictionary
hadoop fs -mkdir /user/adi/timezonemap




-- Load data to respective directories

hadoop fs -copyFromLocal Desktop/TwitterDemo/RawTweets.txt /user/adi/tweets_raw
hadoop fs -copyFromLocal Desktop/TwitterDemo/dictionary.tsv /user/adi/dictionary
hadoop fs -copyFromLocal Desktop/TwitterDemo/time_zone_map.tsv /user/adi/timezonemap


--Add the custom serializer-deserializer (SerDe)
ADD JAR Desktop/TwitterDemo/hive-serdes-1.0-SNAPSHOT.jar;

--Drop tables and views--
drop table tweets_raw;
drop table tweets_sentiment;
drop table tweetsbi;
drop view l1;
drop view l2;
drop view l3;
drop view tweets_clean;
drop view tweets_simple;

--End Drop--



--create the tweets_raw table containing the records as received from Twitter

CREATE EXTERNAL TABLE tweets_raw (
   id BIGINT,
   created_at STRING,
   source STRING,
   favorited BOOLEAN,
   retweet_count INT,
   retweeted_status STRUCT<text:STRING,usr:STRUCT<screen_name:STRING,name:STRING>>,
   entities STRUCT<urls:ARRAY<STRUCT<expanded_url:STRING>>,
   user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
   hashtags:ARRAY<STRUCT<text:STRING>>>,
   text STRING,
   user1 STRUCT<screen_name:STRING,name:STRING,friends_count:INT,followers_count:INT,statuses_count:INT,verified:BOOLEAN,utc_offset:STRING,    time_zone:STRING>,
   in_reply_to_screen_name STRING,
   year int,
   month int,
   day int,
   hour int
)
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/user/adi/tweets_raw'
;

SELECT * FROM tweets_raw LIMIT 100;

-- create sentiment dictionary (ONE TIME PROCESS)
CREATE EXTERNAL TABLE dictionary (
    type string,
    length int,
    word string,
    pos string,
    stemmed string,
    polarity string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE
LOCATION '/user/adi/dictionary';

-- create the time zone to country mapper (ONE TIME PROCESS)
CREATE EXTERNAL TABLE time_zone_map (
    time_zone string,
    country string,
    notes string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE
LOCATION '/user/adi/timezonemap';

-- Clean up tweets
CREATE VIEW tweets_simple AS
SELECT
  id,
  cast ( from_unixtime( unix_timestamp(concat( '2014 ', substring(created_at,5,15)), 'yyyy MMM dd hh:mm:ss')) as timestamp) ts,
  text,
  user1.time_zone 
FROM tweets_raw
;

-- Get the tweets based on matching time zones
CREATE VIEW tweets_clean AS
SELECT
  id,
  ts,
  text,
  m.country 
 FROM tweets_simple t LEFT OUTER JOIN time_zone_map m ON t.time_zone = m.time_zone;
 
 
 
 -- Compute sentiment
 create view l1 as select id, words from tweets_raw lateral view explode(sentences(lower(text))) dummy as words;
 create view l2 as select id, word from l1 lateral view explode( words ) dummy as word ;
 create view l3 as select 
     id, 
     l2.word, 
     case d.polarity 
       when  'negative' then -1
       when 'positive' then 1 
       else 0 end as polarity 
  from l2 left outer join dictionary d on l2.word = d.word;

-- Create the sentiments (Note: Need to create - hadoop fs -mkdir \\\apps\hive\warehouse)
create table tweets_sentiment as select 
  id, 
  case 
    when sum( polarity ) > 0 then 'positive' 
    when sum( polarity ) < 0 then 'negative'  
    else 'neutral' end as sentiment 
 from l3 group by id;

-- put everything back together and re-number sentiment
CREATE TABLE tweetsbi 
AS
SELECT 
  t.id,t.country,
  case s.sentiment 
    when 'positive' then 1 
    when 'neutral' then 0 
    when 'negative' then -1 
  end as sentiment  
FROM tweets_clean t LEFT OUTER JOIN tweets_sentiment s on t.id = s.id;


--SAMPLE SECTION--
select id,sentiment from tweetsbi where id in('435389091073363968','435389251526471680','435389483555368960','435389565256212482','435389636760727552');

435389091073363968 : 1
435389251526471680 : 1
435389483555368960 : -1
435389565256212482 : -1
435389636760727552 : 0

--- find positive , negetive and nutral reviews ----
select sum(if(sentiment=1,1,0)) as positive_sentiment,
    sum(if(sentiment=0,1,0)) as nutral_reviws,
     sum(if(sentiment=-1,1,0)) as negetive_reviws from tweetsbi;
o/p
598	2684	255


--END SAMPLE SECTION--
