create database if not exists custom;

use custom;

create table if not exists stocks
(
date_ String,
Ticker String,
Open Double,
High Double,
Low Double,
Close Double,
Volume_for_the_day int
)
row format delimited fields terminated by ',';

load data local inpath '/home/acadgild/Desktop/TestHadoop/hive/spmohst.txt' into table stocks;


--List out closing price for the day along with the yesterday's closing price.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/closing_lag'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
SELECT Ticker,date_,Close,lag(Close,1) 
over(partition by Ticker) as yesterday_price FROM stocks;

set hive.cli.print.header=true;


--Find out whether the following day’s closing price is higher or lesser than today’s.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/closing_lead'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select Ticker,date_,Close,case(lead(Close,1) over(partition by Ticker)-Close)>0 
when true then "higher" when false then "lesser" end as Changes from stocks;


--Find the highest price of the ticker for all the days.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/first_high'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,date_,first_value(high) over(partition by ticker) 
as first_high from stocks;


--Find the last row high price value of the ticker for all the days.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/last_high'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,date_,last_value(high) over(partition by ticker) 
as last_high from stocks;


--Find the number of rows present for each ticker.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/count_ticker'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,count(ticker) over(partition by ticker) 
as count_ticker from stocks;


--Find the sum of all the closing stock prices for that particular ticker.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/sum_closing'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,sum(close) over(partition by ticker) 
as total from stocks;


--Find the running total of the volume_for_the_day for all the days for every ticker

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/total_Vol_day'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,date_,volume_for_the_day,sum(volume_for_the_day) 
over(partition by ticker order by date_) 
as running_total from stocks;


--Find the percentage of the volume_for_the_day on the total volumes for that particular ticker

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/percent_total_Vol_day'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,date_,volume_for_the_day,(volume_for_the_day*100/(sum(volume_for_the_day) 
over(partition by ticker))) from stocks;

--Find the minimum closing stock price for each particular ticker.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/min_ticker'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker, min(close) over(partition by ticker) as minimum 
from stocks;


--Find the maximum closing stock price for each particular ticker.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/max_ticker'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker, max(close) over(partition by ticker) as maximum
from stocks;


--Find the average closing stock price for each particular ticker.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/avg_ticker'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker, avg(close) over(partition by ticker) as average
from stocks;


--How to rank the closing prices of the stock for each ticker.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/rank_ticker'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,close,rank() over(partition by ticker order by close) as closing 
from stocks;

--How to get the ticker, closing price and its row number for each ticker.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/rowNum_ticker'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,close,row_number() over(partition by ticker order by close) 
as num from stocks;

--How would you rank the closing prices of the stock for each ticker.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/denseRank_ticker'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,close,dense_rank() over(partition by ticker order by close) as 
closing from stocks;


--Find the cumulative of each record for every ticker.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/cummu_ticker'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,cume_dist() over(partition by ticker order by close) 
as cummulative from stocks;


--Calculate the percent_rank for every row in each partition.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/percentRank_ticker'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,close,percent_rank() over(partition by ticker order by close) 
as closing from stocks;


--Create 5 buckets for every ticker such that, the first 20% records for every ticker will be in the 1st bucket and so on.

INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/Desktop/TestHadoop/hive/output/bucket_ticker'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
select ticker,ntile(5) over(partition by ticker order by close ) as 
bucket from stocks;


