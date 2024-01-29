Question1:Homework Which tag has the following text? - Automatically remove the container when it exits→ 
Ans:–rm
 docker run --help


Question2:What is the version of the package wheel ?
 Ans:docker run -it --entrypoint=bash python:3.9
root@0a1a4448ec9e:/# pip list
Package    Version
---------- -------
pip        23.0.1
setuptools 58.1.0
wheel      0.42.0

Question3:How many taxi trips were totally made on September 18th 2019?
Tip: started and finished on 2019-09-18.
Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.-->15612
 Ans: select count(*) from green_taxi_data where date(lpep_pickup_datetime) = '2019-09-18' and date(lpep_dropoff_datetime) = '2019-09-18';
+-------+
| count |
|-------|
| 15612 |
+-------+
SELECT 1
Time: 0.102s

Question4:Which was the pick up day with the largest trip distance Use the pick up time for your calculations.
Ans: 2019-09-26
select *
from public.green_taxi_data
order by trip_distance desc
limit 10

Question5:Three biggest pick up Boroughs
Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown
Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
Ans:"Brooklyn" "Manhattan" "Queens"
select gzp."Borough"
,sum(gd.total_amount) as tot_amt
from green_taxi_data gd left join
green_taxi_zone_lookup gzp on gd."PULocationID" = gzp."LocationID"
where date(lpep_pickup_datetime) = '2019-09-18'
group by 1
order by tot_amt desc;
"Borough"	"tot_amt"
"Brooklyn"	96333.23999999925
"Manhattan"	92271.29999999955
"Queens"	78671.7099999997

Question6:For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.
Note: it's not a typo, it's tip , not trip
Ans:JFK Airport
select lpep_pickup_datetime,gzp."Zone" as pickup_zone,gzd."Zone" as dropoff_zone
,tip_amount
from green_taxi_data gd join
green_taxi_zone_lookup gzp on gd."PULocationID" = gzp."LocationID" join
green_taxi_zone_lookup gzd on gd."DOLocationID" = gzd."LocationID"
where extract('MONTH' FROM date(lpep_pickup_datetime)) = 9 
and gzp."Zone" = 'Astoria'
order by tip_amount desc;
"lpep_pickup_datetime"	"pickup_zone"	"dropoff_zone"	"tip_amount"
"2019-09-08 18:10:40"	"Astoria"	"JFK Airport"	62.31
"2019-09-15 02:01:47"	"Astoria"	"Woodside"	30
"2019-09-25 10:24:32"	"Astoria"	"Kips Bay"	28
"2019-09-03 04:25:59"	"Astoria"	"NV"	25
