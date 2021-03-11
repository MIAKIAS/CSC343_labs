-- Q3. North and South Connections

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q3 CASCADE;

CREATE TABLE q3 (
    outbound VARCHAR(30),
    inbound VARCHAR(30),
    direct INT,
    one_con INT,
    two_con INT,
    earliest timestamp
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS FlightwithAirportOut CASCADE;
DROP VIEW IF EXISTS FlightwithAirport CASCADE;
DROP VIEW IF EXISTS DirectFlightsFromCA CASCADE;
DROP VIEW IF EXISTS DirectFlightsFromUS CASCADE;
DROP VIEW IF EXISTS TwoFlightwithAirport CASCADE;
DROP VIEW IF EXISTS OneTranFlightsFromCA CASCADE;
DROP VIEW IF EXISTS OneTranFlightsFromUS CASCADE;
DROP VIEW IF EXISTS ThreeFlightwithAirport CASCADE;
DROP VIEW IF EXISTS TwoTranFlightsFromCA CASCADE;
DROP VIEW IF EXISTS TwoTranFlightsFromUS CASCADE;
DROP TABLE IF EXISTS combined CASCADE;
DROP VIEW IF EXISTS AllCities CASCADE;
DROP VIEW IF EXISTS AllCityPairs CASCADE;
DROP VIEW IF EXISTS PairsWithoutFlights CASCADE;

-- Define views for your intermediate steps here:

-- 1. Relate the flights with the inbound/outbound airport, city and country information, also extract the desire flights with departure and arrival time within 2021-04-30

create view FlightwithAirportOut as
select id, outbound, city as out_city, country as out_country, inbound, s_dep, s_arv
from Flight, Airport
where Flight.outbound = Airport.code;


create view FlightwithAirport as
select id, outbound, out_city, out_country, inbound, city as in_city, country as in_country, s_dep, s_arv
from FlightwithAirportOut, Airport
where inbound = code and 
		s_dep>='2021-04-30 00:00:00' and s_dep<='2021-04-30 23:59:59' and 
		s_arv>='2021-04-30 00:00:00' and s_arv<='2021-04-30 23:59:59';

-- 2. count for direct flights of two directions with earlist arrival time for each flights

create view DirectFlightsFromCA as
select count(id) as num_direct, out_city, in_city, min(s_arv) as earlist_arrival
from FlightwithAirport
where out_country = 'Canada' and in_country = 'USA'
group by out_city, in_city;

create view DirectFlightsFromUS as
select count(id) as num_direct, out_city, in_city, min(s_arv) as earlist_arrival
from FlightwithAirport
where in_country = 'Canada' and out_country = 'USA'
group by out_city, in_city;

-- 3. Relate the two flights with the same transition bound with information.

create view TwoFlightwithAirport as
select a.id,
		a.outbound, a.out_city, a.out_country,
		a.inbound as tranbound, a.in_city as tran_city, a.in_country as tran_country,
		b.inbound, b.in_city, b.in_country,
		a.s_dep, a.s_arv as s_tran_arv,
		b.s_dep as s_tran_dep, b.s_arv
from FlightwithAirport a, FlightwithAirport b
where a.inbound = b.outbound and b.s_dep - a.s_arv >= '00:30:00';

-- 4. count for one transition flights of two directions with earlist arrival time for each 

create view OneTranFlightsFromCA as
select count(id) as num_one, out_city, in_city, min(s_arv) as earlist_arrival
from TwoFlightwithAirport
where out_country = 'Canada' and in_country = 'USA'
group by out_city, in_city;

create view OneTranFlightsFromUS as
select count(id) as num_one, out_city, in_city, min(s_arv) as earlist_arrival
from TwoFlightwithAirport
where in_country = 'Canada' and out_country = 'USA'
group by out_city, in_city;

-- 5. Relate the three flights with the same transition bound with information.

create view ThreeFlightwithAirport as
select a.id,
		a.outbound, a.out_city, a.out_country,
		a.tranbound as tran_1_bound, a.tran_city as tran_1_city, a.tran_country as tran_1_country,
		a.inbound as tran_2_bound, a.in_city as tran_2_city, a.in_country as tran_2_country,
		b.inbound, b.in_city, b.in_country,
		
		a.s_dep, a.s_tran_arv as s_tran_1_arv,
		a.s_tran_dep as s_tran_1_dep, a.s_arv as s_tran_2_arv,
		b.s_dep as s_tran_2_dep, b.s_arv
		
from TwoFlightwithAirport a, FlightwithAirport b
where a.inbound = b.outbound and b.s_dep - a.s_arv >= '00:30:00';

-- 6. count for two transition flights of two directions with earlist arrival time for each 

create view TwoTranFlightsFromCA as
select count(id) as num_two, out_city, in_city, min(s_arv) as earlist_arrival
from ThreeFlightwithAirport
where out_country = 'Canada' and in_country = 'USA'
group by out_city, in_city;

create view TwoTranFlightsFromUS as
select count(id) as num_two, out_city, in_city, min(s_arv) as earlist_arrival
from ThreeFlightwithAirport
where in_country = 'Canada' and out_country = 'USA'
group by out_city, in_city;

-- 7. Combined the direct / one transition / two transition, count on for flights and earlist arrival time

CREATE TABLE combined (
    outbound VARCHAR(30),
    inbound VARCHAR(30),
    direct INT default 0,
    one_con INT default 0,
    two_con INT default 0,
    earliest timestamp
);

insert into combined(outbound, inbound, direct, earliest)
select out_city, in_city, num_direct, earlist_arrival
from DirectFlightsFromCA;

insert into combined(outbound, inbound, one_con, earliest)
select out_city, in_city, num_one, earlist_arrival
from OneTranFlightsFromCA;

insert into combined(outbound, inbound, two_con, earliest)
select out_city, in_city, num_two, earlist_arrival
from TwoTranFlightsFromCA;

insert into combined(outbound, inbound, direct, earliest)
select out_city, in_city, num_direct, earlist_arrival
from DirectFlightsFromUS;

insert into combined(outbound, inbound, one_con, earliest)
select out_city, in_city, num_one, earlist_arrival
from OneTranFlightsFromUS;

insert into combined(outbound, inbound, two_con, earliest)
select out_city, in_city, num_two, earlist_arrival
from TwoTranFlightsFromUS;

-- 8. Find the other pairs of cities that are not included, does not have flights in between

create view AllCities as
select Distinct city, country
from airport
where country = 'Canada' or country = 'USA';

create view AllCityPairs as
select a.city as outbound, b.city as inbound
from AllCities a, AllCities b
where a.country<>b.country;

create view PairsWithoutFlights as
(select outbound, inbound
from AllCityPairs)
EXCEPT
(select distinct outbound, inbound
from combined);

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q3 (outbound, inbound, direct, one_con, two_con, earliest)
select outbound, inbound, sum(direct), sum(one_con), sum(two_con), min(earliest)
from combined
group by outbound, inbound;

INSERT INTO q3 (outbound, inbound, direct, one_con, two_con)
select outbound, inbound, 0, 0, 0
from PairsWithoutFlights;