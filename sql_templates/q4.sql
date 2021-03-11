-- Q4. Plane Capacity Histogram

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q4 CASCADE;

CREATE TABLE q4 (
	airline CHAR(2),
	tail_number CHAR(5),
	very_low INT,
	low INT,
	fair INT,
	normal INT,
	high INT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS DeparturedFlight CASCADE;
DROP VIEW IF EXISTS DeparturedFlightCapacity CASCADE;
DROP TABLE IF EXISTS FlightTotalCapacity CASCADE;
DROP VIEW IF EXISTS FlightCapacity CASCADE;
DROP VIEW IF EXISTS FlightBooking CASCADE;
DROP VIEW IF EXISTS NotDepartured CASCADE;

-- Define views for your intermediate steps here:

-- 1. Find the departured flight with each plane's capacities

create view DeparturedFlight as
select id, airline, plane
from Flight, Departure
where id = flight_id;  

create view DeparturedFlightCapacity as
select id as flight_id, DeparturedFlight.airline, tail_number, capacity_economy, capacity_business, capacity_first
from DeparturedFlight, Plane
where plane = tail_number;

CREATE TABLE FlightTotalCapacity (
	flight_id INT,
	airline CHAR(2),
	tail_number CHAR(5),
	capacity INT
);

insert into FlightTotalCapacity
select flight_id, airline, tail_number, capacity_economy
from DeparturedFlightCapacity;

insert into FlightTotalCapacity
select flight_id, airline, tail_number, capacity_business
from DeparturedFlightCapacity;

insert into FlightTotalCapacity
select flight_id, airline, tail_number, capacity_first
from DeparturedFlightCapacity;

create view FlightCapacity as
select flight_id, airline, tail_number, sum(capacity) as total_capacity
from FlightTotalCapacity
group by flight_id, airline, tail_number;

-- 2. Find the number of booking for each departured flight

create view FlightBooking as
select count(id) as num_booking, flight_id
from Booking
where id in (select id from DeparturedFlight)
group by flight_id;

-- 3. calculate and category the pencentage of booking capacity

create view CapacityBooking as
select a.flight_id, airline, tail_number, COALESCE((100 * num_booking / total_capacity), 0) as percentage
from FlightCapacity a natural left join FlightBooking b;

-- 4. keep track of the not departured flights and still not in the list

create view NotDepartured as
(select airline, plane as tail_number
from Flight)
EXCEPT
(select airline, tail_number
from CapacityBooking);

-- Your query that answers the question goes below the "insert into" line:

INSERT INTO q4
select airline, tail_number, 
		0 as very_low,
		0 as low,
		0 as fair,
		0 as normal,
		0 as high
from NotDepartured;

INSERT INTO q4
select airline, tail_number, 
		0 as very_low,
		0 as low,
		0 as fair,
		0 as normal,
		count(flight_id) as high 
from CapacityBooking
where percentage>=80
group by airline, tail_number;

INSERT INTO q4
select airline, tail_number, 
		0 as very_low,
		0 as low,
		0 as fair,
		count(flight_id) as normal,
		0 as high 
from CapacityBooking
where percentage>=60 and percentage<80
group by airline, tail_number;

INSERT INTO q4
select airline, tail_number, 
		0 as very_low,
		0 as low,
		count(flight_id) as fair,
		0 as normal,
		0 as high 
from CapacityBooking
where percentage>=40 and percentage<60
group by airline, tail_number;

INSERT INTO q4
select airline, tail_number, 
		0 as very_low,
		count(flight_id) as low,
		0 as fair,
		0 as normal,
		0 as high 
from CapacityBooking
where percentage>=20 and percentage<40
group by airline, tail_number;

INSERT INTO q4
select airline, tail_number, 
		count(flight_id) as very_low,
		0 as low,
		0 as fair,
		0 as normal,
		0 as high 
from CapacityBooking
where percentage>=0 and percentage<20
group by airline, tail_number;
