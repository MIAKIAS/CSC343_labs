-- Q2. Refunds!

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q2 CASCADE;

CREATE TABLE q2 (
    airline CHAR(2),
    name VARCHAR(50),
    year CHAR(4),
    seat_class seat_class,
    refund REAL
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS Country CASCADE;
DROP VIEW IF EXISTS Domestic CASCADE;
DROP VIEW IF EXISTS International CASCADE;
DROP VIEW IF EXISTS Ref_Domestic CASCADE;
DROP VIEW IF EXISTS Ref_international CASCADE;
DROP VIEW IF EXISTS Ref_Perc_Domestic CASCADE;
DROP VIEW IF EXISTS Ref_Perc_International CASCADE;
DROP VIEW IF EXISTS Refund_Flights CASCADE;
DROP VIEW IF EXISTS Refund_Booking CASCADE;
DROP VIEW IF EXISTS ANS CASCADE;

-- Define views for your intermediate steps here:

-- Find the departure and arrival country(s) of each flight
CREATE VIEW Country AS
SELECT F.id, F.airline, A_out.country AS dept_country, A_in.country AS arr_country
, F.s_dep, F.s_arv
FROM Flight F, Airport A_in, Airport A_out
WHERE F.outbound = A_out.code AND F.inbound = A_in.code;

-- Find domestic flights with their scheduled and actual dept/arr times
-- Ignore flights that have not departed
CREATE VIEW Domestic AS
SELECT C.id, C.airline, C.s_dep, C.s_arv, D.datetime AS a_dep, A.datetime AS a_arv
FROM Country C, Departure D, Arrival A
WHERE C.id = D.flight_id AND C.id = A.flight_id
AND C.dept_country = C.arr_country;

-- Find international flights with their scheduled and actual dept/arr times
-- Ignore flights that have not departed
CREATE VIEW International AS
SELECT C.id, C.airline, C.s_dep, C.s_arv, D.datetime AS a_dep, A.datetime AS a_arv
FROM Country C, Departure D, Arrival A
WHERE C.id = D.flight_id AND C.id = A.flight_id
AND C.dept_country <> C.arr_country;

-- Find the refundable domestic flights
CREATE VIEW Ref_Domestic AS
SELECT id, airline, s_dep, s_arv, a_dep, a_arv
FROM Domestic 
WHERE (a_dep - s_dep) >= '05:00:00'
AND (a_arv - s_arv) > (0.5 * (a_dep - s_dep));

-- Find the refundable international flights
CREATE VIEW Ref_international AS
SELECT id, airline, s_dep, s_arv, a_dep, a_arv
FROM International 
WHERE (a_dep - s_dep) >= '08:00:00'
AND (a_arv - s_arv) > (0.5 * (a_dep - s_dep));

-- Find the percentage of refund of fefundable domestic flights
CREATE VIEW Ref_Perc_Domestic AS
(SELECT id, airline, EXTRACT(YEAR FROM s_dep) AS year, 0.35 AS refund
FROM Ref_Domestic
WHERE (a_dep - s_dep) >= '05:00:00' AND (a_dep - s_dep) < '10:00:00')
UNION 
(SELECT id, airline, EXTRACT(YEAR FROM s_dep) AS year, 0.50 AS refund
FROM Ref_Domestic
WHERE (a_dep - s_dep) >= '10:00:00');

-- Find the percentage of refund of refundable international flights
CREATE VIEW Ref_Perc_International AS
(SELECT id, airline, EXTRACT(YEAR FROM s_dep) AS year, 0.35 AS refund
FROM Ref_international
WHERE (a_dep - s_dep) >= '08:00:00' AND (a_dep - s_dep) < '12:00:00')
UNION 
(SELECT id, airline, EXTRACT(YEAR FROM s_dep) AS year, 0.50 AS refund
FROM Ref_international
WHERE (a_dep - s_dep) >= '12:00:00');

-- Combine to get the perfentage of refund of all refundable flights
CREATE VIEW Refund_Flights AS
(SELECT id AS flight_id, airline, year, refund FROM Ref_Perc_Domestic)
UNION
(SELECT * FROM Ref_Perc_International);

-- Find bookings of these flights
CREATE VIEW Refund_Booking AS
SELECT B.id AS booking_id, B.flight_id, R.airline, A.name, B.seat_class, R.year, R.refund * B.price AS dollar_refund
FROM Refund_Flights R, Booking B, Airline A
WHERE R.flight_id = B.flight_id AND R.airline = A.code;

-- Get the total refund money for each airline each seat class per year
CREATE VIEW ANS AS
SELECT airline, name, year, seat_class, sum(dollar_refund)
FROM Refund_Booking
GROUP BY airline, name, year, seat_class;

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q2 
SELECT * from ANS;
