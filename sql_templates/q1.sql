-- Q1. Airlines

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q1 CASCADE;

CREATE TABLE q1 (
    pass_id INT,
    name VARCHAR(100),
    airlines INT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS FinishedFlight CASCADE;
DROP VIEW IF EXISTS Took CASCADE;
DROP VIEW IF EXISTS TookBy CASCADE;

-- Define views for your intermediate steps here:

-- Find those flights that have departured and arrived
-- Assume if a departure was make, an arrival also occured, as mentioned in handout
CREATE VIEW FinishedFlight AS
SELECT flight_id, airline
FROM Flight F JOIN Departure D
ON F.id = D.flight_id;

-- Relate Booking and FinishedFlight to find airlines each passenger took
CREATE VIEW Took AS
SELECT DISTINCT pass_id, airline
FROM Booking B JOIN FinishedFlight F
ON F.flight_id = B.flight_id;

-- Find the name of the passengers, and the number of different airlines they took
CREATE VIEW TookBy AS
SELECT p.id, surname||' '||firstname AS name, count(DISTINCT airline)
FROM Took T RIGHT JOIN Passenger P
ON T.pass_id = P.id
GROUP BY P.id;

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q1 
SELECT * from TookBy;
