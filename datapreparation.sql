-- Inpatients Admissions table
-- Creating a sub table with relatable features
CREATE TABLE ccaei_20 AS
SELECT enrolid, year, age, sex, days, dxver, drg, mdc, dstatus, agegrp, rx, admtyp,
	ARRAY[dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12, dx13, dx14, dx15] AS dxcodes
FROM public.ccaei200_a;

-- removed features after final discussion
ALTER TABLE ccaei_20
DROP COLUMN drg,
DROP COLUMN rx,
DROP COLUMN admtyp,
DROP COLUMN dstatus;

-- removing missing values
DELETE FROM ccaei_20 WHERE enrolid IS NULL;
DELETE FROM ccaei_20 WHERE age IS NULL;
DELETE FROM ccaei_20 WHERE sex IS NULL;
DELETE FROM ccaei_20 WHERE dxver IS NULL;

-- dealing with only icd-10 codes
DELETE FROM ccaei_20 WHERE dxver = '9';
ALTER TABLE ccaei_20 DROP COLUMN dxver;

-- dealing with agegrp of age starting from 18
DELETE FROM ccaei_20 WHERE agegrp = '1';

-- Feature Mdc transformation:
ALTER TABLE ccaei_20 ADD flag_mdc INT;

UPDATE ccaei_20
SET flag_mdc = CASE
    WHEN mdc IN ('01','02','03','04','05','06','07','08','10','11','16','17',
                 '19','20','21','24') THEN 1
    ELSE 0
END;

ALTER TABLE ccaei_20
ADD COLUMN mdc_flag INT,
ADD COLUMN non_mdc_flag INT;

UPDATE ccaei_20
SET mdc_flag = CASE
    WHEN flag_mdc IN ('1') THEN 1
    ELSE 0
END;

UPDATE ccaei_20
SET non_mdc_flag = CASE
    WHEN flag_mdc IN ('0') THEN 1
    ELSE 0
END;

-- Feature Days transformation:
ALTER TABLE ccaei_20 
ADD COLUMN short_stay INT,
ADD COLUMN medium_stay INT,
ADD COLUMN long_stay INT;

UPDATE ccaei_20
SET short_stay = CASE
    WHEN days IN ('1','2','3') THEN 1
    ELSE 0
END;

UPDATE ccaei_20
SET medium_stay = CASE
    WHEN days IN ('4','5','6','7') THEN 1
    ELSE 0
END;

UPDATE ccaei_20
SET long_stay = CASE
    WHEN days IN ('1','2','3','4','5','6','7') THEN 0
    ELSE 1
END;

-- creating a final version of ccaei table 
-- Aggregating enrolids by selecting the same enrolids for features-target 
CREATE TABLE ccaei_final_20 AS
SELECT
    enrolid, year, age, sex, agegrp,
    JSON_AGG(dxcodes) AS merged_dxcodes,     
    SUM(short_stay) AS tot_shortstay,
    SUM(medium_stay) AS tot_mediumstay,
    SUM(long_stay) AS to_longstay,
    SUM(mdc_flag) AS tot_mdc_imp,
    SUM(non_mdc_flag) AS tot_non_mdc
FROM ccaei_20
WHERE enrolid IN (
    SELECT DISTINCT c20.enrolid
    FROM ccaei_20 c20
    INNER JOIN ccaei_19 c19 ON c20.enrolid = c19.enrolid)
GROUP BY enrolid, year, age, sex, agegrp;



-- Inpatient services table:
-- creating a sub table with relatable features
CREATE TABLE ccaes_20 AS
SELECT enrolid,year,age,svcscat FROM public.ccaes200_a WHERE enrolid IS NOT null;

-- Creating flag columns of emergencies for svcscat feature
ALTER TABLE ccaes_20
ADD COLUMN emer_flag INT,
ADD COLUMN non_emer_flag INT;

UPDATE ccaes_20
SET emer_flag = CASE
    WHEN RIGHT(svcscat, 2) = '20' THEN 1
    ELSE 0
END;

UPDATE ccaes_17
SET non_emer_flag = CASE
    WHEN RIGHT(svcscat, 2) <> '20' THEN 1
    ELSE 0
END;

-- creating a final version of ccaes table 
-- Aggregating enrolids by selecting the same enrolids that exist in final ccaei 
CREATE TABLE ccaes_final_20 AS
SELECT 
	enrolid, year, MAX(age) AS age,
    SUM(emer_flag) AS emer_visits, 
    SUM(non_emer_flag) as non_emer_visits
FROM ccaes_20
WHERE enrolid IN (
    SELECT DISTINCT ci20.enrolid
    FROM ccaei_final_20 ci20
    INNER JOIN ccaes_20 cs20 ON ci20.enrolid = cs20.enrolid)
GROUP BY enrolid, year;



-- Outpatient pharmaceutical table:
-- creating a sub table with relatable features
CREATE TABLE ccaed_20 AS
SELECT enrolid, year, age, thergrp FROM public.ccaed172 WHERE enrolid IS NOT null;

-- removing missing values
Delete from ccaed_17 where thergrp is null; 

-- removing the unrelated vlaues of thergrp
DELETE FROM ccaed_17
WHERE thergrp::text NOT IN ('1', '5', '6', '7', '8', '14', '15', '16', '27');

-- creating a final version of ccaed table 
-- Aggregating enrolids by selecting the same enrolids that exist in final ccaei 
CREATE TABLE ccaed_final_20 AS
SELECT
    enrolid, year, MAX(age) AS age,
    COUNT(thergrp) as thergrp_count
FROM ccaed_20
WHERE enrolid IN (
    SELECT DISTINCT ci20.enrolid
    FROM ccaei_final_20 ci20
    INNER JOIN ccaed_20 cd20 ON ci20.enrolid = cd20.enrolid)
GROUP BY enrolid, year;



--- Combining final ccaei and ccaes tables
CREATE TABLE merged_20_in AS
SELECT
    a.enrolid, 
	a.year, 
	a.age, 
	a.sex, 
	a.agegrp,
    a.merged_dxcodes AS dxcodes,
    a.tot_shortstay AS shortstay,
    a.tot_mediumstay AS mediumstay,
    a.to_longstay AS longstay,
    a.tot_mdc_imp AS imp_mdc,
    a.tot_non_mdc AS non_mdc,
    b.emer_visits,
    b.non_emer_visits
FROM
    ccaei_final_20 a
JOIN
    ccaes_final_20 b
ON
    a.enrolid = b.enrolid;



--- Combining final ccaei and ccaes tables (merged_20_in)
--- with the final ccaed table
CREATE TABLE merged_20_inout AS
SELECT
    a.enrolid,
	a.year,
    a.age,
    a.sex,
    a.agegrp,
    a.dxcodes,
    a.shortstay,
    a.mediumstay,
    a.longstay,
    a.imp_mdc,
    a.non_mdc,
    a.emer_visits,
    a.non_emer_visits,
    COALESCE(b.thergrp_count, 0) AS thergrp_count
FROM
    merged_20_in a
LEFT JOIN
    ccaed_final_20 b
ON
    a.enrolid = b.enrolid;

-- transformed the dxcodes to chapter codes as transformed_20
-- 