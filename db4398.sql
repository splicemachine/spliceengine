CREATE TABLE AMEX.DimCustomer_agg(
CustomerKey INT NOT NULL PRIMARY KEY,
GeographyKey INT ,
CustomerAlternateKey VARCHAR(15) NOT NULL,
Title VARCHAR(8) ,
FirstName VARCHAR(50) ,
MiddleName VARCHAR(50) ,
LastName VARCHAR(50) ,
NameStyle CHAR(2) ,
BirthDate TIMESTAMP ,
MaritalStatus CHAR(1) ,
Suffix VARCHAR(10) ,
Gender VARCHAR(1) ,
EmailAddress VARCHAR(50) ,
YearlyIncome NUMERIC(8,2) ,
TotalChildren SMALLINT ,
NumberChildrenAtHome SMALLINT ,
EnglishEducation VARCHAR(40) ,
SpanishEducation VARCHAR(40) ,
FrenchEducation VARCHAR(40) ,
EnglishOccupation VARCHAR(100) ,
SpanishOccupation VARCHAR(100) ,
FrenchOccupation VARCHAR(100) ,
HouseOwnerFlag CHAR(1) ,
NumberCarsOwned SMALLINT ,
AddressLine1 VARCHAR(120) ,
AddressLine2 VARCHAR(120) ,
Phone VARCHAR(20) ,
DateFirstPurchase TIMESTAMP ,
CommuteDistance VARCHAR(15) ,
IndividualKey VARCHAR(25) );


CREATE TABLE AMEX.dimcustomer_sampling_agg
(
CustomerKey INT NOT NULL PRIMARY KEY,
GeographyKey INT ,
CustomerAlternateKey VARCHAR(15) NOT NULL,
Title VARCHAR(8) ,
FirstName VARCHAR(50) ,
MiddleName VARCHAR(50) ,
LastName VARCHAR(50) ,
NameStyle CHAR(2) ,
BirthDate TIMESTAMP ,
MaritalStatus CHAR(1) ,
Suffix VARCHAR(10) ,
Gender VARCHAR(1) ,
EmailAddress VARCHAR(50) ,
YearlyIncome NUMERIC(8,2) ,
TotalChildren SMALLINT ,
NumberChildrenAtHome SMALLINT ,
EnglishEducation VARCHAR(40) ,
SpanishEducation VARCHAR(40) ,
FrenchEducation VARCHAR(40) ,
EnglishOccupation VARCHAR(100) ,
SpanishOccupation VARCHAR(100) ,
FrenchOccupation VARCHAR(100) ,
HouseOwnerFlag CHAR(1) ,
NumberCarsOwned SMALLINT ,
AddressLine1 VARCHAR(120) ,
AddressLine2 VARCHAR(120) ,
Phone VARCHAR(20) ,
DateFirstPurchase TIMESTAMP ,
CommuteDistance VARCHAR(15) ,
IndividualKey VARCHAR(25) );

explain 
SELECT COUNT(*) FROM "AMEX"."DIMCUSTOMER_AGG" a1 
WHERE EXISTS 
  (SELECT 1 FROM (SELECT a3."CUSTOMERKEY" FROM "AMEX"."DIMCUSTOMER_AGG" a3 
    LEFT JOIN (SELECT a4."CUSTOMERKEY", MAX(a4."YEARLYINCOME") AS "RPIAggregate" FROM "AMEX"."DIMCUSTOMER_SAMPLING_AGG" a4 WHERE EXISTS 
      (SELECT 1 FROM "AMEX"."DIMCUSTOMER_AGG" a5 WHERE a5."CUSTOMERKEY" = a4."CUSTOMERKEY" AND EXISTS 
         (SELECT 1 FROM (SELECT a7."CUSTOMERKEY" FROM "AMEX"."DIMCUSTOMER_AGG" a7 WHERE a7."GENDER" = 'M' UNION ALL 
            SELECT a8."CUSTOMERKEY" FROM "AMEX"."DIMCUSTOMER_AGG" a8 WHERE (a8."FIRSTNAME" < 'A' OR a8."FIRSTNAME" > 'J' OR a8."FIRSTNAME" IS NULL)) AS a6 
            WHERE a6."CUSTOMERKEY" = a5."CUSTOMERKEY")) GROUP BY a4."CUSTOMERKEY") 
      AS a9 ON a3."CUSTOMERKEY"=a9."CUSTOMERKEY" WHERE a9."RPIAggregate" IN (40000, 70000) 
      UNION ALL SELECT a10."CUSTOMERKEY" FROM "AMEX"."DIMCUSTOMER_AGG" a10 WHERE EXISTS 
        (SELECT 1 FROM (SELECT a12."CUSTOMERKEY" FROM "AMEX"."DIMCUSTOMER_AGG" a12 WHERE a12."GENDER" = 'M' 
                        UNION ALL SELECT a13."CUSTOMERKEY" FROM "AMEX"."DIMCUSTOMER_AGG" a13 
                          WHERE (a13."FIRSTNAME" < 'A' OR a13."FIRSTNAME" > 'J' OR a13."FIRSTNAME" IS NULL)) AS a11 
                         WHERE a11."CUSTOMERKEY" = a10."CUSTOMERKEY")) AS a2 
    WHERE a2."CUSTOMERKEY" = a1."CUSTOMERKEY");


