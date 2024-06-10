'''sql

-- STPE 1 : CREATE DATABASE, SCHEMA, TABLE 
CREATE OR REPLACE  TABLE brands (
  json_data VARIANT);
  --keep your data as JSON then need to load it into a column with a datatype of VARIANT, not VARCHAR 
SELECT * FROM brands;

-- STEP2: CREATE STAGE & FORMAT
show stages;
LIST @my_internal_stage;

show file formats;
DESC file format myjsonformat;


--STEP3  STAGE THE DATA FILES 
-- USE SNOWSQL COMMAND ON CLI
PUT file:///Users/sakichen/Desktop/Fetch/Data/brands.json @my_internal_stage;

--STPE4 COPY DATA INTO THE TARGET TABLE
  COPY INTO brands
  FROM @my_internal_stage/brands.json.gz
  FILE_FORMAT = (FORMAT_NAME = myjsonformat)
  ON_ERROR = 'skip_file';

  DESC table receipts;

--STEP5 CHECK IF LOAD ERRORS AND FIX 
SELECT * FROM brands LIMIT 5;


--STEP6 FLATTEN
--   attributes make sure its original capital or lower cases 
CREATE OR REPLACE table brands_flatten 
AS SELECT *
from(
 SELECT 
    JSON_DATA:_id.     "$oid"::VARCHAR AS brandID
    ,JSON_DATA:barcode::STRING AS barcode   
    ,JSON_DATA:brandCode::STRING AS BrandCode
    ,JSON_DATA:category::STRING AS category
    ,JSON_DATA:categoryCode::STRING AS categoryCode
    -- ,JSON_DATA:cpg AS CPG
    ,BRANDS.JSON_DATA:cpg:"$id":"$oid"::STRING AS cpg_id
    ,BRANDS.JSON_DATA:cpg:"$ref"::STRING AS cpg_ref
    ,JSON_DATA:topBrand::Boolean AS topbrand
    ,JSON_DATA:name::STRING AS brandname

from FETCH_REWARDS.PUBLIC.BRANDS
-- ,LATERAL FLATTEN(input => BRANDS.JSON_DATA:cpg) AS item
);


select * from brands_flatten; 


-- -------------------JOIN TEST -------------------------------

SELECT a.brandID,a.category, a.brandname,a.barcode, b.receiptID, b.ITEM_BARCODE
FROM brands_flatten a 
JOIN FETCH_REWARDS.PUBLIC.RECEIPTS_FLATTEN b 
ON a.barcode = b.ITEM_BARCODE;

,,,
