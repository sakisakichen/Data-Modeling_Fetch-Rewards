
<details>
<summary>BRAND TABLE</summary>

#### table create for brand

--   attributes make sure its original capital or lower cases , and empty space in front

```sql


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
CREATE OR REPLACE table brand
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


select * from brand; 
```
</details>


<details>
<summary>USER TABLE</summary>

#### table create for user

--   attributes make sure its original capital or lower cases , and empty space in front    

--  with space in each column ) , speacil character '$', no unique identifier userID1,2,3......., 
```sql

--STEP1   CREATE DATABASE & SCHEMA  & TABLES 
show databases
use fetch_rewards;

show schemas;
use schema public;

CREATE OR REPLACE TEMPORARY TABLE users (
  json_data VARIANT);
  --keep your data as JSON then need to load it into a column with a datatype of VARIANT, not VARCHAR 
SELECT * FROM Users;


--STEP2  CREATE INTERNAL STAGE
CREATE OR REPLACE STAGE my_internal_stage;

--STEP3  CREATE FILE FORMAT 
CREATE OR REPLACE FILE FORMAT myjsonformat
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;

list @my_internal_stage;

--STEP4  STAGE THE DATA FILES 
-- USE SNOWSQL COMMAND ON CLI
PUT file:///Users/sakichen/Desktop/Fetch/Data/users.json @my_internal_stage;

-- STPE5 COPY DATA INTO THE TARGET TABLE
  COPY INTO users
  FROM @my_internal_stage/users.json.gz
  FILE_FORMAT = (FORMAT_NAME = myjsonformat)
  ON_ERROR = 'skip_file';

--STEP6 CHECK IF LOAD ERRORS AND FIX 
SELECT * FROM USERS LIMIT 5;

-- STEP7 REMOVE THE DATA FILE FROM STAGE
REMOVE @my_internal_stage PATTERN='.*.json.gz';

-- FLATTEN

SELECT replace(JSON_DATA:_id:     "$oid",'"') as user_id,
       JSON_DATA:active::Boolean as active,
       JSON_DATA:createdate:     "$date"::NUMBER as createdate,
       JSON_DATA:laslogin:     "$date" as lastlogin,
       replace(JSON_DATA:role, '"')as role,
       replace(JSON_DATA:signUpSource, '"')as signUpSource,
       JSON_DATA:state::string as state 
from FETCH_REWARDS.PUBLIC.USERS;



select JSON_DATA:createdate as tmp
from users;

select json_data from users limit 1;

WITH users_temp AS (
  SELECT 
    json_data:_id.     "$oid"::VARCHAR AS oid,
    json_data:state::VARCHAR AS state,
    json_data:createdDate.     "$date"::NUMBER AS created_date_ms,
    json_data:lastLogin.     "$date"::NUMBER AS last_login_ms,
    json_data:role::VARCHAR AS role,
    json_data:signUpSource::VARCHAR AS signUpSource,
    json_data:active::VARCHAR AS active
  FROM users
)
 SELECT 
  oid,
  state,
  TO_TIMESTAMP(created_date_ms / 1000) AS createdDate,
  TO_TIMESTAMP(last_login_ms / 1000) AS lastLogin,
  role,
  signUpSource,
  active
FROM users_temp;


-- ==========================================================
CREATE OR REPLACE table user
AS 
select 
-- row_number()over(order by oid) as rid_no,
  userid,
  state,
  TO_TIMESTAMP(created_date_ms / 1000) AS createdDate,
  TO_TIMESTAMP(last_login_ms / 1000) AS lastLogin,
  role,
  signUpSource,
  active
FROM 
(SELECT 
    json_data:_id.     "$oid"::VARCHAR AS userid,
    json_data:state::VARCHAR AS state,
    json_data:createdDate.     "$date"::NUMBER AS created_date_ms,
    json_data:lastLogin.     "$date"::NUMBER AS last_login_ms,
    json_data:role::VARCHAR AS role,
    json_data:signUpSource::VARCHAR AS signUpSource,
    json_data:active::VARCHAR AS active
  FROM users);

select * FROM users_flatten limit 3;

```
</details>



<details>
<summary>RECIEIPT TABLE</summary>

#### table create for receipt

--   attributes make sure its original capital or lower cases , and empty space in front

```sql
CREATE OR REPLACE table receipt
AS SELECT 
      receiptID, bonusPointsEarned, bonusPointsEarnedReason,   
      TO_TIMESTAMP(createDate/1000) AS createDate,
      TO_TIMESTAMP(dateScanned/1000) AS dateScanned,
      TO_TIMESTAMP(finishedDate/1000) AS finishedDate,
      TO_TIMESTAMP(modifyDate/1000) AS modifyDate,
      TO_TIMESTAMP(pointsAwardedDate/1000) AS pointsAwardedDate,
      pointsEarned, 
      TO_TIMESTAMP(purchaseDate/1000) AS purchaseDate,
      purchasedItemCount,rewardsReceiptStatus,totalSpent, userID
FROM (
SELECT 
    JSON_DATA:_id.     "$oid"::VARCHAR AS receiptID,
    JSON_DATA:bonusPointsEarned::INT AS bonusPointsEarned,
    JSON_DATA:bonusPointsEarnedReason::STRING AS bonusPointsEarnedReason,
    JSON_DATA:createDate. "$date" ::NUMBER AS createDate,
    JSON_DATA:dateScanned. "$date" ::NUMBER AS dateScanned,
    JSON_DATA:finishedDate. "$date" ::NUMBER AS finishedDate,
    JSON_DATA:modifyDate. "$date" ::NUMBER AS modifyDate,
    JSON_DATA:pointsAwardedDate. "$date" ::NUMBER AS pointsAwardedDate,
    JSON_DATA:pointsEarned::FLOAT AS pointsEarned,
    JSON_DATA:purchaseDate. "$date" ::NUMBER AS purchaseDate,
    JSON_DATA:purchasedItemCount::INT AS purchasedItemCount,
    JSON_DATA:rewardsReceiptStatus::STRING AS rewardsReceiptStatus,
    JSON_DATA:totalSpent::FLOAT AS totalSpent,
    JSON_DATA:userId::VARCHAR AS userID


from FETCH_REWARDS.PUBLIC.RECEIPTS
);
select * FROM RECEIPT;

```
</details>


<details>
<summary>RECIEIPT_ITEM TABLE</summary>

#### table create for receipt_item

--   attributes make sure its original capital or lower cases , and empty space in front

```sql
CREATE OR REPLACE table receipt_item
AS SELECT concat(receiptID,'-',partnerItemId) as receipt_item_pk, t.*
from (
SELECT 
    JSON_DATA:_id.     "$oid"::VARCHAR AS receiptID
    ,item.value:"barcode"::STRING AS item_barcode
    ,item.value:description::STRING AS item_description
    ,item.value:finalPrice::FLOAT AS item_finalPrice
    ,item.value:itemPrice::FLOAT AS itemPrice
    ,item.value:partnerItemId::STRING AS partnerItemId
    ,item.value:quantityPurchased::NUMBER AS item_quantityPurchased
    ,item.value:rewardsGroup::VARCHAR AS rewardsGroup
    ,item.value:rewardsProductPartnerId::VARCHAR AS rewardsProductPartnerId
    ,item.value:needsFetchReview::Boolean AS needsFetchReview
    ,item.value:needsFetchReviewReason::Varchar as needsFetchReviewReason
from FETCH_REWARDS.PUBLIC.RECEIPTS 
,LATERAL FLATTEN(input => FETCH_REWARDS.PUBLIC.RECEIPTS.JSON_DATA:rewardsReceiptItemList) as item
ORDER BY  item_barcode,needsFetchReview,rewardsgroup
)t;



```
</details>

<details>
<summary>USER_FLAGGED TABLE</summary>

#### table create for USER_FLAGGED

--   attributes make sure its original capital or lower cases , and empty space in front

```sql
 CREATE OR REPLACE TABLE user_flagged 
 AS SELECT  CONCAT(receiptID, '-', userFlaggedBarcode) AS user_flag_pk,
            t.*
 FROM 
 (
    SELECT 
        JSON_DATA:_id.     "$oid"::VARCHAR AS receiptID,
        item.value:userFlaggedBarcode::VARCHAR AS userFlaggedBarcode,
        item.value:userFlaggedNewItem::BOOLEAN AS userFlaggedNewItem,
        item.value:userFlaggedPrice::FLOAT AS userFlaggedPrice,
        item.value:userFlaggedQuantity::NUMBER AS userFlaggedQuantity,
        item.value:userFlaggedDescription::VARCHAR AS userFlaggedDescription,
        item.value:needsFetchReview::BOOLEAN AS needsFetchReview,
        item.value:needsFetchReviewReason::VARCHAR AS needsFetchReviewReason
    FROM FETCH_REWARDS.PUBLIC.RECEIPTS  
    ,LATERAL FLATTEN(input => FETCH_REWARDS.PUBLIC.RECEIPTS.JSON_DATA:rewardsReceiptItemList) AS item
)t

WHERE needsFetchReviewReason = 'USER_FLAGGED'
ORDER BY userFlaggedBarcode, userFlaggedNewItem DESC, userFlaggedPrice, userFlaggedQuantity, userFlaggedDescription;
    



```
</details>

