
```sql

-- STPE 1 : CREATE DATABASE, SCHEMA, TABLE 
CREATE OR REPLACE  TABLE receipts (
  json_data VARIANT);
  --keep your data as JSON then need to load it into a column with a datatype of VARIANT, not VARCHAR 
SELECT * FROM receipts;

-- STEP2: CREATE STAGE & FORMAT
show stages;
LIST @my_internal_stage;

show file formats;
DESC file format myjsonformat;


--STEP3  STAGE THE DATA FILES 
-- USE SNOWSQL COMMAND ON CLI
PUT file:///Users/sakichen/Desktop/Fetch/Data/receipts.json @my_internal_stage;

--STPE4 COPY DATA INTO THE TARGET TABLE
  COPY INTO receipts
  FROM @my_internal_stage/receipts.json.gz
  FILE_FORMAT = (FORMAT_NAME = myjsonformat)
  ON_ERROR = 'skip_file';

  DESC table receipts;

--STEP5 CHECK IF LOAD ERRORS AND FIX 
SELECT * FROM Receipts LIMIT 5;

--STEP6 REMOVE THE DATA FILE FROM STAGE
REMOVE @my_internal_stage PATTERN='.*.json.gz';

--STEP7 FLATTEN
      -- bonus points earned is INT but point is FLOAT => BONUS POINT ALWAYS INT ?
      -- ITEM PRICE & ITEM final price should be float same as total spent 

CREATE OR REPLACE table receipts_flatten 
AS SELECT 
      receiptID, bonusPointsEarned, bonusPointsEarnedReason,   
      TO_TIMESTAMP(createDate/1000) AS createDate,
      TO_TIMESTAMP(dateScanned/1000) AS dateScanned,
      TO_TIMESTAMP(finishedDate/1000) AS finishedDate,
      TO_TIMESTAMP(modifyDate/1000) AS modifyDate,
      TO_TIMESTAMP(pointsAwardedDate/1000) AS pointsAwardedDate,
      pointsEarned, 
      TO_TIMESTAMP(purchaseDate/1000) AS purchaseDate,
      purchasedItemCount
      ,item_barcode,item_description,item_finalPrice,itemPrice,item_needsFetchReview,item_partnerItemId
      ,item_preventTargetGapPoints,item_quantityPurchased,item_userFlaggedBarcode,item_userFlaggedNewItem
      ,item_userFlaggedPrice,item_userFlaggedQuantity
      ,rewardsReceiptStatus,totalSpent,USERID
      ,row_number()over(partition by receiptID order by receiptID) as row_num
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
    JSON_DATA:rewardsReceiptItemList AS rewardsReceiptItemList,
    item.value:barcode::STRING AS item_barcode,
    item.value:description::STRING AS item_description,
    item.value:finalPrice::FLOAT AS item_finalPrice,
    item.value:itemPrice::FLOAT AS itemPrice,
    item.value:needsFetchReview::BOOLEAN AS item_needsFetchReview,
    item.value:partnerItemId::STRING AS item_partnerItemId,
    item.value:preventTargetGapPoints::BOOLEAN AS item_preventTargetGapPoints,
    item.value:quantityPurchased::NUMBER AS item_quantityPurchased,
    item.value:userFlaggedBarcode::STRING AS item_userFlaggedBarcode,
    item.value:userFlaggedNewItem::BOOLEAN AS item_userFlaggedNewItem,
    item.value:userFlaggedPrice::NUMBER AS item_userFlaggedPrice,
    item.value:userFlaggedQuantity::NUMBER AS item_userFlaggedQuantity,
    JSON_DATA:rewardsReceiptStatus::STRING AS rewardsReceiptStatus,
    JSON_DATA:totalSpent::FLOAT AS totalSpent,
    JSON_DATA:userId::VARCHAR AS userID

from FETCH_REWARDS.PUBLIC.RECEIPTS 
,LATERAL FLATTEN(input => RECEIPTS.JSON_DATA:rewardsReceiptItemList) AS item

);


SELECT * FROM FETCH_REWARDS.PUBLIC.RECEIPTS_FLATTEN where receiptid= '5ff1e1eb0a720f0523000575';
-- 
--   TROUBLE SHOOTING     
-- BY USING FLATTEN FUNCION IN SNOWFLAKE, its cross join and cause duplicated row => add row_number 

--  DATA VALIDATION 1: CHECK IF RECEIPTID records number 
select receiptID, count(*) 
from receipts_flatten
group by 1;

-- 60023e8f0a720f05f300008b   with 137 records , 5ff618e30a7214ada10005fa with 6 records

select * FROM RECEIPTS_FLATTEN where receiptID = '5ff618e30a7214ada10005fa';

--  DATA VALIDATION 2: CHECK IF RECEIPTID records number -- ======================================================================
select a.*, b.PURCHASEDITEMCOUNT
FROM FETCH_REWARDS.PUBLIC.USERS_FLATTEN a 
JOIN FETCH_REWARDS.PUBLIC.RECEIPTS_FLATTEN b
ON a.oid = b.userID;


```
