# Fetch-Rewards


<details>

<summary>First: Relational Data Model</summary>

#### Review Existing Unstructured Data and Diagram a New Structured Relational Data Model


```
   puts "Hello World"
```

</details>

<details>

<summary>Second: SQL Queries </summary>

#### Write queries that directly answer predetermined questions from a business stakeholder

##### Q1 What are the top 5 brands by receipts scanned for most recent month?

```sql
'''
CLARIFICATION: How to define top brand? 
               the most frequently purchased? or the most total spent or item qty?
                I assume the most frequently purchased item as top 
Note:
1. there are several null values in the receipt item table
1. based on the purchased qty to filter top 5 brands, only 2 record with item barcode in the recent month (2021-03) , and no brandname information 
'''    
    -- Q1-1 In the set query , I get the data in most recent month:2021-03 , only get 2 item_barcode B076FJ92M4 & B07BRRLSVC
        with cte as (
       SELECT
        b.item_barcode, to_char(DATESCANNED,'yyyy-mm') as DATESCANNED,
        sum(b.item_quantitypurchased) as purchase_qty
        from receipt a JOIN receipt_item b 
        ON a.receiptid = b.receiptid 
        group by 1,2
        order by DATESCANNED DESC
        )
            SELECT  cte.item_barcode,brand.brandname,brand.brandcode,DATESCANNED,purchase_qty
            FROM CTE
            left JOIN brand
            ON cte.item_barcode = brand.barcode 
         where DATESCANNED like '2021-03%'
        order by datescanned DESC;
```
![image](https://github.com/sakisakichen/Fetch-Rewards/assets/72574733/709f6152-3519-4912-9fb2-971f8ce048db)

##### Q2 How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
```sql
 -- Q2-1 from the previous query, the most recent month is 2021/03, which I need to find the data in 2021/02
  with cte as (
       SELECT b.item_barcode, b.item_description,to_char(DATESCANNED,'yyyy-mm') as DATESCANNED,  sum(b.item_quantitypurchased) as purchase_qty
        from receipt a JOIN receipt_item b 
        ON a.receiptid = b.receiptid 
        group by 1,2,3
        order by DATESCANNED DESC
        )
            SELECT   cte.item_barcode,cte.item_description, brand.brandname,brand.brandcode,DATESCANNED,purchase_qty
            FROM CTE
            left JOIN brand
            ON cte.item_barcode = brand.barcode 
         where DATESCANNED like '2021-02%' 
        order by datescanned DESC,purchase_qty DESC;
        
select item_barcode,item_description 
FROM receipt_item
 ;
 -- Q2-2 From the query result, I fount out these 2 item_barcode B076FJ92M4 & B07BRRLSVC also on the list, but no further information about brandname or brandcode,
 -- there also item code listed in 2021/02, purchased qty between 5 and 4, if steakholder needs to find the name of the brand, I suggest to find the data in 2021/1
```
![image](https://github.com/sakisakichen/Fetch-Rewards/assets/72574733/d7ec493d-6125-4a5c-962e-b7d207fa0908)
![image](https://github.com/sakisakichen/Fetch-Rewards/assets/72574733/28d707f1-3034-431e-9935-69e80ed6fae9)


##### Q3 When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
```sql
--  Status with Finshed is greater in Average spent 
-- Note: status with FINISHED AVG spent is 80.854305019
SELECT rewardsReceiptStatus, AVG(totalspent) as avg_spent
from receipt
where rewardsreceiptstatus= 'FINISHED'
group by 1;

-- Note: status with REJECTED AVG spent is 23.326056338
SELECT rewardsReceiptStatus, AVG(totalspent) as avg_spent
from receipt
where rewardsreceiptstatus= 'REJECTED'
group by 1;

```
##### Q4 When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

```sql
- Since question asks the number of item purchased, in my assumption, some items with null barcode will be counted 
--  REJECTED number is 164
SELECT  count(receipt_item_pk) as cnt
FROM receipt a JOIN receipt_item b ON a.receiptID = b.receiptID 
where rewardsreceiptstatus= 'REJECTED'
;
-- What is accepted status?  including finished, pending, submitted? only finished status is 5918 ,  if these status included, the number cnt is 5967
SELECT  count(receipt_item_pk) as cnt
FROM receipt a JOIN receipt_item b ON a.receiptID = b.receiptID 
where rewardsreceiptstatus= 'FINISHED' 
-- OR rewardsreceiptstatus= 'SUBMITTED' 
-- OR rewardsreceiptstatus= 'PENDING' 
;

```
##### Q5 Which brand has the most spend among users who were created within the past 6 months?

```sql
-- Stpe 1: find the most recent user account create date 
-- Note: The most recent user created account is in 2021/2, the past 6 month is 2020/9- 2021/2
SELECT oid, CREATEDDATE
from users_flatten
order by CREATEDDATE DESC
;
-- Step 2: Find the user account created in the period and what receipt item they purchase  
with cte as (
SELECT b.userid, b.RECEIPTID,totalspent,RECEIPT_ITEM_PK,item_barcode, item_finalprice
from users_flatten a 
JOIN receipt b 
JOIN receipt_item c 
ON b.receiptid = c.receiptID
ON a.oid = b.userid
WHERE LEFT(CREATEDDATE, 7) BETWEEN '2020-09' AND '2021-02'
AND item_finalprice is not null
order by item_finalprice DESC 
) 
-- Step3:find the item barcode and corresponding brandcode& brand name
SELECT item_barcode,item_finalprice,
brandID, brandCode,
FROM cte
LEFT JOIN BRAND 
ON brand.barcode = cte.item_barcode
order by item_finalprice DESC


```

##### Q6 Which brand has the most transactions among users who were created within the past 6 months?

```sql
-- Q6:Which brand has the most transactions among users who were created within the past 6 months?
-- what is transaction?  points awarded by brand to user?

-- Step 1: Find the user account created in the period and what receipt item they purchase  
with cte as (
SELECT b.userid, b.RECEIPTID,totalspent,RECEIPT_ITEM_PK,item_barcode, REWARDSPRODUCTPARTNERID
from users_flatten a 
JOIN receipt b 
JOIN receipt_item c 
ON b.receiptid = c.receiptID
ON a.oid = b.userid
WHERE LEFT(CREATEDDATE, 7) BETWEEN '2020-09' AND '2021-02'
AND REWARDSPRODUCTPARTNERID is not null
) 
-- Step2: find the brand name in brand table 
SELECT b.brandID, brandname, count(*) as cnt 
from brand b
JOIN CTE 
ON b.cpg_ID = cte.REWARDSPRODUCTPARTNERID
GROUP BY 1,2
order by cnt DESC 
```
</details>


<details>

<summary>Third: Identify Data Quality Issues </summary>

#### Evaluate Data Quality Issues in the Data Provided


```sql
```
</details>

<details>

<summary>Forth:Communicate with Stakeholders</summary>

#### Communicate with Stakeholders

preventTargetGapPoints   => 不知道什麼用途  null /true

</details>

