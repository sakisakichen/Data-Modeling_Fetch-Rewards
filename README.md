# Fetch-Rewards


<details>

<summary>First: Relational Data Model</summary>

#### Review Existing Unstructured Data and Diagram a New Structured Relational Data Model

You can add text within a collapsed section. 

You can add an image or a code block, too.

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

</details>
