{{ config(materialized='table') }}
SELECT *
FROM devcatalog1.default.test_tb1
WHERE amount > 2000
  
