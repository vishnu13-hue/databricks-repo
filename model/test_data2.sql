{{ config(materialized="table") }}
 select * from devcatalog1.default.sales_table
