{{ config(materialized='table') }}
with new_tb2(
    select * from devcatalog1.default.sales_table where Quantity >1
)
select * from new_tb2