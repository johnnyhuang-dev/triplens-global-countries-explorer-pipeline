{{ config(materialized='view') }}

select * from {{source('rest_countries', 'STG_COUNTRIES_RAW')}}