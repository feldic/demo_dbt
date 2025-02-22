{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date_utc', 'media_source','country'],
    partition_by={'field': 'date_utc', 'data_type': 'date'},
    cluster_by=['media_source']
) }}


select date_utc,
  media_source,
  country,
  spend,
  cpi
from {{ ref('stg_marketing_cpi') }}