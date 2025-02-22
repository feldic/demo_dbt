{{ config(
    materialized='table',
    full_refresh=true
) }}

with installs as (
select install_date,
  media_source,
  install_country,
  count(distinct player_id) install_players
from playperfect-451608.play_perfect_dataset.installs_attribution
    where install_date = '{{ var("run_date" , "2024-07-21") }}'
group by 1,2,3
)
select m.date_utc,
  m.media_source,
  m.country,
  m.spend,
  m.spend/i.install_players cpi
from playperfect-451608.play_perfect_dataset.marketing_spend m
  left join installs i
    on m.date_utc = i.install_date
      and m.media_source = i.media_source
      and m.country = i.install_country
where m.date_utc = '{{ var("run_date" , "2024-07-21") }}'