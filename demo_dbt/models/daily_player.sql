{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date_utc', 'player_id'],
    partition_by={'field': 'date_utc', 'data_type': 'date'},
    cluster_by=['player_id']
) }}


--agg
with score_position as
(
  --score + position
  select events.date_utc,
    events.player_id,
    max(score) max_score,
    avg(score) avg_score,
    max(position) max_position,
    avg(position) avg_position
  from `playperfect-451608.play_perfect_dataset.pp_events` events
  where 1=1
    and date_utc = '2024-02-07'
      -- and player_id = '66716170654e6a2692d762d3'
      and event_name = 'tournamentRoomClosed'
  group by 1,2
),
balance as (
  select distinct events.date_utc,
  events.player_id,
  first_value(balance_before) over (partition by date_utc,player_id order by timestamp_utc) balance_day_start,
  first_value(balance_before) over (partition by date_utc,player_id order by timestamp_utc desc) balance_day_end
from `playperfect-451608.play_perfect_dataset.pp_events` events
where 1=1
  and date_utc = '2024-02-07'
   -- and player_id = '66716170654e6a2692d762d3'
),
agg as (
select date_utc,
  player_id,
  count(distinct room_id) matches_played,
  sum(play_duration) total_matches_duration,
  sum(case when event_name = 'tournamentRoomClosed' and coalesce(reward,0)>0 then reward end) matches_won_reward,
  count(distinct case when event_name = 'tournamentRoomClosed' and coalesce(reward,0)>0 then room_id end) matches_claimed,
  sum(case when event_name = 'purchase' then price_usd end) revenue,
  sum(case when event_name = 'purchase' then coins_claimed end) coins_source_purchases
from `playperfect-451608.play_perfect_dataset.pp_events`
where 1=1
  and date_utc = '2024-02-07'
    --and player_id = '66716170654e6a2692d762d3'
group by 1,2
)
select a.date_utc,
  a.player_id,
  b.balance_day_start,
  b.balance_day_end,
  a.matches_played,
  a.total_matches_duration,
  a.matches_won_reward,
  a.matches_claimed,
  a.revenue,
  a.coins_source_purchases,
  sp.max_score,
  sp.avg_score,
  sp.max_position,
  sp.avg_position,
  null coins_sink_tournaments,
  null coins_source_tournaments,
  null max_reward_won_streak,
  null max_losing_streak
from agg a
  left join score_position sp
    on a.date_utc = sp.date_utc
      and a.player_id = sp.player_id
  left join balance b
    on a.date_utc = b.date_utc
      and a.player_id = b.player_id