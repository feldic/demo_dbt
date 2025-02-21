{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date_utc', 'player_id', 'tournament_id', 'room_id'],
    partition_by={'field': 'date_utc', 'data_type': 'date'},
    cluster_by=['player_id']
) }}


-- fct_games

-- Calculate population take only the tournaments that had events in the last 2 hours to reduce population size
with population as (
  select tournament_id
  from `playperfect-451608.play_perfect_dataset.pp_events`
  where timestamp_utc >= timestamp('2024-02-07') - INTERVAL 2 HOUR
    and tournament_id is not null
  group by 1
),
room_data as
(
  select e.tournament_id,
    room_id,
    max(case when event_name = 'tournamentRoomClosed' then timestamp_utc end) room_close_time,
    max(case when event_name = 'tournamentRoomClosed' then players_capacity end) players_capacity,
    count(distinct player_id) actual_players_in_room
from `playperfect-451608.play_perfect_dataset.pp_events` e
  join population p
    on e.tournament_id = p.tournament_id
where 1=1
  -- and tournament_id = '633ac296e9bc5193f7ee9e1a'
  -- and event_name = 'tournamentRoomClosed'
group by 1,2
)
select events.date_utc,
  events.player_id,
  events.tournament_id,
  events.room_id,
  rd.room_close_time,
  rd.players_capacity,
  rd.actual_players_in_room,
  min(case when events.event_name = 'tournamentJoined' then timestamp_utc end) joined_time,
  max(case when events.event_name = 'tournamentFinished' then timestamp_utc end) submit_time,
  min(case when events.event_name = 'tournamentJoined' then balance_before end) balance_before,
  min(case when events.event_name = 'tournamentJoined' then balance_before end) + 
  max(case when events.event_name = 'tournamentRoomClosed' then reward end) balance_after_claim,
  min(case when events.event_name = 'tournamentJoined' then entry_fee end) entry_fee,
  max(case when events.event_name = 'tournamentRoomClosed' then score end) score,
  max(case when events.event_name = 'tournamentRoomClosed' then position end) position,
  max(case when events.event_name = 'tournamentRoomClosed' then reward end) reward,
  case when coalesce(max(case when events.event_name = 'tournamentRoomClosed' then reward end),0)>0 then true else false end did_claim_reward,
  case when coalesce(max(case when events.event_name = 'tournamentRoomClosed' then reward end),0)>0 then 
    max(case when events.event_name = 'tournamentRoomClosed' then timestamp_utc end) end claim_time,
  max(case when events.event_name = 'tournamentFinished' then play_duration end) play_duration
from `playperfect-451608.play_perfect_dataset.pp_events` events
  join population p
    on events.tournament_id = p.tournament_id
  left join room_data rd
    on events.tournament_id = rd.tournament_id
      and events.room_id = rd.room_id
where 1=1
  -- and events.tournament_id = '633ac296e9bc5193f7ee9e1a'
group by 1,2,3,4,5,6,7
