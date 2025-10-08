select * from {{ref('stg_taxis_yellow')}}
UNION ALL
select * from {{ref('stg_taxis_green')}}