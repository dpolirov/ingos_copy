create or replace view replicais.replication_tasks_incr_monitor as
    select
        case when (rn = 1) then 'Completed within last  1 minute'
             when (rn = 2) then 'Completed within last  5 minutes'
             when (rn = 3) then 'Completed within last 15 minutes'
             when (rn = 4) then 'Completed within last 30 minutes'
             when (rn = 5) then 'More to load'
             when (rn = 6) then 'Failed within last  5 minutes'
             when (rn = 7) then 'Failed within last 30 minutes'
        end as message,
        case when (rn = 1) then compl_1_min
             when (rn = 2) then compl_5_min
             when (rn = 3) then compl_15_min
             when (rn = 4) then compl_30_min
             when (rn = 5) then more_to_go
             when (rn = 6) then fld_5_min
             when (rn = 7) then fld_30_min
        end as number
    from (    
        select
            sum(case when (replication_status='DONE' and current_timestamp - completed_dttm < interval '1 minute') then 1 else 0 end) as compl_1_min,
            sum(case when (replication_status='DONE' and current_timestamp - completed_dttm < interval '5 minutes') then 1 else 0 end) as compl_5_min,
            sum(case when (replication_status='DONE' and current_timestamp - completed_dttm < interval '15 minutes') then 1 else 0 end) as compl_15_min,
            sum(case when (replication_status='DONE' and current_timestamp - completed_dttm < interval '30 minutes') then 1 else 0 end) as compl_30_min,
            sum(case when (replication_status in ('NEW', 'RUN')) then 1 else 0 end) as more_to_go,
            sum(case when (replication_status='FLD' and current_timestamp - completed_dttm < interval '5 minutes') then 1 else 0 end) as fld_5_min,
            sum(case when (replication_status='FLD' and current_timestamp - completed_dttm < interval '30 minutes') then 1 else 0 end) as fld_30_min
        from replicais.replication_tasks_incr
    ) as q, generate_series(1,7) as rn;

/*
alias replistat="psql -c 'select * from replicais.replication_tasks_incr_monitor;'"
*/