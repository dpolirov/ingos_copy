create or replace function replicais.replicate_full_init(p_table_list varchar[]) returns void as $$
declare
    v_missing_tables    varchar[];
    v_missing_qty       int;
    v_table_from_list   varchar;
    v_rep_full_seq      int;
    v_job_code          varchar;
    v_job_isn           int;
    v_full_rep_tables    varchar[];
begin
    --Check if not exists tables from p_table_list in GreenPlum
    select array_agg(t)
        into v_missing_tables
        from unnest(p_table_list) as t
            left join replicais.replication_tables as r
            on r.replication_table_name = t and r.replication_active = 1
        where r.replication_table_name is null;

    --Raise exception if tables from list is not exists in GP        
    select count(*) into v_missing_qty 
        from (select unnest(v_missing_tables)) as t;
    if v_missing_qty > 0 then
        raise exception 'Error! You should first run replication initialization for following tables: %', v_missing_tables; 
    end if;      
    
    select array_agg(replication_table)
        into v_full_rep_tables
        from replicais.replication_tasks_full
        where array[replication_table] <@ p_table_list and replication_status in ('NEW','RUN');
        
    if v_full_rep_tables is not null then
        raise exception 'In replication tasks exist tables in status ''NEW'' or ''RUN'': %', array_to_string(v_full_rep_tables, ', ');
    end if;    
    
    --Initialize full replication for tables in p_table_list
    for v_table_from_list in (
        select replication_table_name
            from replicais.replication_tables
            where array[replication_table_name] <@ p_table_list
            order by FullReloadPriority
    )
    loop
        select nextval('replicais.replication_full_seq') into v_rep_full_seq;
            
        v_job_code := 'select replicais.replicate_full_run('||v_rep_full_seq||');';
        select dbms_jobs.job_submit(v_job_code,2) into v_job_isn;
            
        insert into replicais.replication_tasks_full(replication_task_isn, replication_table, replication_job_isn, replication_status)
            values(v_rep_full_seq, v_table_from_list, v_job_isn, 'RUN');
    end loop;        
end;
$$language plpgsql
volatile;