create or replace function replicais.replicate_full_clear_failed() returns void as $BODY$
declare
    v_tasks_list    varchar[];
    v_table_list    varchar[];
begin
    select array_agg(r.replication_task_isn), array_agg(r.replication_table)
        into v_tasks_list, v_table_list
        from (
                select replication_task_isn, replication_table, replication_job_isn
                    from replicais.replication_tasks_full
                    where replication_job_isn is not null
                        and replication_status = 'RUN'
            ) as r 
            inner join (
                select job_isn
                    from dbms_jobs.job_queue
                    where job_status = 'FLD'
            ) as j
            on r.replication_job_isn = j.job_isn;
    if array_upper(v_tasks_list, 1) > 0 then
        update replicais.replication_tasks_full
            set replication_status = 'FLD'
            where array[replication_task_isn::varchar] <@ v_tasks_list;
        perform shared_system.send_email ('Full replication failed', 'Failed full replication for tables ' || array_to_string(v_table_list, ', ') || '. Related ISNs are: , '||array_to_string(v_tasks_list, ', '));
    end if;
end;
$BODY$
language plpgsql
volatile;