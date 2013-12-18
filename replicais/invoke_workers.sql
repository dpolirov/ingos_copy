create or replace function replicais.invoke_workers(p_nworkers int) returns void as $BODY$
declare
    v_nJob      int;
    v_chk_queue int;
    v_job_code  varchar;
    v_result    int;
begin
    for v_nJob in 0..(p_nworkers-1) loop
        select count(*) 
            into v_chk_queue
            from dbms_jobs.job_queue
            where job_session_id = v_nJob and job_status in ('NEW', 'RUN');
            
        continue when v_chk_queue > 0;
        
        v_job_code := 'begin;
                       lock table REPLICAIS.MX_REPLICATION_WORKER_'||v_nJob||' nowait; 
                       select REPLICAIS.REPLICATION_WORKER('||v_nJob||');
                       commit;';
                            
        select dbms_jobs.job_submit(v_job_code, 1) into v_result;
        if v_result is null then
            raise exception 'Cannot submit job for execution';
        end if;
    end loop;    
end;
$BODY$
language plpgsql
volatile;