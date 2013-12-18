/*
 * Copyright (c) EMC Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: I.Putyatin
 * Email:  iputyatin@gopivotal.com
 * Date:   22 Nov 2013
 * Description: This module contains plpgsql functions that implement SQL jobs scheduling framework
 *
 */

-- Put new job in queue
create or replace function DBMS_JOBS.job_submit(p_job_code      varchar(3000),
                                                p_job_group     int,
                                                p_job_attempts  int) returns int as $BODY$
declare
    v_new_job_isn   int;
    v_function_name CHARACTER VARYING = 'DBMS_JOBS.job_submit()';
    v_step          CHARACTER VARYING = 'NA';
    v_group_exists  int;
begin
    v_step = 'Check group isn';
    select 1 into v_group_exists from DBMS_JOBS.job_group where group_isn = p_job_group;
    if not FOUND then
        RAISE EXCEPTION 'Invalid value in p_job_group. No group exists with group_isn=%', p_job_group;
    end if;
    v_step = 'Create record in job_queue';
    v_new_job_isn = nextval('DBMS_JOBS.job_seq');
    insert into DBMS_JOBS.job_queue(job_isn,
                                    job_status,
                                    job_attempt,
                                    job_attempt_limit,
                                    job_submit_dttm,
                                    job_group,
                                    job_code) 
                            values (v_new_job_isn,
                                    'NEW',
                                    1,
                                    p_job_attempts,
                                    current_timestamp,
                                    p_job_group,
                                    p_job_code);
    return v_new_job_isn;
    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            RAISE EXCEPTION '(%:%:%)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;

-- overload of job_submit with p_job_attempts=1
create or replace function DBMS_JOBS.job_submit(p_job_code  varchar(3000), p_job_group int) returns int as $BODY$
begin
    return DBMS_JOBS.job_submit(p_job_code, p_job_group, 1);
end;
$BODY$
language plpgsql;


-- Get new job for execution from queue and set its status to RUN
create or replace function DBMS_JOBS.job_next_for_execution(out po_job_isn         int,
                                                            out po_job_code        varchar(3000),
                                                            out po_job_attempt     smallint,
                                                            out po_job_attempt_limit smallint) returns record as $BODY$
declare
    v_session_id            integer;
    v_session_start_dttm    timestamp;
    v_function_name         CHARACTER VARYING = 'DBMS_JOBS.job_next_for_execution()';
    v_step                  CHARACTER VARYING = 'NA';
begin
    v_step = 'Get next job';
    select q.job_isn, q.job_code, q.job_attempt, q.job_attempt_limit
        into po_job_isn, po_job_code, po_job_attempt, po_job_attempt_limit
        from DBMS_JOBS.job_group g
            left join (
                --running jobs per group
                select q2.job_group, count(*) as jobs_running_count
                    from DBMS_JOBS.job_queue q2
                    where q2.job_status = 'RUN'
                    group by q2.job_group      
            ) g2
            on g.group_isn = g2.job_group 
            inner join DBMS_JOBS.job_queue q on g.group_isn = q.job_group
        where q.job_status='NEW' and coalesce(g2.jobs_running_count, 0) < g.max_jobs
        order by g.priority asc, q.job_submit_dttm asc;

    if not FOUND then
        --either no new jobs or no free execution slots in groups - nothing to do
        return;
    end if;

    v_step = 'Get session information';
    select sess_id, backend_start 
        into v_session_id, v_session_start_dttm
        from pg_stat_activity 
        where procpid = pg_backend_pid();

    v_step = 'Set job status to RUN';
    update DBMS_JOBS.job_queue set
        job_status              = 'RUN',
        job_session_id          = v_session_id,
        job_session_start_dttm  = v_session_start_dttm
    where job_isn = po_job_isn and job_status = 'NEW';

    v_step = 'Add log record';
    insert into DBMS_JOBS.job_log(job_isn, job_attempt, job_message)
                          values (po_job_isn, po_job_attempt, 'started');
    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            RAISE EXCEPTION '(%:%:%)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;

-- Set job status after execution
create or replace function DBMS_JOBS.job_set_status(p_job_isn     int,
                                                    p_job_attempt int,
                                                    p_job_status  varchar(5),
                                                    p_job_message varchar(300)) returns void as $BODY$
declare
    v_job DBMS_JOBS.job_queue%ROWTYPE;
    v_function_name CHARACTER VARYING = 'DBMS_JOBS.job_set_status()';
    v_step CHARACTER VARYING = 'NA';
begin
    v_step = 'Add log record';
    insert into DBMS_JOBS.job_log(job_isn, job_attempt, job_message) 
                           values(p_job_isn, p_job_attempt, p_job_message);

    -- update job status in queue
    if p_job_status in ('FLD', 'END') then
        -- with moving record to another partition
        v_step = 'Moving record to another partition';
        select *
            into v_job
            from DBMS_JOBS.job_queue
            where job_isn = p_job_isn and job_status = 'RUN';
        if not FOUND then
            raise exception 'Job (job_isn=%) is not in RUN status or does not exists.', p_job_isn;
        end if;
        v_job.job_status = p_job_status;
        delete from DBMS_JOBS.job_queue
            where job_isn = p_job_isn and job_status = 'RUN';
        insert into DBMS_JOBS.job_queue
            select v_job.*;
    elsif p_job_status = 'RERUN' then
        -- without changing partition
        v_step = 'Updating record to rerun the job';
        update DBMS_JOBS.job_queue 
            set job_status              = 'NEW',
                job_attempt             = job_attempt+1,
                job_session_id          = null,
                job_session_start_dttm  = null 
            where job_isn = p_job_isn and job_status = 'RUN'; 
        if not FOUND then
            raise exception 'Job (job_isn=%) is not in RUN status or does not exists.', p_job_isn;
        end if;
    else
        raise exception 'Incorrect status "%" for job (job_isn=%)', p_job_status, p_job_isn;
    end if;
    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            RAISE EXCEPTION '(%:%:%)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;


-- Find jobs that failed but have job_status RUN and corrects their status
create or replace function DBMS_JOBS.job_clear_failed() returns void as $BODY$
declare
    v_hanged_job    record;
    v_function_name CHARACTER VARYING = 'DBMS_JOBS.job_clear_failed()';
    v_step          CHARACTER VARYING = 'NA';
    v_pid           int;
    v_message       character varying;
begin
    v_step = 'Get hanged job list';
    -- criteria for determining hanged jobs:
    --  * no session with jobs's session id, or
    --  * session exists but it is idle for a long time
    for v_hanged_job in (
        select q.*, a.procpid
            from DBMS_JOBS.job_queue q
                left join pg_stat_activity a on q.job_session_id = a.sess_id
                          and q.job_session_start_dttm = a.backend_start
            where q.job_status = 'RUN'
                and (a.sess_id is null
                    or (current_timestamp - a.query_start >= interval '5 minutes'
                            and(a.current_query in ('<IDLE>', '<IDLE> in transaction'))
                        )
                    )
    ) loop
        -- if session is alive - kill it
        if v_hanged_job.procpid is not null then
            v_step = 'Kill hanged job';
            perform pg_terminate_backend(v_hanged_job.procpid);
            v_message = 'failed. Job session hanged in idle state';
        else
            v_message = 'failed. Job finished with unknown status';
        end if;
        
        -- attempts left for the job - rerun
        if v_hanged_job.job_attempt < v_hanged_job.job_attempt_limit then
            v_step = 'Rerun hanged job';
            update DBMS_JOBS.job_queue
                set job_status              = 'NEW',
                    job_attempt             = v_hanged_job.job_attempt+1,
                    job_session_id          = null,
                    job_session_start_dttm  = null
                where job_isn = v_hanged_job.job_isn and job_status = 'RUN';
            v_message = v_message + ', job restarted';
        else
            -- no attempts left - mark as failed
            v_step = 'Mark job as failed';
            delete from DBMS_JOBS.job_queue
                where job_isn = v_hanged_job.job_isn  and job_status = 'RUN';
            insert into DBMS_JOBS.job_queue(job_isn,
                                            job_status,
                                            job_attempt,
                                            job_attempt_limit,
                                            job_submit_dttm,
                                            job_session_id,
                                            job_session_start_dttm,
                                            job_group,
                                            job_code) 
                                    values (v_hanged_job.job_isn,
                                            'FLD',
                                            v_hanged_job.job_attempt,
                                            v_hanged_job.job_attempt_limit,
                                            v_hanged_job.job_submit_dttm,
                                            v_hanged_job.job_session_id,
                                            v_hanged_job.job_session_start_dttm,
                                            v_hanged_job.job_group,
                                            v_hanged_job.job_code);
        end if;
        v_step = 'Log result';
        insert into DBMS_JOBS.job_log ( job_isn,
                                        job_attempt,
                                        job_message) 
                               values ( v_hanged_job.job_isn,
                                        v_hanged_job.job_attempt,
                                        v_message);
    end loop;
    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            RAISE EXCEPTION '(%:%:%)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;
