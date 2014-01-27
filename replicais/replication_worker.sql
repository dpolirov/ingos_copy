create or replace function replicais.replication_worker(p_worker_isn int) returns void as $BODY$
declare
    v_rep_tasks_incr        record;
    v_execute_code          varchar;
    v_call_result           text;
    v_replication_start     timestamp;
    v_skiphistlog           varchar;
begin     
    --create temp table selecting all the needed information from replicais.replication_tasks_incr
    drop table if exists tmp_rep_incr;
    create temp table tmp_rep_incr as (
        select  i.replication_iteration,
                i.replication_table,
                i.replication_start_isn,
                i.replication_end_isn
            from replicais.replication_tasks_incr as i
                left join (
                    select replication_table
                        from replicais.replication_tasks_full
                        where replication_status in ('NEW' , 'RUN')
                        group by replication_table
                ) as f
                on i.replication_table = f.replication_table
            where i.replication_worker = p_worker_isn and
                  i.replication_status = 'NEW' and
                  f.replication_table is null
    );
    
    for v_rep_tasks_incr in (select replication_iteration,
                                    replication_table,
                                    replication_start_isn,
                                    replication_end_isn
                                from tmp_rep_incr)     
    loop
        -- Replicate chunk of data
        v_execute_code := 'select REPLICAIS.REPLICATE_CHUNK('||v_rep_tasks_incr.REPLICATION_ITERATION||',
                                                          '''||v_rep_tasks_incr.REPLICATION_TABLE||''');'; 
        select shared_system.current_timestamp_py() into v_replication_start;                                                  
        select shared_system.autonomous_transaction (v_execute_code) into v_call_result;
        
        -- Analyze replication result
        if v_call_result = '' then
            -- If replication was successful - move data to hist.histlog and mark replication iteration
            -- as completed
            select skiphistlog
                into v_skiphistlog
                from replicais.replication_tables
                where replication_table_name = v_rep_tasks_incr.replication_table;
            if v_skiphistlog <> 'Y' then
                select shared_system.autonomous_transaction ('
                    -- Move data from temp_histlog into histlog
                    insert into hist.histlog (   isn, node, tablename,
                                                recisn, agrisn, isn3,
                                                sessionid, transid, status,
                                                operation, updated, updatedby,
                                                unloadisn )
                        select  isn, node, tablename,
                                recisn, agrisn, isn3,
                                sessionid, transid, status,
                                operation, updated, updatedby,
                                unloadisn
                            from hist.temp_histlog
                            where tablename = '''||v_rep_tasks_incr.replication_table||'''
                                and unloadisn between '||v_rep_tasks_incr.replication_start_isn||' and '||v_rep_tasks_incr.replication_end_isn||';
                        ') into v_call_result;        
                if v_call_result <> '' then
                    -- Failed hist.histlog or replication_tasks_incr transaction
                    perform replicais.log_error (v_rep_tasks_incr.replication_iteration,
                                                 v_rep_tasks_incr.replication_table,
                                                 'Failed to update hist.histlog: ' || v_call_result);                            
                end if;
            end if;
            select shared_system.autonomous_transaction ('
                delete from hist.temp_histlog h
                    where tablename = '''||v_rep_tasks_incr.replication_table||'''
                        and unloadisn between '||v_rep_tasks_incr.replication_start_isn||' and '||v_rep_tasks_incr.replication_end_isn||';           
                            
                -- Update replication_tasks_incr
                update replicais.replication_tasks_incr 
                    set started_dttm   = '''||v_replication_start||''',
                        completed_dttm = shared_system.current_timestamp_py(),
                        replication_status = ''DONE''
                    where replication_iteration = '||v_rep_tasks_incr.replication_iteration||';
                    ') into v_call_result;
            if v_call_result = '' then
                -- If replication succeeded - cleanup data in Oracle
                select replicais.replication_cleanup(v_rep_tasks_incr.replication_start_isn,
                                                     v_rep_tasks_incr.replication_end_isn, 
                                                     array[v_rep_tasks_incr.replication_table]) into v_call_result;
                if v_call_result <> 'success' then
                    -- If cleanup failed - rollback table replication and exit the worker.
                    -- This error means that we have problems in Oracle that should be fixed                    
                    select shared_system.autonomous_transaction (
                        'update replicais.replication_tasks_incr as i
                            set i.replication_status = ''NEW'' where 
                            '||v_rep_tasks_incr.replication_iteration||' = i.replication_iteration'
                        ) into v_call_result;
                    raise exception 'Cleanup failed. Problems with data source: %', v_call_result;
                end if;
            else
                -- Failed hist.histlog or replication_tasks_incr transaction
                perform replicais.log_error (v_rep_tasks_incr.replication_iteration,
                                             v_rep_tasks_incr.replication_table,
                                             'Failed to update gp_user.histlog: ' || v_call_result);
            end if;
        else
            -- Chunk replication failed
            perform replicais.log_error(v_rep_tasks_incr.replication_iteration,
                                        v_rep_tasks_incr.replication_table,
                                        'Failed replication of new data chunk: ' || v_call_result);
        end if;
    end loop;
    /*
    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            -- Chunk replication failed
            select replicais.log_error(v_rep_tasks_incr.replication_iteration,
                                        v_rep_tasks_incr.replication_table,
                                        'Failed replication of new data chunk: ' || v_call_result) into v_log_result;
            RAISE EXCEPTION '(%:%:%)', v_function_name, v_step, sqlerrm;
        END;
    */
end;
$BODY$
language plpgsql
volatile;