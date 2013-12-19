-- Function: replicais.replication_iteration(integer)

-- DROP FUNCTION replicais.replication_iteration(integer);

CREATE OR REPLACE FUNCTION replicais.replication_iteration(p_nworkers integer) RETURNS void AS
$BODY$
declare
    v_max_unloadisn   int;
    v_flag            int;
    v_num_rows        int;
    v_new_tables      varchar[]; 
    v_maxu            int;  
    v_minu            int;
    v_arr             varchar;   
    v_qty_rows        int;
    v_connect_id      int;  
begin
    select id into v_connect_id
        from replicais.replication_connection_id;
        
    -- Read flagslow bit responsible for replication to Greenplum
    select flag 
		into v_flag
		from replicais.replication_flag;
    
    select coalesce(max(replication_end_isn), 0) 
		into v_max_unloadisn
		from replicais.replication_tasks_incr;
    
    -- Create external table to unload new part of HISTLOG
    perform os.fn_create_ext_table(
		'replicais.ext_histlog', 
        ARRAY['isn                            NUMERIC',
              'node                           NUMERIC(38,0)',
              'tablename                      VARCHAR(32)',
              'recisn                         NUMERIC',
              'agrisn                         NUMERIC',
              'isn3                           NUMERIC',
              'sessionid                      NUMERIC(38,0)',
              'transid                        VARCHAR(30)',
              'unloadisn                      NUMERIC',
              'operation                      VARCHAR(1)'],
		v_connect_id, 
        'select histisn,
                node,
                tablename,
                recisn,
                agrisn,
                isn3,
                sessionid,
                transid,
                unloadisn,
                operation
			from gp_user.histunloaded where unloadisn > '||v_max_unloadisn||'
			and bitand(flagslow,'||v_flag||') <> 0'
    );
         
    insert into hist.temp_histlog ( isn,
                                    node,
                                    tablename,
                                    recisn,
                                    agrisn,
                                    isn3,
                                    sessionid,
                                    transid,
                                    status,
                                    operation,
                                    updated,
                                    updatedby,
                                    unloadisn )
        select isn,
               node,
               tablename,
               recisn,
               agrisn,
               isn3,
               sessionid,
               transid,
               null,
               operation,
               current_timestamp,
               null,
               unloadisn
        from replicais.ext_histlog;
    
    --Get list of tables from temp_histlog, replication_cleanup() and insert new tables into replication_new_tables
    select array_agg(tablename), max(max_unloadisn), min(min_unloadisn)
        into v_new_tables, v_maxu, v_minu
        from (
            select tablename, max(unloadisn) as max_unloadisn, min(unloadisn) as min_unloadisn
                from hist.temp_histlog h
                    left join replicais.replication_tables r
                    on h.tablename = r.replication_table_name and r.replication_active = 1
                where r.replication_table_name is null
                group by tablename
            ) as t;
        
    perform replicais.replication_cleanup(v_minu, v_maxu, v_new_tables);
    
    delete from hist.temp_histlog
            where array[tablename] <@ v_new_tables and unloadisn > v_max_unloadisn;
    
    insert into replicais.replication_new_tables(replication_table_name)
        select t
            from unnest(v_new_tables) as t 
                left join replicais.replication_new_tables as r
                on t = r.replication_table_name
            where r.replication_table_name is null;        
    
    update replicais.replication_new_tables
        set updated_dttm = current_timestamp
        where array[replication_table_name] <@ v_new_tables;
     
    --Schedule replication tasks for each of the tables in histlog increment
    insert into replicais.replication_tasks_incr(
                            replication_iteration, 
                            replication_table, 
                            replication_table_rows,
                            replication_start_isn, 
                            replication_end_isn, 
                            replication_worker, 
                            replication_status)
    select  nextval('replicais.replication_iteration_seq'), 
            tablename, 
            recnum, 
            minunloadisn, 
            maxunloadisn, 
            mod((row_number() over (order by recnum)),p_nworkers),
            'NEW'
        from (
            select  tablename,
                    max(unloadisn) as maxunloadisn,
                    min(unloadisn) as minunloadisn,
                    count(*) as recnum
                from hist.temp_histlog
                where unloadisn > v_max_unloadisn
                group by tablename
            ) as q; 
    GET DIAGNOSTICS v_num_rows = ROW_COUNT;
    RAISE NOTICE 'Number of new rows inserted: %', v_num_rows;
end;
$BODY$
LANGUAGE plpgsql
VOLATILE;
  
--select replicais.replication_iteration(1);