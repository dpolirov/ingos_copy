create or replace function replicais.replicate_chunk(p_iteration_isn int, p_table_name varchar) returns void as $BODY$
declare
    v_chk_table             int;        -- check if exists tables in replication_tasks_full
    v_replication_start     timestamp; 
    v_start_isn             int;
    v_end_isn               int;
    v_schema                varchar;    -- schema of table
    v_field_list_csv_ora    varchar;    -- comma-separated list of fields
    v_field_list_csv_gp     varchar;    -- comma-separated list of fields
    v_field_list_csv_p      varchar;    -- comma-separated list of fields with prefix 'p.'
    v_skiphist              varchar;
    v_connect_id            int;
    v_pkeys                 varchar[];
    v_join_condition        varchar := '';
    v_len                   int;
    v_hist_key              varchar;
    v_blob_fields           varchar[];
    v_renamed_fields        varchar[];
begin
    select id into v_connect_id
        from replicais.replication_connection_id;
    
    -- Timestamp when the replication started
    select shared_system.current_timestamp_py() into v_replication_start;
    
    -- Check whether the full replication process for this table is running or scheduled
    select count(*)
        into v_chk_table
        from replicais.replication_tasks_full
        where replication_status in ('NEW' , 'RUN')
            and replication_table = p_table_name;
        
    if v_chk_table > 0 then 
        raise exception 'Full replication for table % is in process. Aboring incremental replication', p_table_name;
    end if;

    -- Get table schema and field list
    select replication_table_schema, skiphist, blob_fields, renamed_fields
        into v_schema, v_skiphist, v_blob_fields, v_renamed_fields
        from replicais.replication_tables
        where replication_table_name = p_table_name;
        
    -- Generate field lists
    select  array_to_string(array_agg(
                        shared_system.field_rename(v_renamed_fields, q.column_name)
                    ), ','),
            array_to_string(array_agg(
                        q.column_name
                    ), ','),
            array_to_string(array_agg(
                    case 
                        when position('|' || upper(q.column_name) || '|' in upper('|' || array_to_string(v_blob_fields,'|') || '|')) > 0 then 'EMPTY_CLOB() ' || q.column_name
                        else 'p.' || q.column_name
                    end
                ),
                ','
            )
        into v_field_list_csv_ora,
             v_field_list_csv_gp,
             v_field_list_csv_p
        from (
            select case
                       when c.column_name like '%#%' then '"' || c.column_name || '"'
                       else c.column_name
                   end as column_name
                from information_schema.columns as c
                where c.table_schema || '.' || c.table_name = lower(v_schema || '.' || p_table_name)
                order by ordinal_position
        ) as q;
    
    -- Get replication borders    
    select replication_start_isn, replication_end_isn
        into v_start_isn, v_end_isn
        from replicais.replication_tasks_incr
        where replication_iteration = p_iteration_isn;
        
    -- Truncate temp increment tables in Oracle (main and hist)
    perform shared_system.execute_oracle(v_connect_id, 'truncate table gp_user.temp_'||p_table_name);
    perform shared_system.execute_oracle(v_connect_id, 'truncate table gp_user.temp_hist_'||p_table_name);
    
    -- Get primary keys for table;
    select primary_keys into v_pkeys 
        from replicais.replication_tables
        where replication_table_name = p_table_name;
    
    -- Construct condition for join with ais.histunloaded
    v_len = array_upper(v_pkeys,1);
    v_hist_key = '';
    if v_len >= 1 then v_join_condition := v_join_condition||     'p.'||v_pkeys[1]||'=h.recisn';  v_hist_key = v_hist_key || 'recisn';  end if;
    if v_len >= 2 then v_join_condition := v_join_condition||' and p.'||v_pkeys[2]||'=h.agrisn';  v_hist_key = v_hist_key || ',agrisn'; end if;
    if v_len >= 3 then v_join_condition := v_join_condition||' and p.'||v_pkeys[3]||'=h.isn3';    v_hist_key = v_hist_key || ',isn3';   end if;
    if v_len >= 4 then v_join_condition := v_join_condition||' and p.'||v_pkeys[4]||'=h.histisn'; v_hist_key = v_hist_key || ',histisn'; end if;

    -- Get replicated chunk of data for main table
    perform shared_system.execute_oracle(v_connect_id,
        'insert into gp_user.temp_' || p_table_name || ' (' || v_field_list_csv_ora || ')
            select ' || v_field_list_csv_p || '
                from ' || v_schema || '.' || p_table_name || ' p
                    inner join (
                        select distinct ' || v_hist_key || '
                            from gp_user.histunloaded
                            where unloadisn between ' || v_start_isn::varchar || ' and ' || v_end_isn::varchar || '
                    ) h on ('||v_join_condition||')'
                );

    -- Get replicated chunk of data for hist table if needed
    if v_skiphist = 'N' then
        perform shared_system.execute_oracle(v_connect_id,
            'insert into gp_user.temp_hist_' || p_table_name || ' (' || v_field_list_csv_ora || ', histisn)
                select ' || v_field_list_csv_p || ', p.histisn 
                    from hist.' || p_table_name || ' p
                        inner join gp_user.histunloaded h on ('||v_join_condition||' and p.histisn = h.histisn)
                    where h.unloadisn between ' || v_start_isn::varchar || ' and ' || v_end_isn::varchar);
    end if;
      
    --Load into main table 
    execute 'delete from ' || v_schema || '.' || p_table_name || ' as p
                using hist.temp_histlog as h
                where h.tablename = ''' || p_table_name || '''
                    and h.unloadisn between ' || v_start_isn::varchar || ' and ' || v_end_isn::varchar || ' and '||
                    v_join_condition;
    execute 'insert into ' || v_schema || '.' || p_table_name || ' (' || v_field_list_csv_gp || ')          
                select ' || v_field_list_csv_gp || '
                    from replicais.ext_' || p_table_name;
                    
     -- Load into hist table 
    if v_skiphist = 'N' then
        execute 'truncate table replicais_incr.hist_' || p_table_name;
        execute 'insert into replicais_incr.hist_' || p_table_name ||' (' || v_field_list_csv_gp || ', histisn)
                    select ' || v_field_list_csv_gp || ', histisn
                        from replicais.ext_hist_' || p_table_name;              

        execute 'delete from hist.' || p_table_name || ' as p
                    using (select h.*
                            from hist.temp_histlog as h
                                 inner join replicais_incr.hist_' || p_table_name || ' as p
                                 on ' || v_join_condition || ' and p.histisn = h.isn
                            where h.unloadisn between ' || v_start_isn::varchar || ' and ' || v_end_isn::varchar || ') as h
                    where ' || v_join_condition || ' and p.histisn = h.isn';              
        
        execute 'insert into hist.' || p_table_name || ' (' || v_field_list_csv_gp || ', histisn)
                    select ' || v_field_list_csv_gp || ', histisn 
                        from replicais_incr.hist_' || p_table_name;
    end if;                    
    
    -- Cleanup replicated chunk of data    
    perform replicais.replication_cleanup(v_start_isn, v_end_isn, array[p_table_name]);
end;
$BODY$
language plpgsql
volatile;