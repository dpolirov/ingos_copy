create or replace function replicais.replicate_full_run(p_task_isn int) returns void as $$
declare
    v_replication_start    timestamp;
    v_table_details        record;
    v_schema               varchar;   -- table schema
    v_table                varchar;   -- table name
    v_field_list           varchar;   -- list of table fields
    v_connect_id           int; 
    v_skip_hist            varchar;
    v_renamed_fields       varchar[];
    v_blob_fields          varchar[];
begin
    select id into v_connect_id
        from replicais.replication_connection_id;
    select shared_system.current_timestamp_py() into v_replication_start;
    
    -- Get schema and fields for table in iteration
    select *
        into v_table_details
        from replicais.replication_tasks_full
        where replication_task_isn = p_task_isn;
        
    select   replication_table_schema, replication_table_name, SkipHist,    renamed_fields,   blob_fields
        into v_schema,                 v_table,                v_skip_hist, v_renamed_fields, v_blob_fields
        from replicais.replication_tables
        where replication_table_name = v_table_details.replication_table;
    
    raise notice 'here, %, %', v_renamed_fields, v_blob_fields;
    -- Generate field list for main table
    select  array_to_string(array_agg(
                    case 
                        when position('|' || upper(q.column_name) || '|' in upper('|' || array_to_string(v_blob_fields,'|') || '|')) > 0 then 'EMPTY_CLOB() ' || shared_system.field_rename(v_renamed_fields, q.column_name)
                        else shared_system.field_rename(v_renamed_fields, q.column_name)
                    end
                ),
                ','
            )
        into v_field_list
        from (
            select case
                       when c.column_name like '%#%' then '"' || c.column_name || '"'
                       else c.column_name
                   end as column_name
                from information_schema.columns as c
                where c.table_schema || '.' || c.table_name = lower(v_schema || '.' || v_table)
                order by ordinal_position
        ) as q;
    
    raise notice 'here2';
    -- Create external table for main table
    -- Truncate and insert into main table    
    perform shared_system.load_from_ora(v_schema || '.' || v_table_details.replication_table,
                                        'replicais.ext_full_'||v_table_details.replication_table,
                                        v_connect_id,
                                        'select '||v_field_list||' from '||v_schema || '.' || v_table_details.replication_table);
    if v_skip_hist = 'N' then
        -- Generate field list for hist table
        select  array_to_string(array_agg(
                    case 
                        when position('|' || upper(q.column_name) || '|' in upper('|' || array_to_string(v_blob_fields,'|') || '|')) > 0 then 'EMPTY_CLOB() ' || shared_system.field_rename(v_renamed_fields, q.column_name)
                        else shared_system.field_rename(v_renamed_fields, q.column_name)
                    end
                ),
                ','
            )
        into v_field_list
        from (
            select case
                       when c.column_name like '%#%' then '"' || c.column_name || '"'
                       else c.column_name
                   end as column_name
                from information_schema.columns as c
                where 'hist.' || c.table_name = lower(v_schema || '.' || v_table)
                order by ordinal_position
        ) as q;
        
        -- Truncate and insert into hist table
        -- Create external table for hist table
        perform shared_system.load_from_ora('hist.' || v_table_details.replication_table,
                                            'replicais.ext_full_hist_'||v_table_details.replication_table,
                                            v_connect_id,
                                            'select '||v_field_list||' from hist.' || v_table_details.replication_table);
    end if;
    
    -- Update replication_tasks_full
    execute 'update replicais.replication_tasks_full
                set replication_status = ''DONE'', started_dttm = '''||v_replication_start||''',
                    completed_dttm = shared_system.current_timestamp_py()
                    where replication_task_isn = '||p_task_isn;
end;
$$language plpgsql
volatile;