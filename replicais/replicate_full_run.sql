create or replace function replicais.replicate_full_run(p_task_isn int) returns void as $$
declare
    v_replication_start    timestamp;
    v_table_details        record;
    v_schema               varchar;   -- table schema
    v_field_list           varchar[]; -- list of table fields (array) for main
    v_field_list_csv       varchar;   -- comma-separated list of fields for main
    v_connect_id           int; 
begin
    select id into v_connect_id
        from replicais.replication_connection_id;
    select shared_system.current_timestamp_py() into v_replication_start;
    
    -- Get schema and fields for table in iteration
    select *
        into v_table_details
        from replicais.replication_tasks_full
        where replication_task_isn = p_task_isn;
        
    select replication_table_schema 
        into v_schema
        from replicais.replication_tables
        where replication_table_name = v_table_details.replication_table;
    
    select shared_system.get_field_list(v_schema || '.' || v_table_details.replication_table)
        into v_field_list;
    select shared_system.put_to_str(v_field_list)
        into v_field_list_csv;   
        
    -- Create external table for main table
    -- Truncate and insert into main table    
    perform shared_system.load_from_ora(v_schema || '.' || v_table_details.replication_table,
                                        'replicais.ext_full_'||v_table_details.replication_table,
                                        v_connect_id,
                                        'select '||v_field_list_csv||' from '||v_schema || '.' || v_table_details.replication_table);
    -- Truncate and insert into hist table
    -- Create external table for hist table    
    perform shared_system.load_from_ora('hist.' || v_table_details.replication_table,
                                        'replicais.ext_full_hist_'||v_table_details.replication_table,
                                        v_connect_id,
                                        'select '||v_field_list_csv||', histisn from hist.' || v_table_details.replication_table);                                              
    -- Update replication_tasks_full
    execute 'update replicais.replication_tasks_full
                set replication_status = ''DONE'', started_dttm = '''||v_replication_start||''',
                    completed_dttm = shared_system.current_timestamp_py()
                    where replication_task_isn = '||p_task_isn;
end;
$$language plpgsql
volatile;