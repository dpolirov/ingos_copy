create or replace function replicais.replication_add_table (
            p_schema_name    varchar,
            p_table_name     varchar,
            p_SendBlob       varchar(1),
            p_SkipHist       varchar(1),
            p_SkipHistlog    varchar(1),
            p_pkeys          varchar[],
            p_blob_fields    varchar[],
            p_renamed_fields varchar[]
        ) returns varchar as $BODY$
declare
    v_schema_name   varchar;
    v_table_name    varchar;
    v_main_table    int;
    v_hist_table    int;
    v_rep_table     int;
    v_full_name     varchar;
    v_ret_code      varchar;
    v_connect_id    int;
    v_select_list   varchar;
    v_cnt           int;
    v_tmp_hist      int;
begin
    select id into v_connect_id
        from replicais.replication_connection_id;
    v_ret_code      = 'success';
    v_schema_name   = upper(p_schema_name);
    v_table_name    = upper(p_table_name);

    -- Checking whether exist tables in GreenPlum 
    select count(*)
        into v_main_table
        from information_schema.tables where upper(table_schema) = v_schema_name and upper(table_name) = v_table_name;
    select count(*)
        into v_hist_table
        from information_schema.tables where table_schema = 'hist' and upper(table_name) = v_table_name;
    
    v_full_name := v_schema_name ||'.'|| v_table_name;
    if v_main_table < 1 then
        raise exception 'Table with name % does not exist in GreenPlum!', v_full_name;
    end if;
    
    if v_hist_table < 1 and p_SkipHist = 'N' then
        raise exception 'Table with name hist.% does not exist in GreenPlum!', v_table_name;
    end if;
    
    -- Validate parameters
    if p_SendBlob not in ('Y', 'N') then
        raise exception 'Parameter p_SendBlob can be Y or N only!',p_SendBlob;
    end if;    
    if p_SkipHist not in ('Y', 'N') then
        raise exception 'Parameter p_SkipHist can be Y or N only!',p_SkipHist;
    end if;
    if p_SkipHistlog not in ('Y', 'N') then
        raise exception 'Parameter p_SkipHistlog can be Y or N only!',p_SkipHistlog;
    end if;
    
    -- Checking whether exists table in replicais.replication_tables  
    select count(*) 
        into v_rep_table
        from replicais.replication_tables
        where upper(replication_table_name) = v_table_name;
        
    if v_rep_table > 0 then    
        raise exception 'Table % already exists in replicais.replication_tables!', v_full_name;
    end if;

    -- Create table for HIST table data chunks loaded in process of replication
    if p_SkipHist = 'N' then
        select count(*) into v_tmp_hist
            from information_schema.tables where upper(table_schema) = 'REPLICAIS_INCR' and upper(table_name) = 'HIST_' || v_table_name;
        if v_tmp_hist = 0 then
            execute 'create table replicais_incr.hist_' || v_table_name || ' (like HIST.' || v_table_name ||')'; 
        end if;
    end if;
    
    select count(*)
        into v_cnt
        from (
            select case
                       when c.column_name like '%#%' then '"' || c.column_name || '"'
                       else c.column_name
                   end as column_name
                from information_schema.columns as c
                where c.table_schema || '.' || c.table_name = lower(v_schema_name || '.' || v_table_name)
                order by ordinal_position
        ) as q
        where position('|' || upper(q.column_name) || '|' in upper('|' || array_to_string(p_blob_fields,'|') || '|')) > 0;
    if v_cnt <> array_upper(p_blob_fields,1) then
        raise exception 'Columns from p_blob_fields not found in table';
    end if;
    
    -- Add table to the list of the tables being replicated
    insert into replicais.replication_tables(
                                             replication_table_isn,
                                             replication_table_schema,
                                             replication_table_name,
                                             SendBlob,
                                             SkipHist,
                                             SkipHistlog,
                                             primary_keys,
                                             blob_fields,
                                             renamed_fields
                                            )
        values( 
                nextval('replicais.replication_table_seq'),
                v_schema_name,
                v_table_name,
                p_SendBlob,
                p_SkipHist,
                p_skipHistlog,
                p_pkeys,
                p_blob_fields,
                p_renamed_fields
            );

    -- Generate select with blob replacements
    select  array_to_string(array_agg(
                    case 
                        when position('|' || upper(q.column_name) || '|' in upper('|' || array_to_string(p_blob_fields,'|') || '|')) > 0 then 'EMPTY_CLOB() ' || shared_system.field_rename(p_renamed_fields, q.column_name)
                        else shared_system.field_rename(p_renamed_fields, q.column_name) || ' ' || q.column_name
                    end
                ),
                ','
            ),
            array_to_string(array_agg(
                    q.column_name
                ),
                ','
            )
        into v_select_list
        from (
            select case
                       when c.column_name like '%#%' then '"' || c.column_name || '"'
                       else c.column_name
                   end as column_name
                from information_schema.columns as c
                where c.table_schema || '.' || c.table_name = lower(v_schema_name || '.' || v_table_name)
                order by ordinal_position
        ) as q;
    
    -- Create temp table in Oracle (main)
    select shared_system.execute_oracle(v_connect_id, 'create table gp_user.temp_'||v_table_name||' tablespace "TRNDATA"
                                             as (select ' || v_select_list || ' from '||v_schema_name||'.'||v_table_name||' where 1 = 0)') into v_ret_code;
    if v_ret_code <> 'success' then
        raise exception 'Error %', v_ret_code;
    end if;
    
    if p_SkipHist = 'N' then
        -- Create temp table in Oracle (hist)                            
        select shared_system.execute_oracle(v_connect_id, 'create table gp_user.temp_hist_'||v_table_name||' tablespace "TRNDATA"
                                                 as (select ' || v_select_list || ', histisn from hist.'||v_table_name||' where 1 = 0)') into v_ret_code;
        if v_ret_code <> 'success' then
            raise exception 'Error %', v_ret_code;
        end if;
    end if;

    -- Create external table in GreenPlum (main)
    perform shared_system.create_ext_table( v_schema_name||'.'||v_table_name,
                                            'replicais.ext_'||v_table_name,
                                            v_connect_id,
                                            'gp_user.temp_' || v_table_name);
    if p_SkipHist = 'N' then
        -- Create external table in GreenPlum (hist)                                        
        perform shared_system.create_ext_table( 'hist.'||v_table_name,
                                                'replicais.ext_hist_'||v_table_name,
                                                v_connect_id,
                                                'gp_user.temp_hist_' || v_table_name);
    end if;
    return v_ret_code;
end;
$BODY$
language plpgsql
volatile;