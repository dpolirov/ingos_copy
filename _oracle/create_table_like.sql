create or replace procedure gp_user.create_table_like(p_schema_name in varchar2,
                                                      p_table_name  in varchar2,
                                                      p_create_hist in varchar2)
is
    str varchar2(4000); 
    cnt number;
begin
    select count(1)
        into cnt
        from all_tables
        where OWNER = 'GP_USER' and TABLE_NAME = 'TEMP_' || p_table_name;
    if cnt > 0 then
        raise_application_error (-20001, 'Table GP_USER.TEMP_' || p_table_name || ' already exists');
    end if;
    
    if p_create_hist = 'Y' then
        select count(1)
            into cnt
            from all_tables
            where OWNER = 'GP_USER' and TABLE_NAME = 'TEMP_HIST_' || p_table_name;
        if cnt > 0 then
            raise_application_error (-20001, 'Table GP_USER.TEMP_HIST_' || p_table_name || ' already exists');
        end if;
    end if;
    
    for r in (select OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE 
                  from all_tab_columns
                  where owner = p_schema_name
                      and table_name = p_table_name
                  order by column_id) loop
        if (str is not null) then
            str := str || ',';
        end if;
        if r.data_type in ('BLOB', 'RAW', 'LONG', 'LONG RAW') then
            str := str || 'EMPTY_CLOB() ' || r.column_name;
        else
            str := str || r.column_name;
        end if;
    end loop;
    
    execute immediate 'create table gp_user.temp_' || p_table_name || --' TABLESPACE TRNDATA ' ||
                      ' as select ' || str || ' from ' || p_schema_name || '.' || p_table_name || ' where 1=0'; 
    if p_create_hist = 'Y' then
        execute immediate 'create table gp_user.temp_hist_' || p_table_name ||-- ' TABLESPACE TRNDATA ' ||
                          ' as select ' || str || ' from ' || p_schema_name || '.' || p_table_name || ' where 1=0'; 
    end if;
    
    commit;
end;

--create table test (a numeric, b blob);
--exec gp_user.create_table_like('GP_USER', 'TEST', 'Y');