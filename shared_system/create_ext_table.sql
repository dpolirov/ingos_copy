create or replace function shared_system.create_ext_table (
        target_table varchar,
        ext_table varchar,
        connection_id int,
        remote_table varchar) returns void as $BODY$
declare
    fld_format varchar[];
    fld_list   varchar(10000);
    query      varchar(10000);
    fn_res     varchar(1);
begin
    --List of columns from target_table with data types as array
    select array_agg(q.desc)
        into fld_format
        from (
            select  case
                        when column_name like '%#%' then '"' || column_name || '"'
                        else column_name
                    end
                    || ' ' ||
                    case
                        when data_type in ('character varying', 'character') then data_type || '(' || character_maximum_length::varchar(5) || ')'
                        when data_type in ('numeric') then
                            case
                                when numeric_precision is null or numeric_scale is null then data_type
                                else data_type || '(' || numeric_precision::varchar(2) || ',' || numeric_scale::varchar(2) || ')'
                            end
                        else data_type
                    end as desc
                from information_schema.columns
                where table_schema || '.' || table_name = lower(target_table)
                order by ordinal_position
            ) as q;
    raise notice 'Field format array: %', fld_format;

    --List of columns from target_table as a string
    select array_to_string(a,',')
        into fld_list
        from (
            select array_agg(q.column_name) as a
            from (
                select case
                           when column_name like '%#%' then '"' || column_name || '"'
                           else column_name
                       end
                    from information_schema.columns
                    where table_schema || '.' || table_name = lower(target_table)
                    order by ordinal_position
            ) as q
        ) as q2;
    raise notice 'Field list: %', fld_list;

    --Create external table to read data from Oracle
    perform os.fn_create_ext_table(ext_table, fld_format, connection_id, 'select ' || fld_list || ' from ' || remote_table);
end;
$BODY$
language plpgsql
volatile;