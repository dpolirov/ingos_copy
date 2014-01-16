create or replace function STORAGE_ADM.SS_HISTLOG_ADD_CHUNK() returns void as $BODY$
declare
    v_max_dttm      timestamp;
    v_min_dttm      timestamp;
    v_proc_tbl      varchar(32);
    v_proc_tbl_key  varchar(50);
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.SS_HISTLOG_ADD_CHUNK()';
    v_step          CHARACTER VARYING = 'NA';
begin
    v_step = 'Determine max_dttm and min_dttm';
    select coalesce(max(max_completed_dttm), timestamp '1900-01-01 00:00:00') into v_min_dttm 
        from storage_adm.histlog_chunks;
    
    select coalesce(max(completed_dttm), v_min_dttm) into v_max_dttm
        from replicais.replication_tasks_incr;    

    --step 0
    --Remove processed records from ss_histlog table
    delete from storage_adm.ss_histlog
        where loadisn is not null;
    
    --step 1
    --gather into tt_histlog hist records from all source tables of active processes considering unloadisns boundaries
    --keep isn of the latest record's update, then we will use it to get foreign isns from hist tables 
    -- this is : ss_get_log_new.Fill_tt_histlog and process_tt_histlog
    --TODO: collect statistics and review execution plan.
    --TODO: check if isn is really needed here (should we get foreign isns from hist. tables?)
    insert into storage_adm.tt_histlog (Isn,table_name,recisn)
    select Max(h.isn),h.tablename, h.recisn
        from hist.histlog h
        inner join
            ( -- get boundaries of unloadisns for source tables of active processes
            select r.replication_table as table_name, r.replication_start_isn, r.replication_end_isn
                from replicais.replication_tasks_incr r
                inner join 
                    (select distinct log_table_name 
                        from storage_adm.v_active_process_source_tables
                    ) t
                    on r.replication_table = t.LOG_TABLE_NAME
                where v_min_dttm < r.completed_dttm and r.completed_dttm <= v_max_dttm
            ) bnd
            on h.tablename = bnd.table_name
                and h.unloadisn between bnd.replication_start_isn and bnd.replication_end_isn
        Group By h.tablename,h.recisn;
    /*
    --DEBUG
    --temporary - without use unloadisn
    insert into storage_adm.tt_histlog (Isn,table_name,recisn)
    select Max(h.isn),h.tablename, h.recisn
        from hist.histlog h
        inner join
            (  
                    select distinct log_table_name  as table_name
                        from storage_adm.v_active_process_source_tables
                     
            ) bnd
            on h.tablename = bnd.table_name
                and h.updated between '2014-01-07 00:00:00' and '2014-01-08 00:00:00'
        Group By h.tablename,h.recisn;
    --DEBUG end
    */
    --step 2
    --gather isns from all input tables into temp table tt_input
    --this is: ss_get_log_new.process_log_buffer (rewritten without tt_histload_N)
    truncate table storage_adm.tt_input;
    --for each input table
    for v_proc_tbl in (select distinct log_table_name 
                        from storage_adm.v_active_process_source_tables) loop
        --for each key in table
        for v_proc_tbl_key in (select distinct LOG_TABLE_ISNFLD 
                                from storage_adm.v_active_process_source_tables
                                where log_table_name = v_proc_tbl) loop
            if v_proc_tbl_key = 'ISN' then
                --if key is isn
                --simple take isn
                insert into tt_input(table_name, findisn, recisn)
                select v_proc_tbl, recisn, recisn from storage_adm.tt_histlog h
                    where h.table_name = v_proc_tbl;
            else
                --if key is not isn
                --take specified field or expression from ais table
                execute ('
                insert into tt_input(table_name, findisn, recisn)
                    select '''||v_proc_tbl||''', t.findisn, t.isn
                        from storage_adm.tt_histlog h inner join 
                            (select isn, '||v_proc_tbl_key||' findisn from ais.'||v_proc_tbl||') t
                            on h.recisn = t.isn
                        where h.table_name='''||v_proc_tbl||''')');
            end if;
        end loop;
        --deduplicate by table_name, findisn taking max recisn
        --TODO: check if recisn is used further
        --duplicate by procid
        insert into storage_adm.ss_histlog(Isn,FINDISN,ProcIsn,TABLE_NAME,RECISN)
        select nextval('storage_adm.ss_seq'), h.findisn, p.procisn, h.table_name, h.max_recisn
        from (select table_name, findisn, max(recisn) as max_recisn
                from tt_input
                group by table_name, findisn) h
            inner join v_active_process_source_tables p
                on h.table_name = p.log_table_name;
    end loop;

    insert into storage_adm.histlog_chunks(max_completed_dttm) values (v_max_dttm);
    
    --TODO: Move records from histlog to histlog_arch. Think about non-process records and those were changed several times since last load.
    --   move to arch by isn or by loadisn interval?

    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            RAISE EXCEPTION '(%:%:%)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;

create or replace function STORAGE_ADM.CalcHistDB() returns timestamp as $BODY$
declare
    pHistDB         timestamp;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.CalcHistDB()';
    v_step          CHARACTER VARYING = 'NA';
begin
    select
        date_trunc('month',
            Case When date_trunc('day', Min(updated)) = date_trunc('day', current_timestamp)  and  Max(datequit) > current_timestamp 
                    then Max(datequit) - interval '1 month'
                 else Max(datequit)
            end)
        Into pHistDB
        from Ais.buhsubacc_t
        where id like '92%' 
            and length (id) > 4 
            and dateend > current_timestamp;

    Return coalesce(pHistDB, date_Trunc('day', current_timestamp));
    
    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            RAISE EXCEPTION '(%:%:%)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;

--returns list of columns except isn, loadisn, replaces pEND_DATE_FIELD with value of vHistDB
create or replace function STORAGE_ADM.GetHistTableFldList (pTABLE_NAME Varchar, pEND_DATE_FLD Varchar) Returns VARCHAR as $BODY$
declare
    vSql    Varchar(4000);
    Cur_Col record;
    vSchema varchar(32);
    vTable  varchar(32);
Begin
    vSchema = substr(pTABLE_NAME,1,instr(pTABLE_NAME,'.')-1);
    vTable =  substr(pTABLE_NAME,instr(pTABLE_NAME,'.')+1);
    vSql:='LOADISN ';
    For Cur_Col In (
        select a.attname
            from pg_attribute a 
            where a.attrelid = 
                (select oid from pg_class where relname = lower(vTable) and relnamespace = 
                    (select oid from pg_namespace where nspname = lower(vSchema))) 
                and a.attnum > 2
                and not attisdropped) Loop

        if (Cur_Col.attname <> lower(pEND_DATE_FLD)) Then
            vSql = vSql||','||Cur_Col.attname;
        else
            vSql = vSql||',To_DAte('''||To_Char(vHistDB-1,'DD.MM.YYYY')||''',''DD.MM.YYYY'') AS '||pEND_DATE_FLD;
        end if;
    end loop;

    Return vSql;
end;
$BODY$
language plpgsql;



create or replace function Get_Minus_slq(   pFromTable varchar,
                                            ptoTable varchar,
                                            pLinkFld Varchar,
                                            pFromWhere Varchar,
                                            pToWhere Varchar) returns Varchar as $BODY$
declare
    sqlMerge varchar(32000) = '
    Select pLinkFld
        from(
            Select pfInsertColumns
                from  pFromTable
            minus
            Select pfInsertColumns 
                from ptoTable
            )';

    vCol varchar(32);

    vUpdstr varchar(32000) = '';
    vInsstr varchar(32000) = '';

    vSchemaTo varchar(32000);
    vTableTo varchar(32000);
    vSchemaFrom varchar(32000);
    vTableFrom varchar(32000);

begin

    vSchemaTo = substr(ptoTable,1,instr(ptoTable,'.')-1);
    vTableTo = substr(ptoTable,instr(ptoTable,'.')+1);
    vSchemaFrom = substr(pFromTable,1,instr(pFromTable,'.')-1);
    vTableFrom = substr(pFromTable,instr(pFromTable,'.')+1);

    for vCol in 
    (   select a.attname
            from pg_attribute a, pg_attribute b
            where a.attrelid = 
                (select oid from pg_class where relname = lower(vTableFrom) and relnamespace = 
                    (select oid from pg_namespace where nspname = lower(vSchemaFrom))) 
                and a.attnum > 0
                and not a.attisdropped
                and b.attrelid =
                (select oid from pg_class where relname = lower(vTableTo) and relnamespace = 
                    (select oid from pg_namespace where nspname = lower(vSchemaTo))) 
                and b.attnum > 0
                and not b.attisdropped
                and a.attname = b.attname
    ) loop
        If Upper(vCol)<>trim(Upper(pLinkFld))  then
            vUpdstr:=vUpdstr||'d.'||vCol||'=s.'||vCol||',';
        end if;
        vInsstr:=vInsstr||'d.'||vCol||',';
    end loop;

    vUpdstr = Substr(vUpdstr,1,Length(vUpdstr)-1);
    vInsstr = Substr(vInsstr,1,Length(vInsstr)-1);


    sqlMerge = Replace(sqlMerge,'ptoTable',ptoTable||' s '||pToWhere);
    sqlMerge = Replace(sqlMerge,'pFromTable',pFromTable||' s '||pFromWhere);
    sqlMerge = Replace(sqlMerge,'pLinkFld',pLinkFld);
    sqlMerge = Replace(sqlMerge,'pUpdateColumns',vUpdstr);
    sqlMerge = Replace(sqlMerge,'pInsertColumns',vInsstr);
    sqlMerge = Replace(sqlMerge,'pfInsertColumns',Replace(vInsstr,'d.','s.'));


    return sqlMerge;
end;
$BODY$
language plpgsql;



create or replace function STORAGE_ADM.LOADSTORAGE(pProcIsn int, pFull smallint) returns int as $BODY$
declare
    vLoadIsn        numeric;
    vHistDb         timestamp; --was global
    vSql            varchar(1000);
    v_view          varchar(32);    
    v_dest_tbl      record;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.LOADSTORAGE(pProcIsn int, pFull smallint)';
    v_step          CHARACTER VARYING = 'NA';
begin
    -- this is : Prepare_Log_Buffer
    -- вызывается при pFillBufer=1. Т.е. может не вызываться
    v_step = 'Create record in repload table';
    vLoadIsn = nextval('storage_adm.repload_seq');
    insert into storage_adm.repload(isn, loadtype, procisn) values
        (vLoadIsn, pFull, pProcIsn);
    
    v_step = 'Clear SS_BUG_LOG';
    if pFull = 0 then
        delete from storage_adm.SS_BUG_LOG where procisn = pProcIsn and loadisn is not null;
    else
        delete from storage_adm.SS_BUG_LOG where procisn = pProcIsn;
    end if;
    
    v_step = 'Fill SS_BUG_LOG';
    
    if pFull=0 then
        --incremental load
        --Mark records in SS_HISTLOG for loading
        update storage_adm.SS_HISTLOG
            set Loadisn = Nvl(vLoadisn,-10)
            where procisn = pProcIsn and loadisn is null ;

        --Get records
        --for non-fullload tables - get from ss_histlog
        Insert Into storage_adm.SS_BUF_LOG (isn, loadisn, recisn, procisn)
        select nextval('storage_adm.ss_seq'), 0, t.findisn, pProcIsn
            from
            (select h.findisn
                from storage_adm.ss_histlog h
                    inner join storage_adm.ss_process_source_tables p
                    on h.table_name=p.LOG_TABLE_NAME
                where p.get_view is null and p.isn = pProcIsn and h.findisn is not null
                group by h.findisn) t;
        
        --for fullload tables - get from get_view query
        for v_view in (select get_view 
                        from storage_adm.ss_process_source_tables
                        where get_view is not null and procisn = pProcIsn) loop
            execute('insert into storage_adm.ss_buf_log(isn, loadisn, recisn, procisn)
                select nextval(''storage_adm.ss_seq''), 0, v.*, pProcIsn
                    from '||v_view||' v where v.* is not null');
            --v returns one variable that have type of isn and can have any name
        end loop;
    else
        --if pFull=1
        --for load type full get data from fullloadview query
        for v_view in (select FullLoadView 
                        from storage_adm.ss_process_source_tables
                        where FullLoadView is not null and p.isn = pProcIsn) loop
            execute('insert into storage_adm.ss_buf_log(isn, loadisn, recisn, procisn)
                select nextval(''storage_adm.ss_seq''), 0, v.*, pProcIsn
                    from '||v_view||' v');
            --v returns one variable that have type of isn and can have any name
        end loop;
    end if;

    --TODO: where is vHistDB initialized first?
    if pFull = 0 then
        If vHistDB = trunc(current_timestamp) then
            vHistDB = CalcHistDB();
        end if;
    else
        vHistDB = to_date('01-01-1900','dd-mm-yyyy');
    end if;
    
    --Load_Proc_By_tt_RowId
    
    -- clear and fill tt_rowid
    truncate table storage_adm.tt_rowid;
    
    insert into tt_rowid(Isn)
        select distinct RECISN
            from storage_adm.SS_BUF_LOG
    where ProcIsn=pProcIsn;

    for v_dest_tbl In (Select * from ss_proces_dest_tables Where Procisn = pProcIsn order by PRIORITY Asc nulls first) loop
        if pFull = 0 then
            --incremental load
            execute '  truncate table '||v_dest_tbl.TT_TABLE_NAME;       
            execute '  Insert into '||v_dest_tbl.TT_TABLE_NAME||'
                             select * from '||v_dest_tbl.VIEW_NAME;
            if length(v_dest_tbl.after_script) > 1 then
                execute v_dest_tbl.after_script;
            end if;
            vSql =
                '   /* delete records that were deleted in source - set of records tt_rowid minus tt_table*/
                Delete 
                    from '||v_dest_tbl.TABLE_NAME||' a
                    Where ('||v_dest_tbl.HIST_KEYFIELD||')  In
                        (Select B.'||v_dest_tbl.HIST_KEYFIELD||' 
                            from '||v_dest_tbl.TABLE_NAME||' b
                            Where B.'||v_dest_tbl.HIST_KEYFIELD||' In 
                                (Select T.Isn from tt_rowid T)   
                                Minus
                                Select TT.'||v_dest_tbl.HIST_KEYFIELD||' from '||v_dest_tbl.TT_TABLE_NAME||' TT)';
            execute vSql;
            
            /* remove missing keys in case of set of records by v_dest_tbl.HIST_KEYFIELD has changed  */
            
            if NVL(v_dest_tbl.IS_HISTTABLE,0) = 0 then
            
                --non hist table
                vSql =
                'Delete
                    from '||v_dest_tbl.TABLE_NAME||' a
                    Where ('||v_dest_tbl.KeyField||')  In
                       (Select '||v_dest_tbl.KeyField||' from '||v_dest_tbl.TABLE_NAME||' b
                            Where B.'||v_dest_tbl.HIST_KEYFIELD||' In 
                                (Select t.Isn from tt_rowid t )
                                Minus
                                Select  '||v_dest_tbl.KeyField||' from '||v_dest_tbl.TT_TABLE_NAME||' TT)';
            else
                --table with history
                vSql = GetHistTableFldList(v_dest_tbl.TABLE_NAME,v_dest_tbl.END_DATE_FLD);

                vSql = '
                    Begin
                        Insert Into '||v_dest_tbl.TABLE_NAME||'
                            SELECT NEXTVAL(''storage_adm.ss_seq'') ISN,S.*
                                from
                                    (Select '||vSql ||'
                                        from  '||v_dest_tbl.TABLE_NAME||'
                                        Where
                                            '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB ||'''
                                            AND ('||v_dest_tbl.KeyField||')  In
                                            (Select  '||v_dest_tbl.KeyField||' 
                                                from '||v_dest_tbl.TABLE_NAME||' b
                                                Where '||v_dest_tbl.HIST_KEYFIELD||' In 
                                                    (Select Isn from tt_rowid )
                                            Minus
                                            Select  '||v_dest_tbl.KeyField||' 
                                                from '||v_dest_tbl.TT_TABLE_NAME||'
                                            )
                                    ) S
                                WHERE '|| v_dest_tbl.END_DATE_FLD||'>='||v_dest_tbl.BEG_DATE_FLD|| ';

                        Delete
                            from '||v_dest_tbl.TABLE_NAME||' a
                            Where  '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB ||''' 
                                AND ('||v_dest_tbl.KeyField||')  In
                                    (Select '||v_dest_tbl.KeyField||' 
                                        from '||v_dest_tbl.TABLE_NAME||' b
                                        Where '||v_dest_tbl.HIST_KEYFIELD||' In 
                                            (Select Isn from tt_rowid )
                                    Minus
                                    Select  '||v_dest_tbl.KeyField||' 
                                        from '||v_dest_tbl.TT_TABLE_NAME||');

                        /* remove records that begin after hist date */
                        Delete 
                            from '||v_dest_tbl.TABLE_NAME||' a
                                Where  '||v_dest_tbl.BEG_DATE_FLD||'>='''||vHistDB ||''' 
                                    AND ('||v_dest_tbl.KeyField||')  In
                                        (Select  '||v_dest_tbl.KeyField||' 
                                            from '||v_dest_tbl.TT_TABLE_NAME||');

                     end;';
            end if;
            execute vSql;

            --insert new records
         if NVL(v_dest_tbl.IS_HISTTABLE,0) = 0 THen

            vdsql = '
                Delete 
                    from '||v_dest_tbl.TABLE_NAME||' a 
                    Where ('||v_dest_tbl.KeyField||')  In 
                        ('||
                          Get_Minus_slq(v_dest_tbl.TABLE_NAME,
                                        v_dest_tbl.TT_TABLE_NAME,
                                        v_dest_tbl.KeyField,
                                        'Where   ('||v_dest_tbl.KeyField||') in 
                                            (Select '||v_dest_tbl.KeyField||' 
                                                from tt_rowid tt, '||v_dest_tbl.TT_TABLE_NAME||' t 
                                                Where t.rowid =tt.rId )',
                                        'Where   rowid in (select rId from tt_rowid)',
                                        v_dest_tbl.DEST_TABLE_INDEX
                                    )||
                        ');';

                     vHistAdd:=' ';
         else

            vdsql = GetHistTableFldList(v_dest_tbl.TABLE_NAME,v_dest_tbl.END_DATE_FLD);

            vdsql = '
                Insert Into '||v_dest_tbl.TABLE_NAME||'

                SELECT NEXTVAL(''storage_adm.ss_seq''), ISN,S.*
                    FROM
                    (
                        Select '||vdsql ||' from  '||v_dest_tbl.TABLE_NAME||'
                        Where '
                            ||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB ||''' AND
                            ('||v_dest_tbl.KeyField||')  In ('
                            ||Get_Minus_slq(v_dest_tbl.TABLE_NAME,
                                            v_dest_tbl.TT_TABLE_NAME,
                                            v_dest_tbl.KeyField,
                                            'Where '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB ||''' And  ('||v_dest_tbl.KeyField||') in (
                                                        Select '||v_dest_tbl.KeyField||' from  tt_rowid TT,'||v_dest_tbl.TT_TABLE_NAME||' T 
                                                                Where t.rowid =tt.rId  )',
                                            'Where   rowid in (select rId from tt_rowid)',
                                            v_dest_tbl.DEST_TABLE_INDEX)||')
                    ) S
                    WHERE   '|| v_dest_tbl.END_DATE_FLD||'>='||v_dest_tbl.BEG_DATE_FLD||';


                Delete 
                    from '||v_dest_tbl.TABLE_NAME||'
                    Where
                        '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB ||''' AND
                        ('||v_dest_tbl.KeyField||')  In ('
                        ||Get_Minus_slq(v_dest_tbl.TABLE_NAME,
                                        v_dest_tbl.TT_TABLE_NAME,
                                        v_dest_tbl.KeyField,
                                        'Where '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB ||''' And  ('||v_dest_tbl.KeyField||') in 
                                            (Select '||v_dest_tbl.KeyField||' from tt_rowid tt,'||v_dest_tbl.TT_TABLE_NAME||' t 
                                                Where t.rowid=tt.rId )',
                                        'Where   rowid in (select rId from tt_rowid)',
                                        v_dest_tbl.DEST_TABLE_INDEX)||');';


              vHistAdd:=' AND '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB ||'''';

         end if;

         vsql:=
            'begin
                /* process changed records - delete and add */
                RepLog_i ('||pLoadIsn||', ''Load '||pProcName||' By_tt_RowId'', '''||v_dest_tbl.TABLE_NAME||' DeleteInsert'',  pAction => ''Begin'');'
                ||vdsql||

               '
               Insert  Into '||v_dest_tbl.TABLE_NAME||'
               Select NEXTVAL(''storage_adm.ss_seq'') Isn,'||pLoadIsn||',S.*
               FROM (

                   With TT_DATA AS
                          ( Select  T.*
                            from tt_rowid_r r,'||v_dest_tbl.TT_TABLE_NAME||' T
                           WHERE  T.RowID= r.RId
                          ),
                    Changed_Data AS (
                      SELECT /*+ materialize */
                       *
                      FROM ('
                     ||Get_Minus_slq(v_dest_tbl.TT_TABLE_NAME,v_dest_tbl.TABLE_NAME,v_dest_tbl.KeyField,'Where rowid  in (select /*+ Cardinality (r 50000) */ rId from tt_rowid_r r ) ',
                                    'Where   ('||v_dest_tbl.KeyField||') in (Select  /*+ Ordered Use_Nl(tt t) */ '||v_dest_tbl.KeyField||' from TT_DATA )'||vHistAdd,'',v_dest_tbl.DEST_TABLE_INDEX)
                     ||' )
                     )

                  Select /*+ NO_PARALLEL(T) HASH_SJ(T) */
                       *
                   from  TT_DATA T
                  Where ('||v_dest_tbl.KeyField||') in (Select  * from Changed_Data)

                ) s;

                vrc:=SQL%ROWCount;

                RepLog_i ('||pLoadIsn||', ''Load '||pProcName||' By_tt_RowId'', '''||v_dest_tbl.TABLE_NAME||' DeleteInsert'',  pAction => ''End'',pobjCount => vrc, pBlockIsn=>vBlockIsn);
              end;';

            DEBUG_PUT_LINE(vSql);
            execute immediate vsql;

            Execute Immediate '
              delete from '||v_dest_tbl.TT_TABLE_NAME||' where  rowid in (select rId from tt_rowid_r)';

            
        else
            --full load
        end if;
        
    end loop

    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            RAISE EXCEPTION '(%:%:%)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;
