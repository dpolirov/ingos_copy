create or replace function STORAGE_ADM.SS_HISTLOG_ADD_CHUNK() returns void as $BODY$
declare
    v_max_dttm      timestamp;
    v_min_dttm      timestamp;
    v_proc_tbl      varchar(32);
    v_proc_tbl_key  varchar(50);
    vRc             bigint;
    vSql            varchar;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.SS_HISTLOG_ADD_CHUNK';
    v_step          CHARACTER VARYING = 'NA';
begin

    v_step = 'Determine max_dttm and min_dttm';
    select coalesce(max(max_completed_dttm), timestamp '2014-01-28 00:00:00') into v_min_dttm 
        from storage_adm.histlog_chunks;
    
    select coalesce(max(completed_dttm), v_min_dttm) into v_max_dttm
        from replicais.replication_tasks_incr;    

    --step 0
    --get chunk of data from histlog
    v_step = 'Populate tt_histlog_chunk';
    truncate table storage_adm.tt_histlog_chunk;
    insert into storage_adm.tt_histlog_chunk
    select h.*
        from hist.histlog h
        inner join replicais.replication_tasks_incr r
            on h.tablename = r.replication_table 
                and h.unloadisn between r.replication_start_isn and r.replication_end_isn
        where v_min_dttm < r.completed_dttm and r.completed_dttm <= v_max_dttm;

    get diagnostics vRc = ROW_COUNT;
    RAISE NOTICE '%: % records were inserted to tt_histlog_chunk for interval v_min_dttm=% v_max_dttm =%', v_step, vRc, v_min_dttm, v_max_dttm;

    analyze storage_adm.tt_histlog_chunk;
    
    --step 1
    --gather into tt_histlog hist records from all source tables of active processes considering unloadisns boundaries
    --keep isn of the latest record's update, then we will use it to get foreign isns from hist tables 
    -- this is : ss_get_log_new.Fill_tt_histlog and process_tt_histlog
    --TODO: collect statistics and review execution plan.
    --TODO: check if isn is really needed here (should we get foreign isns from hist. tables?)
    v_step = 'Populate tt_histlog';
    truncate table storage_adm.tt_histlog;
    insert into storage_adm.tt_histlog (Isn,table_name,recisn)
    select Max(h.isn),h.tablename, h.recisn
        from storage_adm.tt_histlog_chunk h
        inner join
        -- get source tables of active processes
            (select distinct log_table_name 
                from storage_adm.v_active_process_source_tables
            ) t
            on h.tablename = t.LOG_TABLE_NAME
        Group By h.tablename,h.recisn;

    get diagnostics vRc = ROW_COUNT;
    RAISE NOTICE '%: % records were inserted to tt_histlog for active processses', v_step, vRc;


    --step 0
    --Remove processed records from ss_histlog table
    v_step = 'Clear ss_histlog';
    delete from storage_adm.ss_histlog
        where loadisn is not null;
    get diagnostics vRc = ROW_COUNT;
    RAISE NOTICE '%: %records were deleted from ss_histlog', v_step, vRc;
   

    --step 2
    --gather isns from all input tables into temp table tt_input
    --this is: ss_get_log_new.process_log_buffer (rewritten without tt_histload_N)
    v_step = 'Populate ss_histlog';
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
                --just take isn
                insert into storage_adm.tt_input(table_name, findisn, recisn)
                select v_proc_tbl, recisn, recisn from storage_adm.tt_histlog h
                    where h.table_name = v_proc_tbl;
                
            else
                --if key is not isn
                --take specified field or expression from ais table
                vsql = '
                insert into storage_adm.tt_input(table_name, findisn, recisn)
                    select '''||v_proc_tbl||''', t.findisn, t.isn
                        from storage_adm.tt_histlog h inner join 
                            (select isn, '||v_proc_tbl_key||' findisn from ais.'||v_proc_tbl||') t
                            on h.recisn = t.isn
                        where h.table_name='''||v_proc_tbl||'''';
                execute vsql;
            end if;
            get diagnostics vRc = ROW_COUNT;
            RAISE NOTICE '%: % records were inserted to tt_input for table %, key %', v_step,vRc, v_proc_tbl,v_proc_tbl_key;
            --TODO:
            --also isns from hist. tables ?
        end loop;
        --deduplicate by table_name, findisn taking max recisn
        --TODO: check if recisn is used further
        --duplicate by procid
        insert into storage_adm.ss_histlog(Isn,FINDISN,ProcIsn,TABLE_NAME,RECISN)
        select nextval('storage_adm.ss_seq'), h.findisn, p.procisn, h.table_name, h.max_recisn
        from (select table_name, findisn, max(recisn) as max_recisn
                from storage_adm.tt_input
                where table_name=v_proc_tbl
                group by table_name, findisn) h,
             (select distinct procisn 
                from storage_adm.v_active_process_source_tables
                where log_table_name=v_proc_tbl) p;
                
        get diagnostics vRc = ROW_COUNT;
        RAISE NOTICE '%: % records were inserted to ss_histlog for table %',v_step,vRc, v_proc_tbl;
    end loop;
    
    analyze storage_adm.ss_histlog;
    
    insert into storage_adm.histlog_chunks(max_completed_dttm) values (v_max_dttm);
    
/*
    --Move records from histlog to histlog_arch
    v_step = 'Move histlog chunk to arch';
    insert into hist.histlog_arch
    select * from histlog_chunk;

    delete from hist.histlog h 
        using storage_adm.tt_histlog_chunk c 
        where h.isn=c.isn;
*/
        
    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            RAISE EXCEPTION '(% : % : %)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;




--------------------------------------------------------------------------------
-- Utility functions for LOAD_STORAGE
--------------------------------------------------------------------------------
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

--fields list  for closing hist records: all except isn, loadisn,  pEND_DATE_FIELD replaced with value of vHistDB
create or replace function STORAGE_ADM.GetHistTableFldList (pTABLE_NAME Varchar, pEND_DATE_FLD Varchar) Returns VARCHAR as $BODY$
declare
    vSql    Varchar(4000);
    Cur_Col record;
    vSchema varchar(32);
    vTable  varchar(32);
Begin
    vSchema = substr(pTABLE_NAME,1,strpos(pTABLE_NAME,'.')-1);
    vTable =  substr(pTABLE_NAME,strpos(pTABLE_NAME,'.')+1);
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



create or replace function storage_adm.Get_Minus_slq(   pFromTable varchar,
                                                        ptoTable varchar,
                                                        pLinkFld Varchar,
                                                        pFromWhere Varchar,
                                                        pToWhere Varchar) returns Varchar as $BODY$
declare
    sqlMerge varchar(32000) = '
    Select minus_slq.pLinkFld
        from(
            Select pfInsertColumns
                from  pFromTable
            except
            Select pfInsertColumns 
                from ptoTable
            ) minus_slq';

    vCol varchar(32);

    vInsstr varchar(32000) = '';

    vSchemaTo varchar(32);
    vTableTo varchar(32);
    vSchemaFrom varchar(32);
    vTableFrom varchar(32);

begin

    vSchemaTo = substr(ptoTable,1,strpos(ptoTable,'.')-1);
    vTableTo = substr(ptoTable,strpos(ptoTable,'.')+1);
    vSchemaFrom = substr(pFromTable,1,strpos(pFromTable,'.')-1);
    vTableFrom = substr(pFromTable,strpos(pFromTable,'.')+1);

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
        vInsstr:=vInsstr||'s.'||vCol||',';
    end loop;

    vInsstr = Substr(vInsstr,1,Length(vInsstr)-1);


    sqlMerge = Replace(sqlMerge,'ptoTable',ptoTable||' s '||pToWhere);
    sqlMerge = Replace(sqlMerge,'pFromTable',pFromTable||' s '||pFromWhere);
    sqlMerge = Replace(sqlMerge,'pLinkFld',pLinkFld);
    sqlMerge = Replace(sqlMerge,'pfInsertColumns',vInsstr);


    return sqlMerge;
end;
$BODY$
language plpgsql;

--GET DIAGNOSTICS vRc = ROW_COUNT;

--TODO: Autonomous transaction 
create or replace function STORAGE_ADM.replog_i(pLoadIsn numeric, pModule varchar, pOperation varchar,
                                                   pAction varchar, pObjCount bigint, pSqlText text) returns void as $BODY$
begin
    insert into storage_adm.replog(isn, loadisn, module, operation, action, objcount, sqltext) values
    (nextval('storage_adm.replog_seq'), pLoadIsn, pModule, pOperation, pAction, pObjCount, pSqlText);
    RAISE NOTICE '% : ploadisn=%, % %, % records affected. Query:\n %', pOperation, pLoadIsn, pModule, pAction, pObjCount, pSqlText;
end;
$BODY$
language plpgsql;


--TODO: Autonomous transaction 
create or replace function STORAGE_ADM.replog_i(pLoadIsn numeric, pModule varchar, pOperation varchar,
                                                   pAction varchar) returns void as $BODY$
begin
    insert into storage_adm.replog(isn, loadisn, module, operation, action) values
    (nextval('storage_adm.replog_seq'), pLoadIsn, pModule, pOperation, pAction);
    RAISE NOTICE '% : ploadisn=%, % %', pOperation, pLoadIsn, pModule, pAction;
end;
$BODY$
language plpgsql;


--RepLog_i ('||pLoadIsn||', ''Load '||pProcName||' By_tt_RowId'', '''||Cur.TABLE_NAME||' DeleteInsert'',  pAction => ''End'',pobjCount => vrc


--------------------------------------------------------------------------------
--
--  LOAD STORAGE
-- 
--------------------------------------------------------------------------------
create or replace function STORAGE_ADM.LOADSTORAGE(pProcIsn int, pFull smallint, pFillBuffer smallint) returns void as $BODY$
declare
    vLoadIsn        numeric;
    vHistDb         timestamp; --was global
    vSql            varchar(1000);
    vProc           record;
    vRc             bigint;
    v_view          varchar;    
    v_dest_tbl      record;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.LOADSTORAGE(pProcIsn int, pFull smallint)';
    v_step          CHARACTER VARYING = 'NA';
begin
    --originally was logged to ais
    --ais.logevent('LOAD_STORAGE','storage_adm.load_storage.LoadStorage pProcIsn='||pProcIsn,pCode => 143503,pTComponent => 'LoadStorage');
    
    -- Prepare_Log_Buffer
    -- called if  pFillBufer=1
    RAISE NOTICE 'STORAGE_ADM.LOADSTORAGE started for procisn=% pFull=%', pProcIsn, pFull;
    
    select * into vProc from storage_adm.ss_processes where isn = pProcIsn;

    vLoadIsn = nextval('storage_adm.repload_seq');
    
    --REPLOG_I (vLoadIsn, 'LoadStorage','Get Log process '||vProc.Name, 'Begin', null, null);
    v_step = 'Create record in repload table';
    insert into storage_adm.repload(isn, loadtype, procisn) values
        (vLoadIsn, pFull, pProcIsn);
    
    if pFillBuffer=1 then
        v_step = 'Clear SS_BUF_LOG';
        if pFull = 0 then
            delete from storage_adm.SS_BUF_LOG where procisn = pProcIsn and loadisn is not null;
        else
            delete from storage_adm.SS_BUF_LOG where procisn = pProcIsn;
        end if;
        get diagnostics vRc = ROW_COUNT;
        RAISE NOTICE '%: % records were deleted from ss_buf_log',v_step,vRc;
        
        v_step = 'Fill SS_BUF_LOG';
        if pFull=0 then
            --incremental load
            --Mark records in SS_HISTLOG for loading
            update storage_adm.SS_HISTLOG
                set Loadisn = vLoadisn
                where procisn = pProcIsn and loadisn is null ;
            get diagnostics vRc = ROW_COUNT;
            RAISE NOTICE '%: % records were updated in ss_histlog with loadisn=%',v_step,vRc,vLoadIsn;

            --Get records
            --for non-fullload tables - get from ss_histlog
            Insert Into storage_adm.SS_BUF_LOG (isn, loadisn, recisn, procisn)
            select nextval('storage_adm.ss_seq'), 0, t.findisn, pProcIsn
                from
                (select h.findisn
                    from storage_adm.ss_histlog h
                        inner join storage_adm.ss_process_source_tables p
                        on h.table_name=p.LOG_TABLE_NAME
                    where coalesce(p.get_view, '') = '' and p.procisn = pProcIsn 
                        and h.procisn = pProcIsn and h.loadisn = vLoadIsn and h.findisn is not null
                    group by h.findisn) t;
            get diagnostics vRc = ROW_COUNT;
            RAISE NOTICE '%: % records were inserted in ss_buflog for non-fullload tables',v_step,vRc;
            
            --for fullload tables - get from get_view query
            for v_view in (select get_view 
                            from storage_adm.ss_process_source_tables
                            where coalesce(get_view,'') <> '' and procisn = pProcIsn) loop
                execute('insert into storage_adm.ss_buf_log(isn, loadisn, recisn, procisn)
                    select nextval(''storage_adm.ss_seq''), 0, v.*, '||pProcIsn||'
                        from '||v_view||' v where v.* is not null');
                --v returns one variable that have type of isn and can have any name
                get diagnostics vRc = ROW_COUNT;
                RAISE NOTICE '%: % records were inserted in ss_buflog from get_view %',v_step,vRc,v_view;
            end loop;
        else
            --if pFull=1
            --for load type full get data from fullloadview query
            for v_view in (select FullLoadView 
                            from storage_adm.ss_process_source_tables
                            where coalesce(FullLoadView,'') <> '' and procisn = pProcIsn) loop
                execute('insert into storage_adm.ss_buf_log(isn, loadisn, recisn, procisn)
                    select nextval(''storage_adm.ss_seq''), 0, v.*, '||pProcIsn||'
                        from ('||v_view||') v');
                --v returns one variable that have type of isn and can have any name
                get diagnostics vRc = ROW_COUNT;
                RAISE NOTICE '%: % records were inserted in ss_buflog from fullLoadView %',v_step,vRc,v_view;
            end loop;
        end if;
    end if;
    --REPLOG_I (vLoadIsn, 'LoadStorage','Get Log process '||vProc.Name, 'End', null, null);

    --call Load_Proc_By_tt_RowId
end;
$BODY$
language plpgsql;



    
create or replace function STORAGE_ADM.Load_Proc_By_tt_RowId(pLoadIsn numeric, pProcIsn int, pProcName varchar, pFull smallint) returns void as $BODY$
declare
    --vLoadIsn        numeric;
    vHistDb         timestamp; --was global
    vSql            varchar;
    vdsql           varchar;
    vHistAdd        varchar;
    vRc             bigint;
    v_dest_tbl      record;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.Load_Proc_By_tt_RowId';
    v_step          CHARACTER VARYING = 'NA';
begin
    
    v_step = 'Calc vHistDB';
    if pFull = 0 then
        vHistDB = storage_adm.CalcHistDB();
    else
        vHistDB = to_date('01-01-1900','dd-mm-yyyy');
    end if;
    RAISE NOTICE 'vHistDB=%', vHistDB;

    v_step = 'Populate tt_rowid';
    -- clear and fill tt_rowid
    truncate table storage_adm.tt_rowid;
    
    insert into storage_adm.tt_rowid(Isn)
        select distinct RECISN
            from storage_adm.SS_BUF_LOG
    where ProcIsn=pProcIsn;

    --for each dest table of the process
    for v_dest_tbl In (Select * from ss_process_dest_tables Where Procisn = pProcIsn order by PRIORITY Asc) loop
        if pFull = 0 then
            --incremental load
            v_step = v_dest_tbl.TABLE_NAME||' Insert tt';
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step,  'Begin');
            execute '  truncate table '||v_dest_tbl.TT_TABLE_NAME;       
            vSql =  '  Insert into '||v_dest_tbl.TT_TABLE_NAME||'
                             select nextval(''storage_adm.tt_seq''), v.* from '||v_dest_tbl.VIEW_NAME||' v';
            execute vSql;
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step,  'End', -1, vSql);
            
            v_step = 'Execute after_script';
            -- empty for all proceses as of now
            if length(v_dest_tbl.after_script) > 1 then
                execute v_dest_tbl.after_script;
            end if;
            
            v_step = v_dest_tbl.TABLE_NAME||' Delete deleted';
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step,  'Begin');
            vSql =
                '   /* delete records that were deleted in source - set of records tt_rowid minus tt_table*/
                Delete 
                    from '||v_dest_tbl.TABLE_NAME||' a
                    Where ('||v_dest_tbl.HIST_KEYFIELD||')  In
                        (Select B.'||v_dest_tbl.HIST_KEYFIELD||' 
                            from '||v_dest_tbl.TABLE_NAME||' b
                            Where B.'||v_dest_tbl.HIST_KEYFIELD||' In 
                                (Select T.Isn from storage_adm.tt_rowid T)   
                                except
                                Select TT.'||v_dest_tbl.HIST_KEYFIELD||' from '||v_dest_tbl.TT_TABLE_NAME||' TT)';
            execute vSql;
            GET DIAGNOSTICS vRc = ROW_COUNT;
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step,  'End', vRc, vSql);
            
            /* remove missing keys in case of set of records by v_dest_tbl.HIST_KEYFIELD has changed  */
            v_step = v_dest_tbl.TABLE_NAME||' Delete by KeyField';
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step,  'Begin');
            if coalesce(v_dest_tbl.IS_HISTTABLE,0) = 0 then
            
                --non hist table
                vSql =
                'Delete
                    from '||v_dest_tbl.TABLE_NAME||' a
                    Where ('||v_dest_tbl.KeyField||')  In
                       (Select '||v_dest_tbl.KeyField||' from '||v_dest_tbl.TABLE_NAME||' b
                            Where B.'||v_dest_tbl.HIST_KEYFIELD||' In 
                                (Select t.Isn from storage_adm.tt_rowid t )
                                except
                                Select  '||v_dest_tbl.KeyField||' from '||v_dest_tbl.TT_TABLE_NAME||' TT)';
            else
                --table with history
                vSql = storage_adm.GetHistTableFldList(v_dest_tbl.TABLE_NAME,v_dest_tbl.END_DATE_FLD);

                vSql = 'Insert Into '||v_dest_tbl.TABLE_NAME||'
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
                                                    (Select Isn from storage_adm.tt_rowid )
                                            except
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
                                            (Select Isn from storage_adm.tt_rowid )
                                    except
                                    Select  '||v_dest_tbl.KeyField||' 
                                        from '||v_dest_tbl.TT_TABLE_NAME||');

                        /* remove records that begin after hist date */
                        Delete 
                            from '||v_dest_tbl.TABLE_NAME||' a
                                Where  '||v_dest_tbl.BEG_DATE_FLD||'>='''||vHistDB ||''' 
                                    AND ('||v_dest_tbl.KeyField||')  In
                                        (Select  '||v_dest_tbl.KeyField||' 
                                            from '||v_dest_tbl.TT_TABLE_NAME||');';
            end if;
            execute vSql;
            
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step,  'End', -1, vSql);
            
            --here was inserting by blocks in loop. this is second part of load_tt_by_rowid procedure
            v_step = v_dest_tbl.TABLE_NAME||' Populate tt_rowid_r';
            execute 'truncate table storage_adm.tt_rowid_r';
            execute 'Insert into storage_adm.tt_rowid_r(RId)
                        Select RowId from '||v_dest_tbl.TT_TABLE_NAME;
                    
            --insert new records
            v_step = v_dest_tbl.TABLE_NAME||' DeleteInsert';
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step,  'Begin');
            if coalesce(v_dest_tbl.IS_HISTTABLE,0) = 0 then
                vdsql = 
                    'Delete 
                        from '||v_dest_tbl.TABLE_NAME||' a 
                        Where ('||v_dest_tbl.KeyField||')  In
                            ('||storage_adm.Get_Minus_slq(
                                    v_dest_tbl.TABLE_NAME,
                                    v_dest_tbl.TT_TABLE_NAME,
                                    v_dest_tbl.KeyField,
                                    'Where   ('||v_dest_tbl.KeyField||') in (
                                        Select '||v_dest_tbl.KeyField||' 
                                            from storage_adm.tt_rowid_r tt, '||v_dest_tbl.TT_TABLE_NAME||' t 
                                            Where t.rowid =tt.rId )',
                                    'Where   rowid in (select rId from storage_adm.tt_rowid_r)'
                                  )||
                           ');';

                vHistAdd = ' ';
            else

                vdsql = storage_adm.GetHistTableFldList(v_dest_tbl.TABLE_NAME, v_dest_tbl.END_DATE_FLD);

                vdsql = '
                    Insert Into '||v_dest_tbl.TABLE_NAME||'
                        SELECT Seq_SS.NEXTVAL ISN,S.*
                            FROM(
                                Select '||vdsql||' 
                                    from  '||v_dest_tbl.TABLE_NAME||'
                                    Where
                                        '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB||''' 
                                        AND ('||v_dest_tbl.KeyField||')  In 
                                        ('||storage_adm.Get_Minus_slq(
                                                v_dest_tbl.TABLE_NAME,
                                                v_dest_tbl.TT_TABLE_NAME,
                                                v_dest_tbl.KeyField,
                                                'Where '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB||''' 
                                                    And  ('||v_dest_tbl.KeyField||') in (
                                                        Select '||v_dest_tbl.KeyField||' 
                                                            from  storage_adm.tt_rowid_r TT,'||v_dest_tbl.TT_TABLE_NAME||' T 
                                                            Where t.rowid =tt.rId  )',
                                                'Where   rowid in (select rId from storage_adm.tt_rowid_r)'
                                             )||
                                       ')
                                ) S
                            WHERE   '|| v_dest_tbl.END_DATE_FLD||'>='||v_dest_tbl.BEG_DATE_FLD||';


                    Delete
                        from '||v_dest_tbl.TABLE_NAME||'
                        Where '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB ||'''
                            AND ('||v_dest_tbl.KeyField||')  In
                            ('||storage_adm.Get_Minus_slq(
                                        v_dest_tbl.TABLE_NAME,
                                        v_dest_tbl.TT_TABLE_NAME,
                                        v_dest_tbl.KeyField,
                                        'Where '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB ||''' 
                                            And  ('||v_dest_tbl.KeyField||') in 
                                            (Select '||v_dest_tbl.KeyField||' 
                                                from storage_adm.tt_rowid_r tt,'||v_dest_tbl.TT_TABLE_NAME||' t 
                                                Where t.rowid=tt.rId )',
                                        'Where   rowid in (select rId from storage_adm.tt_rowid_r)'
                                     )||
                           ');';

                vHistAdd:=' AND '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB ||'''';

            end if;

            vsql = vdsql||
                '
                Insert  Into '||v_dest_tbl.TABLE_NAME||'
                Select nextval(''storage_adm.ss_seq'') Isn, '||pLoadIsn||',S.*
                    FROM (
                        With TT_DATA AS
                        (
                            Select T.*
                                from storage_adm.tt_rowid_r r, '||v_dest_tbl.TT_TABLE_NAME||' T
                                WHERE  T.RowID = r.RId
                        ),
                        Changed_Data AS
                        (' ||storage_adm.Get_Minus_slq(
                                          v_dest_tbl.TT_TABLE_NAME,
                                          v_dest_tbl.TABLE_NAME,
                                          v_dest_tbl.KeyField,
                                          'Where rowid  in (select rId from storage_adm.tt_rowid_r r ) ',
                                          'Where   ('||v_dest_tbl.KeyField||') in (
                                                Select '||v_dest_tbl.KeyField||' from TT_DATA ) '||vHistAdd
                                          ) ||
                       ')
                        Select *
                            from TT_DATA T
                            Where ('||v_dest_tbl.KeyField||') in 
                                (Select  * from Changed_Data)
                    ) s;'; 
            
            --DEBUG_PUT_LINE(vSql);
            execute vSql;
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step,  'End', -1, vSql);

            --seems not needed here as it is deleting block of data
            --execute 'delete from '||v_dest_tbl.TT_TABLE_NAME||' where  rowid in (select rId from tt_rowid_r)';


            /* грязно. для таблиц с датой начала-окнчания зачищаем неликвидные записи  */

            if coalesce(v_dest_tbl.IS_HISTTABLE, 0) = 1 then
                v_step = v_dest_tbl.TABLE_NAME||' Delete closed versions (dirty operation)';
                vSql ='
                    Delete
                        from '||v_dest_tbl.TABLE_NAME||' a
                        Where ('||v_dest_tbl.HIST_KEYFIELD||') In
                            (Select Isn from storage_adm.tt_rowid)
                            AND '|| v_dest_tbl.END_DATE_FLD||' <' ||v_dest_tbl.BEG_DATE_FLD||';';

                execute vSql;
                perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step,  '', -1, vSql);
            end if;
                
        else
            --full load
            v_step = v_dest_tbl.TABLE_NAME||' Insert';
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step,  'Begin');
            
            vSql:='
                Insert Into '||v_dest_tbl.TABLE_NAME||'
                Select nextval(''storage_adm.ss_seq''), '||pLoadIsn||', S.*
                    From (select * from '||v_dest_tbl.VIEW_NAME||') s;';
            execute vSql;
            
            GET DIAGNOSTICS vRc = ROW_COUNT;
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step, 'End', vRc, vSql);

        end if;
        
        perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_dest_tbl.TABLE_NAME||' Insert tt', 'End');
    end loop;  --by dest tables

    
    If (pFull=0) then
        -- clear log
        v_step = v_dest_tbl.TABLE_NAME||' Clear Log';
        perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step, 'Begin');
        
        Update storage_adm.SS_BUF_LOG
            Set Loadisn = pLoadIsn
            where coalesce(loadisn, 0) = 0
                and recisn in 
                    (Select Isn from storage_adm.tt_rowId)
                and procisn = pProcIsn;
                
        GET DIAGNOSTICS vRc = ROW_COUNT;
        perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcName||' By_tt_RowId', v_step, 'End', vRc, '');
    end if;


    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            RAISE EXCEPTION '(%:%:%)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;


