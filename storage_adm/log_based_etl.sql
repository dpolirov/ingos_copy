create or replace function STORAGE_ADM.SS_HISTLOG_ADD_CHUNK() returns void as $BODY$
declare
    v_max_dttm      timestamp;
    v_min_dttm      timestamp;
    v_proc_tbl      varchar(32);
    v_proc_tbl_key  varchar(1000);
    v_skiphist      varchar(1);
    vRc             bigint;
    vChunkRows      bigint;
    vSql            varchar;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.SS_HISTLOG_ADD_CHUNK';
    v_step          CHARACTER VARYING = 'NA';
begin

    v_step = 'Determine max_dttm and min_dttm';
    select coalesce(max(max_completed_dttm), timestamp '2014-01-31 00:00:00') into v_min_dttm 
        from storage_adm.histlog_chunks;
    
    select coalesce(max(completed_dttm), v_min_dttm) into v_max_dttm
        from replicais.replication_tasks_incr;    

    if v_max_dttm = v_min_dttm then
        -- no new replicator loads since last run
        return;
    end if;

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

    get diagnostics vChunkRows = ROW_COUNT;
    RAISE NOTICE '%: % records were inserted to tt_histlog_chunk for interval v_min_dttm=% v_max_dttm =%', v_step, vChunkRows, v_min_dttm, v_max_dttm;

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

    --get diagnostics vRc = ROW_COUNT;
    --RAISE NOTICE '%: % records were inserted to tt_histlog for tables of active processes', v_step, vRc;

    --Remove processed records from ss_histlog table
    v_step = 'Clear ss_histlog';
    delete from storage_adm.ss_histlog
        where loadisn is not null;
    --get diagnostics vRc = ROW_COUNT;
    --RAISE NOTICE '%: % records were deleted from ss_histlog', v_step, vRc;
   

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
            RAISE NOTICE 'processing for table %, key %', v_proc_tbl,v_proc_tbl_key;
            if v_proc_tbl_key = 'ISN' then
                --if key is isn
                --just take isn
                insert into storage_adm.tt_input(table_name, findisn, recisn)
                select v_proc_tbl, recisn, recisn from storage_adm.tt_histlog h
                    where h.table_name = v_proc_tbl;
                --get diagnostics vRc = ROW_COUNT;
                --RAISE NOTICE '%: % records were inserted to tt_input for table %, key %', v_step,vRc, v_proc_tbl,v_proc_tbl_key;
                
            else
                --if key is not isn
                --take specified field or expression from ais table
                vsql = '
                insert into storage_adm.tt_input(table_name, findisn, recisn)
                    select '''||v_proc_tbl||''', t.findisn, t.isn
                        from storage_adm.tt_histlog h inner join 
                            (select isn, '||v_proc_tbl_key||' findisn from ais.'||v_proc_tbl||' )t
                            on h.recisn = t.isn
                        where h.table_name='''||v_proc_tbl||'''';
                execute vsql;
                --get diagnostics vRc = ROW_COUNT;
                --RAISE NOTICE '%: % records were inserted to tt_input for table %, key %', v_step,vRc, v_proc_tbl,v_proc_tbl_key;

                --from hist table
                select skiphist into v_skiphist from replicais.replication_tables where replication_table_name = v_proc_tbl;
                if v_skiphist = 'N' then
                    vsql = '
                    insert into storage_adm.tt_input(table_name, findisn, recisn)
                        select '''||v_proc_tbl||''', t.'||v_proc_tbl_key||' findisn, t.isn
                            from storage_adm.tt_histlog h inner join 
                                hist.'||v_proc_tbl||' t
                                on h.recisn = t.isn and h.isn = t.histisn
                            where h.table_name='''||v_proc_tbl||'''';
                    execute vsql;
                end if;
            end if;
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
                
        --get diagnostics vRc = ROW_COUNT;
        --RAISE NOTICE '%: % records were inserted to ss_histlog for table %',v_step,vRc, v_proc_tbl;
    end loop;
    
    analyze storage_adm.ss_histlog;
    
    insert into storage_adm.histlog_chunks(isn, max_completed_dttm, chunk_rows) values 
        (nextval('storage_adm.histlog_chunks_seq'), v_max_dttm, vChunkRows);
    

    --Move records from histlog to histlog_arch
    v_step = 'Move histlog chunk to arch';
    insert into hist.histlog_arch
    select * from storage_adm.tt_histlog_chunk;

    delete from hist.histlog h 
        using storage_adm.tt_histlog_chunk c 
        where h.isn=c.isn;

        
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
create or replace function storage_adm.GetHistDB() returns timestamp as $BODY$
begin
    return shared_system.getparamt('histdb');
end;
$BODY$
language plpgsql;

create or replace function storage_adm.SetHistDB(vHistDB timestamp) returns void as $BODY$
begin
    perform shared_system.setparamt('histdb', vHistDB);
end;
$BODY$
language plpgsql;

create or replace function storage_adm.GetLoadIsn() returns numeric as $BODY$
begin
    return coalesce(shared_system.getparamn('loadisn'), 0);
end;
$BODY$
language plpgsql;

create or replace function storage_adm.SetLoadIsn(vLoadIsn numeric) returns void as $BODY$
begin
    perform shared_system.setparamn('loadisn', vLoadIsn);
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

--fields list  for closing hist records: all except isn, loadisn,  pEND_DATE_FIELD replaced with value of vHistDB
create or replace function STORAGE_ADM.GetHistTableFldList (pTABLE_NAME Varchar, pEND_DATE_FLD Varchar, vHistDB timestamp) Returns VARCHAR as $BODY$
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
                and not attisdropped
            order by a.attnum) Loop

        if (Cur_Col.attname <> lower(pEND_DATE_FLD)) Then
            vSql = vSql||','||Cur_Col.attname;
        else
            vSql = vSql||',timestamp '''||To_Char(vHistDB - interval '1 day', 'yyyy-mm-dd HH:MI:SS')||''' AS '||pEND_DATE_FLD;
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


--------------------------------------------------------------------------------
--
--  LOAD STORAGE
-- 
--------------------------------------------------------------------------------
create or replace function STORAGE_ADM.LOADSTORAGE(pProcIsn int, pFull int) returns void as $BODY$
declare
    vLoadIsn        numeric;
    vHistDb         timestamp; --was global
    vSql            varchar(1000);
    vProc           record;
    vRc             bigint;
    vTaskIsn        numeric;
    vProcessShortname varchar;
    vStartId        numeric;
    v_view          varchar;    
    v_dest_tbl      record;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.LOADSTORAGE(pProcIsn int, pFull smallint)';
    v_step          CHARACTER VARYING = 'NA';
    v_process_name  CHARACTER VARYING = 'Load From Logs';
begin
    --originally was logged to ais
    --ais.logevent('LOAD_STORAGE','storage_adm.load_storage.LoadStorage pProcIsn='||pProcIsn,pCode => 143503,pTComponent => 'LoadStorage');
    
    -- Prepare_Log_Buffer
    -- called if  pFillBufer=1
    RAISE NOTICE 'STORAGE_ADM.LOADSTORAGE started for procisn=% pFull=%', pProcIsn, pFull;
    v_step = 'Get Process Info';
    
    select * into vProc from storage_adm.ss_processes where isn = pProcIsn;
    v_process_name = v_process_name||' ('||pProcIsn||':'||vProc.Name||')';
    --REPLOG_I (vLoadIsn, 'LoadStorage','Get Log process '||vProc.Name, 'Begin', null, null);
    
    vLoadIsn = storage_adm.GetLoadIsn();
    if vLoadIsn = 0 then
        --process was started not from prcLoadStorage_*** therefore we dont know task isn
        vLoadIsn = storage_adm.CreateLoad(0);
    end if;
    perform storage_adm.RepLog_i (vLoadIsn, v_process_name, '',  'Begin');

    perform storage_adm.SetLoadParams(vLoadIsn, pProcIsn, pFull);
    
    -- if pFillBuffer=1 then
    -- actully buffer is always filled
    v_step = 'Clear SS_BUF_LOG';
    if pFull = 0 then
        delete from storage_adm.SS_BUF_LOG where procisn = pProcIsn and loadisn is not null;
    else
        delete from storage_adm.SS_BUF_LOG where procisn = pProcIsn;
    end if;
    get diagnostics vRc = ROW_COUNT;
    perform storage_adm.RepLog_i (vLoadIsn, v_process_name, v_step,  'Del', vRc, null);
    
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
        perform storage_adm.RepLog_i (vLoadIsn, v_process_name, v_step||' from ss_histlog',  'Ins', vRc, null);
        
        --for fullload tables - get from get_view query
        for v_view in (select get_view 
                        from storage_adm.ss_process_source_tables
                        where coalesce(get_view,'') <> '' and procisn = pProcIsn) loop
            execute('insert into storage_adm.ss_buf_log(isn, loadisn, recisn, procisn)
                select nextval(''storage_adm.ss_seq''), 0, v.*, '||pProcIsn||'
                    from '||v_view||' v where v.* is not null');
            --v returns one variable that have type of isn and can have any name
            get diagnostics vRc = ROW_COUNT;
            perform storage_adm.RepLog_i (vLoadIsn, v_process_name, v_step||' from get_view',  'Ins', vRc, null);
        end loop;
    else
        --pFull=1
        --for load type full get data from fullloadview query
        for v_view in (select FullLoadView 
                        from storage_adm.ss_process_source_tables
                        where coalesce(FullLoadView,'') <> '' and procisn = pProcIsn) loop
            execute('insert into storage_adm.ss_buf_log(isn, loadisn, recisn, procisn)
                select nextval(''storage_adm.ss_seq''), 0, v.*, '||pProcIsn||'
                    from ('||v_view||') v');
            --v returns one variable that have type of isn and can have any name
            get diagnostics vRc = ROW_COUNT;
            perform storage_adm.RepLog_i (vLoadIsn, v_process_name, v_step||' from fullLoadView',  'Ins', vRc, null);
        end loop;
    end if;
    --end if; pFillBuffer
    --REPLOG_I (vLoadIsn, 'LoadStorage','Get Log process '||vProc.Name, 'End', null, null);

    --call Load_Proc_By_tt_RowId
    --vSql = 'select STORAGE_ADM.Load_Proc_By_tt_RowId('||vLoadIsn||','||pProcIsn||','||pFull||')';
    --execute vSql;    
    /*dbms_jobs.job_submit(vSql, 1);    
    RAISE NOTICE '%: Job submitted%',v_step,vSql;*/
    perform STORAGE_ADM.Load_Proc_By_tt_RowId(vLoadIsn,pProcIsn,pFull);
    perform storage_adm.RepLog_i (vLoadIsn, v_process_name, '',  'End');

    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            /*vTaskIsn = shared_system.getparamn('taskisn');
            vProcessShortname = shared_system.getparamv('processshortname');
            vStartId = shared_system.getparamv('startid');            
            perform storage_adm.LOGREP(vTaskIsn,vProcessShortname,'('||v_function_name||' : '||v_step||' : '||sqlerrm,vStartId,-1,null,null);*/
            perform storage_adm.RepLog_i (vLoadIsn, v_process_name, v_step,  'ERROR', null, null, sqlerrm);
            RAISE EXCEPTION '(%:%:%)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;



    
create or replace function STORAGE_ADM.Load_Proc_By_tt_RowId(pLoadIsn numeric, pProcIsn int, pFull int) returns void as $BODY$
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
    perform storage_adm.SetHistDB(vHistDB);
    perform storage_adm.SetLoadIsn(pLoadIsn);
    
    RAISE NOTICE 'vHistDB=%', vHistDB;

    v_step = 'Populate tt_rowid';
    -- fill tt_rowid
    
    insert into storage_adm.tt_rowid(Isn)
        select distinct RECISN
            from storage_adm.SS_BUF_LOG
    where ProcIsn=pProcIsn;

    perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', '',  'Begin');
    --for each dest table of the process
    for v_dest_tbl In (Select * from storage_adm.ss_process_dest_tables Where Procisn = pProcIsn order by PRIORITY Asc) loop
        if pFull = 0 then
            --incremental load
            v_step = 'Execute before_script';
            if length(v_dest_tbl.before_script) > 1 then
                execute v_dest_tbl.before_script;
            end if;

            v_step = v_dest_tbl.TABLE_NAME||' Fill tt table from view';
            --perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Begin');
            execute '  truncate table '||v_dest_tbl.TT_TABLE_NAME;
            if coalesce(v_dest_tbl.TT_FUNCTION_NAME,'') <> '' then
                vSql = 'perform '||v_dest_tbl.TT_FUNCTION_NAME;
            else
                vSql = 'Insert into '||v_dest_tbl.TT_TABLE_NAME||' select v.* from '||v_dest_tbl.VIEW_NAME||' v';
            end if;
            execute vSql;
            GET DIAGNOSTICS vRc = ROW_COUNT;
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Ins', vRc, vSql);
            
            v_step = 'Execute after_script';
            -- empty for all proceses as of now
            if length(v_dest_tbl.after_script) > 1 then
                execute v_dest_tbl.after_script;
            end if;
            
            v_step = v_dest_tbl.TABLE_NAME||' Delete deleted';
            --perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Begin');
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
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Del', vRc, vSql);
            
            /* remove missing keys in case of set of records by v_dest_tbl.HIST_KEYFIELD has changed  */
            v_step = v_dest_tbl.TABLE_NAME||' Delete by KeyField';
            --perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Begin');
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
                execute vSql;
                GET DIAGNOSTICS vRc = ROW_COUNT;
                perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Del', vRc, vSql);
            else
                --table with history
                vSql = storage_adm.GetHistTableFldList(v_dest_tbl.TABLE_NAME, v_dest_tbl.END_DATE_FLD, vHistDB);

                vSql = '
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
                                                    (Select Isn from storage_adm.tt_rowid )
                                            except
                                            Select  '||v_dest_tbl.KeyField||' 
                                                from '||v_dest_tbl.TT_TABLE_NAME||'
                                            )
                                    ) S
                                WHERE '|| v_dest_tbl.END_DATE_FLD||'>='||v_dest_tbl.BEG_DATE_FLD|| ';';
                execute vSql;
                GET DIAGNOSTICS vRc = ROW_COUNT;
                perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Ins-hist', vRc, vSql);
                
                vSql='
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
                                        from '||v_dest_tbl.TT_TABLE_NAME||');';
                execute vSql;
                GET DIAGNOSTICS vRc = ROW_COUNT;
                perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Del1-hist', vRc, vSql);
                
                /* remove records that begin after hist date */
                vSql='
                        Delete 
                            from '||v_dest_tbl.TABLE_NAME||' a
                                Where  '||v_dest_tbl.BEG_DATE_FLD||'>='''||vHistDB ||''' 
                                    AND ('||v_dest_tbl.KeyField||')  In
                                        (Select  '||v_dest_tbl.KeyField||' 
                                            from '||v_dest_tbl.TT_TABLE_NAME||');';
                execute vSql;
                GET DIAGNOSTICS vRc = ROW_COUNT;
                perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Del2-hist', vRc, vSql);
            end if;
            
            /*--here was inserting by blocks in loop. this is second part of load_tt_by_rowid procedure
            v_step = v_dest_tbl.TABLE_NAME||' Populate tt_rowid_r';
            execute 'truncate table storage_adm.tt_rowid_r';
            execute 'Insert into storage_adm.tt_rowid_r(RId)
                        Select RowId from '||v_dest_tbl.TT_TABLE_NAME;
            */
            --insert new records
            v_step = v_dest_tbl.TABLE_NAME||' DeleteInsert';
            if coalesce(v_dest_tbl.IS_HISTTABLE,0) = 0 then
                vSql = 
                    'Delete 
                        from '||v_dest_tbl.TABLE_NAME||' a 
                        Where ('||v_dest_tbl.KeyField||')  In
                            ('||storage_adm.Get_Minus_slq(
                                    v_dest_tbl.TABLE_NAME,
                                    v_dest_tbl.TT_TABLE_NAME,
                                    v_dest_tbl.KeyField,
                                    'Where   ('||v_dest_tbl.KeyField||') in (
                                        Select '||v_dest_tbl.KeyField||' 
                                            from '||v_dest_tbl.TT_TABLE_NAME||' t)',
                                    ''
                                  )||
                           ');';

                vHistAdd = ' ';
                execute vSql;
                GET DIAGNOSTICS vRc = ROW_COUNT;
                perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Del', vRc, vSql);
            else

                vsql = storage_adm.GetHistTableFldList(v_dest_tbl.TABLE_NAME, v_dest_tbl.END_DATE_FLD, vHistDB);

                vsql = '
                    Insert Into '||v_dest_tbl.TABLE_NAME||'
                        SELECT nextval(''storage_adm.ss_seq'') ISN,S.*
                            FROM(
                                Select '||vsql||' 
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
                                                            from  '||v_dest_tbl.TT_TABLE_NAME||' T)',
                                                ''
                                             )||
                                       ')
                                ) S
                            WHERE   '|| v_dest_tbl.END_DATE_FLD||'>='||v_dest_tbl.BEG_DATE_FLD||';';
                execute vSql;
                GET DIAGNOSTICS vRc = ROW_COUNT;
                perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Ins-hist', vRc, vSql);

                vSql = '
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
                                                from '||v_dest_tbl.TT_TABLE_NAME||' t)',
                                        ''
                                     )||
                           ');';
                execute vSql;
                GET DIAGNOSTICS vRc = ROW_COUNT;
                perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Del-hist', vRc, vSql);

                vHistAdd:=' AND '||v_dest_tbl.END_DATE_FLD||'>='''||vHistDB ||'''';

            end if;

            vsql = '
                Insert  Into '||v_dest_tbl.TABLE_NAME||'
                Select nextval(''storage_adm.ss_seq'') Isn, '||pLoadIsn||',S.*
                    FROM (
                        WITH Changed_Data AS
                        (' ||storage_adm.Get_Minus_slq(
                                          v_dest_tbl.TT_TABLE_NAME,
                                          v_dest_tbl.TABLE_NAME,
                                          v_dest_tbl.KeyField_named,
                                          '',
                                          'Where   ('||v_dest_tbl.KeyField||') in (
                                                Select '||v_dest_tbl.KeyField||' from '||v_dest_tbl.TT_TABLE_NAME||' ) '||vHistAdd
                                          ) ||
                       ')
                        Select *
                            from '||v_dest_tbl.TT_TABLE_NAME||' T
                            Where ('||v_dest_tbl.KeyField||') in 
                                (Select  * from Changed_Data)
                    ) s;'; 
            execute vSql;
            GET DIAGNOSTICS vRc = ROW_COUNT;
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Ins', vRc, vSql);
            

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
                GET DIAGNOSTICS vRc = ROW_COUNT;
                perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Del-hist', vRc, vSql);
            end if;
                
        else
            --full load
            v_step = v_dest_tbl.TABLE_NAME||' Full load from view';
            
            vSql:='
                Insert Into '||v_dest_tbl.TABLE_NAME||'
                Select nextval(''storage_adm.ss_seq''), '||pLoadIsn||', S.*
                    From (select * from '||v_dest_tbl.VIEW_NAME||') s;';
            execute vSql;
            GET DIAGNOSTICS vRc = ROW_COUNT;
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'Ins', vRc, vSql);
        end if;
        
    end loop;  --by dest tables

    
    If (pFull=0) then
        -- clear log
        v_step = v_dest_tbl.TABLE_NAME||' Set loadisn to SS_BUF_LOG';
        --perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step, 'Begin');
        
        Update storage_adm.SS_BUF_LOG
            Set Loadisn = pLoadIsn
            where coalesce(loadisn, 0) = 0
                and recisn in 
                    (Select Isn from storage_adm.tt_rowId)
                and procisn = pProcIsn;
                
        GET DIAGNOSTICS vRc = ROW_COUNT;
        perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step, 'Del', vRc, '');
    end if;
    
    perform shared_system.session_data_table_cleanup('storage_adm', 'tt_rowid');
    perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', '',  'End');


    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            perform storage_adm.RepLog_i (pLoadIsn, 'Load '||pProcIsn||' By_tt_RowId', v_step,  'ERROR', null, vSql, sqlerrm);
            RAISE EXCEPTION '(%:%:%)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;



--------------------------------------------------------------------------------
--
--  LOGGING
-- 
--------------------------------------------------------------------------------
create or replace function STORAGE_ADM.replog_i(pLoadIsn numeric, pModule varchar, pOperation varchar,
                                                   pAction varchar, pObjCount bigint, pSqlText text, pErrMsg varchar) returns void as $BODY$
    declare vResult varchar;
    declare vSql varchar;
    declare dq char(2);
begin
    dq = '$'||'$';    
    vsql = 
        'insert into storage_adm.replog(isn, loadisn, module, operation, action, objcount, sqltext, errmsg) values ('||
        nextval('storage_adm.replog_seq')||','||
        coalesce(cast(pLoadIsn  as varchar), 'null')||','||
        coalesce(dq||pModule   ||dq, 'null')||','||
        coalesce(dq||pOperation||dq, 'null')||','||
        coalesce(dq||pAction   ||dq, 'null')||','||
        coalesce(cast(pObjCount as varchar), 'null')||','||
        coalesce(dq||pSqlText  ||dq,'null')||','||
        coalesce(dq||pErrMsg   ||dq,'null')||')';
    RAISE NOTICE '% : ploadisn=%, % %, % records affected. Query:%', pOperation, pLoadIsn, pModule, pAction, pObjCount, pSqlText;
    vResult = shared_system.autonomous_transaction_pl(vSql);
    if vResult <> '' then 
        RAISE EXCEPTION 'STORAGE_ADM.replog_i : %', vResult;
    end if;
end;
$BODY$
language plpgsql;

create or replace function STORAGE_ADM.replog_i(pLoadIsn numeric, pModule varchar, pOperation varchar,
                                                   pAction varchar, pObjCount bigint, pSqlText text) returns void as $BODY$
begin
    perform STORAGE_ADM.replog_i(pLoadIsn, pModule, pOperation, pAction, pObjCount, pSqlText, null);
end;
$BODY$
language plpgsql;

create or replace function STORAGE_ADM.replog_i(pLoadIsn numeric, pModule varchar, pOperation varchar,
                                                   pAction varchar) returns void as $BODY$
begin
    perform STORAGE_ADM.replog_i(pLoadIsn, pModule, pOperation, pAction, null, null, null);
end;
$BODY$
language plpgsql;

