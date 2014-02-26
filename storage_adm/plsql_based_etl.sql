create or replace function STORAGE_ADM.TaskManager() returns void as $BODY$
declare
    v_now           timestamp;
    v_process       record;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.TaskManager';
    v_step          CHARACTER VARYING = 'NA';
begin
    select current_timestamp into v_now;
    for v_process in (
        Select * from storage_adm.Sa_Processes 
            where STOPREP = 0 and NextRun is not null 
            order by nextrun) loop
        if  (v_process.NextRun1 <= v_now) Then
            perform storage_adm.execute_loged_report(v_process.isn, 1, 1);
            RAISE NOTICE 'rerun %: nextrun1=% now=%', v_process.isn, v_process.NextRun1, v_now;
        else
            if (v_process.NextRun1 is null) and (v_process.NextRun <= v_now) then
                RAISE NOTICE 'run %: nextrun=% now=%', v_process.isn, v_process.NextRun, v_now;
                perform storage_adm.execute_loged_report(v_process.isn, 1, 0);
            end if;
        end if;
    end loop;
    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            RAISE EXCEPTION '(% : % : %)', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql
volatile;

create or replace function STORAGE_ADM.CreateLoad(pProcType numeric) returns numeric as $BODY$
declare
    loadisn numeric;
Begin
    LoadIsn = nextval('storage_adm.repload_seq');
    insert into storage_adm.repload (isn, procisn)
        values (LoadIsn,pProcType); 
  Return LoadIsn;
End;
$BODY$
language plpgsql;

create or replace function STORAGE_ADM.SetLoadParams(pLoadIsn numeric, pProcIsn numeric, pFull int) returns void as $BODY$
Begin
    update storage_adm.repload 
        set classisn = pProcIsn, 
            loadtype = pFull 
        where isn = pLoadIsn;
End;
$BODY$
language plpgsql;


/* ставит метку pLoadisn в задачу  pTaskIsn*/
--TODO: Autonomos transaction?
create or replace function STORAGE_ADM.SetLoadToTask(pTaskIsn numeric,pLoadisn numeric) returns void as $BODY$
Begin
    Update storage_adm.Sa_Processes
         Set LASTRUNISN=pLoadIsn
         Where Isn=pTaskIsn;
end;
$BODY$
language plpgsql;


create or replace function STORAGE_ADM.GetLoadFromTask(pTaskIsn numeric) returns numeric as $BODY$
declare
  vLoadIsn numeric;
Begin
    select lastrunisn
        into vLoadIsn
        from storage_adm.Sa_Processes
        Where Isn=pTaskIsn;
    return vLoadIsn;
end;
$BODY$
language plpgsql;


create or replace function STORAGE_ADM.SetLoadStep(pLoadIsn numeric, pStep numeric) returns void as $BODY$
Begin
    Update storage_adm.Repload
        Set LASTISNLOADED = pStep
        Where Isn = pLoadIsn;
end;
$BODY$
language plpgsql;


create or replace function STORAGE_ADM.GetLoadStep(pLoadIsn numeric) returns numeric as $BODY$
declare
  vResult numeric;
Begin
  Select  coalesce(Max(LASTISNLOADED),0)
      Into vResult
      From storage_adm.Repload
      Where Isn=pLoadIsn;
      
  return vResult;
end;
$BODY$
language plpgsql;


create or replace function STORAGE_ADM.GetTaskStatus(pTask numeric) returns numeric as $BODY$
declare
    vResult numeric;
Begin
    Select Max( p.isruning )
        Into vResult
        From storage_adm.Sa_Processes p
    Where Isn=pTask;


    if vResult = 0 then --если не запущена - ищем признак завершения с ошибкой

    Select coalesce( coalesce(Max(decode(p1,null,null,-1)),Max(p.isruning)),0)
        Into vResult
        From storage_adm.Sa_Processes p 
            left join storage_adm.SA_TASKLOG tl
                on p.isn = tl.repisn
                and p.runisn = tl.p1
                and tl.p1 = -1            
        Where p.Isn=pTask;
    end if;
    return vResult;
end;
$BODY$
language plpgsql;


create or replace function STORAGE_ADM.GetTaskErrCode(pTask numeric) returns varchar as $BODY$
declare
    vResult varchar;
begin
    Select  Max(tl.event)
        Into vResult
        From storage_adm.Sa_Processes p, storage_adm.SA_TASKLOG tl
        Where p.Isn = pTask
            and  tl.repisn = pTask 
            and tl.p1 = p.runisn 
            and tl.p2 = -1;

    return vResult;
end;
$BODY$
language plpgsql;


CREATE OR REPLACE FUNCTION storage_adm.set_rep_nextrun(ptaskisn numeric, pnextrun timestamp)
  RETURNS void AS
$BODY$
declare
    vLastRun timestamp;
    vUpdSql varchar;
    vNextRun timestamp = pNextRun;
begin
    if vNextRun is null then 
        Select coalesce(NEXTRUN, current_timestamp), Fr.dateadd
            into vLastRun, vUpdSql
            from storage_adm.Sa_Processes r, storage_adm.Sa_freq fr
            where R.isn = pTaskIsn
                And R.usefreq = Fr.freq;

        vUpdSql:=replace(lower('select '||vUpdSql),
                        'lastrun',
                        'timestamp '''||to_char(vLastRun,'yyyy-mm-dd HH24:MI:SS')||'''');
        execute vUpdSql  into vNextRun;
        if current_timestamp > vNextRun Then
            vNextRun = vNextRun + (date_trunc('day', current_timestamp) - date_trunc('day', vNextRun) + interval '1 day');
        end if;
    end if;
    
    update storage_adm.Sa_Processes
    Set
        nextrun = vNextRun,
        nextrun1= Null,
        ErrCnt  = 0
    where isn = pTaskIsn;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;
  
  
  
--Log task status in autonomous transaction
create or replace function storage_adm.LOGREP
  (pAutonomous boolean,
   pREPISN numeric,
   pREPNAME VARCHAR,
   pEVENT VARCHAR,
   --pPDATE DATE:=NULL,
   --pPDATE2 DATE:=NULL,
   pP1 numeric,
   pP2 numeric,
   pREMARK VARCHAR,
   --pUPDATED date:=Sysdate,
   --pUPDATEDBY NUMBER:=NULL,
   --pTERMINAL VARCHAR2:=NULL,
   pRUNJOBISN numeric
   )returns void as $BODY$
declare
    vSql varchar ;
    vResult varchar;
    spP1 varchar = coalesce(cast(pP1 as varchar), 'null');
    spP2 varchar = coalesce(cast(pP2 as varchar), 'null');
    spRemark varchar = coalesce(pREMARK, '');
    spRUNJOBISN varchar = coalesce(cast(pRUNJOBISN as varchar),'null');
begin
    vSql = '
    insert into storage_adm.SA_TASKLOG (
        REPISN,
        REPNAME,
        EVENT,
        PDATE,PDATE2,
        P1,
        P2,
        pREMARK,
        UPDATED,
        UPDATEDBY,TERMINAL,
        job_isn)
    values ('||
        pREPISN||','''||
        pREPNAME||''',$event$'||
        pEVENT||
        '$event$,null,null,'||
        spP1||','||
        spP2||',$remark$'||
        spREMARK||'$remark$,'||
        'current_timestamp,'||
        'null,null,'||
        spRUNJOBISN||');';

    If pP2 = 0 Then
        vSql = vSql||'
        Update storage_adm.Sa_Processes 
        Set 
            IsRuning  = 1,
            LastRun   = date_trunc(''minute'', current_timestamp),
            RunIsn    = '||spP1||',
            RUNJOBISN = '||spRUNJOBISN||'
        Where Isn = '||pRepIsn||'; /*пишем запущен*/';
    elsif pP2 = -1 then
        vSql = vSql||'
        Update storage_adm.Sa_Processes
        Set
            IsRuning = 0,
            ErrCnt   = coalesce(ErrCnt,0) + 1,
            NextRun1 = null,
            NextRun  = null 
        Where Isn = '||pRepIsn||';  /*пишем остановлен + пишем ошибка (на счетчик)+след. запуск*/';
    else
        vSql = vSql||'
        Update storage_adm.Sa_Processes 
        Set 
            IsRuning = 0,
            ErrCnt   = 0
        Where Isn = '||pRepIsn||'; /*пишем остановлен + сброс счетчика ошибок*/';
    end if;

    if pAutonomous  then
    vResult = shared_system.autonomous_transaction(vSql);
    
        if vResult <> '' then 
            RAISE notice 'STORAGE_ADM.LOGREP : %', vResult;
        end if;
    else
    execute vSql;
    end if ;
END; -- Procedure
$BODY$
language plpgsql;

create or replace function  storage_adm.TaskStarter(pTaskIsn numeric, pStartId numeric, pProcessShortname varchar, pSql varchar) returns void as $BODY$
begin
    perform shared_system.setparamn('taskisn',pTaskIsn);
    perform shared_system.setparamv('processshortname',pProcessShortname);
    perform shared_system.setparamn('startid', pStartId);
    execute pSql;
    --do not log end of complex task if not all child tasks ended. processfinished is set in the end of complex task
    if coalesce(shared_system.getparamn('processfinished'), 1) <> 1 then
        perform storage_adm.LOGREP(false, pTaskIsn, pProcessShortname, 'END TASK', pStartId, 1, pSql, null);
    end if;
    EXCEPTION
    WHEN OTHERS THEN
    BEGIN
        perform storage_adm.LOGREP(false, pTaskIsn, pProcessShortname,'ERROR in '||sqlerrm,pStartId,-1,null,null);
    END;
end;
$BODY$
language plpgsql;


--PROCEDURE EXECUTE_LOGED_REPORT
CREATE OR REPLACE FUNCTION storage_adm.execute_loged_report(ptaskisn numeric, pmovetime integer, prerun integer)
  RETURNS void AS
$BODY$
declare
    /*parameters always constant */
    pBlockMode Integer = 0; /* блокирующий или не блокирующий режим запуска */
    pIsRaise Integer = 0;  /* признак генерации ошибки запуска*/
    pLogEnd Integer = 1; /* признак автологировани завершения выполнения*/
    pNextRun  Varchar = 'Null'; /* дата следующего запуска*/
    
    vNumRunning integer;
    vjobid numeric;
    vStartId integer;
    vBaseSql varchar;
    vComplexTaskSql varchar;
    vSql varchar;
    vStarterSql varchar;
    vRunJobIsn int;
    vNextRun  Varchar(32):=pNextRun;

    v_process       record;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.EXECUTE_LOGED_REPORT';
    v_step          CHARACTER VARYING = 'NA';
begin
/*ВЫПОЛНЯЕТ ОТЧЕТ КАК ДЖОБ С ЛОГИРОВАНИЕМ*/

    /* ПРОВЕРКА И ОГРАНИЧЕНИЕ НА ЧИСЛО ЗАПУЩЕННЫХ ЗАДАЧ*/
    v_step = 'Check number of running tasks';
    SELECT COUNT(*) INTO vNumRunning FROM storage_adm.Sa_Processes WHERE ISRUNING=1;
    if vNumRunning > 10 then   
        return;
    end if;
    
    v_step = 'Get task details';
    vStartId = NEXTVAL('storage_adm.seq_task_start'); /*ИДЕНТИФИКАТОР ЗАПУСКА*/    
    SELECT * into v_process FROM storage_adm.Sa_Processes WHERE Isn = pTaskIsn;
    -- there was a cycle but actually there is only one process per pTaskIsn
    

    if v_process.ISDISPECHER=1 and vNextRun='Null' and pRerun=0 then 
        --set dispetcher task for execution for iterating through child tasks
        perform storage_adm.Set_Rep_NextRun(pTaskIsn, cast(date_trunk('minute', current_timestamp + interval '1 minute') as timestamp without time zone));
    elsif pMoveTime = 1 then
        --pMoveTime is always 1
        perform storage_adm.Set_Rep_NextRun(pTaskIsn, null);
    end if;
    
    vBaseSql = v_process.sqltext;
    if v_process.rerunsqltext is not null then
        vBaseSql = v_process.rerunsqltext;
    end if;
    vSql = 'select '||v_process.SHEMANAME||'.'||vBaseSql||';';

    --sql for dbms_jobs
    vStarterSql = 'select storage_adm.TaskStarter('||pTaskIsn||','||vStartId||','''||
                    v_process.Shortname||''','''||vSql||''')';
    --if task is complex then do it in the same transaction - it only checks statuses and submit other tasks
    if v_process.ISDISPECHER=1 then
        v_step = 'Execute complex task';
        --log start only whe the task initially starts, do not log when it iterates through child tasks
        if v_process.lastrunisn = 0 then
            perform storage_adm.LOGREP(false, pTaskIsn, v_process.shortname,'START COMPLEX TASK',vStartId,0,vSql,null);
        end if;
        execute vStarterSql;
    else
        --sole tasks are executed via dbms_jobs
        v_step = 'Submit task to dbms_jobs';
        select dbms_jobs.job_submit(vStarterSql, 1) into vRunJobIsn;
        --log start of task 
        perform storage_adm.LOGREP(false, pTaskIsn, v_process.shortname,'START TASK',vStartId,0,vSql,vRunJobIsn);
    end if;
    
    exception
        when Others then
        begin
            perform storage_adm.LOGREP(false, pTaskIsn,v_process.shortname,
                'ERROR in '||v_function_name||' : '||v_step||' : '||SQLERRM,
                vStartId,-1,vSql,null);
        end;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;