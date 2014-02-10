create or replace function STORAGE_ADM.TaskManager() returns void as $BODY$
declare
    v_now           timestamp;
    v_process       record;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.TaskManager';
    v_step          CHARACTER VARYING = 'NA';
begin
    v_now = date_trunc('minute', current_timestamp);
    for v_process in (
        Select * from storage_adm.Sa_Processes 
            where isruning = 0 and STOPREP = 0 and v_process.NextRun is not null 
            order by nextrun) loop
        if  (v_process.NextRun1 <= v_now) Then
            perform execute_loged_report(v_process.isn, 1, 1);
            RAISE NOTICE 'rerun';
        else
            if (v_process.NextRun1 is null) and (v_process.NextRun <= v_now) then
                RAISE NOTICE 'run %', v_process.isn;
                perform execute_loged_report(v_process.isn, 1, 0);
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
language plpgsql;



/* ставит метку pLoadisn в задачу  pTaskIsn*/
--TODO: Autonomos transaction?
create or replace function STORAGE_ADM.SetLoadToTask(pTaskIsn numeric,pLoadisn numeric) returns void as $BODY$
declare
  vSql varchar(4000);

Begin
    vSql:='
    Update storage_adm.Sa_Processes
         Set LASTRUNISN='||pLoadIsn||'
         Where Isn='||pTaskIsn||';';
    execute vSql;
end;
$BODY$
language plpgsql;

create or replace function storage_adm.Set_Rep_NextRun(pTaskIsn numeric) returns void as $BODY$
declare
    vLastRun timestamp;
    vNextRun timestamp;
    vUpdSql varchar;
begin
    Select coalesce(NEXTRUN, current_timestamp), Fr.dateadd
        into vLastRun, vUpdSql
        from storage_adm.Sa_Processes r, storage_adm.Sa_freq fr
        where R.isn = pTaskIsn
            And R.usefreq = Fr.freq;

    vUpdSql:=replace(lower('select '||vUpdSql),
                    'lastrun',
                    'timestamp '''||to_char(vLastRun,'yyyy-mm-dd HH24:MI:SS')||'''');
                    raise notice '%' , vUpdSql;
    execute vUpdSql  into vNextRun;
    if current_timestamp > vNextRun Then
        vNextRun = vNextRun + (date_trunc('day', current_timestamp) - date_trunc('day', vNextRun) + interval '1 day');
    end if;

    update storage_adm.Sa_Processes
    Set
        nextrun = vNextRun,
        nextrun1= Null,
        ErrCnt  = 0
    where isn = pTaskIsn;
end;
$BODY$
language plpgsql;


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
        pREPNAME||''','''||
        pEVENT||
        ''',null,null,'||
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




--PROCEDURE EXECUTE_LOGED_REPORT
create or replace function STORAGE_ADM.EXECUTE_LOGED_REPORT(pTaskIsn numeric, pMoveTime integer) returns void as $BODY$
declare
    /*parameters always constant */
    pReRun  Integer = 0;
    pBlockMode Integer = 0; /* блокирующий или не блокирующий режим запуска */
    pIsRaise Integer = 0;  /* признак генерации ошибки запуска*/
    pLogEnd Integer = 1; /* признак автологировани завершения выполнения*/
    pNextRun  Varchar = 'Null'; /* дата следующего запуска*/
    
    vNumRunning integer;
    vjobid numeric;
    vStartId integer;
    vBaseSql varchar;
    vSql varchar;
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
    
    v_step = 'Generate task sql';
    vBaseSql = 'select '||v_process.SHEMANAME||'.'||v_process.sqltext||';';    
    if pMoveTime = 1 then
        --pMoveTime is always 1
        perform storage_adm.Set_Rep_NextRun(pTaskIsn);
    end if;

    --log end of task in the end of job
    vSql = 'begin;
	select shared_system.setparamn(''taskisn'','||pTaskIsn||
		'), shared_system.setparamv(''processshortname'','''||v_process.Shortname||
		'''), shared_system.setparamn(''startid'','||vStartId||');'||	
	vBaseSql||
	'select storage_adm.LOGREP(false, '||pTaskIsn||','''||v_process.Shortname||''','''||'END TASK'||''','||vStartId||',1,$SQL$'||vBaseSql||'$SQL$,null);'||
	'commit;';

    v_step = 'Submit task to dbms_jobs';
    raise notice 'prepared to submit: %',vSql;
    select dbms_jobs.job_submit(vSql, 1) into vRunJobIsn;
    raise notice 'submitted: %',vRunJobIsn;
    --log start of task 
    perform storage_adm.LOGREP(false, pTaskIsn, v_process.shortname,'START TASK',vStartId,0,vBaseSql,vRunJobIsn);
    
    exception
        when Others then
        begin
            perform storage_adm.LOGREP(false, pTaskIsn,v_process.shortname,
                'ERROR in '||v_function_name||' : '||v_step||' : '||SQLERRM,
                vStartId,-1,vSql,null);
            --RAISE EXCEPTION '(% : % : %)', v_function_name, v_step, sqlerrm;
        end;
end;
$BODY$
language plpgsql;
