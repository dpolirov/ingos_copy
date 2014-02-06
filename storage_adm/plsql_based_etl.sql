create or replace function STORAGE_ADM.TaskManager() returns void as $BODY$
declare
    v_now           timestamp;
    v_process       record;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.TaskManager';
    v_step          CHARACTER VARYING = 'NA';
begin
    v_now = date_trunc('minute', current_timestamp);
    for v_process in (Select * from storage_adm.Sa_Processes where isruning = 0 and STOPREP = 0 order by nextrun) loop
        if  (v_process.NextRun1 <= v_now) Then
            perform execute_loged_report(v_process.isn, 1, 1);
            RAISE NOTICE 'rerun';
        else
            if (v_process.NextRun1 is null) and ((v_process.NextRun <= v_now) or (v_process.NextRun is null)) then
                RAISE NOTICE 'run %', v_process.isn;
                --perform execute_loged_report(v_process.isn, 1, 0);
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
                    'timestamp '''||to_char(vLastRun,'yyyy-mm-dd HH:MI:SS')||'''');
                    raise notice '%' , vUpdSql;
    execute vUpdSql  into vNextRun;
    if current_timestamp > vNextRun Then
        vNextRun = vNextRun + (date_trunc('day', current_timestamp) - date_trunc('day', vNextRun) + interval '1 day');
    end if;

    update storage_adm.Sa_Processes R
    Set
        R.nextrun = vNextRun,
        R.nextrun1= Null,
        R.ErrCnt  = 0
    where R.isn = pTaskIsn;
end;
$BODY$
language plpgsql;

--Log task status in autonomous transaction
create or replace function storage_adm.LOGREP
  (pREPISN numeric,
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
    spRemark varchar = coalesce(pREMARK, 'null');
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

    raise notice '%', vSql;
    vResult = shared_system.autonomous_transaction(vSql);
    if vResult <> '' then 
        RAISE EXCEPTION 'STORAGE_ADM.LOGREP : %', vResult;
    end if;
END; -- Procedure
$BODY$
language plpgsql;




--PROCEDURE EXECUTE_LOGED_REPORT
create or replace function STORAGE_ADM.EXECUTE_LOGED_REPORT(pTaskIsn numeric, pMoveTime integer, pReRun integer) returns void as $BODY$
declare
    /*parameters always constant */
    pBlockMode Integer:=0, /* блокирующий или не блокирующий режим запуска */
    pIsRaise Integer:=0,  /* признак генерации ошибки запуска*/
    pLogEnd Integer:=1, /* признак автологировани завершения выполнения*/
    pNextRun  Varchar2:='Null' /* дата следующего запуска*/
    
    vNumRunning integer;
    vjobid numeric;
    vStartId integer;
    vBaseSql varchar;
    vSql varchar;
    vI numeric;
    vNextRun  Varchar(32):=pNextRun;

    v_process       record;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.EXECUTE_LOGED_REPORT';
    v_step          CHARACTER VARYING = 'NA';
begin
/*ВЫПОЛНЯЕТ ОТЧЕТ КАК ДЖОБ С ЛОГИРОВАНИЕМ*/

    /* ПРОВЕРКА И ОГРАНИЧЕНИЕ НА ЧИСЛО ЗАПУЩЕННЫХ ЗАДАЧ*/
    SELECT COUNT(*) INTO vNumRunning FROM storage_adm.Sa_Processes WHERE ISRUNING=1;
    if vNumRunning > 10 then   
        return;
    end if;
    
    vStartId = NEXTVAL('storage_adm.seq_task_start'); /*ИДЕНТИФИКАТОР ЗАПУСКА*/
    
    SELECT * into v_process FROM storage_adm.Sa_Processes WHERE Isn = pTaskIsn;
    -- there was a cycle but actually there is only one process per pTaskIsn
    
    /*В КОНЕЦ БЛОКА ПИШЕМ ОБРАБОЧИК "ЗАКОНЧИЛИ" И ОШИБКИ ВЫПОЛНЕНИЯ*/
    IF (pRerun=1) and (v_process.RERUNSQLTEXT is not Null) Then
        -- RERUNSQLTEXT is always null
        vSql = v_process.SHEMANAME||'.'||v_process.RERUNSQLTEXT;
    ELSE
        vSql = v_process.SHEMANAME||'.'||v_process.sqltext;
    END IF;

    
    vSql = vSql||'select storage_adm.LOGREP('||pTaskIsn||','''||v_process.shortname||''','''||'END TASK'||''','||vStartId||',1);'; --task finished
    if pMoveTime = 1 then
        --pMoveTime is always 1
        vSql = vSql||'select storage_adm.Set_Rep_NextRun('||pTaskIsn||');';
    end if;
    --Move exception handler to executed procedures
    /*
    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            perform storage_adm.LOGREP(shared_system.getparamn('taskisn'), shared_system.getparamn('processshortname'), sqlerrm, shared_system.getparamn('startid'), -1);
        END;    
    */
    --vSql = vSql||'Exception When Others Then ';
    --vSql = vSql||'   perform storage_adm.LOGREP('||pTaskIsn||','''||v_process.shortname||''',SQLCODE ||'''||':'||'''||SQLERRM,null,null,'||vStartId||',-1);';
    --vSql = vSql||'End;';

    Raise notice '%', vSql;
    --DBMS_OUTPUT.Put_Line( Upper(Nvl(v_process.SHEMANAME,User )));
    --DBMS_OUTPUT.Put_Line( vNextRun );
    
    /*ЗАПУСТИЛИ ОБРАБОТЧИК ДЖОБОМ*/
    if v_process.ISDISPECHER = 1 and vNextRun = 'Null' and pRerun = 0 then
        vNextRun = current_timestamp + interval '1 minute';
    end if;
    if pRerun = 0 then
        storage_adm.SetLoadToTask(pTaskIsn, 0); -- при новом запуске сбрасываем старый Loadisn в 0
    end if;
    --jobid = runjob(vSql, Upper(v_process.SHEMANAME), vNextRun);
    --DBMS_JOB.SUBMIT(jobid,vSql,SYSDATE,null);
    perform LOGREP(pTaskIsn,v_process.shortname,'START TASK',null,null,StartId,0,pRemark=>vSql||' ' ||User||' '||Nvl(v_process.SHEMANAME,User),
      pRUNJOBISN=>jobid);
    
    /* pBlockMode is never specified
      IF pBlockMode=1 Then
        loop
          Select Count(*)
          into vI
          from sys.dba_jobs
          Where Job=jobid;
          Exit When  vI=0;
          DBMS_LOCK.SLEEP(60);
          rollback; -- чтоб не тух
          -- commit;
        end loop;

        select Max(tl.event),count(*)
        into vBaseSql,vI
        from storage_adm.SA_TASKLOG tl
        where tl.repisn=pTaskisn and tl.p1=StartId and tl.p2=-1;

        if vI>0 and pIsRaise=1 Then
        RAISE_APPLICATION_ERROR(-20000,vBaseSql);
        end if;
      END IF;
    */
      
    exception
      when Others then
        /*ПИШЕМ ОШИБКУ*/
        perform LOGREP(pTaskIsn,v_process.shortname,SQLERRM,StartId,-1,vSql,null);
        Raise;
     /* pIsRaise is never specified
       If pIsRaise=1 Then Raise; end if;
     */
    end;
end;
$BODY$
language plpgsql;


