
--default call
--select coalesce(storage_adm.GenerateSoleProcedure(1026, false, 0, null, null, null, null) ,'null');

create or replace function storage_adm.GenerateSoleProcedure(
        pTaskIsn        numeric,
        pDeploy           boolean,
        IsErrContinue     numeric  ,
        pUserTopBanner    varchar,
        pUserBottomBanner varchar,
        pTaskName varchar,
        pParentSchema varchar
)
RETURNS TEXT as $GBODY$
Declare
  vsql      varchar;
  vProcName varchar;
  vUserBegBanner     varchar;
  vUserEndBanner     varchar;
  vChildSql          varchar;
  vBegAutoBanner     varchar;
  vEndAutoBanner     varchar;
  vSqlText           varchar;
  vTemplateCore      varchar;
  vCore              varchar;
  vTemplCoreTop      varchar;
  vTemplCoreBot      varchar;
  vSqlCoreTop        varchar;
  vSqlCoreBot        varchar;
  vTaskName          varchar;
  vParentSchema      varchar;
  vCurrdate          timestamp;
  vLastIsnLoaded     numeric;
  Cur record;
begin
    -- Этап 1: генерация кода процедруры и компиляция ---------------------------------------------------------
    select coalesce(PROCNAME, '') into vProcName FROM storage_adm.Sa_Processes where ISN = pTaskIsn;
    if not FOUND then
        vProcName = 'prcLoadStorageTest_'||pTaskIsn;
    end if;


    select SHEMANAME into vParentSchema  from storage_adm.Sa_Processes  where ISN = pTaskIsn;
    if not FOUND then
        vParentSchema = pParentSchema;
    end if;

    if pTaskName is null then
        select coalesce(shortname, '') into vTaskName  from storage_adm.Sa_Processes  where ISN = pTaskIsn;
    else 
        vTaskName = pTaskName;
    end if;
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

--******************************************************************************
--** СБОРКА ЯДРА ПРОЦЕДУРЫ *****************************************************
--******************************************************************************
  if pUserTopBanner is null then
      select coalesce(cvalue, '') into vUserBegBanner from storage_adm.sa_params where NAME = 'sole:user:top_banner';
   else
      vUserBegBanner = pUserTopBanner;
   end if;

   if pUserBottomBanner is null then
      select coalesce(cvalue, '') into vUserEndBanner from storage_adm.sa_params where NAME = 'sole:user:bottom_banner';
   else
      vUserEndBanner = pUserBottomBanner;
   end if;

   select coalesce(cvalue, '') into vTemplCoreTop from storage_adm.sa_params where NAME = 'sole:core:top_banner';
   select coalesce(cvalue, '') into vTemplCoreBot from storage_adm.sa_params where NAME = 'sole:core:bottom_banner';

   if IsErrContinue = 0 then
       select coalesce(cvalue, '') into vTemplateCore      from storage_adm.sa_params where NAME = 'sole:core:failed:stop';
   else
       select coalesce(cvalue, '') into vTemplateCore      from storage_adm.sa_params where NAME = 'sole:core:failed:continue';
   end if;

   select current_timestamp into vCurrdate;

   select '--*** Generated Date '||vCurrdate||' ************************************ ' into vChildSql;
   vLastIsnLoaded = 0;

   FOR Cur In (select coalesce(atomname, '') as ATOMNAME, coalesce(atomsource, '') as ATOMSOURCE
        from storage_adm.sa_sole_ref sc where sc.taskisn = pTaskIsn order by ATOMROWNUM
   ) Loop
      -- обработка sqltext дочернего процесса **********************************
      vLastIsnLoaded = vLastIsnLoaded + 1;

      vSqlCoreTop = replace(vTemplCoreTop,'{ATOMNAME}',Cur.ATOMNAME );

      vSqlCoreBot = replace(vTemplCoreBot,'{ATOMNAME}',Cur.ATOMNAME );
      vSqlCoreBot = replace(vSqlCoreBot,'{ATOMLASTISNLOADED}', vLastIsnLoaded );

      vCore = replace(vTemplateCore,'{ATOMTASKISN}',pTaskIsn);
      vCore = replace(vCore,'{ATOMNAME}',pTaskIsn);

      vSqlText =         chr(10)||
         Cur.ATOMSOURCE ||chr(10)||
         vCore          ||chr(10);

      -- конечная сборка ---------------
      vChildSql = vChildSql ||chr(10)||
      '--******************************************************************************************'||chr(10)||
      '--*********  '||vLastIsnLoaded|| '  ****************************************************************************'||chr(10)||
      vUserBegBanner         ||chr(10)||
      vSqlCoreTop            ||chr(10)||
      vSqlText               ||chr(10)||
      vSqlCoreBot            ||chr(10)||
      vUserEndBanner         ||
      '--******************************************************************************************'||chr(10);

	
   end loop;

--******************************************************************************
--** Сборка Верхней и Нижней Шапки *********************************************
--******************************************************************************
  vBegAutoBanner = replace(storage_adm.ComplexTaskGetAutoTopBanner(pTaskIsn,vParentSchema),'{TASKNUMBER}',pTaskIsn::varchar );
  vEndAutoBanner = replace(storage_adm.ComplexTaskGetAutoBottomBanner(pTaskIsn,vTaskName),'{ISTEPNUMBER1}',vLastIsnLoaded);
  vEndAutoBanner = replace(vEndAutoBanner,'{ISTEPNUMBER2}',vLastIsnLoaded+1);


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
--******************************************************************************
--** КОНЕЧНАЯ СБОРКА ***********************************************************
--******************************************************************************
--    Execute Immediate 'ALTER SESSION SET CURRENT_SCHEMA='||vParentSchema;

    vsql = 'create or replace function '||vParentSchema||'.'||vProcName||'() returns void as $BODY$

  -- Procedure Has been Generated by '
   || vCurrdate                    ||chr(10)
   || storage_adm.SoleTaskGetAutoTopBanner(pTaskIsn, vParentSchema)||chr(10)
   || coalesce(pUserTopBanner,'')                                     ||chr(10)
   || vChildSql                                          ||chr(10)
   || coalesce(pUserBottomBanner,'')                                  ||chr(10)
   || storage_adm.SoleTaskGetAutoBottomBanner(pTaskIsn, vTaskName) ||chr(10)
   || '$BODY$' || chr(10)
   || 'language plpgsql volatile;';

   vsql=replace(vsql, CHR (13),'');

--   insert into tbl_kudi_x3 values (vsql);
--   commit;

   --execute vsql;


--  Execute Immediate 'ALTER SESSION SET CURRENT_SCHEMA=STORAGE_ADM';
    if pDeploy then
        execute vSql;
    end if;
    
    RETURN vsql;
/*
Exception When Others Then
    begin
        STORAGE_ADM.TASK_ADMIN.LOGREPCHILD(pTaskIsn,'Task_ADMIN.GenerateParentProcedure:Error',SQLCODE ||':'||SQLERRM,
                               null,null,0,
                               -1,null,sysdate,
                               NULL,null,null,
                               null, vsql);
        raise;
    end;*/

end;-- GenerateSoleProcedure
$GBODY$
language plpgsql;




--default call
--select coalesce(storage_adm.GenerateComplexProcedure(663, null, 0, null, null, null) ,'null');

create or replace function storage_adm.GenerateComplexProcedure(
        pParentIsn        numeric ,
        pDeploy           boolean,
        pParentSchema     varchar,
        IsErrContinue  numeric  ,
        pUserTopBanner    varchar,
        pUserBottomBanner varchar,
        pTaskName varchar
)
RETURNS TEXT as $GBODY$
Declare
  vsql               varchar;
  vProcName          varchar;
  vTemplateBegBanner varchar;
  vTemplateEndBanner varchar;
  vChildSql          varchar;
  vBegAutoBanner     varchar;
  vEndAutoBanner     varchar;
  vSqlText           varchar;
  vTemplateCore      varchar;
  vTaskName          varchar(100);
  vCurrdate          timestamp;
  vLastIsnLoaded     numeric;
  vCurSchema          varchar;
  vPrevUser          varchar;
  cur           record;
begin
    -- Этап 1: генерация кода процедруры и компиляция ---------------------------------------------------------
    
--    if vProcName = '' then
   vProcName = 'prcLoadStorageTest_'||pParentIsn;
--    end if;
   if pTaskName is null then
        select shortname into vTaskName from storage_adm.sa_processes where isn=pParentIsn;
   else
        vTaskName = pTaskName;
   end if;
--******************************************************************************
--** СБОРКА ЯДРА ПРОЦЕДУРЫ *****************************************************
--******************************************************************************
  if pUserTopBanner is null then
      select coalesce(cvalue, '') into vTemplateBegBanner from storage_adm.sa_params where NAME = 'comlex:user:top_banner';
   else
      vTemplateBegBanner = pUserTopBanner;
   end if;

   if pUserBottomBanner is null then
      select coalesce(cvalue, '') into vTemplateEndBanner from storage_adm.sa_params where NAME = 'comlex:user:bottom_banner';
   else
      vTemplateEndBanner = pUserBottomBanner;
   end if;

   if IsErrContinue = 0 then
       select coalesce(cvalue, '') into vTemplateCore      from storage_adm.sa_params where NAME = 'comlex:core:failed:stop';
   else
       select coalesce(cvalue, '') into vTemplateCore      from storage_adm.sa_params where NAME = 'comlex:core:failed:continue';
   end if;

    if pParentSchema is null then
        SELECT SHEMANAME into vCurSchema FROM  storage_adm.sa_Processes where ISN = pParentIsn;
    else
        vCurSchema = pParentSchema;
    end if;



   select current_timestamp into vCurrdate;

   select '--*** Generated Date '||vCurrdate||' ************************************ ' into vChildSql;
   vLastIsnLoaded = 0;

   FOR cur In (select childisn from storage_adm.sa_complex_ref where parentisn = pParentIsn order by CHILDROWNUM
   ) Loop
      -- обработка sqltext дочернего процесса **********************************
      vSqlText = vTemplateCore;
      vSqlText = replace(vSqlText,'{TASKNUMBER}',cur.CHILDISN::varchar) ;
      vSqlText = replace(vSqlText,'{ITASKSTEP0}',vLastIsnLoaded::varchar) ;
      vSqlText = replace(vSqlText,'{ITASKSTEP1}',(vLastIsnLoaded+1)::varchar) ;
      vSqlText = replace(vSqlText,'{ITASKSTEP2}',(vLastIsnLoaded+2)::varchar) ;
      vLastIsnLoaded = vLastIsnLoaded + 2;

      -- конечная сборка
      vChildSql = vChildSql ||chr(10)||
      vTemplateBegBanner     ||chr(10)||
      vSqlText               ||chr(10)||
      vTemplateEndBanner     ||chr(10);
	
   end loop;

--******************************************************************************
--** Сборка Верхней и Нижней Шапки *********************************************
--******************************************************************************
  vBegAutoBanner = replace(storage_adm.ComplexTaskGetAutoTopBanner(pParentIsn,vCurSchema),'{TASKNUMBER}',pParentIsn::varchar );
  vEndAutoBanner = replace(storage_adm.ComplexTaskGetAutoBottomBanner(pParentIsn,vTaskName),'{ISTEPNUMBER1}',vLastIsnLoaded::varchar);
  vEndAutoBanner = replace(vEndAutoBanner,'{ISTEPNUMBER2}',(vLastIsnLoaded+1)::varchar);

--******************************************************************************
--** КОНЕЧНАЯ СБОРКА ***********************************************************
--******************************************************************************


    vsql = 'create or replace function '||vCurSchema||'.'||vProcName||'() returns void as $BODY$
  -- Procedure Has been Generated by '
   || vCurrdate||chr(10)
   || vBegAutoBanner ||chr(10)
   || chr(10)
--   || storage_adm.GenerateComplexChildSql(pParentIsn,IsErrContinue,pUserTopBanner,pUserBottomBanner)||chr(10)
   || vChildSql  || chr(10)
   || chr(10)
   || vEndAutoBanner   ||chr(10)
   || '$BODY$'  ||chr(10)
   || 'language plpgsql volatile;';

   --execute vsql;
    if pDeploy then
        execute vSql;
    end if;

    RETURN vsql;

/*
Exception When Others Then
    begin
        storage_adm.LOGREPCHILD(pParentIsn,'storage_adm.GenerateParentProcedure:Error',SQLCODE ||':'||SQLERRM,
                               null,null,0,
                               -1,null,null,
                               null,null,null,
                               null, vsql);
        raise;
    end;
*/
end;-- GenerateComplexProcedure
$GBODY$
language plpgsql;
--******************************************************************************************************************************************
--******************************************************************************************************************************************


create or replace function storage_adm.SoleTaskGetAutoTopBanner( pParentIsn numeric, pParentSchema varchar )
RETURNS varchar as $BODY$
declare
    vReturnSql varchar;
begin

    select coalesce(CVALUE, '')
      into vReturnSql
      from storage_adm.sa_params
     where NAME = 'sole:auto:top_banner';

select replace(vReturnSql,'{ParentSchema}',pParentSchema )
  into vReturnSql;

select replace(vReturnSql,'{PARENTISN}',pParentIsn )
  into vReturnSql;

  RETURN vReturnSql;

END; -- SoleTaskGetAutoTopBanner
$BODY$
language plpgsql;
--******************************************************************************************************************************************

--******************************************************************************************************************************************
create or replace function storage_adm.SoleTaskGetAutoBottomBanner( pParentIsn numeric, pTaskName varchar )
RETURNS varchar as $BODY$
declare
    vReturnSql varchar;
begin

    select coalesce(sprm.CVALUE, '')
      into vReturnSql
      from storage_adm.sa_params sprm
     where sprm.NAME = 'sole:auto:bottom_banner';

/*select replace(vReturnSql,'{TASKNAME}',s.shortname )
  into vReturnSql
  from storage_adm.Sa_Processes s
 where s.isn = pParentIsn;*/

 select replace(vReturnSql,'{TASKNAME}',pTaskName )
  into vReturnSql;


  RETURN vReturnSql;

END; -- SoleTaskGetAutoBottomBanner
$BODY$
language plpgsql;
--******************************************************************************************************************************************
--******************************************************************************************************************************************
create or replace function storage_adm.ComplexTaskGetAutoTopBanner( pParentIsn numeric,pParentSchema varchar)
RETURNS varchar as $BODY$
declare
    vReturnSql varchar;
    vParentSchema varchar;
begin

    select coalesce(CVALUE, '')
      into vReturnSql
      from storage_adm.sa_params
     where NAME = 'comlex:auto:top_banner';

/*select SHEMANAME
  into vParentSchema
  from storage_adm.Sa_Processes
 where ISN = pParentIsn;*/

select replace(vReturnSql,'{ParentSchema}',pParentSchema )
  into vReturnSql;

select replace(vReturnSql,'{PARENTISN}',pParentIsn )
  into vReturnSql;

  RETURN vReturnSql;
END; -- ComplexTaskGetAutoTopBanner
$BODY$
language plpgsql;
--******************************************************************************************************************************************

--******************************************************************************************************************************************
create or replace function storage_adm.ComplexTaskGetAutoBottomBanner( pParentIsn numeric, pTaskName varchar )
RETURNS varchar as $BODY$
declare
    vReturnSql varchar;
begin

    select coalesce(sprm.CVALUE, '')
      into vReturnSql
      from storage_adm.sa_params sprm
     where sprm.NAME = 'comlex:auto:bottom_banner';

/*select replace(vReturnSql,'{TASKNAME}',s.shortname )
  into vReturnSql
  from storage_adm.Sa_Processes s
 where s.isn = pParentIsn;*/

  SELECT replace(vReturnSql,'{TASKNAME}',pTaskName )
    into vReturnSql;

  RETURN vReturnSql;

END; -- ComplexTaskGetAutoBottomBanner
$BODY$
language plpgsql;