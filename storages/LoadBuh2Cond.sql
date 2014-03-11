create or replace FUNCTION STORAGES.LoadBuh2Cond(IN pLoadIsn DOUBLE PRECISION)
RETURNS VOID

/*Загрузка витрины repbuh2cond*/
   AS $procedure$
   DECLARE
   vMinIsn  DOUBLE PRECISION DEFAULT -1e30;

   vMaxIsn  INTEGER DEFAULT 0;
   SesId  DOUBLE PRECISION;
   vSql  VARCHAR(4000);
   vCnt  DOUBLE PRECISION DEFAULT 0;
   vBlockIsn  DOUBLE PRECISION;
   vBlockIsn1  DOUBLE PRECISION;
   vLoadObjcnt  DOUBLE PRECISION DEFAULT 10000;
BEGIN
-- This function was converted on Thu Feb 20 16:44:44 2014 using Ispirer SQLWays 6.0 Build 2162 32bit Licensed to ispirer.
   select NEXTVAL('STORAGES.SEQ_REP_BLOCK') into vBlockIsn;
   PERFORM STORAGES.RepLog_i(pLoadIsn,'LoadRepBuh2Cond','Begin',vBlockIsn);

/*определяем точку остановки для дозагрузки  */
   Select coalesce(Max(LASTISNLOADED),vMinIsn)
   Into vMinIsn
   from STORAGES.repload
   WHere Isn = pLoadIsn;

   If vMinIsn < 0 THen
      Execute 'truncate table Storages.RepBuh2Cond';
--      Execute 'truncate table storages.repbuh2cond_small';
--  EXECUTE IMMEDIATE 'ALTER TABLE RepBuh2Cond MODIFY budgetgroupisn NUMBER';
   end if;

--Execute Immediate 'truncate table tt_add_info drop storage';
/* определяем, есть ли чего догружать
и если есть - убиваем индексы*/
   select /*+ index ( a X_REPBUHBODY_BODYISN ) */Count(*)
   into vCnt
   from STORAGES.repbuhbody a
   where a.bodyisn > vMinIsn LIMIT 1;

   /*If vCnt > 0 tHEN
      PERFORM STORE_AND_DROP_TABLE_INDEX('repbuh2cond');
      PERFORM STORE_AND_DROP_TABLE_INDEX('repbuh2cond_small',1);
   END IF;*/
   vCnt := 0;
   --PERFORM DBMS_APPLICATION_INFO.set_module('LoadBuh2Cond','');

-- заполняем буффер для простановки учетных групп

   execute 'truncate table Storages.TT_RULE_RPNGRP';
--/*EGAO 27.02.2012 В комментариях ниже неустойчивый код (зависит от плана выполнения запроса)
/*for cur in (Select Isn from ais.rule) loop

   select Nvl(Max(groupisn),0)
   into SesId
   from (select level lv, isn
   from rule
   WHERE isn =Cur.Isn OR lEVEL>1
   start with isn =Cur.Isn
   connect by prior parentisn = isn
   Order by Lv
   ) r, (Select classisn1 groupisn,classisn2 ruleisn from dicx where classisn=2031712503) t--rep_tt_rules2groups t
  where r.isn = t.ruleisn
--  And       param is null
  and  RowNum=1;

       If (SesId>0) then
               Insert into Storages.TT_RULE_RPNGRP Values
               (Cur.ISN,SesId,'COND');
       end if;

 end loop;*/

   INSERT INTO storages.tt_rule_rpngrp(ruleisn,groupisn,typerule)
   SELECT a.ruleisn, a.rptgroupisn, 'COND' AS typerule
   FROM STORAGES.v_rptgroup2rule a;
 --*/
--/*EGAO 27.02.2012 В комментариях ниже неустойчивый код (зависит от плана выполнения запроса)
/*for cur in (select ISN
             from dICTI
             start with PARENTISN =24890816
             connect by prior isn=parentisn) loop

   select Nvl(Max(groupisn),0)
   into SesId
   from (select level lv, isn
   from Dicti
   WHERE isn =Cur.Isn OR lEVEL>1
   start with isn =Cur.Isn
   connect by prior parentisn = isn
   Order by Lv
   ) r,  (Select classisn1 groupisn,classisn2 ruleisn from dicx where classisn=2031719303) t --rep_tt_rules2groups t
  where r.isn = t.ruleisn
--  And       param is null
  and  RowNum=1;

       If (SesId>0) then
               Insert into TT_RULE_RPNGRP Values
               (Cur.ISN,SesId,'AGR');
       end if;

 end loop;*/
   INSERT INTO storages.tt_rule_rpngrp(ruleisn,groupisn,typerule)
   SELECT a.agrruleisn, a.rptgroupisn, 'AGR'
   FROM STORAGES.v_rptgroup2agrrule a;

--*/
   -- commit



/* режем repbuhbody вдоль bodyisn и загружаем*/
 --SesId := PARALLEL_TASKS.createnewsession('LoadBuh2cond');

--/*EGAO 06.12.2012
--В связи с изменениями в расчете РЗУ обновление таблицы REPDOCSUM закомментировано
/*if vMinIsn<0 then
vSql:='Storages.report_buh_storage_new.LoadDocSumm_WO_Buhbody('||pLoadIsn||');';
PARALLEL_TASKS.processtask(sesid,vSql);
end if;*/
--*/

  -- loop
      --vMaxIsn := cut_table('storage_source.RepBuhBody','BodyIsn',vMinIsn,null,vLoadObjcnt);
      --Exit when vMaxIsn is null;
      --vCnt := vCnt+1;
      --vSql := 'Begin
                 --DBMS_APPLICATION_INFO.set_module(''LoadBuh2Cond'',''Process: ' || vCnt || ''');
                 PERFORM STORAGES.REPORT_BUH_STORAGE_NEW_LoadBuh2cond_By_Isns(pLoadIsn,vMinIsn,vMaxIsn);
                 --END;';
      --PERFORM PARALLEL_TASKS.processtask(SesId,vSql,1);
      vMinIsn := vMaxIsn;
      PERFORM STORAGES.RepLoad_U(pLoadIsn,vMaxIsn);
      --PERFORM DBMS_APPLICATION_INFO.set_module('LoadBuh2Cond','Applied : ' || vCnt*vLoadObjcnt);
   --end loop;


   --PERFORM PARALLEL_TASKS.endsession(SesId);

   select NEXTVAL('STORAGES.SEQ_REP_BLOCK') into vBlockIsn1;
   PERFORM STORAGES.RepLog_i(pLoadIsn,'compress tables','Begin',vBlockIsn1);
   --PERFORM System.move_compressed_table('storages.repbuh2cond');
--System.move_compressed_table('storages.repbuh2cond_small');
   PERFORM STORAGES.RepLog_i(pLoadIsn,'compress tables','End',vBlockIsn1);




   select NEXTVAL('STORAGES.SEQ_REP_BLOCK') into vBlockIsn1;
   PERFORM STORAGES.RepLog_i(pLoadIsn,'restore index','Begin',vBlockIsn1);
   --PERFORM RESTORE_TABLE_INDEX('repbuh2cond');
--RESTORE_TABLE_INDEX('repbuh2cond_small');
   PERFORM STORAGES.RepLog_i(pLoadIsn,'restore index','End',vBlockIsn1);

   PERFORM STORAGES.RepLog_i(pLoadIsn,'LoadRepBuh2Cond','End',vBlockIsn);





   select NEXTVAL('STORAGES.SEQ_REP_BLOCK') into vBlockIsn;
   PERFORM STORAGES.RepLog_i(pLoadIsn,'setrefundrptgroup','Begin',vBlockIsn);
   PERFORM STORAGES.setrefundrptgroup(); -- проставляем УГ для убытков
   PERFORM STORAGES.RepLog_i(pLoadIsn,'setrefundrptgroup','End',vBlockIsn);
   RETURN;
END; $procedure$
LANGUAGE plpgsql;


