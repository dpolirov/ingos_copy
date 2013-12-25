CREATE OR REPLACE PACKAGE "STORAGES"."REPORT_BUH_STORAGE_NEW" IS

LoadIsn Number;
vDateStage DATE := trunc(SYSDATE,'mm')-1;

procedure Create_Agr_Analitiks;

procedure CREATE_AGR_ANALITIKS_By_Isns (vMinIsn Number, vMaxIsn Number);

procedure LoadBuh2Cond (pLoadIsn IN Number);

Procedure LoadBuh2cond_By_Isns (pLoadIsn Number, pMinIsn Number, pMaxIsn Number);

Procedure LoadBuh2Cond_By_List (pLoadIsn IN Number, pIsFull in Number:=1, pCommitEveryPut number:=0);

Procedure LoadBuh2Cond_BY_DATE (pLoadIsn IN Number, pDatebeg DATE);

procedure SetRefundRptGroup (pRefundIsn IN Number := 0);

procedure SetRefundRptGroup_By_Isns (vMinIsn number,vLMaxIsn Number);

-- запускать после простановки учетных групп в repbuh2cond
procedure LoadRZUMemo (pLoadIsn in Number);

Procedure LoadRefund_By_TT_RowId (pLoadIsn IN Number, IsFull in Number:=1);

procedure LoadRepRefund_Hist_By_TT_RowId (pLoadIsn in Number, IsFull number:=1);

End;

CREATE OR REPLACE PACKAGE BODY "STORAGES"."REPORT_BUH_STORAGE_NEW" IS
---------------------------------------------
--Пакет для загрузки бухгалтерских хранилищ--
--М.Зырянов, 26.11.2004----------------------
---------------------------------------------
DateStartBuh  Constant Date := to_date ('01-01-2002','dd-mm-yyyy');
LoadObjCnt    Constant Integer := 5000;
cAutoPartQuit Constant Number  := c.get ('oPartQuit');
qeZabNew      Constant Number  := c.get ('qeZabNew');
QuitType      Constant Number  := c.get ('cSKTypeQuit');
ReprParent    Constant Number  := 28763316;
ShipOwners    Constant Number  := 519;
ShipOwnersNew Constant Number  := 753938200;
BizCenter     Constant Number  := c.get ('cBizCenter');
BizFil        Constant Number  := c.get ('cBizFil');
KindBiz       Constant Number  := c.get ('cKindBiz');
TRFRetention  Constant Number  := c.get ('TRF_FRetention');
cLosses       Constant Number  := c.get ('amLosses');
MedicDept     Constant Number  := c.get ('MedicDept');
ShortCondMonthPeriod CONSTANT NUMBER := 13;

procedure Create_Agr_Analitiks
 Is

 vMinIsn Number:=0;
 vlMaxIsn Number;
 vCnt Number:=0;
 vObjCnt Number:=100000;

 SesId Number;
 vSql Varchar2(4000);


begin


DBMS_APPLICATION_INFO.set_module('Agr analitik','');

execute immediate 'truncate table REP_AGR_ANALITIKS';


Store_and_drop_table_Index('REP_AGR_ANALITIKS',1);

SesId:=PARALLEL_TASKS.createnewsession('REP_AGR_ANALITIKS');


 Loop
       Select Max(AgrIsn)
       Into vLMaxIsn
       From(
        Select --+ Index_Asc(a X_REPBUH2COND_AGRISN)
          AgrIsn
        from repbuh2cond a
        Where AgrIsn >vMinIsn
         And RowNum<=vObjCnt);

            Exit When vlMaxIsn is Null;


        vSql:='REPORT_BUH_STORAGE_NEW.CREATE_AGR_ANALITIKS_By_Isns('||vMinIsn||','||vLMaxIsn||');';
    PARALLEL_TASKS.processtask(sesid,vSql);
 vCnt:=vCnt+1;
 vMinIsn := vLMaxIsn;
 DBMS_APPLICATION_INFO.set_module('Agr analitik','Applied : '||vCnt*vObjCnt);
end loop;

-- ждем окончания всех джобов
PARALLEL_TASKS.endsession(sesid);
Restore_table_Index('REP_AGR_ANALITIKS');

--  EXPORT_DATA.export_to_owb_by_FLD('REP_AGR_ANALITIKS','AgrIsn');
end;


Procedure CREATE_AGR_ANALITIKS_By_Isns
(
vMinIsn Number,
vMaxIsn Number
)
Is
Begin

         Insert Into  REP_AGR_ANALITIKS A
      (a.isn, a.agrisn, a.premusd, a.agentjuridical, a.clientjuridical,
       a.objregion, a.filisn, a.emitisn, a.emittype, a.deptisn,
       a.rptclassisn, a.statcode, a.datebeg, a.dateend, a.montbetween,
       a.COMISSION,a.ingoname,RPTCLASS,ruleisnagr,RptGroupIsn,LBOUNDSTD,
       AgrId,ClientIsn,Dept0Isn,CLIENTRESIDENT,CURRISN,status,AGENTISN,FPREMDATEVAL,
       FDATEPAY,PREMPAYUSD,PREMPAYRUB,BizFlg, AddrIsn)

  Select --+ Ordered Use_Nl(c a sb ing sde) Index(a x_repagr_agr)
    Seq_Reports.NextVal,
  C.AgrIsn,C.premusd,Trim(A.agentjuridical),Nvl(Sb.JURIDICAL,Decode(c.DeptIsn,23735116,'Y','N')),
        C.ObjRegion,A.filisn,A.EmitIsn,Decode(Sde.Isn,Null,Null,'C'),C.DeptIsn,C.rptclassisn,C.Statcode,a.datebeg, a.dateend,
        Months_Between(a.dateend,a.datebeg),COMISSION,Ing.Shortname,C.RPTCLASS,C.ruleisnagr,C.RptGroupIsn,
(select lbound
from ais.histogram
where classisn = 726791516
  and greatest (nvl (C.PremUsd,0),0) >= lbound
  and nvl (C.PremUsd,0) < decode (hbound,0,1000000000000,hbound)) lbound,
  A.ID,A.ClientIsn,A.Dept0Isn,Sb.RESIDENT,A.CURRISN,a.status,a.AGENTISN,
  FPREMDATEVAL,FDATEPAY,PREMPAYUSD,PREMPAYRUB,BizFlg,
  a.addrisn -- EGAO 20.05.2009
   from
   (
    Select --+ Index(bc X_REPBUH2COND_AGRISN)
           AgrIsn,
           Max(DeptIsn) DeptIsn,
           Sum(BuhAmountUsd*condpc*docsumpc*factpc*buhpc) premusd,
           Decode(Count(Distinct ObjRegion),1,Max(ObjRegion)) ObjRegion,
           Decode(Count(Distinct rptclassisn),1,Max(rptclassisn)) rptclassisn,
           Decode(Count(Distinct RPTCLASS),1,Max(RPTCLASS)) RPTCLASS,
           Decode(Count(Distinct ruleisnagr),1,Max(ruleisnagr)) ruleisnagr,
           Decode(Count(Distinct RptGroupIsn),1,Max(RptGroupIsn)) RptGroupIsn,
           Max(statcode) statcode,
           Min(Dateval) FPREMDATEVAL,
           Min(DatePay) FDATEPAY,
           Sum(Gcc2.Gcc2(bc.FactSum*condpc*bc.buhpc,Factcurrisn,53,bc.DatePay)) PREMPAYUSD,
           Sum(Gcc2.Gcc2(bc.FactSum*condpc*bc.buhpc,Factcurrisn,35,bc.DatePay)) PREMPAYRUB



    from  repBuh2Cond bc
    Where AgrISn >vMinIsn And AgrISn<=vMaxIsn and Statcode in (38,34) and SaGroup in (1,3)
    Group by AgrIsn) C, RepAgr A, RepSubject Sb,Rep_IngoGrp Ing,(select Distinct Isn from subject where parentisn = 46598116) sde
   Where C.AgrIsn=A.AgrIsn
    And A.clientisn=Sb.Isn(+)
    And Sb.Isn=Ing.Isn(+)
    ANd A.EmitIsn=Sde.ISn(+);
    Commit;
End;

Procedure CREATE_AGR_ANALITIKS_By_Isns
(
vMinIsn Number,
vMaxIsn Number
)
Is
Begin

         Insert Into  REP_AGR_ANALITIKS A
      (a.isn, a.agrisn, a.premusd, a.agentjuridical, a.clientjuridical,
       a.objregion, a.filisn, a.emitisn, a.emittype, a.deptisn,
       a.rptclassisn, a.statcode, a.datebeg, a.dateend, a.montbetween,
       a.COMISSION,a.ingoname,RPTCLASS,ruleisnagr,RptGroupIsn,LBOUNDSTD,
       AgrId,ClientIsn,Dept0Isn,CLIENTRESIDENT,CURRISN,status,AGENTISN,FPREMDATEVAL,
       FDATEPAY,PREMPAYUSD,PREMPAYRUB,BizFlg, AddrIsn)

  Select --+ Ordered Use_Nl(c a sb ing sde) Index(a x_repagr_agr)
    Seq_Reports.NextVal,
  C.AgrIsn,C.premusd,Trim(A.agentjuridical),Nvl(Sb.JURIDICAL,Decode(c.DeptIsn,23735116,'Y','N')),
        C.ObjRegion,A.filisn,A.EmitIsn,Decode(Sde.Isn,Null,Null,'C'),C.DeptIsn,C.rptclassisn,C.Statcode,a.datebeg, a.dateend,
        Months_Between(a.dateend,a.datebeg),COMISSION,Ing.Shortname,C.RPTCLASS,C.ruleisnagr,C.RptGroupIsn,
(select lbound
from ais.histogram
where classisn = 726791516
  and greatest (nvl (C.PremUsd,0),0) >= lbound
  and nvl (C.PremUsd,0) < decode (hbound,0,1000000000000,hbound)) lbound,
  A.ID,A.ClientIsn,A.Dept0Isn,Sb.RESIDENT,A.CURRISN,a.status,a.AGENTISN,
  FPREMDATEVAL,FDATEPAY,PREMPAYUSD,PREMPAYRUB,BizFlg,
  a.addrisn -- EGAO 20.05.2009
   from
   (
    Select --+ Index(bc X_REPBUH2COND_AGRISN)
           AgrIsn,
           Max(DeptIsn) DeptIsn,
           Sum(BuhAmountUsd*condpc*docsumpc*factpc*buhpc) premusd,
           Decode(Count(Distinct ObjRegion),1,Max(ObjRegion)) ObjRegion,
           Decode(Count(Distinct rptclassisn),1,Max(rptclassisn)) rptclassisn,
           Decode(Count(Distinct RPTCLASS),1,Max(RPTCLASS)) RPTCLASS,
           Decode(Count(Distinct ruleisnagr),1,Max(ruleisnagr)) ruleisnagr,
           Decode(Count(Distinct RptGroupIsn),1,Max(RptGroupIsn)) RptGroupIsn,
           Max(statcode) statcode,
           Min(Dateval) FPREMDATEVAL,
           Min(DatePay) FDATEPAY,
           Sum(Gcc2.Gcc2(bc.FactSum*condpc*bc.buhpc,Factcurrisn,53,bc.DatePay)) PREMPAYUSD,
           Sum(Gcc2.Gcc2(bc.FactSum*condpc*bc.buhpc,Factcurrisn,35,bc.DatePay)) PREMPAYRUB



    from  repBuh2Cond bc
    Where AgrISn >vMinIsn And AgrISn<=vMaxIsn and Statcode in (38,34) and SaGroup in (1,3)
    Group by AgrIsn) C, RepAgr A, RepSubject Sb,Rep_IngoGrp Ing,(select Distinct Isn from subject where parentisn = 46598116) sde
   Where C.AgrIsn=A.AgrIsn
    And A.clientisn=Sb.Isn(+)
    And Sb.Isn=Ing.Isn(+)
    ANd A.EmitIsn=Sde.ISn(+);
            Commit;
End;
    
Procedure LoadBuh2Cond (pLoadIsn IN Number)
/*Загрузка витрины repbuh2cond*/
Is
vMinIsn Number:=-1e30;

     vMaxIsn integer:=0;
     SesId        Number;
     vSql Varchar2(4000);
     vCnt Number:=0;
     vBlockIsn number;
     vBlockIsn1 number;
     vLoadObjcnt Number:=10000;
Begin

    select seq_rep_block.NEXTVAL into vBlockIsn from dual;
     RepLog_i (pLoadIsn, 'LoadRepBuh2Cond', pAction => 'Begin', pBlockIsn=>vBlockIsn);

/*определяем точку остановки для дозагрузки  */
Select Nvl(Max(LASTISNLOADED),vMinIsn)
Into vMinIsn
from repload
WHere Isn=pLoadIsn;

If vMinIsn<0 THen
  Execute Immediate 'truncate table Storages.RepBuh2Cond REUSE storage';
  Execute Immediate 'truncate table storages.repbuh2cond_small drop storage';
--  EXECUTE IMMEDIATE 'ALTER TABLE RepBuh2Cond MODIFY budgetgroupisn NUMBER';
end if;

--Execute Immediate 'truncate table tt_add_info drop storage';
/* определяем, есть ли чего догружать
и если есть - убиваем индексы*/
select /*+ index ( a X_REPBUHBODY_BODYISN ) */Count(*)
into vCnt
from repbuhbody a
where a.bodyisn>vMinIsn And RowNum<=1;

If vcnt>0 tHEN
STORE_AND_DROP_TABLE_INDEX('repbuh2cond');
STORE_AND_DROP_TABLE_INDEX('repbuh2cond_small',1);
END IF;
vCnt:=0;
DBMS_APPLICATION_INFO.set_module('LoadBuh2Cond','');

-- заполняем буффер для простановки учетных групп

execute immediate 'truncate table Storages.TT_RULE_RPNGRP';

  INSERT INTO storages.tt_rule_rpngrp(ruleisn,groupisn,typerule)
  SELECT a.ruleisn, a.rptgroupisn, 'COND' AS typerule
  FROM v_rptgroup2rule a;
 
  INSERT INTO storages.tt_rule_rpngrp(ruleisn,groupisn,typerule)
  SELECT a.agrruleisn, a.rptgroupisn, 'AGR'
  FROM v_rptgroup2agrrule a;

  commit;


/* режем repbuhbody вдоль bodyisn и загружаем*/
 SesId:=PARALLEL_TASKS.createnewsession('LoadBuh2cond');

loop

vMaxIsn:=cut_table('storage_source.RepBuhBody','BodyIsn',vMinIsn,null,vLoadObjcnt);

    Exit when vMaxIsn is null;
    vCnt:=vCnt+1;
          vSql:='Begin
                 DBMS_APPLICATION_INFO.set_module(''LoadBuh2Cond'',''Process: '||vCnt||''');
                 Storages.REPORT_BUH_STORAGE_NEW.LoadBuh2cond_By_Isns('||ploadisn||','||vMinIsn||','||vMaxIsn||');
                 END;';
          PARALLEL_TASKS.processtask(sesid,vSql,1);
          vMinIsn:=vMaxIsn;
          RepLoad_U(pLoadIsn,pLastisnloaded=>vMaxIsn);

    DBMS_APPLICATION_INFO.set_module('LoadBuh2Cond','Applied : '||vCnt*vLoadObjcnt);
end loop;


PARALLEL_TASKS.endsession(sesid);

select seq_rep_block.NEXTVAL into vBlockIsn1 from dual;
 RepLog_i (pLoadIsn, 'compress tables', pAction => 'Begin', pBlockIsn=>vBlockIsn1);
 System.move_compressed_table('storages.repbuh2cond');
--System.move_compressed_table('storages.repbuh2cond_small');
 RepLog_i (pLoadIsn, 'compress tables', pAction => 'End', pBlockIsn=>vBlockIsn1);




select seq_rep_block.NEXTVAL into vBlockIsn1 from dual;
 RepLog_i (pLoadIsn, 'restore index', pAction => 'Begin', pBlockIsn=>vBlockIsn1);
RESTORE_TABLE_INDEX('repbuh2cond');
--RESTORE_TABLE_INDEX('repbuh2cond_small');
 RepLog_i (pLoadIsn, 'restore index', pAction => 'End', pBlockIsn=>vBlockIsn1);

     RepLog_i (pLoadIsn, 'LoadRepBuh2Cond', pAction => 'End', pBlockIsn=>vBlockIsn);

 select seq_rep_block.NEXTVAL into vBlockIsn from dual;
 RepLog_i (pLoadIsn, 'setrefundrptgroup', pAction => 'Begin', pBlockIsn=>vBlockIsn);
 setrefundrptgroup; -- проставляем УГ для убытков
 RepLog_i (pLoadIsn, 'setrefundrptgroup', pAction => 'End', pBlockIsn=>vBlockIsn);
end;
    
Procedure LoadBuh2cond_By_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number

) Is
begin

      --  EXECUTE IMMEDIATE 'alter session set sort_area_size = 104857600';
         execute immediate  'Alter session set db_file_multiblock_read_count=256';

/*положили в tt_rowid список bodyisn  */
Delete from tt_rowid;

     Insert Into tt_rowid
     (Isn)
      Select --+ Index_Asc (b X_REPBUHBODY_BODYISN)
       Distinct BodyIsn
      from RepBuhBody b
      where bodyIsn > pMinIsn And  bodyIsn <= pMaxIsn;
 --     And DeptIsn<> 23735116;--c.get('MedicDept')
    Commit;


/* процедура обработки блока*/
     LoadBuh2Cond_By_List(pLoadIsn);
end;

Procedure LoadBuh2Cond_By_List
(pLoadIsn IN Number,
 pIsFull in Number:=1,
 pCommitEveryPut number:=0) IS


vLoadIsn Number := pLoadIsn;
vDateBeg Date;
vBlockIsn number;
vBlockIsn1 number;
vBlockIsn0 number;
vCnt Number;
vBufCnt Number;
vallCnt number:=0;
Begin




 select seq_rep_block.NEXTVAL into vBlockIsn0 from dual;
 RepLog_i (pLoadIsn, 'LoadBuh2Cond','Load_by_List', pAction => 'Begin', pBlockIsn=>vBlockIsn0);

  --Зачищаем промежуточные таблицы...
  /*
    Delete from  Report_BuhBody_List;--  список проводок, которые заливаем
    delete from  REP_COND_LIST; -- список кондов, на которые эти проводки лягут

    Delete from  TT_RepBuh2Cond;-- буфер под готовый кусок результирующей таблицы

    */

/*!!!!! KGS АХТУНГ ! В процессе как можно меньше коммитов - попадаем на ожидания буфера редологов!!!*/

    Execute immediate 'truncate table  Report_BuhBody_List'; /* список проводок, которые заливаем*/
    Execute immediate 'truncate table  REP_COND_LIST'; /* список кондов, на которые эти проводки лягут */

    Execute immediate 'truncate table   TT_RepBuh2Cond';      /* буфер под готовый кусок результирующей таблицы*/





-- залили проводки


/* c tt_ds_db_de - это врезка для определения датебег и дайтенд доксуммы для длинных договоров
должна уйти, когда простановку этих полей втянут в АИС, заполняется это в Rep_longagr



поля ag.classisn AGRCLASSISN,ag.discr agrdiscr,ag.ruleisn RULEISNAGR нужны для функции
SET_RPTGROUPISN_BY_BUH
*/
    insert into Report_BuhBody_List (isn, dpc, bpc, fpc, bodyisn, agrisn, addisn, refundisn,
      statcode, deptisn, classisn, subaccisn, currisn, BuhAmount, BuhAmountRub,BUHAMOUNTUSD, QuitAmount,
      QuitCurrIsn, AmountClosedQuit, DateVal, DateQuit, QuitDateVal, DatePayLast, AgrBuhDate, AccCurrIsn,
      DocSumIsn, DatePay, DocIsn, ReprDeptIsn,SubjIsn,BodySubjIsn,HeadIsn,REFUNDPC,DocIsn2,Sagroup,
      CORSUBACCISN,/*EGAO 20.02.2012 isrevaluation,*/AGRCLASSISN,agrdiscr,RULEISNAGR,vDocsumIsn,dsDatebeg,dsDateend,
      adeptisn, -- EGAO 29.04.2009 ДИТ-09-1-083535
      buhheadfid, dsclassisn, dsclassisn2 -- EGAO 02.02.2010
      ,Agrformisn,     -- EGAO 04.03.2011
      AgrDatebeg,
      AgrDateend,
      bizflg,
      adddatebeg,
      addsign,
      AGRSTATUS, Dsstatus,
      AGRCURRISN
      )
    select --+ ordered Use_Nl(b ag lds adpt ad) index ( b X_REPBUHBODY_BODYISN ) index (r X_REP_ISREVAL )
      B.isn, nvl (DocSumPC,0) dpc,
      decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount) bpc,
      decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit) fpc,
      b.bodyisn, Decode(b.agrisn,0,null,b.agrisn) agrisn, decode(b.addisn,0,null,b.addisn) addisn,
      decode(b.refundisn,0,null,b.refundisn) refundisn, statcode, decode(Nvl(b.deptisn,0),0,DeptIsnAn,b.DeptIsn), b.classisn, subaccisn, b.currisn, BuhAmount, BuhAmountRub,BUHAMOUNTUSD,
      decode (BuhQuitDate, null, 0, nvl (BuhQuitPartAmount,BuhQuitAmount)) QuitAmount,
      QuitCurrIsn, AmountClosedQuit, DateVal,BuhQuitDate dATEQUIT,QuitDateVal, DatePayLast, AgrBuhDate, AccCurrIsn,
      B.DocSumIsn, b.DatePay, b.DocIsn, b.ReprDeptIsn,B.docsumsubj,B.subjisn,B.HeadISn,1,DocIsn2,Sagroup,
      CORSUBACCISN, /*EGAO 20.02.2012 Nvl(r.isrevaluation,0) isrevaluation,*/
      ag.classisn AGRCLASSISN,ag.discr agrdiscr,ag.ruleisn RULEISNAGR,Nvl(b.DocSumIsn,seq_reports.NEXTVAL),
      Nvl(Lds.dsdatebeg,trunc(b.dsDatebeg)), Nvl(Lds.dsdateend,Trunc(b.dsDateend)),
      adpt.adeptisn,
      b.buhheadfid, b.dsclassisn, b.dsclassisn2, ag.formisn,
      Ag.Datebeg AgrDatebeg,
      Ag.Dateend AgrDateend,
      ag.bizflg,
      ad.datebeg,
      ad.datesign,
      Ag.Status,
      b.dsstatus,
      Ag.Currisn
    from tt_rowid a, RepBuhBody b, /*EGAO 20.02.2012  rep_isreval r,*/repagr ag, agreement ad, tt_ds_db_de Lds, repagradept adpt
    where B.bodyisn=A.isn  /*EGAO 20.02.2012 and a.isn=r.bodyisn(+)*/
    and b.agrisn=ag.agrisn(+)
    and b.Addisn=ad.isn(+)
    and b.bodyisn=Lds.bodyisn(+)
    and Nvl(b.docsumisn,0)=Nvl(Lds.docsumisn(+),0)
    AND adpt.agrisn(+)=b.agrisn;




SET_RPTGROUPISN_BY_BUH; -- проставили учетную группу "по бухгалтерии"
set_agr_buhdate_by_buh; -- проставили AgrBuhDate "по бухгалтерии" KGS 15.03.2011

-- залили "древо" адендумов
--    адендум, кому отдает, коэфициент отдачи

  INSERT INTO tt_add_info(agrisn, addisn)
  SELECT x.agrisn, a.isn
  FROM (SELECT DISTINCT x.agrisn FROM Report_BuhBody_List x WHERE x.refundisn IS NULL/*EGAO 27.12.2011 x.statcode NOT IN (220,24)*/) x,
       agreement a
  WHERE ((a.isn = x.agrisn AND (a.discr = 'Д' OR a.discr = 'Г')) OR
         (a.parentisn = x.agrisn AND a.discr = 'А' AND CASE NVL(a.id,'X')
                                                         WHEN 'П' THEN
                                                           CASE WHEN (SELECT COUNT(1)
                                                                         FROM repcond rc
                                                                         WHERE rc.agrisn=x.agrisn
                                                                           AND rc.addisn=a.isn
                                                                           AND rownum<=1
                                                                        )=1 THEN 1
                                                                ELSE 0
                                                           END
                                                         ELSE 1
                                                       END=1))
    AND ((CASE
            WHEN nvl(a.parentisn,a.isn)=a.isn AND a.status IN ('В','Д','Щ') THEN 1
            WHEN nvl(a.parentisn,a.isn)<>a.isn AND a.status='В' THEN 1
            ELSE 0
          END=1 AND a.datesign<=vDateStage) OR
         (SELECT COUNT(1) FROM repbuhbody bb WHERE bb.agrisn=x.agrisn AND nvl(bb.addisn, bb.agrisn)=a.isn AND nvl(bb.amount,0)<>0
         AND STATCODE IN (38,34) /* KGS 18.03.2011 */ AND  ROWNUM<=1)<>0
        );

  --{ EGAO 11.12.2012
  INSERT INTO tt_agr_sum(agrisn, premiumsum, paidsum)
  SELECT a.agrisn,
         NVL((SELECT --+ index ( x X_LONGAGRADDENDUM_AGR )
                     SUM(x.premiumsum)
              FROM rep_longagraddendum x
              WHERE a.agrisn=x.agrisn
                AND least(nvl(x.datebeg, x.datesign), nvl(x.datesign, x.datebeg)) <= vDateStage
             ),0) AS premiumsum,
         NVL((SELECT --+ index ( bb X_REPBUHBODY_AGR )
                     sum(gcc2.gcc2(bb.amount,bb.currisn,a.agrcurrisn,bb.dateval))
              FROM repbuhbody bb
              WHERE bb.Agrisn=a.agrisn
                AND bb.statcode in (34,38, 221, 241)
                AND bb.sagroup in (1, 3)
                AND bb.dateval<=vDateStage
             ),0) AS paidsum
  FROM (SELECT DISTINCT agrisn, agrcurrisn, trunc(agrdatebeg) AS agrdatebeg,
                        trunc(agrdateend) AS agrdateend
        FROM Report_BuhBody_List a
        WHERE a.refundisn IS NULL
          AND sign(months_between (a.agrDateEnd,a.agrDateBeg)-13)=1
          AND vDateStage BETWEEN trunc(agrdatebeg) AND trunc(agrdateend)

        ) a;
  --EGAO 11.12.2012}

  --Инсертим сначала привязанные к рефандам...

  /* НЕ МЕНЯЕМ!*/

    insert into REP_COND_LIST
    (loadtype, bodyisn, condisn, repcondisn, condpc,
     datebeg, dateend, datebegcond, dateendcond, ruleisn, ruleisnagr,
     agrclassisn, comission, agrdiscr, objclassisn,RPTCLASSISN,AgrIsn,OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
     CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,REFUNDEXTISN,addsign,  addbeg,objisn,parentobjisn, refundclassisn,
     BizFlg, premusd, premagr,CARRPTCLASS)

    select --+ ordered use_nl (b r c ag ac) index (c X_REPCOND_COND) no_merge( b ) index ( r X_REPREFUND_REFUNDISN ) index ( ag X_REPAGR_AGR )
    1, b.refundisn, r.condisn,
     c.isn,
     r.condpc,
     ag.datebeg agrdatebeg,ag.dateend agrdateend,
     ac.datebeg, ac.dateend, --!!!
     r.riskruleisn,
     ag.ruleisn agrruleisn,
     ag.classisn agrclassisn, ag.comission agrcomission,ag.discr agrdiscr,
     r.objclassisn,
     aC.rptclassisn, --!!!
     nvl(r.agrisn,C.AgrIsn),
    C.OBJREGION,
    C.OBJCOUNTRY,
    r.Riskclassisn,
    C.RISKPRNCLASSISN,
    ag.CLIENTISN,ag.olddateend,Nvl(R.PARENTOBJCLASSISN,R.OBJCLASSISN) PARENTOBJCLASSISN,
    AgrEXTISN,addsign,  addbeg,r.objisn,Nvl(r.parentobjisn,r.objisn) PARENTOBJISN,
    r.classisn,-- EGAO 02.03.2011
    ag.bizflg,
    decode(nvl(c.premusd,0),0,gcc2.gcc2(c.premiumsum,c.premcurrisn,53,sysdate-1),c.premusd) -- EGAO 15.03.2011
    ,c.premagr -- EGAO 07.09.2011
    ,C.CARRPTCLASS
    from (select distinct refundisn from Report_BuhBody_List
    where  refundisn is not null) b, reprefund r, repcond c,repagr ag,agrcond ac
    where b.refundisn = r.refundisn
--      and r.condisn is not null
      and r.condisn = c.condisn(+)
      and r.agrisn=ag.agrisn(+)
      and r.condisn = ac.isn(+);

   update --+ use_hash (b)
    Report_BuhBody_List b set
      loadtype = 1
    where refundisn In (select bodyisn from REP_COND_LIST);
 --  Commit;




-- льем на все конды - тем где 1 конд с премией 0 или адендум , где все конды с премией 0 - хрен с ними,
-- льем на них с фиктивным condPC, иначе проблемы классификации.
-- конды с отрицательной премией не учитываем

    --EGAO Все кроме медиков, туристов и парковых

    --{EGAO 11.12.2012
    -- Прерыдущий вариант insert into REP_COND_LIST ниже в комментариях
    insert into REP_COND_LIST
    (loadtype, bodyisn, condisn, repcondisn, condpc, datebeg, dateend,
     datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
     objisn,PARENTOBJISN,bizflg, premusd, premagr,CARRPTCLASS
    )
    Select * from
    (
     select 2 loadtype, agrisn AS Bodyisn,  condisn, repcondisn,
            decode(nvl(Trunc(addprem,2),0),0,1/addcnt,(premagr/*EGAO 08.09.2011 premusd*/*lengthpc)/addprem) CondPc,
            agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn, agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
            OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
            CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
            objisn, PARENTOBJISN,bizflg, premusd, premagr,CARRPTCLASS
    from
        (
        select addisn,condisn, isn repcondisn, premusd, premagr,
               agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
               agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
               OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
               SUM (premagr/*EGAO 08.09.2011 premusd*/*lengthpc) OVER (PARTITION BY agrisn) as AddPrem,
               Count(*) OVER (PARTITION BY agrisn) as AddCnt,
               CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
               objisn,PARENTOBJISN,bizflg, lengthpc,CARRPTCLASS
        FROM (SELECT a.*
              FROM
                  (
                   select --+  ordered USe_Nl(b c) Index(C X_REPCOND_ADDISN) no_merge ( bb ) use_hash ( bb b )
                         c.addisn , c.newaddisn,
                         c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                         c.agrclassisn, c.agrcomission, c.agrdiscr, c.objclassisn, c.rptclassisn, c.agrisn,
                         c.objregion, c.objcountry, c.riskclassisn, c.riskprnclassisn, nvl(c.objprnclassisn,c.objclassisn) AS parentobjclassisn,
                         c.isn AS isn, condisn,
                         decode(nvl(c.premusd,0),0,gcc2.gcc2(c.premiumsum,c.premcurrisn,53,sysdate-1),c.premusd) AS premusd,
                         c.premagr, -- EGAO 07.09.2011
                         ((Least(trunc(c.dateend),  CASE
                                                      WHEN trunc(c.dateend)>vDateStage AND months_between(trunc(c.dateend), trunc(c.datebeg))>ShortCondMonthPeriod THEN
                                                        add_months(trunc(c.datebeg),trunc(greatest(0,months_between(vDateStage,trunc(c.datebeg)))/12)*12+12)-1
                                                      ELSE trunc(c.dateend)
                                                    END) - trunc(c.datebeg)+1)/(trunc(c.dateend)-trunc(c.datebeg)+1)) AS lengthpc,
                         c.Clientisn, c.AgrOldDateEnd, c.addsign, c.addbeg, c.objisn, Nvl(c.parentobjisn,c.objisn) AS parentobjisn,
                         bb.bizflg,
                         bb.agrinsurancedateend,
                         C.CARRPTCLASS
                   from (
                         SELECT a.*,
                                 CASE
                                   WHEN iscalendaryear=1 THEN
                                     least(trunc(a.agrdateend), add_months(trunc(a.agrdatebeg),trunc(months_between(vDateStage,trunc(a.agrdatebeg))/12)*12+12)-1)
                                   ELSE trunc(a.agrdateend)
                                 END agrinsurancedateend
                         FROM (
                               SELECT --+ use_hash ( a x )
                                      a.*,
                                      CASE
                                        WHEN months_between(a.agrDateEnd,a.agrDateBeg)>ShortCondMonthPeriod AND
                                             vDateStage BETWEEN agrdatebeg AND agrdateend AND
                                             nvl(x.premiumsum,0)>0 AND
                                             nvl(x.paidsum,0)>0 AND
                                             round(x.paidsum/x.premiumsum,2)<1/*EGAO 30.09.2013 x.paidsum/x.premiumsum<1*/ THEN 1
                                        ELSE 0
                                      END AS iscalendaryear
                               FROM (
                                     SELECT DISTINCT a.agrisn,
                                                     a.bizflg,
                                                     trunc(a.agrdatebeg) AS agrdatebeg,
                                                     trunc(a.agrdateend) AS agrdateend,
                                                     a.agrcurrisn
                                     FROM report_buhbody_list a,
                                          (select isn from dicti start with isn IN (686160416, 683205716, 47160616) connect by prior isn = parentisn) x
                                     WHERE a.loadtype IS NULL AND a.ruleisnagr=x.isn(+) AND x.isn IS NULL
                                    ) a, tt_agr_sum x
                                WHERE a.agrisn=x.agrisn(+)
                              ) a
                        ) bb,
                        tt_add_info b,
                        repcond c
                   where b.agrisn=bb.agrisn
                     AND b.addisn = c.addisn
                     and Nvl(c.premiumsum,0)>=0
                     AND (c.newaddisn is null or c.newaddisn not in (select addisn FROM tt_add_info x where x.agrisn=b.agrisn))
                  ) a
              WHERE trunc(a.datebeg)<=a.agrinsurancedateend
             )
        )
    ) Where Nvl(CondPc,0)<>0;
    
    update --+ use_hash (b)
    Report_BuhBody_List b set loadtype = 2
    where loadtype is null
      and b.agrisn in (select bodyisn from REP_COND_LIST where loadtype = 2);-- EGAO 02.03.2011 and decode(dsdatebeg,null,decode(dsdateend,null,Nvl(b.addisn,b.agrisn),vDocsumIsn),vDocsumIsn) in (select bodyisn from Rep_buh2cond_list where loadtype = 2);

  --  commit;

    --Парковые
    insert into REP_COND_LIST
    (loadtype, bodyisn, condisn, repcondisn, condpc, datebeg, dateend,
     datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
     objisn,PARENTOBJISN,bizflg, premusd, premagr,CARRPTCLASS
    )
    Select * from
    (
     select 5 loadtype, addisn AS Bodyisn,  condisn, repcondisn,
            decode(nvl(Trunc(addprem,2),0),0,1/addcnt,(premagr/*EGAO 08.09.2011 premusd*/*lengthpc*premsign)/addprem) CondPc, -- EGAO 30.08.2011 decode(nvl(Trunc(addprem,2),0),0,1/addcnt,(premusd*lengthpc)/addprem) CondPc,
            agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn, agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
            OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
            CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
            objisn, PARENTOBJISN,bizflg, premusd, premagr,CARRPTCLASS
    from
        (

        select addisn,condisn, isn repcondisn,
               agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
               agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
               OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
               SUM (premagr/*EGAO 08.09.2011 premusd*/*lengthpc*premsign) OVER (PARTITION BY addisn) as AddPrem, -- EGAO 30.08.2011 SUM (premusd*lengthpc) OVER (PARTITION BY addisn) as AddPrem,
               Count(*) OVER (PARTITION BY addisn) as AddCnt,
               CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
               objisn,PARENTOBJISN,bizflg, premusd, premagr, lengthpc, premsign,CARRPTCLASS
        from
            (SELECT --+ Ordered Use_Nl(ag)
                    c.addisn,
                    c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                    c.agrclassisn, c.agrcomission, c.agrdiscr,c.rptclassisn,c.AgrIsn,c.riskclassisn, c.riskprnclassisn,
                    decode(Nvl(c.premusd,0),0,gcc2.gcc2(c.premiumsum,c.premcurrisn,53,sysdate-1),c.premusd) AS premusd,
                    c.premagr, -- EGAO 07.09.2011
                    c.premcurrisn, c.clientisn, c.AgrOldDateEnd, c.addsign, c.addbeg,
                    c.condisn, c.isn, c.objisn, c.parentobjisn, c.parentobjclassisn, c.objregion,c.objcountry, c.objclassisn,
                    c.bizflg,
                    c.newaddisn,
                    ((Least(trunc(c.dateend),  CASE
                                                 WHEN trunc(c.dateend)>vDateStage AND months_between(trunc(c.dateend), trunc(c.datebeg))>ShortCondMonthPeriod THEN
                                                   add_months(trunc(c.datebeg),trunc(greatest(0, months_between(vDateStage,trunc(c.datebeg)))/12)*12+12)-1
                                                 ELSE trunc(c.dateend)
                                               END) - trunc(c.datebeg)+1)/(trunc(c.dateend)-trunc(c.datebeg)+1)) AS lengthpc
                    ,c.premsign
                    ,c.CARRPTCLASS
             FROM ( WITH a AS (SELECT DISTINCT
                                      nvl(b.addisn, agrisn) AS addisn,
                                      b.bizflg,
                                      CASE
                                        WHEN vDateStage BETWEEN trunc(b.agrdatebeg) AND trunc(b.agrdateend) THEN
                                          least(trunc(b.agrdateend), add_months(trunc(b.agrdatebeg),trunc(months_between(vDateStage,trunc(b.agrdatebeg))/12)*12+12)-1)
                                        ELSE trunc(b.agrdateend)
                                      END AS agrinsurancedateend
                               FROM (select isn from dicti start with isn = 47160616 connect by prior isn = parentisn) x,
                                    report_buhbody_list b
                               WHERE b.ruleisnagr=x.isn  and loadtype is null
                              )
                    SELECT --+ index ( c X_REPCOND_ADDISN ) ordered use_nl ( a c )
                           1 AS premsign, -- EGAO 30.08.2011
                           c.addisn,
                           c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                           c.agrclassisn, c.agrcomission, c.agrdiscr,c.rptclassisn,c.AgrIsn,c.riskclassisn, c.riskprnclassisn,
                           c.premusd, c.premagr, c.premiumsum,
                           c.premcurrisn, c.clientisn, c.AgrOldDateEnd, c.addsign, c.addbeg,
                           c.condisn, c.isn,
                           c.parentisn,
                           c.objisn, nvl(c.parentobjisn,c.objisn) AS parentobjisn,
                           nvl(c.objprnclassisn,c.objclassisn) AS parentobjclassisn,
                           c.objregion,c.objcountry, c.objclassisn
                           ,c.newaddisn
                           ,a.bizflg
                           ,a.agrinsurancedateend
                           ,c.CARRPTCLASS
                    FROM a, repcond c
                    WHERE nvl(c.premiumsum,0)>=0
                      AND c.addisn=a.addisn
                    UNION ALL
                    SELECT --+ index ( c X_REPCOND_NEWADDISN ) ordered use_nl ( a c )
                           -1 AS premsign, -- EGAO 30.08.2011
                           c.newaddisn AS addisn,
                           c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                           c.agrclassisn, c.agrcomission, c.agrdiscr,c.rptclassisn,c.AgrIsn,c.riskclassisn, c.riskprnclassisn,
                           c.premusd, c.premagr, c.premiumsum,/*EGAO 30.08.2011 -c.premusd AS premusd, -c.premiumsum AS premiumsum,*/
                           c.premcurrisn, c.clientisn, c.AgrOldDateEnd, c.addsign, c.addbeg,
                           c.condisn, c.isn,
                           c.parentisn,
                           c.objisn, nvl(c.parentobjisn,c.objisn) AS parentobjisn,
                           nvl(c.objprnclassisn,c.objclassisn) AS parentobjclassisn,
                           c.objregion,c.objcountry, c.objclassisn
                           ,c.newaddisn
                           ,a.bizflg
                           ,a.agrinsurancedateend
                           ,C.CARRPTCLASS
                    FROM a, repcond c
                    WHERE nvl(c.premiumsum,0)>=0
                      AND c.newaddisn=a.addisn

                  ) c
             WHERE trunc(c.datebeg)<=c.agrinsurancedateend
            )
        )
    ) Where Nvl(CondPc,0)<>0;

    update --+ use_hash (b)
    Report_BuhBody_List b set loadtype = 5
    where loadtype is null
      and nvl(b.addisn, b.agrisn) in (select bodyisn from REP_COND_LIST where loadtype = 5);

    insert into REP_COND_LIST
    (loadtype, bodyisn, condisn, repcondisn, condpc, datebeg, dateend,
     datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
     objisn,PARENTOBJISN,bizflg, premusd, premagr
    )
    Select * from
    (
     select 3 loadtype, addisn AS Bodyisn,  condisn, repcondisn,
            decode(nvl(Trunc(addprem,2),0),0,1/addcnt,st_premagr/*EGAO 08.09.2011 st_premusd*//addprem) CondPc,
            agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn, agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
            OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
            CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
            objisn, PARENTOBJISN,bizflg, premusd, premagr
    from
        (

        select addisn,condisn, isn repcondisn, st_premagr, /*EGAO 08.09.2011 st_premusd,*/
               agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
               agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
               OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
               SUM (st_premagr/*EGAO 08.09.2011 st_premusd*/) OVER (PARTITION BY addisn) as AddPrem,
               Count(*) OVER (PARTITION BY addisn) as AddCnt,
               CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
               objisn,PARENTOBJISN,bizflg, premusd, premagr
        from
            (
            select --+  ordered USe_Nl(b c) Index(C X_REPCOND_AGR) no_merge ( b ) no_merge ( x ) use_hash ( x )
                   c.addisn , c.newaddisn,
                   c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                   c.agrclassisn, c.agrcomission, c.agrdiscr,
                   to_number(NULL) AS objclassisn,
                   c.rptclassisn,c.AgrIsn,
                   to_number(NULL) AS objregion,
                   to_number(NULL) AS objcountry,
                   c.riskclassisn, c.riskprnclassisn,
                   to_number(NULL) AS parentobjclassisn,
                   max (c.isn) isn, max (c.condisn) condisn,
                   sum (
                        c.PREMAGRKOEF
                        /* KGS 13.01.2012 временно на пробу
                        *((Least(trunc(c.dateend),  CASE
                                                                   WHEN trunc(c.dateend)>vDateStage AND months_between(trunc(c.dateend), trunc(c.datebeg))>ShortCondMonthPeriod THEN
                                                                     add_months (trunc(c.datebeg),trunc(greatest(0, months_between(vDateStage,trunc(c.datebeg)))/12)*12+12)-1
                                                                   ELSE trunc(c.dateend)
                                                                 END) - trunc(c.datebeg)+1)/(trunc(c.dateend)-trunc(c.datebeg)+1))
                          */                                       )  st_premagr,
                   MAX(c.clientisn) Clientisn ,Max(c.AgrOldDateEnd) AgrOldDateEnd,Max(c.addsign) addsign,Max(c.addbeg) addbeg,
                   to_number(NULL) AS objisn,
                   to_number(NULL) AS parentobjisn,
                   Max(bizflg) bizflg,
                   SUM(c.premusd) AS premusd,
                   SUM(c.premagr) AS premagr
            FROM (
                   SELECT --+ Ordered Use_Nl(ag)
                           c.addisn,
                           c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                           c.agrclassisn, c.agrcomission, c.agrdiscr,c.rptclassisn,c.AgrIsn,c.riskclassisn, c.riskprnclassisn,
                           decode(Nvl(c.premusd,0),0,gcc2.gcc2(c.premiumsum,c.premcurrisn,53,sysdate-1),c.premusd) AS premusd,
                           c.premagr, -- EGAO 07.09.2011
                           c.premcurrisn, c.clientisn, c.AgrOldDateEnd, c.addsign, c.addbeg,
                           c.condisn, c.isn, c.objisn, c.parentobjisn, c.parentobjclassisn, c.objregion,c.objcountry, c.objclassisn,
                           c.bizflg,
                           c.newaddisn,
                           premagrKoef,
                           SourceAdd
                   FROM ( WITH a AS (SELECT DISTINCT
                                            nvl(b.addisn, b.agrisn) AS addisn,
                                            b.bizflg,
                                            CASE
                                              WHEN vDateStage BETWEEN trunc(b.agrdatebeg) AND trunc(b.agrdateend) THEN
                                                least(trunc(b.agrdateend), add_months(trunc(b.agrdatebeg),trunc(months_between(vDateStage,trunc(b.agrdatebeg))/12)*12+12)-1)
                                              ELSE trunc(b.agrdateend)
                                            END AS agrinsurancedateend
                                     FROM (select isn from dicti start with isn IN (686160416, 683205716) connect by prior isn = parentisn) x,
                                          report_buhbody_list b
                                     WHERE b.ruleisnagr=x.isn  and loadtype is NULL
                                    )
                          SELECT --+ index ( c X_REPCOND_ADDISN ) ordered use_nl ( a c )
                                 c.addisn,
                                 c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                                 c.agrclassisn, c.agrcomission, c.agrdiscr,c.rptclassisn,c.AgrIsn,c.riskclassisn, c.riskprnclassisn,
                                 c.premusd, c.premagr, c.premiumsum,
                                 c.premcurrisn, c.clientisn, c.AgrOldDateEnd, c.addsign, c.addbeg,
                                 c.condisn, c.isn,
                                 c.objisn, nvl(c.parentobjisn,c.objisn) AS parentobjisn,
                                 nvl(c.objprnclassisn,c.objclassisn) AS parentobjclassisn,
                                 c.objregion,c.objcountry, c.objclassisn
                                 ,c.newaddisn
                                 ,a.bizflg
                                 ,agrinsurancedateend
                                 , c.premagr  AS premagrKoef
                                 , c.addisn SourceAdd /* KGS 10.01.2012 поле определяет, из какого аддендума пришел конд. чтобы "пачки также жались"*/
                          FROM a, repcond c
                          WHERE nvl(c.premiumsum,0)>=0
                            AND c.addisn=a.addisn
                          /*UNION ALL
                          SELECT --+ index ( c X_REPCOND_NEWADDISN ) ordered use_nl ( a c )
                                 c.newaddisn AS addisn,
                                 c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                                 c.agrclassisn, c.agrcomission, c.agrdiscr,c.rptclassisn,c.AgrIsn,c.riskclassisn, c.riskprnclassisn,
                                 c.premusd AS premusd, c.premagr AS premagr, c.premiumsum AS premiumsum,
                                 c.premcurrisn, c.clientisn, c.AgrOldDateEnd, c.addsign, c.addbeg,
                                 c.condisn, c.isn,
                                 c.objisn, nvl(c.parentobjisn,c.objisn) AS parentobjisn,
                                 nvl(c.objprnclassisn,c.objclassisn) AS parentobjclassisn,
                                 c.objregion,c.objcountry, c.objclassisn
                                 ,c.newaddisn
                                 ,a.bizflg
                                 ,agrinsurancedateend
                                 ,-c.premagr AS premagrKoef
                                 , c.addisn SourceAdd
                          FROM a, repcond c
                          WHERE nvl(c.premiumsum,0)>=0
                            AND c.newaddisn=a.addisn*/
                        ) c
                   WHERE trunc(c.datebeg)<=c.agrinsurancedateend
                 )c
            group by c.addisn, c.newaddisn,
                     c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                     c.agrclassisn, c.agrcomission, c.agrdiscr, c.rptclassisn,c.agrisn,
                     c.riskclassisn,c.riskprnclassisn,SourceAdd


            )
        )
    ) Where Nvl(CondPc,0)<>0;

    --=======борьба с минусоми на переходах на аддендумы НЕ ТОЛЬКО с нулевой плановой премией по медицинским договорам
    insert into REP_COND_LIST
    (loadtype, bodyisn, condisn, repcondisn, condpc, datebeg, dateend,
     datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
     objisn,PARENTOBJISN,bizflg, premusd, premagr
    )
    WITH x AS (
                SELECT c.addisn, c.newaddisn, c.pc, c.agrinsurancedateend, c.bizflg, premagrall#2, cntall#2
                FROM (
                      SELECT c.addisn, c.newaddisn, c.bizflg, c.agrinsurancedateend,
                             decode(c.premagrall,0,1/c.cntall,c.premagr/c.premagrall) AS pc
                             ,premagr AS premagrall#2 /*EGAO 23.11.2012 premagrall#2*/,cntall#2
                      FROM (
                            SELECT --+ index ( c X_REPCOND_ADDISN ) ordered use_nl ( a c )
                                   c.addisn,
                                   c.newaddisn,
                                   SUM(c.premagr) AS premagr,
                                   SUM(sum(c.premagr)) over (PARTITION BY c.addisn) AS premagrall,
                                   COUNT(1)  over (PARTITION BY c.addisn) AS cntall,
                                   max(a.bizflg) AS bizflg,
                                   max(a.agrinsurancedateend) AS agrinsurancedateend,
                                   /*EGAO 23.11.2012 SUM(CASE WHEN newaddisn IS NOT NULL THEN c.premagr END) AS premagrall#2,*/
                                   COUNT(1)/*EGAO 23.11.2012 COUNT(CASE WHEN newaddisn IS NOT NULL THEN 1 END)*/ AS cntall#2
                            FROM (SELECT DISTINCT
                                         nvl(b.addisn, b.agrisn) AS addisn,
                                         b.bizflg,
                                         CASE
                                           WHEN vDateStage BETWEEN trunc(b.agrdatebeg) AND trunc(b.agrdateend) THEN
                                             least(trunc(b.agrdateend), add_months(trunc(b.agrdatebeg),trunc(months_between(vDateStage,trunc(b.agrdatebeg))/12)*12+12)-1)
                                           ELSE trunc(b.agrdateend)
                                         END AS agrinsurancedateend
                                  FROM (select isn from dicti start with isn IN (686160416, 683205716) connect by prior isn = parentisn) x,
                                       report_buhbody_list b
                                  WHERE b.ruleisnagr=x.isn  and loadtype is NULL
                                 ) a, repcond c
                            WHERE c.addisn=a.addisn
                              AND nvl(c.premiumsum,0)>=0
                              AND trunc(c.datebeg)<=a.agrinsurancedateend
                            GROUP BY c.addisn, c.newaddisn
                           ) c

                     ) c
                WHERE nvl(pc, 0)<>0
               ),
          c AS (
                SELECT c.addisn, c.newaddisn, c.bizflg, c.agrinsurancedateend,
                        c.pc, premagrall#2, cntall#2,newaddpremagr,newaddcnt
                FROM (
                      SELECT --+ use_hash ( b c )
                             x.*,
                             b.newaddpremagr,
                             b.newaddcnt
                      FROM (
                            SELECT --+ index ( c X_REPCOND_ADDISN ) ordered use_nl ( a c )
                                   c.addisn,
                                   SUM(c.premagr) AS newaddpremagr,
                                   COUNT(1) AS newaddcnt
                            FROM (SELECT DISTINCT newaddisn, agrinsurancedateend FROM x) a, repcond c
                            WHERE c.addisn=a.newaddisn
                              AND nvl(c.premiumsum,0)>=0
                              AND trunc(c.datebeg)<=a.agrinsurancedateend
                            GROUP BY c.addisn
                           ) b, x
                      WHERE b.addisn=x.newaddisn
                     ) c
               )
    Select * from
    (
     select 3 loadtype, addisn AS Bodyisn,  condisn, repcondisn,
            CondPc,
            agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn, agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
            OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
            CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
            objisn, PARENTOBJISN,bizflg, premusd, premagr

    from
        (
            select c.addisn , c.newaddisn,
                   c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                   c.agrclassisn, c.agrcomission, c.agrdiscr,
                   to_number(NULL) AS objclassisn,
                   c.rptclassisn,c.AgrIsn,
                   to_number(NULL) AS objregion,
                   to_number(NULL) AS objcountry,
                   c.riskclassisn, c.riskprnclassisn,
                   to_number(NULL) AS parentobjclassisn,
                   max (c.isn) repcondisn, max (c.condisn) condisn,
                   sum (c.PREMAGRKOEF)  st_premagr,
                   MAX(c.clientisn) Clientisn ,Max(c.AgrOldDateEnd) AgrOldDateEnd,Max(c.addsign) addsign,Max(c.addbeg) addbeg,
                   to_number(NULL) AS objisn,
                   to_number(NULL) AS parentobjisn,
                   Max(bizflg) bizflg,
                   SUM(c.premusd) AS premusd,
                   SUM(c.premagr) AS premagr,
                   SUM(c.condpc*c.pc*c.ssign) AS condpc,
                   sourceadd,
                   transfernewadd,
                   ssource
            FROM (
                   SELECT c.*,
                          decode(nvl(Trunc(addprem,2),0),0,1/addcnt,premagr/addprem) AS condpc
                   FROM (
                         SELECT --+ Ordered Use_Nl(ag)
                                 c.addisn,
                                 c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                                 c.agrclassisn, c.agrcomission, c.agrdiscr,c.rptclassisn,c.AgrIsn,c.riskclassisn, c.riskprnclassisn,
                                 decode(Nvl(c.premusd,0),0,gcc2.gcc2(c.premiumsum,c.premcurrisn,53,sysdate-1),c.premusd) AS premusd,
                                 c.premagr,
                                 c.premcurrisn, c.clientisn, c.AgrOldDateEnd, c.addsign, c.addbeg,
                                 c.condisn, c.isn, c.objisn, c.parentobjisn, c.parentobjclassisn, c.objregion,c.objcountry, c.objclassisn,
                                 c.bizflg,
                                 c.newaddisn,
                                 premagrKoef,
                                 SourceAdd,
                                 c.pc,
                                 c.ssign,
                                 c.transfernewadd,
                                 c.ssource,
                                 c.addprem,
                                 c.addcnt
                         FROM (
                               SELECT 1 AS ssource,
                                      c.addisn,
                                      c.pc,
                                      cd.agrdatebeg, cd.agrdateend, cd.datebeg, cd.dateend, cd.riskruleisn, cd.agrruleisn,
                                      cd.agrclassisn, cd.agrcomission, cd.agrdiscr,cd.rptclassisn,cd.AgrIsn,cd.riskclassisn, cd.riskprnclassisn,
                                      cd.premusd, cd.premagr, cd.premiumsum,
                                      cd.premcurrisn, cd.clientisn, cd.AgrOldDateEnd, cd.addsign, cd.addbeg,
                                      cd.condisn, cd.isn,
                                      cd.objisn, nvl(cd.parentobjisn,cd.objisn) AS parentobjisn,
                                      nvl(cd.objprnclassisn,cd.objclassisn) AS parentobjclassisn,
                                      cd.objregion,cd.objcountry, cd.objclassisn
                                      ,cd.newaddisn
                                      ,c.bizflg
                                      ,c.agrinsurancedateend
                                      ,cd.premagr  AS premagrKoef
                                      ,cd.addisn SourceAdd
                                      ,1 AS ssign
                                      ,c.newaddisn AS transfernewadd
                                      ,c.newaddpremagr AS addprem
                                      ,c.newaddcnt AS addcnt
                               FROM c, repcond cd
                               WHERE cd.addisn=c.newaddisn
                                 AND nvl(cd.premiumsum,0)>=0
                               UNION ALL
                               SELECT 2 AS ssource,
                                      c.addisn,
                                      c.pc,
                                      cd.agrdatebeg, cd.agrdateend, cd.datebeg, cd.dateend, cd.riskruleisn, cd.agrruleisn,
                                      cd.agrclassisn, cd.agrcomission, cd.agrdiscr,cd.rptclassisn,cd.AgrIsn,cd.riskclassisn, cd.riskprnclassisn,
                                      cd.premusd, cd.premagr, cd.premiumsum,
                                      cd.premcurrisn, cd.clientisn, cd.AgrOldDateEnd, cd.addsign, cd.addbeg,
                                      cd.condisn, cd.isn,
                                      cd.objisn, nvl(cd.parentobjisn,cd.objisn) AS parentobjisn,
                                      nvl(cd.objprnclassisn,cd.objclassisn) AS parentobjclassisn,
                                       cd.objregion,cd.objcountry, cd.objclassisn
                                      ,cd.newaddisn
                                      ,c.bizflg
                                      ,c.agrinsurancedateend
                                      ,-cd.premagr  AS premagrKoef
                                      ,cd.addisn SourceAdd
                                      ,-1 AS ssign
                                      ,c.newaddisn AS transfernewadd
                                      ,c.premagrall#2 AS addprem
                                      ,c.cntall#2 AS addcnt
                               FROM  c, repcond cd
                               WHERE cd.addisn=c.addisn AND cd.newaddisn=c.newaddisn
                                 AND nvl(cd.premiumsum,0)>=0
                              ) c
                         WHERE trunc(c.datebeg)<=c.agrinsurancedateend
                        ) c
                        WHERE decode(nvl(Trunc(addprem,2),0),0,1/addcnt,premagr/addprem)<>0
                 )c
            group by c.addisn, c.newaddisn,
                     c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                     c.agrclassisn, c.agrcomission, c.agrdiscr, c.rptclassisn,c.agrisn,
                     c.riskclassisn,c.riskprnclassisn,SourceAdd, transfernewadd, ssource
           )
    ) Where Nvl(CondPc,0)<>0;
     --} END OF EGAO 31.08.2012


    --}
    update --+ use_hash (b)
    Report_BuhBody_List b set loadtype = 3
    where loadtype is null
      and nvl(b.addisn, b.agrisn) in (select bodyisn from REP_COND_LIST where loadtype = 3);

/* добавляем кусок для слоя 3 - для него проводки уже нагенерированны*/

  --...потом привязанные к договорам без кондов...
    insert into REP_COND_LIST (loadtype, bodyisn, repcondisn, condpc,
     datebeg, dateend, ruleisnagr, agrclassisn, comission, agrdiscr,AgrIsn,CLIENTISN,bizflg)
    select 4, agrisn, repcondisn, 1,
     datebeg, dateend, ruleisn, classisn, comission, discr,AgrIsn,CLIENTISN,bizflg
    from (select --+ ordered use_nl (b a) index (a X_REPAGR_AGR) no_merge ( b )
    a.agrisn, isn repcondisn,
    datebeg, dateend, ruleisn, classisn, comission, discr,CLIENTISN,bizflg
    from (select distinct agrisn from Report_BuhBody_List
     where loadtype is null  and agrisn is not null) b, repagr a
    where b.agrisn = a.agrisn);

    update --+ use_hash (b)
    Report_BuhBody_List b set loadtype = 4
    where loadtype is null
      and agrisn in (select bodyisn from REP_COND_LIST where loadtype = 4);

  --...очищаем мусор...
    delete from REP_COND_LIST where nvl (condpc,0) = 0;

SET_RPTGROUPISN_BY_COND; -- проставили учетную группу "по кондам"

set_budgetroupisn_by_cond; --  проставили мотивационную группу "по кондам" KGS. 14.03.2011



IF pIsFull=0 Then

 select seq_rep_block.NEXTVAL into vBlockIsn1 from dual;
 RepLog_i (pLoadIsn, 'LoadBuh2Cond','Delete exists', pAction => 'Begin', pBlockIsn=>vBlockIsn1);


loop
  Delete --+ Index (a x_RepBuh2Cond_bodyisn)
   From RepBuh2Cond A Where BodyIsn In (Select isn From tt_rowId) and rownum<=300000;
exit When sql%rowcount<=0;

 If pCommitEveryPut=1 then commit; end if;

end loop;

Loop
  Delete --+ Index (a X_REPBUH2COND_PARENT)
   From RepBuh2Cond A Where ParentIsn In (Select isn From tt_rowId) and rownum<=300000;
exit When sql%rowcount<=0;

 If pCommitEveryPut=1 then commit; end if;

end loop;


 RepLog_i (pLoadIsn, 'LoadBuh2Cond','Delete exists', pAction => 'End', pBlockIsn=>vBlockIsn1);
 end if;






select count(*) into vBufCnt from Report_BuhBody_List;

    -- контроль размера буфера, собираем результат по 1-й проводке
for cur in (select rowid rid, rownum rn from Report_BuhBody_List)/*(select DECODE(vCnt,0,rowid,NUll) rid, rownum rn from Report_BuhBody_List where (vCnt=0 or rownum <=1) )*/ loop


  --...собираем RepBuh2Cond...
    insert into TT_RepBuh2Cond (Isn, LoadIsn, BuhIsn, CondIsn, RepCondIsn, CondPC, DocSumPC, BuhPC, FactPC, BodyIsn, AgrIsn, AddIsn, RefundIsn,
     StatCode, DeptIsn, ClassIsn, SubAccIsn, BuhCurrIsn, BuhAmount, BuhAmountRub,BUHAMOUNTUSD, QuitAmount, FactCurrIsn, FactSum,
     DateVal, DateQuit, DatePay, DatePayLast,  DateBeg, DateEnd, RuleIsn, RuleIsnAgr, AgrClassIsn, Comission,
     AccCurrIsn, AgrDiscr, ObjClassIsn,rptclassisn,datebegcond,dateendcond,OBJREGION,OBJCOUNTRY,Riskclassisn,
     DocSumIsn, DatePayPlan, AgrBuhDate, DocIsn, AddSign, ReprDeptIsn,BODYSUBJISN,SUBJISN,HEADISN,CLIENTISN,CLIENTJURIDICAL,ADDDATEBEG,
     AgrOldDateEnd,AgrCurrIsn,BizFlg,Amount,AmountRub,AmountUsd,PARENTOBJCLASSISN,
     REFUNDEXTISN,RISKPRNCLASSISN,DocIsn2,Sagroup,CORSUBACCISN,Parentisn,RptGroupIsn,ISREVALUATION,objisn,
      PARENTOBJISN,adeptisn, buhheadfid, dsclassisn, dsclassisn2, refundclassisn,
     motivgroupisn,budgetgroupisn /*KGS. 14.03.2011*/, premusd, premagr, Dsstatus,CARRPTCLASS
      )
    select --+ use_hash (b r) use_Nl(r ag sb) ordered
     Seq_Reports.NextVal, vLoadIsn, b.isn, Nvl(r.condisn,0), r.repcondisn, nvl (r.condpc,1)*Nvl(RefundPc,1), dpc, bpc, fpc,
     b.bodyisn, Nvl(r.agrisn,0),Nvl(Nvl( b.addisn,b.agrisn),0), b.refundisn, b.statcode, b.deptisn, b.classisn, b.subaccisn, b.currisn,
     b.BuhAmount, b.BuhAmountRub,B.BUHAMOUNTUSD, QuitAmount, b.QuitCurrIsn, b.AmountClosedQuit, b.DateVal, b.DateQuit,
     b.QuitDateVal, b.DatePayLast,  r.DateBeg, r.DateEnd, r.RuleIsn, r.RuleIsnAgr, r.AgrClassIsn, r.Comission,
     b.AccCurrIsn, r.AgrDiscr, r.ObjClassIsn,R.rptclassisn,r.DateBegCond,r.DateEndCond,
      (Select Isn
           from Region
            Where Nvl(PArentIsn,0)=0
           start with
            isn= Decode(Nvl(ObjRegion,0),0, Nvl((Select REGIONISN from repsubject Where Isn=SubjIsn),
                                  Nvl((Select REGIONISN from repsubject Where Isn=Ag.ClientIsn),
                                  Nvl((Select REGIONISN from repsubject Where Isn=FIlIsn),
                                  Nvl((Select REGIONISN from repsubject Where Isn=EmitIsn),
                                  Nvl((Select REGIONISN from repsubject Where Isn=BodySubjIsn),
                                  (Select REGIONISN from repsubject Where Isn=ReprDeptIsn)
                                  ))))),ObjRegion)
      connect by prior parentisn=isn) ObjRegion,
      OBJCOUNTRY,Riskclassisn,
     b.DocSumIsn, b.DatePay, b.AgrBuhDate, B.docisn, R.AddSign, b.ReprDeptIsn,B.bodysubjisn,B.subjisn,B.headisn,
     NVL(r.ClientIsn,Ag.clientisn),
     Decode(NVL(r.ClientIsn,Ag.clientisn),null,null,Nvl(Sb.JURIDICAL,
     Decode(B.DeptIsn,504,'Y',
     505,'Y',
     742950000,'Y',
     1112083803,'Y',
     707480016,'N',
     506,'Y',
     11414319,'N',
     23735116,'N','N'))) ClientJURIDICAL,
     r.addbeg,r.AgrOldDateEnd,Ag.currisn,ag.bizflg,
     b.BuhAmount*nvl (r.condpc,1)*dpc*bpc*fpc*RefundPc,
     b.BuhAmountRub*nvl (r.condpc,1)*dpc*bpc*fpc*RefundPc,
     B.BUHAMOUNTUSD*nvl (r.condpc,1)*dpc*bpc*fpc*RefundPc,
     PARENTOBJCLASSISN,REFUNDEXTISN,RISKPRNCLASSISN,
     b.DocIsn2,
     b.Sagroup,
     CORSUBACCISN,
     b.parentisn,
     decode(Nvl(b.RptGroupIsn,0),0,Nvl(r.RptGroupIsn,0),Nvl(b.RptGroupIsn,0)) RptGroupIsn,
     --{EGAO 05.06.2012
     /*
     CASE
       WHEN Nvl(r.agrisn,0)=0 THEN
         CASE
           WHEN NVL(b.currisn,-1)=35 THEN 0
           ELSE 1
         END
       ELSE
         (SELECT COUNT(1)
          FROM dual
          WHERE EXISTS (SELECT 'x' FROM rep_isreval b WHERE b.agrisn=r.agrisn)
         )
     END */
     CASE WHEN NVL(b.currisn,-1)=35 THEN 0 ELSE 1 END ISREVALUATION,
     --}
     r.objisn, r.PARENTOBJISN,
     b.adeptisn,b.buhheadfid,b.dsclassisn,b.dsclassisn2, r.refundclassisn,
     r.motivgroupisn, --KGS. 14.03.2011
     r.budgetgroupisn --KGS. 14.03.2011
     ,r.premusd
     ,r.premagr -- EGAO 07.09.2011
     ,b.dsstatus
     ,r.CARRPTCLASS
    from Report_BuhBody_List b, REP_COND_LIST r,repagr ag,repsubject sb
    where b.rowid=cur.rid--(b.rowid=cur.rid  or cur.rid is null) --MSerp 15.10.2010
      and b.loadtype = r.loadtype (+)
      and  decode (b.loadtype,
      1,b.refundisn,
      2,b.agrisn,--EGAO 02.03.2011 2,decode(dsdatebeg,null,decode(dsdateend,null,Nvl(b.addisn,b.agrisn),vDocsumIsn),vDocsumIsn),
      3,nvl(b.addisn, b.agrisn),--EGAO 02.03.2011 3,decode(dsdatebeg,null,decode(dsdateend,null,Nvl(b.addisn,b.agrisn),vDocsumIsn),vDocsumIsn),
      5,nvl(b.addisn, b.agrisn),
      4,b.agrisn) = r.bodyisn (+)
      And R.AgrIsn=Ag.AgrIsn(+)
      And ag.ClientIsn=Sb.Isn(+);
vCnt:=vCnt+sql%rowcount;

If vCnt>=100000 or cur.rn=vBufCnt then --If vCnt>=100000 or cur.rn>=vBufCnt then --MSerp 15.10.2010
/* в буфере больше 100000 или последняя проводка*/
vallCnt:=vallCnt+vCnt;
   select seq_rep_block.NEXTVAL into vBlockIsn from dual;
     RepLog_i (pLoadIsn, 'LoadBuh2Cond','Put data', pAction => 'Begin', pBlockIsn=>vBlockIsn);


  select seq_rep_block.NEXTVAL into vBlockIsn1 from dual;
   RepLog_i (pLoadIsn, 'LoadBuh2Cond','Set analitiks', pAction => 'Begin', pBlockIsn=>vBlockIsn1);


/* проставляем синтетические классификаторы */
     SET_RPTCLASS;
-- KGS. 14.03.2011 теперь по кондам, выше    SET_BUDGETROUPISN_NEW;
     /*закомментировал EG_AO 25.07.2008 т.к. поле motivgroupisn
       таблицы tt_RepBuh2Cond изменяется в процедуре SET_BUDGETROUPISN_NEW
     set_motivgroupisn;
     */
-- KGS 15.03.2011 теперь по проводкам, выше     SET_AGR_BUHDATE;
--     CheckRevaluation;
 RepLog_i (pLoadIsn, 'LoadBuh2Cond','Set analitiks', pAction => 'Begin', pBlockIsn=>vBlockIsn1);


  Insert Into Repbuh2cond(
  isn,loadisn,buhisn,condisn,condpc,bodyisn,agrisn,addisn,refundisn,statcode,
  deptisn,subaccisn,buhcurrisn,buhamount,buhamountrub,quitamount,factcurrisn,
  factsum,dateval,datequit,datepay,classisn,datepaylast,buhpc,factpc,docsumpc,
  agrbuhdate,repcondisn,isrevaluation,datebeg,dateend,ruleisn,ruleisnagr,
  agrclassisn,comission,acccurrisn,rptgroupisn,agrdiscr,objclassisn,reprdeptisn,
  objregion,bizflg,buhamountusd,rptclassisn,datebegcond,dateendcond,agrcurrisn,
  objcountry,riskclassisn,riskprnclassisn,objisn,parentobjisn,riskisn,parentriskisn,
  limitisn,rptclass,docsumisn,docisn,datepayplan,addsign,headisn,bodysubjisn,subjisn,
  clientisn,clientjuridical,adddatebeg,agrolddateend,amount,amountrub,amountusd,
  budgetgroupisn,parentobjclassisn,refundextisn,docisn2,sagroup,corsubaccisn,motivgroupisn,
  parentisn,adeptisn,buhheadfid,dsclassisn,dsclassisn2,agrdetailisn,agrstatus,
  refundclassisn,condpremusd,dsstatus,condpremagr,carrptclass)
  select 
  isn,loadisn,buhisn,condisn,condpc,bodyisn,agrisn,addisn,refundisn,statcode,deptisn,
  subaccisn,buhcurrisn,buhamount,buhamountrub,quitamount,factcurrisn,factsum,dateval,
  datequit,datepay,classisn,datepaylast,buhpc,factpc,docsumpc,agrbuhdate,repcondisn,isrevaluation,
  datebeg,dateend,ruleisn,ruleisnagr,agrclassisn,comission,acccurrisn,rptgroupisn,agrdiscr,
  objclassisn,reprdeptisn,objregion,bizflg,buhamountusd,rptclassisn,datebegcond,dateendcond,
  agrcurrisn,objcountry,riskclassisn,riskprnclassisn,objisn,parentobjisn,riskisn,
  parentriskisn,limitisn,rptclass,docsumisn,docisn,datepayplan,addsign,headisn,
  bodysubjisn,subjisn,clientisn,clientjuridical,adddatebeg,agrolddateend,amount,
  amountrub,amountusd,budgetgroupisn,parentobjclassisn,refundextisn,docisn2,
  sagroup,corsubaccisn,motivgroupisn,parentisn,adeptisn,buhheadfid,dsclassisn,
  dsclassisn2,agrdetailisn,agrstatus,refundclassisn,premusd,dsstatus,premagr,carrptclass
  from tt_repbuh2cond;

--  Insert Into repbuh2cond_small (select * from v_repbuh2cond_small);


 If pCommitEveryPut=1 then commit; end if;

 RepLog_i (pLoadIsn, 'LoadBuh2Cond','Put data',pObjCount=>vCnt, pAction => 'End', pBlockIsn=>vBlockIsn);

 --delete from tt_repbuh2cond;
execute immediate 'truncate table tt_repbuh2cond';

 vCnt:=0;

end if;
end loop;

/* перед коммитом чистим темповые таблицы - чтобы не делать контрольную точку редологов*/
    Execute immediate 'truncate table  Report_BuhBody_List'; /* nienie i?iaiaie, eioi?ua caeeaaai*/
    Execute immediate 'truncate table  REP_COND_LIST'; /* nienie eiiaia, ia eioi?ua yoe i?iaiaee eyaoo */

    Execute immediate 'truncate table   TT_RepBuh2Cond';      /* aooa? iia aioiaue eonie ?acoeuoe?o?uae oaaeeou*/



commit;
 RepLog_i (pLoadIsn, 'LoadBuh2Cond','Load_by_List', pAction => 'End', pObjCount=>vallCnt,pBlockIsn=>vBlockIsn0);

End;

Procedure LoadBuh2Cond_BY_DATE
(
pLoadIsn IN Number,
pDatebeg DATE

)

/*Загрузка витрины repbuh2cond*/
Is
vMinIsn Number:=-1e30;

     vMaxIsn integer:=0;
     SesId        Number;
     vSql Varchar2(4000);
     vCnt Number:=0;
     vBlockIsn number;
     vBlockIsn1 number;
     vLoadObjcnt Number:=10000;

     vDateend date:=ADD_MONTHS(Trunc(sysdate,'mm'),1)-1;
Begin





DBMS_APPLICATION_INFO.set_module('LoadBuh2Cond','');


For Cur in (select To_Char(ADD_MONTHS(Trunc(pDatebeg,'mm'),Level-1),'mmyyyy') Part from dual
connect by Level<= MONTHS_BETWEEN(vDateend+1 ,Trunc(pDatebeg,'mm'))
) Loop

  Execute Immediate 'Alter table Storages.RepBuh2Cond truncate partition P'||Cur.Part ;
 -- Execute Immediate 'Alter table Storages.RepBuh2Cond_Small truncate partition P'||Cur.Part ;

end loop;

-- заполняем буффер для простановки учетных групп

execute immediate 'truncate table Storages.TT_RULE_RPNGRP';

  INSERT INTO storages.tt_rule_rpngrp(ruleisn,groupisn,typerule)
  SELECT a.ruleisn, a.rptgroupisn, 'COND' AS typerule
  FROM v_rptgroup2rule a;

  INSERT INTO storages.tt_rule_rpngrp(ruleisn,groupisn,typerule)
  SELECT a.agrruleisn, a.rptgroupisn, 'AGR'
  FROM v_rptgroup2agrrule a;


commit;


/* режем repbuhbody вдоль bodyisn и загружаем*/
 SesId:=PARALLEL_TASKS.createnewsession('LoadBuh2cond');

--{EGAO 06.12.2012
--В связи с изменениями в расчете РЗУ обновление таблицы REPDOCSUM закомментировано
/*if vMinIsn<0 then
vSql:='Storages.report_buh_storage_new.LoadDocSumm_WO_Buhbody('||pLoadIsn||');';
PARALLEL_TASKS.processtask(sesid,vSql);
end if;*/
--}
select /*+ Full(rb) Parallel (rb 48)*/
Count( Distinct BodyIsn)
into vCnt
 from repbuhbody rb
 Where dateval between Trunc(pDatebeg,'mm') and vDateend;


vLoadObjcnt:=round(vCnt/vLoadObjcnt);

vCnt:=0;

For Cur in (
Select Pocket,Min(BodyIsn) MinIsn,Max(BodyIsn) MaxIsn
from (
Select BodyIsn,Ntile(vLoadObjcnt) over (order by BodyIsn) Pocket
from (
select /*+ Full(rb) Parallel (rb 48)*/
 Distinct BodyIsn
 from repbuhbody rb
 Where dateval between Trunc(pDatebeg,'mm') and vDateend
 )

)
Group by Pocket
order by Pocket

) Loop


    vCnt:=vCnt+1;
          vSql:='Begin
                 DBMS_APPLICATION_INFO.set_module(''LoadBuh2Cond by Period'',''Process: '||vCnt||''');

                 /*положили в tt_rowid список bodyisn  */
                    Delete from tt_rowid;

                    Insert Into tt_rowid (Isn )
      Select --+ Index_Asc (b X_REPBUHBODY_BODYISN)
       Distinct BodyIsn
      from RepBuhBody b
      where bodyIsn between '||Cur.MinIsn||' And '||Cur.MaxIsn||'
      and  dateval between '''||Trunc(pDatebeg,'mm')||''' and '''||vDateend||''' ;
        Commit;


     /* процедура обработки блока*/
     Storages.REPORT_BUH_STORAGE_NEW.LoadBuh2Cond_By_List('||ploadisn||');
     END;';
          PARALLEL_TASKS.processtask(sesid,vSql,1);

    DBMS_APPLICATION_INFO.set_module('LoadBuh2Cond','Applied : '||vCnt*vLoadObjcnt);
end loop;


PARALLEL_TASKS.endsession(sesid);



rebuld_table_index('repbuh2cond');
setrefundrptgroup; -- проставляем УГ для убытков

end;

procedure SetRefundRptGroup
(pRefundIsn IN Number := 0)
 Is
  vCommitCnt Number:=30000;

  vMinIsn Number;
  vLMaxIsn Number;

  vCnt Number;

  SesId Number;
  vSql Varchar2(4000);

 Begin

--------------------------------------

store_and_drop_table_index('storage_source.reprefund',1);

SesId:=Parallel_Tasks.createnewsession;
vMinIsn:=0;
vCnt:=0;
Loop

     if (pRefundIsn = 0) then

           Select Max(Isn)
           Into vLMaxIsn
           From (
             Select --+ Index_Asc(a x_RepRefund)
                 A.Isn
              from RepRefund A
              where Isn>vMinIsn
             And RowNum<=vCommitCnt
                 );
      else

           Select Max(Isn)
           Into vLMaxIsn
           From (
             Select --+ Index(A x_RepRefund_RefundIsn)
                 A.Isn
              from RepRefund A
              where refundisn = pRefundIsn
              and Isn>vMinIsn
                 );
             vMinIsn:=vLMaxIsn-1e-10;

      end if;
   Exit When vLMaxIsn is null;

vSql:=' Begin
           Storages.REPORT_BUH_STORAGE_NEW.setrefundrptgroup_by_isns('||vminisn||','||vlmaxisn||');
        end;';

Parallel_Tasks.processtask(sesid,vsql);
    vMinIsn:=vlMaxIsn;
    vCnt:=vCnt+1;
    DBMS_APPLICATION_INFO.set_module('SetRefundRptGroup','Applied : '||vCnt*vCommitCnt);
 End loop;

Parallel_Tasks.endsession(sesid);

Restore_table_index('storage_source.reprefund');



-- глухая заглушка по указанию Самохвалова Р. Нижеперечисленные убытки отнести к 13 уг не смотря ни на что.
--02.11.06

Update reprefund
set
rptgroupisn=747778600,
CondDeptIsn=11414319
where claimisn in (693641216,692701116,690825516,690823116,690823316,690823016,
690821916 --MSerp. 07.04.2008 Добавил по указанию Дмитревской. Задача 5014140606
);
Commit;

end;

procedure SetRefundRptGroup_By_Isns (vMinIsn number,vLMaxIsn Number)
Is
   rptgrp number;
   RefIsn TNesTable:=TNesTable();
   Rpt TNesTable:=TNesTable();
   Dept TNesTable:=TNesTable();
   IsReval TNesTable:=TNesTable();
   Budget TNesTable:=TNesTable();
Begin

-- Сначала пытаемся гуртом проставить медиков и осаго
-- тупо налили медицину и осаго

Update  --+ Index(rf x_RepRefund ) Ordered Use_Nl(rf)
    RepRefund Rf
Set
    (CondDeptIsn,ISREVALUATION,RptGroupIsn/*,BUDGETGROUPISN*/)=
    (                      Select --+ Index(a X_REPBUH2COND_AGRISN)

                            decode (deptisn,23735116,deptisn,11414319),
                             ISREVALUATION,
                             Decode(deptisn,23735116,747777500,818752900)/*,
                             Decode(deptisn,23735116,BUDGETGROUPISN,1343940003)*/

                            From RepBuh2Cond A
                            Where statcode in (38,34,221,224) and sagroup in (1,3) and a.agrisn=rf.agrisn
                            and (deptisn=23735116 or rptgroupisn=818752900)
                            and rownum<=1
      )
where Isn > vMinIsn and Isn<=vLMaxIsn ANd Nvl(rptgroupisn,0)=0 And nvl(Agrisn,0)>0;

--{EGAO 29.03.2013 в рамках  ДИТ-13-1-198006 Доработки ХД (ОСГОП)
Update  --+ Index(rf x_RepRefund )
    RepRefund Rf
Set
    RptGroupIsn=4138698903,
    CondDeptIsn=(Select MAX(d.classisn1)
                 From dicx d
                 where d.classisn=2106305603
                   and d.classisn2=rf.ruleisnagr
                )
where Isn > vMinIsn and Isn<=vLMaxIsn ANd Nvl(rptgroupisn,0)=0 And nvl(Agrisn,0)>0
AND rf.ruleisnagr IN (SELECT isn FROM dicti d START WITH d.isn=4053506403 CONNECT BY PRIOR d.isn=d.parentisn);
--}

--{ EGAO 06.05.2013 в рамках ДИТ-13-2-199598
UPDATE reprefund rf
SET rf.rptgroupisn=755078500,
    rf.conddeptisn=504
WHERE rf.Isn > vMinIsn and rf.Isn<=vLMaxIsn ANd Nvl(rf.rptgroupisn,0)=0 And nvl(rf.Agrisn,0)>0
AND rf.agrclassisn=8746
AND rf.ruleisnagr IN (SELECT isn FROM dicti d START WITH d.isn=36628616 CONNECT BY PRIOR d.isn=d.parentisn);
--}

Update  --+ Index_Asc(rf x_RepRefund_n)
    RepRefund Rf
Set
    (CondDeptIsn,ISREVALUATION,RptGroupIsn/*,BUDGETGROUPISN*/)=
                          (

                          -- типа, если есть начисления, то DeptIsn,ISREVALUATION,BUDGETGROUPISN - есть всегда
                          -- УГ или есть или нет, там ниже разберемся
                           Select distinct first_value(DeptIsn) over (order by st desc, Cnt Desc),
                                           first_value(ISREVALUATION) over (order by st desc,Cnt Desc),
                                           CASE
                                             WHEN a.ruleisnagr=37504416 THEN 0
                                             ELSE first_value(decode(ruleisn,rf.riskruleisn,RptGroupIsn)) over (order by decode(ruleisn,rf.riskruleisn,1,0) desc,st desc,Cnt Desc)
                                           END -- EGAO 01.03.2012 в рамках ДИТ-12-1-160925 first_value(decode(ruleisn,rf.riskruleisn,RptGroupIsn)) over (order by decode(ruleisn,rf.riskruleisn,1,0) desc,st desc,Cnt Desc)
                            from
                            (
                            Select --+ Index(a X_REPBUH2COND_AGRISN)
                             AgrIsn,DeptIsn,ISREVALUATION,Ruleisn,Sum(AmountUsd) Cnt,
                            Max(RptGroupIsn) RptGroupIsn,
                            /*Max(BUDGETGROUPISN) BUDGETGROUPISN,*/
                            decode(statcode,220,0,24,0,1) st,
                            MAX(a.ruleisnagr) AS ruleisnagr -- EGAO 01.03.2012 в рамках ДИТ-12-1-160925
                            From RepBuh2Cond A
                            Where statcode in (38,34,221,224,220,24) and sagroup in (1,2,3)
                            Group by  AgrIsn,DeptIsn,ISREVALUATION,Ruleisn,decode(statcode,220,0,24,0,1)
                            ) a
                            Where  AgrIsn=Rf.AgrIsn
                            )
where Isn > vMinIsn and Isn<=vLMaxIsn And Nvl(rptgroupisn,0)=0 and Nvl(agrisn,0)>0;

commit;

/* KGS 06.06.2011 простановка признака переоценки, если по договору с убытком нет начислений*/
Update  --+ Index_Asc(rf x_RepRefund_n)
    RepRefund Rf
Set
    (CondDeptIsn)=
                          (Select MAX(d.classisn1) From repagr ra,(Select * from dicx where classisn=2106305603) d
                           Where ra.agrisn=rf.agrisn
                             and ra.ruleisn=d.classisn2 )
where Isn > vMinIsn and Isn<=vLMaxIsn And  Nvl(agrisn,0)>0 and Nvl(CondDeptIsn,0)=0
and Nvl(rptgroupisn,0)=0;

commit;

Update  --+ Index_Asc(rf x_RepRefund_n)
    RepRefund Rf
Set
    (RptGroupIsn,CondDeptIsn)=
                       ( Select --+ use_nl (a b d bc) ordered
                         Nvl(Max(A.groupisn),0),
                         Nvl(Rf.CondDeptIsn,Max(D.filterisn))
                       From TT_RULE_RPNGRP A,dicti d
                        Where A.ruleisn=Rf.riskruleisn
                         And Rf.riskruleisn=D.Isn(+))
where Isn > vMinIsn and Isn<=vLMaxIsn And Nvl(rptgroupisn,0)=0 And Nvl(condisn,0)>0;

commit;

For Cur In (Select --+ Index_Asc(a x_RepRefund_n)
                 RowNum,A.*
              from RepRefund A
              where  Isn > vMinIsn and Isn<=vLMaxIsn
              ANd Nvl(rptgroupisn,0)=0

               ) Loop

  RefIsn.EXTEND;
  Rpt.EXTEND;
  Dept.EXTEND;
  IsReval.EXTEND;
  Budget.EXTEND;

  RefIsn(Cur.RowNum):=Cur.Isn;
  IsReval(Cur.RowNum):=Cur.IsRevaluation;

  If (Rpt(Cur.RowNum) is null)  then -- медецинские убытки, привяз к хоздоговору -

     --{ EGAO 15.03.2012
     Select Max(747777500),Nvl(Cur.Conddeptisn,Max(23735116))
     Into Rpt(Cur.RowNum),Dept(Cur.RowNum)
     from dicti d
     WHERE d.isn IN (1621309703, 977716425)
     START WITH d.isn=Cur.RULEISNCLAIM
     CONNECT BY PRIOR d.parentisn=d.isn
     ;
     --}
   end if;


 IF Cur.conddeptisn=505 then  -- врезка для грузов
  Select max (groupisn)
  into Rpt(Cur.RowNum)
  from   rep_tt_rules2groups
  where deptisn=505 and param=2 and cur.ruleisnagr=46260916;


  Select Nvl(Rpt(Cur.RowNum),max (groupisn))
  into Rpt(Cur.RowNum)
  from   rep_tt_rules2groups
  where deptisn=505 and param=1 and Exists (select /*+ Index (ac)*/1 From Rep_AgrCargo ac Where  sea=1 or More1=1 and agrisn=cur.agrisn);

  Select Nvl(Rpt(Cur.RowNum),max (groupisn))
  into Rpt(Cur.RowNum)
  from   rep_tt_rules2groups
  where deptisn=505 and param=0 ;
end if;

IF (Rpt(Cur.RowNum) is null) Then Rpt(Cur.RowNum):=0; end if;

end loop;


ForAll i in 1..Nvl(RefIsn.Last,0)
     Update  --+ Index_Asc(a x_RepRefund)
      RepRefund A
      Set
          RptGroupIsn=Nvl(Rpt(i),0),
          CondDeptIsn=Dept(i),
          ISREVALUATION=IsReval(i)/*,
          Budgetgroupisn=Budget(i)*/
    Where Isn=RefIsn(i)
      AND Nvl(a.rptgroupisn,0)=0 -- EGAO 05.05.2012
    ;

commit;



    RefIsn.Trim(RefIsn.Last);
    Rpt.Trim(Rpt.Last);
    Dept.Trim(Dept.Last);
    IsReval.Trim(IsReval.Last);
    Budget.Trim(Budget.Last);

SET_REFUND_BUDGETROUPISN_NEW(vMinIsn,vLMaxIsn);
--SET_REFUND_MOTIVGROUPISN(vMinIsn,vLMaxIsn);

UPDATE /*+ index ( a X_REPREFUND )*/
       RepRefund a
SET a.isrevaluation=CASE WHEN NVL(a.currisn,-1)=35 THEN 0 ELSE 1 END
where a.Isn > vMinIsn and a.Isn<=vLMaxIsn;

COMMIT;
end;

procedure LoadRZUMemo (  pLoadIsn   in   Number)
-- запускать после простановки учетных групп в repbuh2cond
Is
Begin

Execute immediate 'Truncate table reprzumemo';
 
Insert Into reprzumemo
Select Seq_Reports.NEXTVAL,pLoadIsn,A.*
From
    (
      select --+ use_nl (d a x) ordered Index (d X_DOCS_BUHACC_STATUS)
         d.isn,d.agrisn,decode (a.discr,'Г',c.get('AgrInOblig'),a.classisn), d.currisn,
         d.amount_cur_1, d.signed, d.credit_amount2, d.PolicyYear,Decode(DeptOwn,c.get ('CarrierDept'),2068, x.classisn2),
         (Case
         When d.Agrisn in (870705616, 870663416, 870783416, 870736216, 870657416, 870660716,
                             870655816, 870658316, 870730616, 870783216, 870787016, 870655716) Then 755075000
         When (a.discr='Г' or a.classisn=9020) Then 755078500
         Else  (Select --+ index ( b X_REPBUH2COND_AGRISN )
                       Max(b.rptgroupisn) from repbuh2cond b Where b.AgrIsn=D.AgrIsn)
         end    ) RptGroupIsn,d.recisn,Nvl(a.deptisn,d.deptown),d.Id,
         a.ruleisn,a.id AgrId,  d.credit_amount
      from Ais.docs d, agreement a,
           (select t.classisn1, MAX(t.classisn2) AS classisn2
            from ais.dicx t WHERE t.classisn=c.get('xRuleInsurType')
            GROUP BY t.classisn1
           ) x
      where d.doc_type='11'
       and d.buhaccisn in (9822516, 690060416)
       and d.status not in ('-1','АН','ОШ','ОЛ')
       and a.isn (+)=d.agrisn
       and x.classisn1 (+)=a.ruleisn
    ) A;
 Commit;
end;


Procedure LoadRefund_By_TT_RowId
(pLoadIsn IN Number,
 IsFull in Number:=1)
IS

Begin

 If IsFull=0 Then

  Delete --+ Index (A X_REPREFUND_REFUNDISN)
   From RepReFund A Where RefundIsn In (Select Isn from TT_RowId);

  Delete --+ Index (A X_REPREFUND_REFUNDISN)
   From RepReFund_Re A Where RefundIsn In (Select Isn from TT_RowId);

 end if;

  insert into reprefund
  (isn, loadisn, refundisn, claimisn, agrisn, rptclassisn, condisn, currisn, claimsum,
  dateloss, dateclaim, datereg, datesolution, dateevent, subjisn, status, claimstatus,
  deptisn, daterefund,--franchtype, franchtariff, franchsum,
  agrdatebeg, lossshare, emplisn, classisn, refundsum, objisn,refundsumUsd,ClaimSumUsd,ClaimId,
  franchtype, franchsum, franchtariff, franchcurrisn, franchdeducted,LimitSum,LimitCurrIsn,
  ruleisnAgr, ruleisnClaim,NRZU,OBJCLASSISN,ParentObjClassisn,firmisn ,AGREXTISN,condpc,RAGRISN,
  ExtDateEvent,TOTALLOSS,RFRANCHCURRISN,RFRANCHSUM,SalerEmplIsn,SalerDeptIsn,RISKRULEISN
  )
  select --+ Use_Nl(s rc a) ordered index ( rc X_REPCOND_COND )
  Seq_Reports.NextVal, pLoadIsn, S.isn, S.claimisn, S.agrisn, S.rptclassisn, S.condisn, S.currisn, S.claimsum,
  S.dateloss, S.dateclaim, S.datereg, S.datesolution, S.dateevent, S.subjisn, S.refstatus, S.clstatus,
  S.deptisn, S.daterefund, --franchtype, franchtariff, franchsum,
  S.agrdatebeg, lossshare, S.emplisn, S.classisn, S.refundsum, S.objisn,
  S.refundsumUsd,
  S.claimsumUsd,
  S.Id,
-- Nvl(s.franchsum,null,rc.franchtype,'Б') franchtype,Nvl(s.franchsum,null,rc.franchsum,s.franchsum) franchsum ,
-- Nvl(s.franchsum,null,rc.franchtariff,0) franchtariff,Nvl(s.franchsum,null,rc.franchcurrisn,s.franchcurrisn) franchcurrisn,
  rc.franchtype, rc.franchsum, rc.franchtariff, rc.franchcurrisn,
    nvl (decode (S.currisn,rc.franchcurrisn, decode (nvl (rc.franchtype,'Б'),'Б',decode (rc.franchtariff,null,rc.franchsum))),0)+
    nvl (S.claimsum*decode (nvl (rc.franchtype,'Б'),'Б',rc.franchtariff),0)/100,
   rc.Limitsum,rc.currisn,
   ag.Ruleisn,
   S.ruleisn,
   S.nrzu,
   rc.OBJCLASSISN,rc.OBJPRNCLASSISN, Ag.FirmIsn,AGREXTISN,
   decode(AllRefundSum,0,1/AllRefund,
   Decode(nvl(refundsum,0),0,Nvl(claimsum,0),nvl(refundsum,0))/
   AllRefundSum),
   RAGRISN,
   ExtDateEvent,
   TOTALLOSS,
   s.franchcurrisn,
   s.franchsum,
   (Select Max(SubjIsn) from agrrole ar Where ar.agrisn=s.agrisn and ar.refundisn=s.isn and ar.classisn=1521585603),
   (Select Max(DeptIsn) from agrrole ar Where ar.agrisn=s.agrisn and ar.refundisn=s.isn and ar.classisn=1521585603),
   rc.RISKRULEISN
From(  --+ Use_Nl(t r cl ext rr) ordered
  Select
  r.isn, r.claimisn, Nvl(ext.agrisn,r.agrisn) agrisn,r.rptclassisn, Nvl(ext.condisn,r.condisn) condisn,
  Decode(Ext.Isn,null,r.currisn,Ext.currisn) currisn,
  Decode(Ext.Isn,null,r.claimsum,Ext.claimsum) claimsum,
  cl.dateloss, cl.dateclaim, cl.datereg, cl.datesolution, Nvl(ext.dateevent,r.dateevent) dateevent, cl.subjisn,r.status refstatus, cl.status clstatus,
  cl.deptisn,r.daterefund,  r.franchsum,r.franchcurrisn,
  cl.agrdatebeg, lossshare, nvl (r.emplisn,cl.emplisn) emplisn, Nvl(Ext.classisn,r.classisn) classisn,
  Decode(Ext.Isn,null,r.refundsum,Ext.refundsum) refundsum  ,
  Nvl(Ext.objisn,r.objisn) objisn,
  Gcc2.gcc2(Decode(Ext.Isn,null,r.refundsum,Ext.refundsum),Decode(Ext.Isn,null,r.currisn,Ext.currisn),53,Nvl(r.daterefund,r.dateevent)) refundsumUsd,
  Gcc2.gcc2(Decode(Ext.Isn,null,r.claimsum,Ext.claimsum),Decode(Ext.Isn,null,r.currisn,Ext.currisn),53,Nvl(cl.dateloss,cl.dateclaim)) claimsumUsd,
   Cl.Id,
  cl.ruleisn,
   r.nrzu,
   Ext.Isn AGREXTISN,
   sum(Decode(Nvl(Decode(Ext.Isn,null,r.refundsum,Ext.refundsum),0),0,Nvl(Decode(Ext.Isn,null,r.claimsum,Ext.claimsum),0),Decode(Ext.Isn,null,r.refundsum,Ext.refundsum))) over (partition by r.isn) AllRefundSum,
   Count(*) over (partition by r.isn) AllRefund,
   r.agrisn RAGRISN,
   ext.DATEEVENT ExtDateEvent,
   cr.TOTALLOSS
  from tt_RowId T, agrrefund r, ais.agrclaim cl,AIS.AgrRefundExt ext,Ais.claimrefundcar cr
    where  t.isn=r.isn
    and  r.claimisn = cl.isn
    And r.Isn=ext.RefundIsn(+)
    ANd r.emplisn not In( Select /*+ Index (sb x_subject_class)*/Isn From subject sb Where ClassIsn=491)
    And t.isn=cr.Isn(+)
   ) s,repcond rc,repagr ag
Where Rc.CondIsn(+)=S.CondIsn
And s.agrisn= ag.agrisn(+);
--and s.AllRefundSum<>0;

  insert  into reprefund_re
(  isn, loadisn, refundisn, currisn, agrxisn, sectisn,
       reisn, sharepc, reagrdeptisn, reagrclassisn, xref,
       refundsum,subjisn, resum,secttype )
  select --+ use_nl (r s c) ordered
  SEQ_REPORTS.NextVal isn, r.*,
  greatest (0,least (getcrosscover(c.limitsum+prioritysum,s.Currisn,53,sysdate),r.RefundSum)-
  getcrosscover(prioritysum,s.Currisn,53,sysdate)) ReSum,
  s.secttype
  from (select --+ use_nl (t r a ra s) index (a X_REP_AGRRE2_AGR) index (ra X_REPAGR_AGR) ordered
  ploadisn, r.Isn refundisn, r.currisn,
  a.isn agrxisn, a.sectisn, a.reisn, a.sharepc,
  ra.deptisn reagrdeptisn,
  ra.classisn reagrclassisn,
  decode (nvl (a.sectisn,0),0,a.sharepc,ais.getrefundre (r.AgrIsn, r.ClaimIsn, 1, a.sectisn)) xref,
  decode (nvl (a.sectisn,0),0,0,ais.getrsum2 (r.ClaimIsn, 53, sysdate, a.objisn, a.riskisn, agrxisn, null, 1)) refundsum,
  a.subjisn
  from TT_RowId t, agrrefund r, rep_agrre a, repagr ra, resection s
  where t.isn = r.isn
  and r.agrisn = a.agrisn
  and a.reisn = ra.agrisn (+)
  and a.sectisn = s.isn (+)
   And ((R.CondIsn=A.CondIsn) or (R.condisn is Null) or (A.CondISn Is Null))
  and not exists (select --+ use_nl (rr ss) ordered
  rr.isn from rep_agrre rr, resection ss
  where rr.agrisn = a.AgrIsn
    and rr.reisn = a.ReIsn
    and rr.sectisn = ss.isn
    and ss.secttype = s.secttype
    and ss.isn <> s.isn
    and rr.agrxisn < a.agrxisn)
  and (R.condisn is not Null Or not exists (select --+ use_nl (rr ss) ordered
  rr.isn from rep_agrre rr
  where rr.agrisn = a.AgrIsn
    and rr.reisn = a.ReIsn
    and rr.sectisn = a.sectisn
    and rr.condisn < a.condisn))
  ) r, resection s, recond c
  where r.sectisn = s.isn (+)
  and s.secttype (+) = 'XL'
  and s.isn = c.sectisn (+);
end;

procedure LoadRepRefund_Hist_By_TT_RowId
(  pLoadIsn   in   Number,
   IsFull number:=1)
Is

 vMinIsn Number;
 vMaxIsn  Number;
 vLMaxIsn  Number;
 vCnt Number;
 vLoadObjCnt Number:=50000;
Begin
--  пишем рефанды

 If isFull=0 Then
  Delete --+ Index(a x_reprefund_hist_refund)
  From reprefund_hist  A Where REFUNDISN In (Select Isn From tt_RowId);
 end if;

insert into reprefund_hist
  (isn, loadisn, refundisn, claimisn, agrisn, rptclassisn, condisn, currisn, claimsum, refundsum,
  dateloss, dateclaim, datereg, datesolution, dateevent, subjisn, status, claimstatus,
  deptisn, agrdatebeg, lossshare, emplisn,recdatebeg,recdateend)
Select --+ Ordered Index (cl X_TT_AGRCLAIM_HIST_ISN)
  Seq_Reports.NextVal,pLoadIsn, r.isn, r.claimisn, r.agrisn, r.rptclassisn, r.condisn, r.currisn, r.claimsum, r.refundsum,
  cl.dateloss, cl.dateclaim, cl.datereg, cl.datesolution, r.dateevent, cl.subjisn, r.status, cl.status,
  cl.deptisn,
  Cl.agrdatebeg, R.lossshare, nvl (r.emplisn,cl.emplisn),
  Greatest(R.RecDateBeg,Cl.RecDateBeg),Least( R.recdateend,Cl.recdateend)

From
(
Select    isn, claimisn, agrisn, rptclassisn, condisn, currisn, claimsum, refundsum,
          dateevent,  status,  lossshare, emplisn,
          Lag(RecDateEnd,1,To_Date('01-jan-1900')) Over (partition By Isn Order by UPDATED) RecDateBeg,RecDateEnd
From
(
Select isn, claimisn, agrisn, rptclassisn, condisn, currisn, claimsum, refundsum,
       dateevent,  status,  lossshare,Max(emplisn) emplisn,Min(RecDateBeg) RecDateBeg,Max(RecDateEnd) RecDateEnd,Max(UPDATED) UPDATED
From
(
  select --+ Index (a X_AGRREFUND) Ordered
  r.isn, r.claimisn, r.agrisn, r.rptclassisn, r.condisn, r.currisn, r.claimsum, r.refundsum,
  r.dateevent,  r.status,
  lossshare,emplisn,To_Date(Null) RecDateBeg,To_Date('01-jan-3000') RecDateEnd,To_Date('01-jan-3000') UPDATED,0 HasNext
  from tt_rowId a, Ais.Agrrefund r
Where R.Isn=A.isn
Union All
Select --+Index (a PK_AGRREFUND)  Ordered
  r.isn, r.claimisn, r.agrisn, r.rptclassisn, r.condisn, r.currisn, r.claimsum, r.refundsum,
  r.dateevent,  r.status,
  lossshare,r.emplisn,To_Date(Null),Trunc(Updated),UPDATED,
  Lag(HistIsn,1,0) Over (partition By R.Isn,trunc(Updated) order by Updated desc) HasNext
From tt_rowId a,Hist.agrrefund r
Where R.Isn=A.isn
)
WHERE HasNext=0
Group by
  isn,
  claimisn,
  agrisn,
  rptclassisn,
  condisn,
  currisn,
  claimsum,
  refundsum,
  dateevent,
  status,
  lossshare
)
) R,TT_AgrClaim_Hist Cl
Where
      R.claimIsn=Cl.Isn
 And ( (R.RecDateBeg>=Cl.RecDateBeg And R.RecDateBeg<Cl.RecDateEnd) Or
       (R.RecDateEnd>=Cl.RecDateBeg And R.RecDateEnd<Cl.RecDateEnd) Or
       (R.RecDateBeg<=Cl.RecDateBeg And R.RecDateEnd>=Cl.RecDateEnd));



update  --+ Index(r x_reprefund_hist_refund)
 reprefund_hist r
set
 franchdeducted =
  (select max (nvl (decode (r.currisn,c.franchcurrisn, decode (nvl (franchtype,'Б'),'Б',decode (franchtariff,null,franchsum))),0)+
    nvl (r.claimsum*decode (nvl (franchtype,'Б'),'Б',franchtariff),0)/100)
  from repcond c where condisn = r.condisn)
 Where REFUNDISN In (Select Isn From tt_RowId);

end;

End;
