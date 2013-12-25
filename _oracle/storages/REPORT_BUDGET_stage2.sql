CREATE OR REPLACE PACKAGE "STORAGES"."REPORT_BUDGET" 
IS

/* загрузка витрины rep_buh2Agr - агркгированный repbuh2cond, исходящее перестрахование
проецируется на прямые договоры. Используется в мотивационных отчетах.
попутно грузится
   витрина rep_body_re - исходящие проводки до секции
   витрина rep_buh2Agr_re - исходящие проводки до секции, прямого договора, ее потом копируем в rep_buh2Agr
*/
procedure Load_Budget_Body_Agrs(pLoadIsn Number:=0);

/* Начисления по исходящему перестрахованию, разваленные на секции договора исходящего
   перестрахования
   EGAO 20.07.2011
*/
PROCEDURE Load_rep_body_re(ploAdIsn IN NUMBER);

--EGAO 29.07.2011 вместо LOAD_OUT_BUH_RE
Procedure LOAD_OUT_BUH_RE_NEW(pLoadIsn Number:=0);

END; -- Package spec

CREATE OR REPLACE PACKAGE BODY "STORAGES"."REPORT_BUDGET" 
IS

procedure Load_Budget_Body_Agrs(pLoadIsn Number:=0)
 Is
 vMinIsn Number:=-1;
 vlMaxIsn Number;
 vCnt Number:=0;
 vLoadObjCnt Number:=100000;

 SesId Number;
 vSql Varchar2(4000);

Begin

/*
--{EGAO 24.06.2011
  --соответствие начислений по прямым/входящим договорам и секциями
  --договоров исходящего перестрахования, которые перестраховывают эти прямые/входящие договора
  replog_i (pLoadIsn, 'BUHRE2DIRANALYTICS', 'make_repbuh2resection',pAction=>'Begin');
  make_repbuh2resection;
  replog_i (pLoadIsn, 'BUHRE2DIRANALYTICS', 'make_repbuh2resection',pAction=>'End');
  --соответствие проводки по исходящему перестрахованию и
  --аналитик начислений по прямым договорам, которые перестрахованы
  --секцией или исходящим договором перестрахования, к которому относится
  --даная проводка
  replog_i (pLoadIsn, 'BUHRE2DIRANALYTICS', 'make_repbuhre2directanalytics',pAction=>'Begin');
  make_repbuhre2directanalytics;
  replog_i (pLoadIsn, 'BUHRE2DIRANALYTICS', 'make_repbuhre2directanalytics',pAction=>'End');
--}

*/
Execute Immediate 'truncate table rep_buh2Agr REUSE STORAGE';
execute Immediate 'truncate table rep_body_re  REUSE STORAGE';
SesId:=Parallel_Tasks.createnewsession();

vMinIsn:=-1;

Store_and_drop_table_Index('storages.rep_buh2Agr');

loop

    select Max (BodyIsn)
    into vLMaxIsn
    From
     (Select  --+ Index_Asc (a x_REPBUH2COND_BodyIsn)
        BodyIsn
      from REPBUH2COND a
       where BodyIsn > vMinIsn
       and rownum <= vLoadObjCnt);

    Exit When vLMaxIsn is null;



vSql:='Declare
vMinIsn number := '||vMinIsn||';
vMaxIsn number := '||vLMaxIsn||';
vLoadIsn number := '||pLoadIsn||';
Begin
Insert Into rep_buh2Agr
Select Seq_Reports.NEXTVAL,s.*
From
(
Select --+ Ordered USe_Nl (a Sb)
      bodyisn, agrisn, budgetgroupisn, buhcurrisn,
       amount, amountrub, amountusd,
       bgpc,
       buhamount,
       buhamountrub, buhamountusd, statcode, rptgroupisn,
       rptclass, rptclassisn, dateval,null ReIsn, vLoadIsn LoadISn,DeptIsn,
       CLIENTISN,sb.classisn CLIENTCLASSISN,BIZFLG,CLIENTJURIDICAL,motivgroupisn,
       sagroup,subaccisn,corsubaccisn,reprdeptisn

From
(
Select --+ Index(b x_REPBUH2COND_BodyIsn)
BodyIsn,
AgrIsn,
Budgetgroupisn, BuhCurrIsn,
Sum(Amount) Amount,
Sum(AmountRub) AmountRub ,
Sum(AmountUsd)  AmountUsd,
Max(BuhAmount) BuhAmount,
Max(BuhAmountRub) BuhAmountRub,
Max(BuhAmountUsd) BuhAmountUsd,
Statcode,
rptgroupisn,
rptclass,
rptclassisn,
dateval,
Sum(Amount)/Max(BuhAmount) bgpc,
DeptIsn,
Max(CLIENTISN)CLIENTISN,
Max(BIZFLG) BIZFLG,
Max(CLIENTJURIDICAL) CLIENTJURIDICAL,
motivgroupisn,
sagroup,
Max(b.subaccisn) subaccisn,
Max(b.corsubaccisn) corsubaccisn,
Max(reprdeptisn) reprdeptisn
from repbuh2cond b
where BodyIsn > vMinIsn And BodyIsn <= vMaxIsn
And Statcode not in (27,33,35, 351,924)
--And sagroup in (1,3)
And sagroup in (1,3,5) --MSerp 30.07.2010 KGR 01.30.2011
Group by BodyIsn,AgrIsn,Budgetgroupisn, BuhCurrIsn,Statcode,
Statcode,
rptgroupisn,
rptclass,
rptclassisn,
sagroup,
dateval,
DeptIsn,
motivgroupisn


Having Max(BuhAmount)<>0)S, Subject Sb
Where S.clientISn=sb.Isn(+))S;

Commit;

--EGAO 29.07.2011 report_budget.insert_bodyre_by_isns(vMinIsn,vMaxIsn,vLoadIsn);
commit;

End;';

 System.Parallel_Tasks.processtask(sesid,vsql);

vCnt:=vCnt+1;

vMinIsn:=vLMaxIsn;
DBMS_APPLICATION_INFO.set_module('Budget_Body_Agrs','Updated: '||vCnt*vLoadObjCnt);

end loop;

-- ждем, пока завершатся все джобы
Parallel_Tasks.endsession(sesid);


--{EGAO 29.07.2011
-- вместо вызова report_budget.insert_bodyre_by_isns(vMinIsn,vMaxIsn,vLoadIsn);
-- в гидре заполнения rep_buh2Agr (см. код выше с комметнариями EGAO 29.07.2011
Load_rep_body_re(ploAdIsn);
--}

--{EGAO 29.07.2011
--LOAD_OUT_BUH_RE(ploAdIsn);
LOAD_OUT_BUH_RE_NEW(ploAdIsn);
--}


DBMS_APPLICATION_INFO.set_module('','');


ReStore_table_Index('storages.rep_buh2Agr');


--EXPORT_DATA.export_to_owb_by_FLD('rep_body_re','BodyIsn');

end;



PROCEDURE Load_Refund_Budget
 Is
 vMinIsn Number:=-1;
 vlMaxIsn Number;
 vCnt Number:=0;
 vLoadObjCnt Number:=20000;

 SesId Number;
 vSql Varchar2(4000);

Begin

Execute Immediate 'truncate table Res_Refund_Budget';
SesId:=Parallel_Tasks.createnewsession();

vMinIsn:=-1;

loop

    select Max (refundIsn)
    into vLMaxIsn
    From
     (Select  --+ Index_Asc (a x_reprefund_refundIsn)
        refundIsn
      from reprefund a
       where refundIsn > vMinIsn
       and rownum <= vLoadObjCnt);

 Exit When vLMaxIsn is null;

vSql:='Begin
Insert Into Res_Refund_Budget
Select Seq_Reports.NEXTVAL,s.*
From
(

Select RefundIsn,budgetgroupisn,Sum(CondPc) CondPc
from reprefund
where refundIsn >'|| vMinIsn||' And refundIsn <= '||vLMaxIsn||'
Group by RefundIsn,budgetgroupisn)S;

Commit;
End;';

 System.Parallel_Tasks.processtask(sesid,vsql);

vCnt:=vCnt+1;

vMinIsn:=vLMaxIsn;
DBMS_APPLICATION_INFO.set_module('Res_Refund_Budget','Updated: '||vCnt*vLoadObjCnt);

end loop;

-- ждем, пока завершатся все джобы
Parallel_Tasks.endsession(sesid);
end;

/* Начисления по исходящему перестрахованию, разваленные на секции договора исходящего
   перестрахования
   EGAO 20.07.2011
*/
PROCEDURE Load_rep_body_re(ploAdIsn IN NUMBER)
IS
BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE rep_body_re REUSE STORAGE';
  DELETE FROM tt_repbuh2cond;

  INSERT INTO tt_repbuh2cond(
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
  )
  SELECT isn,loadisn,buhisn,condisn,condpc,bodyisn,agrisn,addisn,refundisn,statcode,
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
  refundclassisn,condpremusd,dsstatus,condpremagr,carrptclass
  FROM repbuh2cond b
  WHERE B.statcode IN (27,33,35)
    AND sagroup IN (1, 3);


  INSERT INTO rep_body_re
  SELECT --+ use_hash ( a b )
         SEQ_REPORTS.NextVal Isn,
         b.deptisn,
         a.bodyisn,
         a.dateval,
         a.statcode,
         b.buhamountusd,
         b.buhamountrub,
         b.buhamount,
         a.buhcurrisn,
         b.budgetgroupisn,
         b.rptgroupisn,
         b.rptclass,
         b.rptclassisn,
         b.agrisn,
         b.agrclassisn,
         b.agrdiscr,
         b.ruleisnagr,
         a.sectisn,
         a.secttype,
         b.docsumisn,
         a.sectpc*a.docsumpc,
         b.condisn,
         b.isn AS buhcondisn,
         B.condpc*b.factpc*b.buhpc*b.docsumpc AS fullPc,
         b.amount*a.sectpc*a.docsumpc,
         b.amountrub*a.sectpc*a.docsumpc,
         b.amountusd*a.sectpc*a.docsumpc,
         pLoadIsn,
         0 AS isbadsectpc,
         b.motivgroupisn,
         b.sagroup,
         b.subaccisn,
         a.buhdeptisn,
         a.docisn,
         b.subjisn,
         b.agrcurrisn,
         a.sectcurrisn
  FROM tt_repbuh2cond b, REPBUHRE2RESECTION_NEW a -- EGAO 18.12.2013 repbuhre2resection a
  WHERE b.bodyisn=a.bodyisn;

  --2-й insert
  insert into rep_body_re
   Select
    SEQ_REPORTS.NextVal Isn,
     S.deptisn,
     S.bodyisn,
     S.dateval,
     S.statcode,
     S.BuhAmountUsd,
     S.BuhAmountRub,
     S.BuhAmount,
     S.BuhCurrIsn,
     S.budgetgroupisn,S.rptgroupisn,S.rptclass,S.rptclassisn,
     S.agrisn,
     S.agrClassIsn,
     S.agrdiscr,
     S.ruleisnagr,
     S.sectisn, S.secttype, S.docsumisn,
     1/cntfull RsPc,
     condisn,
     Isn,
     FullPc,
     S.Amount*1/cntfull ,
     S.AmountRub*1/cntfull  ,
     S.AmountUsd *1/cntfull,
     pLoadIsn,1 AS isbadsectpc,motivgroupisn, sagroup,
     subaccisn,buhdeptisn,   docisn,  subjisn,
     agrcurrisn,
     sectcurrisn




   From
   (
    select --+ ordered use_nl ( b bb ) index( bb X_BUHBODY )
     b.deptisn,
     B.bodyisn,
     b.dateval,
     B.statcode,
     B.AmountUsd,
     B.AmountRub,
     B.Amount,
     B.BuhAmountUsd,
     B.BuhAmountRub,
     B.BuhAmount,
     B.condpc*b.factpc*b.buhpc*b.docsumpc fullPc,
     b.agrisn,
     B.agrClassIsn,
     b.agrdiscr,
     b.ruleisnagr,
        B.BuhCurrIsn,
      Count(*)  over (partition by b.isn) cntfull,
    b.sectisn, b.secttype, b.docsumisn,b.budgetgroupisn,b.rptgroupisn,b.rptclass,b.rptclassisn,b.condisn,
    b.isn,motivgroupisn, sagroup,b.subaccisn,bb.deptisn buhdeptisn,
    nvl(b.docisn,b.docisn2) docisn,  b.subjisn,
      b.agrcurrisn,
    b.sectcurrisn



    from (SELECT --+ use_hash ( a ) use_hash ( x )
                 b.*, a.sectisn, a.secttype, a.sectcurrisn
          FROM tt_repbuh2cond b,
               (SELECT DISTINCT a.bodyisn FROM REPBUHRE2RESECTION_NEW a/*EGAO 31.12.2013 repbuhre2resection a*/) x,
               (SELECT s.agrisn,
                       MAX(s.secttype) AS secttype,
                       MAX(s.currisn) AS sectcurrisn,
                       MAX(s.isn) AS sectisn
                FROM resection s
                GROUP BY s.agrisn
                HAVING COUNT(s.isn)=1
               ) a
            WHERE b.bodyisn=x.bodyisn(+)
            AND x.bodyisn IS NULL
            AND b.agrisn=a.agrisn
         ) b,
         buhbody bb
    where b.bodyisn=bb.isn
   )s;

  --3-й insert
  insert into rep_body_re
  Select
    SEQ_REPORTS.NextVal Isn,
     S.deptisn,
     S.bodyisn,
     S.dateval,
     S.statcode,
     S.BuhAmountUsd,
     S.BuhAmountRub,
     S.BuhAmount,
     S.BuhCurrIsn,
     S.budgetgroupisn,S.rptgroupisn,S.rptclass,S.rptclassisn,
     S.agrisn,
     S.agrClassIsn,
     S.agrdiscr,
     S.ruleisnagr,
     S.sectisn, S.secttype, S.docsumisn,
     1 RsPc,
     condisn,
     Isn,
     FullPc,
     S.Amount ,
     S.AmountRub  ,
     S.AmountUsd ,
     pLoadIsn,0,motivgroupisn, sagroup,subaccisn,buhdeptisn,
     docisn,
     subjisn,
     agrcurrisn,
     sectcurrisn
  From
   (
    select --+ ordered use_nl (bb b) index ( bb X_BUHBODY )
     b.deptisn,
     B.bodyisn,
     b.dateval,
     B.statcode,
     B.AmountUsd,
     B.AmountRub,
     B.Amount,
     B.BuhAmountUsd,
     B.BuhAmountRub,
     B.BuhAmount,
     B.condpc*b.factpc*b.buhpc*b.docsumpc fullPc,
     b.agrisn,
     B.agrClassIsn,
     b.agrdiscr,
     b.ruleisnagr,
        B.BuhCurrIsn,

    null sectisn, null secttype, b.docsumisn,b.budgetgroupisn,b.rptgroupisn,b.rptclass,b.rptclassisn,b.condisn,
    b.isn,motivgroupisn, sagroup,b.subaccisn,bb.deptisn buhdeptisn,
      nvl(b.docisn,b.docisn2) docisn,
        b.subjisn,
         agrcurrisn,
        null  sectcurrisn

    from (SELECT --+ use_hash ( a ) use_hash ( x )
                 b.*
          FROM tt_repbuh2cond b,
               (SELECT DISTINCT bodyisn FROM REPBUHRE2RESECTION_NEW/*EGAO 18.12.2013 repbuhre2resection*/) x,
               (SELECT agrisn FROM resection s GROUP BY agrisn HAVING COUNT(s.isn)=1) a
          WHERE b.bodyisn=x.bodyisn(+)
            AND x.bodyisn IS NULL
            AND b.agrisn=a.agrisn(+)
            AND a.agrisn IS NULL
         ) b, buhbody bb
    where b.bodyisn=bb.isn
   )s;

  COMMIT;
END;

--EGAO 29.07.2011 вместо LOAD_OUT_BUH_RE
Procedure LOAD_OUT_BUH_RE_NEW(pLoadIsn Number:=0)
IS
  vMinIsn Number:=-1;
  vlMaxIsn Number;
  vCnt Number:=0;
  vLoadObjCnt Number:=10000;
  SesId Number;
  vSql Varchar2(4000);
BEGIN
  SesId:=Parallel_Tasks.createnewsession();
  vCnt:=0;
  vMinIsn:=-1;

  loop
    vLMaxIsn:=Cut_Table('storages.repbuhre2directanalytics','AGRISN',vMinIsn,null,vLoadObjCnt*10);
    Exit When vLMaxIsn is null;

  vSql:='
   Declare
     vMinIsn  number :='||vMinIsn||';
     vMaxIsn  number :='||vLMaxIsn||';
     pLoadIsn number :='||pLoadIsn||';
     vCnt     number := '||vCnt||';
   Begin

   DBMS_APPLICATION_INFO.set_module(''rep_buh2Agr re'',''Process: ''||vCnt);

   Insert into rep_buh2Agr
   Select --+ Ordered Use_Nl(s ag sb)
   Seq_Reports.NextVal,
   s.bodyisn,
   s.agrisn,
   s.budgetgroupisn,
   s.buhcurrisn,
   s.amount,
   S.amountrub,
   S.amountusd,
   S.bgpc,
   S.buhamount,
   S.buhamountrub,
   S.buhamountusd,
   statcode,
   rptgroupisn,
   rptclass,
   rptclassisn,
   dateval,
   ReIsn,
   s.LoadISn,
   s.DeptIsn,
   ag.clientisn,
   sb.classisn,
   ag.bizflg,
   ag.clientjuridical,
   motivgroupisn,
   sagroup,subaccisn,null,null
  From
  (
  Select --+ Index (b X_REP_BODYRE2DIRECT_AGRISN)
   bodyisn,
   agrisn,
   budgetgroupisn,
   buhcurrisn,
   Sum(amount) amount,
   Sum(amountrub) amountrub,
   Sum(amountusd) amountusd,
   Sum(amount)/Max(BuhAmount) bgpc,
   Max(buhamount) buhamount,
   Max(buhamountrub) buhamountrub,
   Max(buhamountusd) buhamountusd,
   statcode,
   rptgroupisn,
   rptclass,
   rptclassisn,
   dateval,
   ReIsn,
   pLoadIsn as LoadISn,
    b.DeptIsn,
    motivgroupisn,
    sagroup,max(subaccisn) subaccisn
  From repbuhre2directanalytics b
  Where agrisn>vMinIsn and AgrIsn<=vMaxIsn

  Group by
   bodyisn,
   agrisn,
   budgetgroupisn,
   buhcurrisn,
   statcode,
   rptgroupisn,
   rptclass,
   rptclassisn,
   dateval, ReIsn,DeptIsn,motivgroupisn,sagroup) S,repagr Ag,Subject Sb
  Where S.agrisn=ag.agrisn(+)
  And ag.clientisn=sb.isn(+);

  Commit;
  end;';
  System.Parallel_Tasks.processtask(sesid,vsql);

  vCnt:=vCnt+1;

  vMinIsn:=vLMaxIsn;
  DBMS_APPLICATION_INFO.set_module('rep_buh2Agr','Process : '||vCnt*vLoadObjCnt*10);

  end loop;

  -- ждем, пока завершатся все джобы
  Parallel_Tasks.endsession(sesid);
  DBMS_APPLICATION_INFO.set_module('','');
End;

END;
