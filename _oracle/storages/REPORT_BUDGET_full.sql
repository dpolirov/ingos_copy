CREATE OR REPLACE PACKAGE "STORAGES"."REPORT_BUDGET" 
IS

/* загрузка отчета "бюджет", бюджет по договорам */
procedure Rep_Load_Budget(pTaskIsn in number); -- обертка для вызова из обновлялки
PROCEDURE Load_Budget
     (pDateBeg Date,
      pDateEnd Date,
      pAgrDateBeg Date:=add_Months(Trunc(trunc(sysdate,'mm')-1,'yyyy'),-48),
      pLoadSalers number:=1);     --SM было -24, т.е. с 2010 г
PROCEDURE Load_Budget_Agrs_By_Isns
    (vMinIsn number,vlMaxIsn number,
      pDateBeg Date,
      pDateEnd Date,
      pAgrDateBeg Date );


/* загрузка инфоструктуры REP_BUDGET_AGR_SALER - нужна для отчета "бюджет"*/
procedure make_REP_BUDGET_AGR_SALER;
procedure make_REP_B_AGR_SALER_by_isn(pMinIsn number,pMaxIsn number);





/* загрузка витрины rep_buh2Agr - агркгированный repbuh2cond, исходящее перестрахование
проецируется на прямые договоры. Используется в мотивационных отчетах.
попутно грузится
   витрина rep_body_re - исходящие проводки до секции
   витрина rep_buh2Agr_re - исходящие проводки до секции, прямого договора, ее потом копируем в rep_buh2Agr
*/
procedure Load_Budget_Body_Agrs(pLoadIsn Number:=0);
PROCEDURE INSERT_BODYRE_By_Isns(pMinIsn number,pMaxIsn Number,pLoadIsn Number:=0); --rep_body_re

Procedure LOAD_OUT_BUH_RE(pLoadIsn Number:=0); --rep_buh2Agr_re
PROCEDURE LOAD_OUT_BUH_RE_BY_ISNS (pMinIsn number,pLMaxIsn Number,pLoadIsn Number:=0); --rep_buh2Agr_re



/* отчет "Убытки по куратору претензии (убытка)"*/
procedure Rep_load_Budget_Refund(pTaskIsn in number); -- обертка для вызова из обновлялки
PROCEDURE Load_Budget_Refund(pDateBeg Date,pDateEnd Date );
procedure Load_Budget_Refund_By_Isns (pMinIsn Number,pMaxIsn number, pDatebeg date, pDateEnd date);





/* старый код отчета "кассовый результат" - не используется*/
procedure Load_Budget_Rep_Cash(pStartDate Date:=null);
procedure Load_Budget_Rep_Cash_BD (pDayBeg Date,pDayEnd Date);

/* старый код отчета  - не используется*/
procedure Load_BuhCor(pStartDate Date:=null);
procedure Load_BuhCor_BD (pDayBeg Date,pDayEnd Date);








/* отчет "мотивация по дате квитовки"*/
procedure Rep_LOad_budget_Rep_Motiv(pTaskIsn in number); -- обертка для вызова из обновлялки
procedure Load_Budget_Rep_motivation (pStartDate Date:=null);
procedure Load_Budget_Rep_motiv_Quit_Bi (pMinIsn number,pMaxIsn Number,pFromDate Date);

/* "сжатие" отчета "мотивация по дате квитовки", сжатый отчет используется для кубов, вывода и т.д.*/
procedure Load_rep_motiv_Grp(pStartDate date:=null);
procedure Load_rep_motiv_Grp_CUbe (pFromDate Date:=null);





/* инфоструктуры по продавцам (tt_agr_salers)*/
Procedure tt_agr_salers_make;
procedure tt_agr_salers_make_By_Isns (pMinIsn number,pMaxIsn number,pLoadisn Number:=0);
Procedure tt_agr_salers_make_test;
procedure tt_agr_salers_make_By_Isns_tst (pMinIsn number,pMaxIsn number,pLoadisn Number:=0);






/* отчет "фил комиссия"*/
procedure Load_rep_fil_com(pTaskIsn in Number);









/* какой-то разовый отчет, не используется?*/
procedure rep_motiv_dks_make;
procedure rep_motiv_dks_insert(vMin rowid,cLoadobjcnt number);


/*отчет "договоры без продавцов"*/
procedure Load_agrs_no_salers(pTaskIsn in number);



/* вагрузка в ОФА.... неликвид*/
--procedure Load_to_Ofa (pdatebeg date,pDateend Date);


-- Претензии со статусом тоталь EGAO 15.12.2008
PROCEDURE RepLoadRefundTotalLoss(pTaskIsn in number);  --EGAO 15.12.2008
PROCEDURE LoadRefundTotalLoss(pDateBeg Date, pDateEnd Date ); --EGAO 15.12.2008
procedure LoadRefundTotalLossByIsns (pMinIsn Number,pMaxIsn number,pDatebeg date, pDateend date); --EGAO 15.12.2008

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



PROCEDURE Load_Budget
     (pDateBeg Date,
      pDateEnd Date,
      pAgrDateBeg Date:=add_Months(Trunc(trunc(sysdate,'mm')-1,'yyyy'),-48),
      pLoadSalers number:=1 ) --SM было -24, т.е. с 2010


 Is
 vMinIsn Number:=-1;
 vlMaxIsn Number;
 vCnt Number:=0;
 vLoadObjCnt Number:=100000;

 vSql Varchar2(4000);
 pSesId number;
  Begin


 pSesId:=Parallel_Tasks.createnewsession;
 Store_and_drop_table_index('storages.rep_budget');

If pLoadSalers=1 then
make_REP_BUDGET_AGR_SALER;
end if;

execute Immediate 'truncate table Storages.rep_budget drop storage';
execute Immediate 'truncate table Storages.rep_budget_agrs reuse storage';

Loop

vLMaxIsn:=cut_table('storages.rep_buh2Agr','Agrisn',vMinIsn);

 Exit When vLMaxIsn is null;

vSql:='
declare
vMinIsn number:='||vMinIsn||';
vLMaxIsn number:='||vLMaxIsn||';

pDateBeg Date:='''||pDateBeg||''';
pDateEnd Date:='''||pDateEnd||''';

pAgrDateBeg Date:='''||pAgrDateBeg||''';

Begin
storages.report_budget.Load_Budget_Agrs_By_Isns(vMinIsn,vlMaxIsn ,pDateBeg,pDateEnd,pAgrDateBeg );
Commit;

end;';

 Parallel_Tasks.processtask(psesid,vsql,1);

vCnt:=vCnt+1;

vMinIsn:=vLMaxIsn;
DBMS_APPLICATION_INFO.set_module('Rep_Budget','Process: '||vCnt*vLoadObjCnt);

end loop;
Parallel_Tasks.endsession(psesid);

 Restore_table_index('storages.rep_budget');
 --vColList:=storages.REP_COGNOS_UTILS.get_not_null_columns_list ('storages.Rep_budget');

execute immediate
'
insert into STORAGES.REP_BUDGET
select T.SALECHANEL,
       T.BUDGETGROUPISN,
       T.DATEVAL,
       T.CLIENTJURIDICAL,
       T.EMITISN,
       T.BEMITISN,
       T.AGENTCLASSISN,
       T.AGENTJURIDICAL,
       T.STATCODE,
       T.STATNAME,
       T.DEPTISN,
       T.AGRDEPTISN,
       T.BIZFLG,
       T.CALCBIZFLG,
       T.REINDEPT,
       T.AGRCLASSISN,
       sum(AMOUNTRUB) AMOUNTRUB,
       sum(AMOUNTUSD) AMOUNTUSD,
       T.BDPDEPTISN,
       T.SAGROUP,
       1 ISGOOD,
       T.REINCLASSISN,
       T.SALERCHANELISN,
       T.COISN,
       T.CPISN,
       T.GOSALERCLASSISN,
       T.GMISN,
       T.AGRRULEISN
  from STORAGES.REP_BUDGET T
 group by T.SALECHANEL, T.BUDGETGROUPISN, T.DATEVAL, T.CLIENTJURIDICAL, T.EMITISN, T.BEMITISN, T.AGENTCLASSISN,
          T.AGENTJURIDICAL, T.STATCODE, T.STATNAME, T.DEPTISN, T.AGRDEPTISN, T.BIZFLG, T.CALCBIZFLG, T.REINDEPT,
          T.AGRCLASSISN, T.BDPDEPTISN, T.SAGROUP, T.REINCLASSISN, T.SALERCHANELISN, T.COISN, T.CPISN, T.GOSALERCLASSISN,
          T.GMISN, T.AGRRULEISN';


Delete From storages.rep_budget where IsGood=0;

-- отправим сообщение о том, что таблица изменилась
rep_message.put(p_recipient => 'COGNOS',p_object => 'REP_BUDGET', pDateBeg => pDateBeg, pDateEnd => pDateEnd);

commit;



--REBULD_TABLE_INDEX('storages.rep_budget');
 /*
Update --+ Index (r X_REP_BUDGET_LOAD)
 rep_budget R
 set
 SaleChanelIsn = (Select Isn from dicti where Parentisn=1366868203 and ShortName=Upper(SaleChanel))
 Where LoadISn=vloadisn ;
Commit;
*/


DBMS_APPLICATION_INFO.set_module('','');



  end;



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


procedure Load_Budget_Rep_Cash (pStartDate Date:=null)
Is
 vRebuildMinDate date :=Nvl(pStartDate, trunc(sysdate-90,'Y'));
 vDayBeg Date;
 vDayStep Number:=10;


 SesId Number;
 vSql Varchar2(4000);
 Begin

  vDayBeg:=vRebuildMinDate-1;

  SesId:=Parallel_Tasks.createnewsession();


Loop
 Exit When vDayBeg>Trunc(Sysdate);

  DBMS_APPLICATION_INFO.set_module('Load_Budget_Rep_Cash','MinDay: '||vDayBeg);



 vSql:='Storages.REPORT_BUDGET.Load_Budget_Rep_Cash_BD(
                 to_date('''||to_char(vDayBeg,'dd.mm.yyyy')||''',''dd.mm.yyyy''),
                 to_date('''||to_char(vDayBeg+vDayStep,'dd.mm.yyyy')||''',''dd.mm.yyyy'')
                  );';


  System.Parallel_Tasks.processtask(sesid,vsql);
  vDayBeg:=vDayBeg+vDayStep;

end loop;

 end;


procedure Load_Budget_Rep_Cash_BD (pDayBeg Date,pDayEnd Date)
 Is
  Begin


Delete --+ Index (a X_REP_CASH_BUDGET_DATEREP)
     From rep_Cash_Budget a Where Daterep>pDayBeg and Daterep<=pDayEnd;

Insert Into rep_Cash_Budget
  (Select --+ Ordered Use_Nl(rc bbad bbac ag sb)
       rc.strcode, rc.deptisn, rc.daterep, rc.dcode, rc.ccode,
       Sum(rc.amountrub*bgPc) amountrub,
       Sum(rc.amountusd*bgPc) amountusd,
       Sum(rc.amountusd*bgPc) amount,
       Sum(rc.equusd*bgPc) equusd,
       Decode(Decode(Dcode,'77175',1,'77164',1,Decode(Ccode,'77175',1,'77164',1,0)),1,0,headisn),
       Decode(Decode(Dcode,'77175',1,'77164',1,Decode(Ccode,'77175',1,'77164',1,0)),1,0,dbodyisn),
       Decode(Decode(Dcode,'77175',1,'77164',1,Decode(Ccode,'77175',1,'77164',1,0)),1,0,cbodyisn),
       rc.dateval,rc.datequit, rc.groupisn, rc.convgroupisn,
       rc.currisn, rc.dsubaccisn, rc.csubaccisn, Sysdate Updated, rc.reprisn,
       rc.bizflg, rc.planisn, rc.budgetgroupisn , Sb.juridical,ReIsn,AG.DEPTISN
 From
 (
 Select  --+ Ordered Use_Nl(rc bbad bbac ag sb) Index (rc X_REP_CASH_DATEREP)
        Rc.*, Nvl(Nvl(bbad.BGPC,bbac.BGPC),1) bgPc, Nvl(bbad.budgetgroupisn,bbac.budgetgroupisn) budgetgroupisn,
        Nvl(bbad.agrisn,bbac.agrisn) agrisn,  Nvl(bbad.BodyIsn,bbac.BodyIsn) BodyIsn,
        Nvl(bbad.reisn,bbac.reisn) ReIsn
  from rep_Cash rc, rep_buh2Agr bbad,rep_buh2Agr bbac
     Where  rc.Daterep>pDayBeg and rc.Daterep<=pDayEnd
      And  rc.dbodyisn=bbad.bodyisn(+)
      And rc.cbodyisn=bbac.bodyisn(+)
) Rc,repagr ag,subject sb
    Where
          Rc.AgrIsn=Ag.AgrIsn(+)
      And ag.clientisn= sb.Isn(+)
Group by  rc.strcode, rc.deptisn, rc.daterep, rc.dcode, rc.ccode,
       Decode(Decode(Dcode,'77175',1,'77164',1,Decode(Ccode,'77175',1,'77164',1,0)),1,0,headisn),
       Decode(Decode(Dcode,'77175',1,'77164',1,Decode(Ccode,'77175',1,'77164',1,0)),1,0,dbodyisn),
       Decode(Decode(Dcode,'77175',1,'77164',1,Decode(Ccode,'77175',1,'77164',1,0)),1,0,cbodyisn),
       rc.dateval,rc.datequit, rc.groupisn, rc.convgroupisn,
       rc.currisn, rc.dsubaccisn, rc.csubaccisn, rc.reprisn,
       rc.bizflg, rc.planisn, rc.budgetgroupisn , Sb.juridical,ReIsn,AG.DEPTISN);

Commit;



  end;


Procedure LOAD_OUT_BUH_RE(pLoadIsn Number:=0)
   IS
 vMinIsn Number:=-1;
 vlMaxIsn Number;
 vCnt Number:=0;
 vLoadObjCnt Number:=10000;

 SesId Number;
 vSql Varchar2(4000);
BEGIN
-- загружаем буфер проводок исх . перестрахования с расшивкой на секции




Execute Immediate 'truncate table rep_buh2Agr_re drop storage';

SesId:=Parallel_Tasks.createnewsession();

vMinIsn:=-1;

loop
vLMaxIsn:=Cut_Table('storages.rep_body_re','AGRISN',vMinIsn,null,vLoadObjCnt);
/*
    select Max (AgrISn)
    into vLMaxIsn
    From
     (Select  --+ Index_Asc (a x_rep_body_re_agr)
        AgrISn
      from rep_body_re a
       where AgrISn > vMinIsn
       and rownum <= vLoadObjCnt);
*/

 Exit When vLMaxIsn is null;

vSql:='Storages.report_Budget.LOAD_OUT_BUH_RE_BY_ISNS('||vMinIsn||','||vlMaxIsn||','||ploAdISn||');';
System.Parallel_Tasks.processtask(sesid,vsql);

vCnt:=vCnt+1;

vMinIsn:=vLMaxIsn;
DBMS_APPLICATION_INFO.set_module('LOAD_OUT_BUH_RE','Process : '||vCnt*vLoadObjCnt);

end loop;

-- ждем, пока завершатся все джобы
Parallel_Tasks.endsession(sesid);




 -- теперь заливаем эти проводки в rep_buh2Agr

SesId:=Parallel_Tasks.createnewsession();
vCnt:=0;
vMinIsn:=-1;

loop


vLMaxIsn:=Cut_Table('storages.rep_buh2Agr_re','AGRISN',vMinIsn,null,vLoadObjCnt*10);

 Exit When vLMaxIsn is null;

vSql:='
 Begin
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
Select --+ Index (b X_REP_BUH2AGR_RE_AGR)
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
 '||pLoadIsn||' LoadISn,
  b.DeptIsn,
  motivgroupisn,
  sagroup,max(subaccisn) subaccisn
From rep_buh2Agr_re b
Where agrisn>'||vMinIsn||' and AgrIsn<='||vlMaxIsn||'

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



PROCEDURE LOAD_OUT_BUH_RE_BY_ISNS (pMinIsn number,pLMaxIsn Number,pLoadIsn Number:=0)
   IS

   vMinIsn Number;
   vMaxIsn Number;
   vRecCnt Number:=10000;
   vCnt Number;
   vAgrCnt Number:=0;


BEGIN





-- агрегируем и загружаем проводки
-- проводки по прямым договорам сразу туда

Insert Into  rep_buh2Agr_re
Select Seq_Reports.NEXTVAL,S.* --,pLoadIsn   Гоша, мля я 4 часа этот глюк искал. Порядок полей соблюдать надо!!!!!
From
(
Select --+ Index_Asc (a x_rep_body_re_agr) ordered use_nl(a b)
b.BodyIsn,
B.AgrIsn,
A.sectisn,
b.Statcode,
B.RptGroupIsn,
B.budgetgroupisn,
B.RptClass,
B.RptClassIsn,
b.BuhCurrIsn,
Max(B.BuhAmount) BuhAmount,
Max(B.BuhAmountUsd) BuhAmountUsd,
Max(B.BuhAmountRuB) BuhAmountRub,
Max(RSPC) SectPc,
Sum(FULLPC) GrpPc,
Null,
b.Dateval,
Sum(B.Amount) ,
Sum(B.AmountRuB),
Sum(B.AmountUsd),
pLoadIsn,  -- MSerp 04.09.2006
a.deptisn,
b.motivgroupisn,
a.sagroup,
a.SUBJISN,Max(b.subaccisn),
null sectcurrisn,
Max(a.agrcurrisn)

from rep_body_re a,repbuh2cond b
Where a.AgrISn>pMinIsn and a.AgrISn<=pLMaxIsn
And A.buhcondisn=b.isn
And Nvl(A.AgrClassIsn,0) not in (c.get ('AgrOutFacult'),c.get ('AgrOutOblig'))
Group by
b.BodyIsn,
B.AgrIsn,
A.sectisn,
b.Statcode,
B.RptGroupIsn,
B.budgetgroupisn,
B.RptClass,
B.RptClassIsn,
b.BuhCurrIsn,
b.Dateval,
a.deptisn,
b.motivgroupisn, a.sagroup,a.SUBJISN
Having Sum(B.Amount)<>0) S;

Delete From tt_rep_buh_re_List;

-- по не прямым - сжали до bodyisn-sectis
  Insert Into tt_rep_buh_re_List
  Select RowNum, S.*
  from
  (
  Select  --+ Index_Asc (b x_rep_body_re_agr)
  b.BodyIsn,
  B.AgrIsn,
  B.sectisn,
  b.Statcode,
  buhcurrisn,
  Max(B.BuhAmount) BuhAmount,
  Max(B.BuhAmountUsd) BuhAmountUsd,
  Max(B.BuhAmountRub) BuhAmountRub,
  Sum(B.Amount)/Max(B.BuhAmount) SectPc,
  b.Dateval,
  Sum(B.Amount) ,
  Sum(B.AmountUsd),
  Sum(B.AmountRuB),
  DeptIsn,
  motivgroupisn,
  sagroup,SUBJISN,Max(subaccisn),
  MaX(sectcurrisn) sectcurrisn,
  Max(agrcurrisn) agrcurrisn
  from rep_body_re B
   Where b.AgrISn>pMinIsn and b.AgrISn<=pLMaxIsn
   And AgrClassIsn  in (c.get ('AgrOutFacult'),c.get ('AgrOutOblig'))
   Group by  b.BodyIsn,
  B.AgrIsn,
  B.sectisn,
  b.Statcode,
  buhcurrisn,
  Dateval,
  DeptIsn,motivgroupisn, sagroup,SUBJISN
  Having Max(B.BuhAmount)<>0) S;




-- идем по 1-му перестраховочному договору

For Cur In (Select  Distinct AgrIsn from tt_rep_buh_re_List) loop

 vMinIsn:=-1;
 vcnt:=0;
 vAgrCnt:=vAgrCnt+1;

 Delete From tt_rep_agr_re_List;

Loop
 -- кусками грузим список перестрахованных договоров
 Select Max(AgrIsn)
 into vMaxIsn
 From
 (
   Select --+ Index_Asc( re X_REP_AGRRE_REISN_AGR)
       AgrIsn
   from rep_agrre re
   Where ReIsn=Cur.AgrIsn
   And AgrIsn>vMinIsn and RowNum<=vRecCnt);

  Exit When vMaxIsn Is Null;

delete from tt_rowid;
insert into tt_rowid(Isn)
select --+ Index_Asc( are X_REP_AGRRE_RE_AGR_COND)
                  distinct agrisn
                  from  rep_agrre are
                  Where Cur.AgrIsn=Are.REISN
             and are.AgrIsn>vMinIsn And are.agrIsn<=vMaxIsn
              AND ARE.condisn is not null;


DBMS_APPLICATION_INFO.set_module('Load reAgr','ReAgr: '||Cur.AgrIsn||'  Step:'||vCnt );

       Insert Into tt_rep_agr_re_List
        Select
          AgrIsn,
          sectisn,
          RptGroupIsn,
          budgetgroupisn,
          RptClass,
          RptClassIsn,
          Nvl(Sum(AmountUsd*sharepc/100),0),
          REISN,
          Nvl(datebegx,datebeg),Nvl(dateendx,dateend),motivgroupisn,
          sagroup -- EGAO 30.11.2009
      From
       (
           Select --+ ordered use_nl(are b) Index_Asc( are X_REP_AGRRE_RE_AGR_COND) index ( b X_REPBUH2COND_AGRISN )
          Are.AgrIsn,
          Are.sectisn,
          B.RptGroupIsn,
          B.budgetgroupisn,
          B.RptClass,
          B.RptClassIsn,
          AmountUsd,
          are.sharepc,
          Are.REISN,
          Are.datebegx,Are.datebeg,Are.dateendx,are.dateend,motivgroupisn,
          b.sagroup -- EGAO 30.11.2009
          From
                rep_agrre are,repbuh2cond b
            Where Cur.AgrIsn=Are.REISN
             and are.AgrIsn>vMinIsn And are.agrIsn<=vMaxIsn
            And are.agrIsn=b.agrisn
            And (are.condisn is  null  )
            ANd   b.statcode in (38,34)
            AND sagroup IN (1, 3)-- EGAO 30.11.2009 and sagroup=1
--            and DateVal<=Nvl(Are.dateendx,are.dateend)
 Union All
 Select --+ ordered  use_hash (b)
          Are.AgrIsn,
          Are.sectisn,
          B.RptGroupIsn,
          B.budgetgroupisn,
          B.RptClass,
          B.RptClassIsn,
          b.amountusd*CondPc,
          are.sharepc,
          Are.REISN,
          Are.datebegx,Are.datebeg,Are.dateendx,are.dateend,motivgroupisn,
          sagroup -- EGAO 30.11.2009

from
(
Select --+ Ordered Use_Nl(rc ag) Index_Asc( are X_REP_AGRRE_RE_AGR_COND) index ( rc X_REPCOND_COND ) index ( ag X_REPAGR_AGR )
          Are.AgrIsn,
          Are.sectisn,
           Least(sum(rc.premusd/ag.premusd),1) CondPc,-- доля перестрахованных кондов в договоре
          are.sharepc,
          Are.REISN,
          Are.datebegx,Are.datebeg,Are.dateendx,are.dateend
            From
                rep_agrre are,REPCOND rc   ,repagr ag

            Where Cur.AgrIsn=Are.REISN
             and are.AgrIsn>vMinIsn And are.agrIsn<=vMaxIsn
             AND ARE.condisn is not null
            And are.condisn=rc.condisn
            and are.agrisn=ag.agrisn
            and newaddisn is null
            and nvl(rc.premusd,0)>0
            AND nvl(AG.premusd,0)>0
group by           Are.AgrIsn,
          Are.sectisn,
          are.sharepc,
          Are.REISN,
          Are.datebegx,Are.datebeg,Are.dateendx,are.dateend

)Are,
      (
           Select --+ fULL (ARE) pARALLEL (ARE,12) ordered USe_Nl(bc) index(bc X_REPBUH2COND_AGRISN)
          bc.agrisn,
          RptGroupIsn,
          budgetgroupisn,
          RptClass,
          RptClassIsn,
          motivgroupisn,
          bc.sagroup, -- EGAO 30.11.2009
          sum(amountusd) amountusd
            from tt_rowid Are,repbuh2cond bc
            where  bc.AgrIsn=are.Isn
            ANd   bc.statcode in (38,34)
            AND sagroup IN (1, 3)-- EGAO 30.11.2009 And sagroup=1
            group by             bc.agrisn,
          RptGroupIsn,
          budgetgroupisn,
          RptClass,
          RptClassIsn,
          motivgroupisn,
          bc.sagroup -- EGAO 30.11.2009
      ) b
 Where   are.agrisn=b.agrisn

          )
               Group by AgrIsn,
                 sectisn,
                 RptGroupIsn,
                 budgetgroupisn,
                 RptClass,
                 RptClassIsn,
                 REISN,Nvl(datebegx,datebeg),Nvl(dateendx,dateend),motivgroupisn, sagroup;

  vMinIsn:=vMaxIsn;
  vcnt:=vcnt+1;

 end loop; -- по rep_agrre





  DBMS_APPLICATION_INFO.set_module('Load reAgr','Compile, Agr:'||vAgrCnt );

-- теперь берем по одной проводке от этого договора

 For Cur1 in (select Isn,bodyisn from tt_rep_buh_re_List Where AgrIsn=Cur.AgrIsn) Loop
 -- теперь сливаем все это дело вместе
 Insert Into rep_buh2Agr_re
 Select --+ ordered
     Seq_Reports.NEXTVAL,
     BodyIsn,
     AgrIsn,
     sectisn,
     Statcode,
     RptGroupIsn,
     budgetgroupisn,
     RptClass,
     RptClassIsn,
     BuhCurrIsn,
     BuhAmount,
     BuhAmountUsd,
     BuhAmountRub,
     SectPc,
     Decode(Nvl(PAgrPrem,0),0,Decode(PAgrCnt,0,1,1/PAgrCnt),PremUsd/PAgrPrem),
     ReISn,
     DateVal,
     Amount*Decode(Nvl(PAgrPrem,0),0,Decode(PAgrCnt,0,1,1/PAgrCnt),PremUsd/PAgrPrem),
     AmountRuB*Decode(Nvl(PAgrPrem,0),0,Decode(PAgrCnt,0,1,1/PAgrCnt),PremUsd/PAgrPrem),
     AmountUsd*Decode(Nvl(PAgrPrem,0),0,Decode(PAgrCnt,0,1,1/PAgrCnt),PremUsd/PAgrPrem),
     pLoadIsn ,
     DeptIsn,motivgroupisn, sagroup,
     SUBJISN,subaccisn,
           sectcurrisn,
      agrcurrisn

   From (

    Select --+ Ordered Use_Hash(a b)
     b.BodyIsn,
     B.AgrIsn ReISn,
     Nvl(A.AgrIsn,B.AgrIsn) AgrIsn, -- подменяем договор на прямой, если есть
     B.sectisn,
     b.Statcode,
     A.RptGroupIsn,
     A.budgetgroupisn,
     A.RptClass,
     A.RptClassIsn,
     b.BuhCurrIsn,
     BuhAmount,
     BuhAmountUsd,
     BuhAmountRub,
     SectPc,
     PremUsd,
     Dateval,
     Sum(PremUsd) over (Partition by B.BodyIsn,B.sectisn) PAgrPrem,
     Count(*) over (Partition by B.BodyIsn,B.sectisn) PAgrCnt,
      B.Amount,
      B.AmountUsd,
      B.AmountRuB,
      DeptIsn,a.motivgroupisn, b.sagroup,SUBJISN,subaccisn,
      sectcurrisn,
      agrcurrisn

    From  tt_rep_buh_re_List b,tt_rep_agr_re_List a
      Where B.Isn=Cur1.Isn
      And b.agrisn=a.reIsn(+)
      And Decode(b.sectisn,null,1,a.sectisn(+),1,0)=1);
--      ANd Decode(b.sectisn,null,Decode(Sign((b.dateval-a.datebegx(+))*(a.dateendx(+)-b.dateval)),1,1,0,1,0),1)=1);

 Commit;

 end loop; -- по записи по проводке

end loop; -- по договору

 END; -- Procedure


procedure Load_BuhCor(pStartDate Date:=null)
Is
 vRebuildMinDate date :=Nvl(pStartDate, trunc(sysdate-90,'Y'));
 vDayBeg Date;
 vDayStep Number:=10;


 SesId Number;
 vSql Varchar2(4000);
 Begin

  vDayBeg:=vRebuildMinDate-1;

  SesId:=Parallel_Tasks.createnewsession();


Loop
 Exit When vDayBeg>Trunc(Sysdate);

  DBMS_APPLICATION_INFO.set_module('Load_Budget_Rep_Cash','MinDay: '||vDayBeg);



 vSql:='REPORT_BUDGET.Load_BuhCor_BD(
                 to_date('''||to_char(vDayBeg,'dd.mm.yyyy')||''',''dd.mm.yyyy''),
                 to_date('''||to_char(vDayBeg+vDayStep,'dd.mm.yyyy')||''',''dd.mm.yyyy'')
                  );';


  System.Parallel_Tasks.processtask(sesid,vsql);
  vDayBeg:=vDayBeg+vDayStep;

end loop;

 end;

procedure Load_BuhCor_BD (pDayBeg Date,pDayEnd Date)
Is
 Begin

Delete --+ Index (a X_REP_CASH_BUDGET_DATEREP)
     From buhcorr_t a Where DATEVAL>pDayBeg and DATEVAL<=pDayEnd;



execute Immediate '
Begin
Insert Into buhcorr_t
  (Select * From storages.buhcorr_t@oraaix Where
  DATEVAL>to_date('''||to_Char(pDayBeg,'DD.MM.YYYY')||''',''DD.MM.YYYY'')
  and DATEVAL<=to_date('''||to_Char(pDayEnd,'DD.MM.YYYY')||''',''DD.MM.YYYY''));

Commit;
end;';

end;





PROCEDURE INSERT_BODYRE_By_Isns(pMinIsn number,pMaxIsn Number,pLoadIsn Number:=0)
   IS
BEGIN
delete from tt_repbuh2cond;
insert into tt_repbuh2cond(
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
select --+ Index( b x_repbuh2cond_bodyisn)
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
  refundclassisn,condpremusd,dsstatus,condpremagr,carrptclass
from repbuh2cond b
where B.statcode in (27,33,35)
   AND sagroup IN (1, 3) -- EGAO 30.11.2009 And sagroup=1
   and B.bodyisn>pMinIsn and B.bodyisn<=pMaxIsn;  --Mserp 13.08.2008 Было "<pMaxIsn", мля...

insert into tt_rep_body_re
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
   decode (nvl (reaccfullamount,0),0,1/cntfull,reaccamount/reaccfullamount) RsPc,
   condisn,
   Isn,
   FullPc,
   S.Amount*decode (nvl (reaccfullamount,0),0,1/cntfull,reaccamount/reaccfullamount) ,
   S.AmountRub*decode (nvl (reaccfullamount,0),0,1/cntfull,reaccamount/reaccfullamount)  ,
   S.AmountUsd *decode (nvl (reaccfullamount,0),0,1/cntfull,reaccamount/reaccfullamount),
   pLoadIsn,0,motivgroupisn, sagroup,subaccisn,buhdeptisn,docisn,
     subjisn,
  agrcurrisn,
  sectcurrisn



 From
 (
  select --+ use_nl (s b c rs a aa ds rc r x bb) index(b) ordered
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
   decode (Ais.Recalc_utils.ReClass2SumClass(RC.classisn, RC.classisn2),DS.CLASSISN2,rc.amount) reaccamount,
   sum (decode (Ais.Recalc_utils.ReClass2SumClass(RC.classisn, RC.classisn2),DS.CLASSISN2,rc.amount))
    over (partition by b.isn, ds.isn) reaccfullamount,
    Count(*)  over (partition by b.isn, ds.isn) cntfull,
  r.isn sectisn, r.secttype, b.docsumisn,b.budgetgroupisn,b.rptgroupisn,b.rptclass,b.rptclassisn,b.condisn,
  b.isn,motivgroupisn, sagroup,
  b.subaccisn,bb.deptisn buhdeptisn,
  nvl(ds.docisn,ds.docisn2) docisn,
  b.subjisn,
  b.agrcurrisn,
  r.currisn sectcurrisn

  from  tt_repbuh2cond b,buhbody bb,
   docsum ds, ais.reaccsum rc, ais.resection r
  where
       b.docsumisn = ds.isn
   and ds.reaccisn = rc.reaccisn
   and ds.subjisn = rc.subjisn
   and ds.currisn = rc.currisn
   and ds.subaccisn = rc.subaccisn
   and rc.sectisn = r.isn
   and b.bodyisn=bb.isn
 )s;

Update tt_repbuh2cond
set loadisn=0
where (bodyisn,docsumisn) in (select bodyisn,docsumisn from tt_rep_body_re);


insert into tt_rep_body_re
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
   pLoadIsn,1,motivgroupisn, sagroup,
   subaccisn,buhdeptisn,   docisn,  subjisn,
     agrcurrisn,
  sectcurrisn




 From
 (
  select --+ use_nl (s b c rs a aa ds rc r x bb) index(b) ordered
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
  r.isn sectisn, r.secttype, b.docsumisn,b.budgetgroupisn,b.rptgroupisn,b.rptclass,b.rptclassisn,b.condisn,
  b.isn,motivgroupisn, sagroup,b.subaccisn,bb.deptisn buhdeptisn,
  nvl(b.docisn,b.docisn2) docisn,  b.subjisn,
    b.agrcurrisn,
  r.currisn sectcurrisn



  from  tt_repbuh2cond b,buhbody bb, ais.resection r
  where b.loadisn<>0
   and b.agrisn = r.agrisn
   and b.bodyisn=bb.isn
 )s;

Update tt_repbuh2cond
set loadisn=0
where bodyisn in (select bodyisn from tt_rep_body_re) and loadisn<>0;


insert into tt_rep_body_re
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
  select --+ use_nl (s b c rs a aa ds rc r x bb) index(b) ordered
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

  from  tt_repbuh2cond b,buhbody bb
  where loadisn<>0
   and b.bodyisn=bb.isn

 )s;

 insert into rep_body_re select * from tt_rep_body_re;

 commit;


END;

procedure Rep_Load_Budget_Refund(pTaskIsn in number)
is
begin
  Load_Budget_Refund(add_months(trunc(sysdate,'yyyy'),-12*4), Trunc(sysdate) );
end;


PROCEDURE Load_Budget_Refund(pDateBeg Date,
                             pDateEnd Date )
 Is
 vMinIsn Number:=-1;
 vlMaxIsn Number;
 vCnt Number:=0;
 vLoadObjCnt Number:=100000;

 vSql Varchar2(4000);
 pSesId number;
  Begin

execute immediate 'truncate table rep_budget_refund';

commit;

 pSesId:=Parallel_Tasks.createnewsession;
Loop

    select Max (CLAIMisn)
    into vLMaxIsn
    From
     (Select  --+ Index_Asc (a X_REPREFUND_CLAIM)
        CLAIMisn
      from reprefund a
       where CLAIMisn > vMinIsn
       and rownum <= vLoadObjCnt);

 Exit When vLMaxIsn is null;

vSql:='
Begin
 storages.REPORT_BUDGET.Load_Budget_Refund_By_Isns('||vMinIsn||','||vLMaxIsn||',
                   to_date('''||to_char(pDateBeg, 'dd.mm.yyyy')||''', ''dd.mm.yyyy''),
                   to_date('''||to_char(pDateEnd, 'dd.mm.yyyy')||''', ''dd.mm.yyyy''));
 Commit;
end;';

 Parallel_Tasks.processtask(psesid,vsql);

vCnt:=vCnt+1;

vMinIsn:=vLMaxIsn;
DBMS_APPLICATION_INFO.set_module('Rep_Budget_Refund','Process: '||vCnt*vLoadObjCnt);

end loop;
Parallel_Tasks.endsession(psesid);


DBMS_APPLICATION_INFO.set_module('','');



  end;

procedure Load_Budget_Refund_By_Isns (pMinIsn Number,pMaxIsn number, pDatebeg date, pDateEnd date)
is
Begin
 pPARAM.Clear;
 pPARAM.SETPARAMD(1, pDateBeg);
 pPARAM.SETPARAMD(2, pDateEnd);

/* 5.02.10 OD Переделал на view для COGNOS */
 insert into REP_BUDGET_REFUND
  select V.*
    from V_REP_BUDGET_REFUND  V
   where V.CLAIMISN >  pMinIsn
     and V.CLAIMISN <= pMaxIsn;
Commit;
end;



procedure Rep_LOad_budget_Rep_Motiv(pTaskIsn in number)
is
begin
    load_budget_rep_motivation(add_months(Trunc(sysdate,'mm'),-6));--SM 06.11.12 было add_months(Trunc(sysdate,'mm'),-3)
null;
end;

procedure Load_Budget_Rep_motivation (pStartDate Date:=null)
Is
 vRebuildMinDate date :=Nvl(pStartDate, Add_Months(Trunc(sysdate,'MM'),-1));


 SesId Number;
 vSql Varchar2(4000);
 vMinIsn Number:=0;
 vMaxIsn Number;
 vcnt Number:=0;


 vDt date;
 Begin
--tt_agr_salers_make;

delete from rep_bg_with_newdsi;
insert into  rep_bg_with_newdsi
(select * from v_bg_with_newdsi);
commit;

/*
loop
Delete --+ Index (a )
     From rep_motivation a Where (a.datepaym>= vRebuildMinDate or a.datequitm>= vRebuildMinDate) and RowNum<=300000;
Exit When Sql%rowcount<=0;
commit;
end loop;
*/

/*
loop
Delete --+ Index (a )
     From rep_motivation_quit a Where a.datequitm>= vRebuildMinDate and RowNum<=300000;
vcnt:= Sql%rowcount    ;
Delete --+ Index (a )
     From rep_motivation_quit a Where a.dateval>=vRebuildMinDate and RowNum<=300000;
Exit When vCnt+Sql%rowcount<=0;
commit;

end loop;
vCnt:=0;
*/
vCnt:=0;
vDt:=trunc(vRebuildMinDate,'MM');

Loop

Execute immediate 'alter table rep_motivation_quit_P truncate partition p'||To_Char(vDt,'MMYYYY');
vDt:=ADD_MONTHS(vDt,1);
Exit When vDt>sysdate;

end loop;

--store_and_drop_table_Index('storages.rep_motivation_quit',1);

--SesId:=gcc2.gcc2(1,35,53,trunc(sysdate));

  SesId:=Parallel_Tasks.createnewsession();


delete from  tt_rep_motiv_exclude;

insert into tt_rep_motiv_exclude
Select isn from agreement Where Id In (
'5024',
'4958',
'4912',
'20023340/2005',
'5010',
'4980',
'4981',
'4982',
'5047',
'5049',
'41-020257/04-F',
'4967') and classisn<>8742;
Commit;


/*проводки, сквитованные или начисленные в периоде */
execute immediate 'truncate table tt_isns';




Insert into tt_isns(Isn)
(
Select
                Distinct  b.bodyisn
              from RepBuhQuit b
                  Where              sagroup in (1,3,5)
                and statcode not in(220,60,221)
                and buhquitDate>=vRebuildMinDate

);

Insert into tt_isns (Isn)
/* кассовый метод*/

            Select  /*+ Index_Combine(b)*/
              Distinct
                  bodyisn
                  from storage_source.repbuhbody  b
             Where statcode in(220,60,221)
             and   ( not (deptisn=23735116 And Statcode=220) or
                    b.corsubaccisn not in (Select Isn from buhsubacc where Id Like '60%'))
                -- по мед убыткам исключаем корреспонденцию с авансами
                and ( not (deptisn=23735116 And Statcode=60) or
                b.corsubaccisn not in (Select subaccisn from subacc4dept sb where sb.statcode=220))
                and dateval>=vRebuildMinDate;
commit;



Loop
 vMaxIsn:=Cut_Table( 'storages.tt_isns','isn',vMinIsn,pRowCount=>10000);

exit When vMaxIsn Is Null;
--  Storages.REPORT_BUDGET.Load_Budget_Rep_motivation_Bi('||vMinIsn||','||vMaxIsn||','''||vRebuildMinDate||''');
 vSql:='
 Begin

   Storages.REPORT_BUDGET.Load_Budget_Rep_motiv_Quit_Bi('||vMinIsn||','||vMaxIsn||','''||vRebuildMinDate||''');
   End;';


  System.Parallel_Tasks.processtask(sesid,vsql);
vMinIsn:=vMaxIsn;
vCnt:=vCnt+1;
  DBMS_APPLICATION_INFO.set_module('Load_Budget_Rep_Cash','Loaded : '||vCnt*10000);


end loop;

Parallel_Tasks.endsession(sesid);


--restore_table_Index('storages.rep_motivation_quit');



Load_rep_motiv_Grp(vRebuildMinDate);
Load_rep_motiv_Grp_CUbe(vRebuildMinDate);
 end;


/*
procedure Load_Budget_Rep_motivation_Bi (pMinIsn number,pMaxIsn Number,pFromDate Date)
 Is
  Begin

Insert Into rep_motivation
Select * from
(
With Ba as(
  Select   --+ Index (ba X_BUDGET_BODY_AGRS_BODY) Ordered
   ba.AgrIsn,bodyIsn,budgetgroupisn,Ba.statcode LineCode,Sum(Ba.amountrub) AmountRub
  from rep_buh2Agr ba
  Where  ba.bodyisn>pMinIsn and ba.bodyisn<=pMaxIsn
  And Nvl(ba.reisn,0) not in (Select AgrIsn From tt_rep_motiv_exclude)
 -- And (Ba.deptIsn<>23735116 or nvl(clientclassisn,0)<>497 )
  Group by ba.AgrIsn,bodyIsn,ba.budgetgroupisn,Ba.statcode)

Select --+ Ordered Use_Nl(baa dp)
baa.AgrIsn,budgetgroupisn, LineCode,AmountRub,
--Dp.DeptIsn,BizFlg,
--ClientJURIDICAL,
datevalm,datequitM,DatePayM,BuhDEptIsn,
AmountFACTRub,AmountqUITRub

--SALERAND,SALERA,SALERF,,SalerNAme
from
(
Select --+ Ordered Use_Hash(ba bc)
 AgrIsn,budgetgroupisn, LineCode, Sum(AmountRub*LPc) AmountRub,
 datevalm,DatePayM,datequitM,BuhDeptIsn,
 Sum(AmountRub*fACTpc) AmountFACTRub,
  Sum(AmountRub*qUITApc) AmountqUITRub--,BizFlg,ClientJURIDICAL
From
(
Select --+ Ordered Use_Nl(bd b) iNDEX (B x_repbuhbody_bodyisn)
 b.bodyisn,
 Max(DeptIsn) BuhDEptIsn,
 Trunc(b.dateval,'MM') datevalm,
 Trunc(decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.QuitDateVal),'MM') DatePayM,
 Trunc(decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.BuhQuitDate),'MM') datequitM,


    --docsumpc*buhpc
Sum(b.buhamountrub*nvl (DocSumPC,1)--dspc
      *decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount) --bpc,
     *decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit)--FactPc
)/ Max(BuhAmountRub) LPc,

-- FactPc именно так - рублевый, чтобы учитывать курсовые разницы
Sum(gcc2.gcc2(b.AmountClosedQuit,b.QuitCurrIsn,35,b.QuitDateVal)*--dspc
      decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount) --bpc,
--      decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit)--FactPc
)/ Max(BuhAmountRub)  fACTpc,

sUM(decode (BuhQuitDate, null, 0, Buhamount)*nvl (DocSumPC,1)
      *decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount) --bpc,
     *decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit)--FactPc
)/ Max(BuhAmount)  qUITApc
from --(Select Distinct bodyisn from ba) Bd,
repbuhbody b
where b.bodyisn>pMinIsn and b.bodyisn<=pMaxIsn
and ( not (deptisn=23735116 And Statcode=220) or
b.corsubaccisn not in (Select subaccisn from subacc4dept where statcode=60))
and ( not (deptisn=23735116 And Statcode=60) or
b.corsubaccisn not in (Select subaccisn from subacc4dept where statcode=220))
group by  b.bodyisn,
 Trunc(b.dateval,'MM') ,
 Trunc(decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.QuitDateVal),'MM') ,
 Trunc(decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.BuhQuitDate),'MM')
-- Trunc(Nvl(BuhQuitDate,DateQuit),'MM')

/*
Select --+ Ordered Use_Nl(bd b)
 b.bodyisn,
 Max(DeptIsn) BuhDEptIsn,
 Trunc(b.dateval,'MM') datevalm,
 Trunc(decode(statcode,220,b.dateval,60,b.dateval,b.DatePay),'MM') DatePayM,
 Trunc(b.datequit,'MM') datequitM,
Sum(b.AmountRub)/ Max(BuhAmountRub) LPc
from
(Select Distinct bodyisn from ba) Bd, REPBUH2COND b
Where bd.bodyisn= b.bodyIsn
and ( not (deptisn=23735116 And Statcode=220) or
b.corsubaccisn not in (Select subaccisn from subacc4dept where statcode=60))
and ( not (deptisn=23735116 And Statcode=60) or
b.corsubaccisn not in (Select subaccisn from subacc4dept where statcode=220))
Group by
 b.bodyIsn,
 Trunc(b.dateval,'MM'),
 Trunc(decode(statcode,220,b.dateval,60,b.dateval,b.DatePay),'MM'),
 Trunc(b.datequit,'MM')
Having Max(BuhAmount)<>0
) bc, ba
Where ba.bodyisn=bc.bodyisn
and (DatePayM >=pFromDate
   Or datequitM>=pFromDate)
Group by AgrIsn,budgetgroupisn, LineCode,
datevalm,DatePayM,datequitM,BuhDEptIsn--,BizFlg,ClientJURIDICAL
) baa

);


Commit;


  end;
  */

procedure Load_Budget_Rep_motiv_Quit_Bi (pMinIsn number,pMaxIsn Number,pFromDate Date)
 Is
  Begin

Insert Into rep_motivation_quit_P
Select * from
(
With Ba as(
              Select   --+ Index (ba X_BUDGET_BODY_AGRS_BODY) Ordered
               ba.AgrIsn,bodyIsn,motivgroupisn ,Sum(Ba.amountrub) AmountRub,deptisn buhdeptisn,reisn,
               budgetgroupisn,Sum(Ba.amountusd) amountusd, dateval, statcode
              from rep_buh2Agr ba
              Where ba.bodyisn in (Select t.isn from storages.tt_isns t where t.isn>pMinIsn and t.isn<=pMaxIsn)
                And Nvl(ba.reisn,0) not in (Select AgrIsn From tt_rep_motiv_exclude)
             -- And (Ba.deptIsn<>23735116 or nvl(clientclassisn,0)<>497 )
              Group by ba.AgrIsn,bodyIsn,ba.motivgroupisn,deptisn,reisn,budgetgroupisn,
                       dateval, statcode)

Select --+ Ordered Use_Nl(baa dp)
    baa.AgrIsn,budgetgroupisn, AmountRub,
    datequitM,BuhDEptIsn,bodyisn,REISN,motivgroupisn,
    amountusd,LineCode, dateval,
  datequit -- EGAO 24.10.2008
from
(
    Select --+  Use_Hash(ba bc)
     AgrIsn,budgetgroupisn,nvl(bc.statcode, ba.statcode) LineCode,
   /*Sum(AmountRub*quitpc)*/ Sum(AmountRub*NVL(quitpc, 1)) AmountRub, --EGAO 24.10.2008
     datequitM,BuhDeptIsn,
   /*bc.bodyisn*/ba.bodyisn, -- EGAO 28.10.2008
   REISN,motivgroupisn,
   /*Sum(amountusdquitpc)*/ Sum(amountusd*NVL(quitpc, 1)) amountusd,  --EGAO 24.10.2008
   dateval,
   datequit -- EGAO 24.10.2008
    From
    ba, (
            Select --+ Ordered Use_Nl(bd b) iNDEX (B x_RepBuhQuit_body)
                 b.bodyisn,
                 /*Trunc(Nvl(BUHQUITDATE,decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.BUHQUITDATE)))datequit, -- EGAO 24.10.2008
                 Trunc(Nvl(BUHQUITDATE,decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.BUHQUITDATE)),'MM')datequitM,*/
                 --EGAO 05.08.2010 в рамках ТЗ на доработку отчета по мотивации от Кубасовой от 04.08.2010. Предыдущий вариант в комментариях выше
                 Trunc(decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.BUHQUITDATE))datequit, -- EGAO 24.10.2008
                 Trunc(decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.BUHQUITDATE),'MM')datequitM,

                 /*Sum(
                 gcc2.gcc2(BUHAMOUNT*BUHPC*QUITPC-Nvl(QUITSUM,0)*BuhQuitPc,currisn,35,Nvl(BUHQUITDATE, Dateval))+ -- валютируем зачет на дату начисления
                 gcc2.gcc2(Nvl(QUITSUM,0)*BuhQuitPc,currisn,35, Decode(FACT,'Y',QUITDATEVAL,Nvl(BUHQUITDATE,Dateval))) -- валютируем поступление на дату оплаты
                 )/Max(BUHAMOUNTRUB) quitpc,*/
         NVL(/*Sum(
                 gcc2.gcc2(BUHAMOUNT*BUHPC*QUITPC-Nvl(QUITSUM,0)*BuhQuitPc,currisn,35,Nvl(BUHQUITDATE, Dateval))+ -- валютируем зачет на дату начисления
                 gcc2.gcc2(Nvl(QUITSUM,0)*BuhQuitPc,currisn,35, Decode(FACT,'Y',QUITDATEVAL,Nvl(BUHQUITDATE,Dateval))) -- валютируем поступление на дату оплаты
                 )/Max(BUHAMOUNTRUB)*/

                 /*KGS 18.1.12 изменил порядок заполнеия витрины. в поле repcursdiff - курсовая разница*/
                  Sum(BUHAMOUNTRUB*BUHPC*QUITPC*BuhQuitPc + b.repcursdiff)/Max(BUHAMOUNTRUB),0) quitpc,

                Max(decode(b.statcode,48,
                Nvl((Select rs.statcode
                                from  rep_budget_statcode rs
                              Where  b.subaccisn=rs.subaccisn
                                and Nvl(rs.corsubaccisn,b.corsubaccisn)=b.corsubaccisn
                                and Nvl(rs.analitikisn,0)=decode(rs.analitikisn,null,0,
                                                    (select ks.classisn from  ais.buhbody bb, ais.kindaccset  ks where bb.isn=b.bodyisn
                                                    and ks.KINDACCISN=bb.SubKindIsn and ks.classisn =rs.analitikisn))),48),b.statcode)
                                        ) statcode

            from --(Select Distinct bodyisn from ba) Bd,
              RepBuhQuit b
            where b.bodyisn in (Select t.isn from storages.tt_isns t where t.isn>pMinIsn and t.isn<=pMaxIsn)
                and  Nvl(BUHQUITDATE,decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.BUHQUITDATE)) is not null
                and ( not (deptisn=23735116 And Statcode=220) or
                b.corsubaccisn not in (Select Isn from buhsubacc where Id Like '60%'))
                -- по мед убыткам исключаем корреспонденцию с авансами
                and ( not (deptisn=23735116 And Statcode=60) or
                b.corsubaccisn not in (Select subaccisn from subacc4dept where statcode=220))
            group by  b.bodyisn,
              /*Trunc(Nvl(BUHQUITDATE,decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.BUHQUITDATE))), -- EGAO 24.10.2008
              Trunc(Nvl(BUHQUITDATE,decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.BUHQUITDATE)),'MM')*/
              --EGAO 05.08.2010 в рамках ТЗ на доработку отчета по мотивации от Кубасовой от 04.08.2010. Предыдущий вариант в комментариях выше
              Trunc(decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.BUHQUITDATE)),
              Trunc(decode(statcode,220,b.dateval,60,b.dateval,221,b.dateval,b.BUHQUITDATE),'MM')

            ) bc
        Where ba.bodyisn=bc.bodyisn(+)
        --and datequitM >=pFromDate
        Group by AgrIsn,budgetgroupisn, nvl(bc.statcode, ba.statcode),motivgroupisn,
                 datequitM,BuhDEptIsn,ba.bodyisn,REISN, ba.dateval, datequit--,BizFlg,ClientJURIDICAL
        --Having Trunc(Sum(AmountRub*quitpc),2)<>0
        ) baa
    where datequitm>=pFromDate or dateval>=pFromDate
);


Commit;







  end;



procedure Load_rep_motiv_Grp(pStartDate date:=null)
Is
 vRebuildMinDate date :=Nvl(pStartDate, Add_Months(Trunc(sysdate,'MM'),-3));


 SesId Number;
 vSql Varchar2(4000);
 vDate date:=vRebuildMinDate;
 Begin


loop
Delete --+ Index (a )
     From storages.rep_motivation_quit_grp a Where (a.datequitm>= vRebuildMinDate) and RowNum<=300000;
Exit When Sql%rowcount<=0;
commit;
end loop;


store_and_drop_table_Index('storages.rep_motivation_quit_grp',1);

--SesId:=gcc2.gcc2(1,35,53,trunc(sysdate));

  SesId:=Parallel_Tasks.createnewsession();


delete from  storages.tt_rep_motiv_exclude;




Loop

exit When vDate>trunc(sysdate);


  DBMS_APPLICATION_INFO.set_module('Load_Budget_Rep_Cash','Loaded : '||vDate);

--  Storages.REPORT_BUDGET.Load_Budget_Rep_motivation_Bi('||vMinIsn||','||vMaxIsn||','''||vRebuildMinDate||''');
 /*vSql:='
 declare
  pDateQuit date:='''||vDate||''';
 Begin
  Insert into storages.rep_motivation_quit_grp
    Select --+ Index_Combine(m)
      agrisn ,motivgroupisn budgetgroupisn,datequitm,buhdeptisn,sum(amountrub) amountrub,reisn,linecode
     from storages.rep_motivation_quit m
     where datequitm=pDateQuit
   group by agrisn ,motivgroupisn ,linecode,datequitm,buhdeptisn,reisn
    Having trunc(sum(amountrub),2)<>0;
commit;

   End;';*/
   --EGAO 07.07.2009 предыдущий вариант в комментариях выше


/* при группировке учитываем смену продавца если была*/
  Insert into storages.rep_motivation_quit_grp
    Select --+ Index_Combine(m) Use_Nl(DP) Index (DP)
      m.agrisn ,motivgroupisn budgetgroupisn,
      Max(datequit),
      buhdeptisn,sum(amountrub) amountrub,reisn,linecode
     from storages.rep_motivation_quit_p m,REP_AGR_SALERS_Line DP
     where datequitm=vDate

     and m.agrisn=DP.agrisn(+)
     and DATEQUIT between nvl(DP.DATEBEG(+), '01-jan-1900') and nvl(DP.DATEEND(+), '01-jan-3000')

   group by m.agrisn ,motivgroupisn ,linecode,buhdeptisn,reisn,Dp.RowId
    Having trunc(sum(amountrub),4)<>0;
commit;



--execute immediate vSql;
--  System.Parallel_Tasks.processtask(sesid,vsql);

vDate:=add_months(vDate,1);




end loop;

Parallel_Tasks.endsession(sesid);


restore_table_Index('storages.rep_motivation_quit_grp');


end;



procedure Load_rep_motiv_Grp_CUbe (pFromDate Date:=null)
is
 vMinIsn number:=0;
 vMaxIsn number;

 vSql varchar2(4000);
  SesId Number;
  vLoadObjCnt number:=100000;
  vCnt number:=0;

  vColList varchar2(32000);

vMinMonth Date:=Nvl(pFromDate,Add_Months(Trunc(sysdate,'MM'),-6)); /* с этой даты грузим мотивац. отчет*/
begin


--truncate_table('storages.tt_rep_motivation_cube');
truncate_table('storages.tt_rep_motivation');

store_and_drop_table_index('storages.tt_rep_motivation',1);
--store_and_drop_table_index('storages.tt_rep_motivation_cube',1);

 SesId:=Parallel_Tasks.createnewsession();



vMinIsn:=-1;

vColList:=REP_COGNOS_UTILS.get_not_null_columns_list ('storages.v_rep_motivation');

loop

--vMaxIsn:=Cut_Table('storages.rep_motivation_quit_grp','agrisn',vMinIsn,pRowCount=>vLoadObjCnt);



-- Exit When vMaxIsn is null;




DBMS_APPLICATION_INFO.set_module('Load','tt_rep_motivation: '||vMinMonth);

vSql:='Begin
pParam.SetParamD(1,to_date('''||vMinMonth||'''));
insert into storages.tt_rep_motivation
Select '||vColList||'
from storages.v_rep_motivation;
commit;
End;';

/* Where agrisn >'|| vMinIsn||' And agrisn <= '||vMaxIsn||';*/

 --System.Parallel_Tasks.processtask(sesid,vsql);
--dbms_output.put_line(vSql);
Execute immediate vSql;





Exit When vMinMonth>sysdate;

vMinMonth:=ADD_MONTHS(vMinMonth,1);



end loop;

-- ждем, пока завершатся все джобы
Parallel_Tasks.endsession(sesid);


Restore_table_index('storages.tt_rep_motivation');

/* SesId:=Parallel_Tasks.createnewsession();

for cur in (Select \*+ Index(a X_REP_MOTIV_QUIT_GRP_DATEQUIT)*\
                   DISTINCT trunc(a.datequitm,'MM') AS DATEQUITM-- EGAO 07.07.2009 DATEQUITM
            from rep_motivation_quit_grp a) loop

DBMS_APPLICATION_INFO.set_module('Load motivation cube ','Load: '||cur.DATEQUITM);

vSql:=
'declare
 vDate date:='''||cur.DATEQUITM||''';

Begin
DBMS_APPLICATION_INFO.set_module(''Load motivation cube '',vDate);

insert into STORAGES.TT_REP_MOTIVATION_CUBE
( select --+ index_combine(r)
         R.BUDGETGROUP,
         R.LINEDESC,
         R.CLIENTJURIDICAL,
         sum(R.AMOUNTRUB) AMOUNTRUB,
         R.DATEQUITM,
         R.SALERGO,
         R.SALERF,
         R.BUHDEPT,
         R.KURATOR,
         R.KURATORDEPT,
         R.FILIAL,
         R.CALCBIZFLG,
         R.EMITENT,
         trunc(R.DATEBEG, ''YYYY'') DATEBEG,
         R.SALERNAME,
         R.SALERCLASS,
         R.AGRSALERCLASS,
         R.DEPT0ISN,
         R.DEPT1ISN,
         R.DEPTISN,
        --
         R.BUDGETGROUPISN, -- OD 08.04.09 9016035703
         R.BUHDEPTISN, -- OD 08.04.09
         R.EMPLISN, -- OD 08.04.09
         R.EMPLDEPTISN, -- OD 08.04.09
         R.FILISN, -- OD 08.04.09
         R.SALERISN, -- OD 08.04.09
         R.SALERCLASSISN, -- OD 08.04.09
         R.AGRSALERCLASSISN, -- OD 08.04.09
         R.LINEDESCCODE, -- OD 23.04.09
         R.EMITISN, -- OD 08.04.09
         R.RULEISN -- OD 27.08.09
    from STORAGES.TT_REP_MOTIVATION R
   where DATEQUITM = vDATE
group by BUDGETGROUP, LINEDESC, CLIENTJURIDICAL, DATEQUITM, SALERGO, SALERF, BUHDEPT, KURATOR, KURATORDEPT,
         FILIAL, CALCBIZFLG, EMITENT, trunc(DATEBEG, ''YYYY''), SALERNAME, SALERCLASS, AGRSALERCLASS,
         DEPT0ISN, DEPT1ISN, DEPTISN, R.BUDGETGROUPISN, R.BUHDEPTISN, R.EMPLISN, R.EMPLDEPTISN,
         R.FILISN, R.EMITISN, R.SALERISN, R.SALERCLASSISN, R.AGRSALERCLASSISN, R.LINEDESCCODE, R.RULEISN );
Commit;
end;';

System.Parallel_Tasks.processtask(sesid,vsql);

rep_message.put(p_recipient => 'COGNOS',p_object => 'REP_MOTVATION');
end loop;

Restore_table_index('storages.tt_rep_motivation_cube');
-- ждем, пока завершатся все джобы
Parallel_Tasks.endsession(sesid);*/
end;



Procedure tt_agr_salers_make
 Is
 vMinIsn Number:=-1;
 vlMaxIsn Number;
 vCnt Number:=0;
 vLoadObjCnt Number:=100000;

 vSql Varchar2(32000);
 SesId number;
 vLoadIsn Number;
  Begin
  -- sts 16.01.2013 - таблица storages.tt_agr_salers теперь не поддерживается.
  -- под этим именем теперь существует вьюха, заточенная на новую таблицу STORAGES.REP_AGR_SALERS
  -- Поэтому этот загрузчик теперь не нужен. Вставил в первой строке выход из процедуры
  -- Удалять код пока не стал...
  RETURN;


/*
Select Max(Loadisn) into vLoadIsn from tt_agr_salers  Where rownum<=1;

Select Nvl(Max(Loadisn),0)
into vCnt
From repagr ag
where  rownum<=1;

If vLoadIsn=vCnt then vCnt:=1; else vCnt:=0; end if;

 If vCnt=0 then
*/
Select  Max(Loadisn) into vLoadisn From repagr where  rownum<=1;
execute Immediate 'truncate table Storages.tt_agr_salers';
execute Immediate 'truncate table Storages.tt_agr_salersb';
execute Immediate 'truncate table Storages.tt_agr_salers_Line';

SesId:=Parallel_Tasks.createnewsession('AgrSalers');

Loop
vLMaxIsn:=cut_table('storage_source.repagr','agrisn',vMinIsn);
 Exit When vLMaxIsn is null;

vSql:='
Declare
  vLoadIsn number := '||TO_CHAR(vLoadIsn)||';
  vMinIsn  number := '||TO_CHAR(vMinIsn)||';
  vlMaxIsn number := '||TO_CHAR(vlMaxIsn)||';
  vCnt     number := '||TO_CHAR(vCnt)||';
Begin
DBMS_APPLICATION_INFO.set_module(''tt_agr_salers'',''Thread: ''||TO_CHAR(vCnt));
storages.report_Budget.tt_agr_salers_make_By_Isns(vMinIsn,vlMaxIsn,vLoadIsn);
end;';
System.Parallel_Tasks.processtask(sesid,vsql);

--storages.report_Budget.tt_agr_salers_make_By_Isns(vMinIsn,vlMaxIsn,vLoadIsn);
vCnt:=vCnt+1;

vMinIsn:=vLMaxIsn;
DBMS_APPLICATION_INFO.set_module('tt_agr_salers','Updated: '||vCnt*vLoadObjCnt);

end loop;
--end if;
-- ждем, пока завершатся все джобы
Parallel_Tasks.endsession(sesid);
end;



procedure Load_rep_fil_com(pTaskIsn in Number)

is
 vMinIsn Number:=-1;
 vlMaxIsn Number;
 vCnt Number:=0;
 vLoadObjCnt Number:=30000;

 SesId Number;
 vSql Varchar2(4000);

cDateend Date:=trunc(sysdate,'mm')-1;  -- отчет нельзя грузить "куском в середине" - разъедется. только от даты и до "конца"
vDatebeg Date:=Trunc(cDateend,'YYYY');

vDaterep date;

vDt Date;

-- cRepFilComPrem number:=c.get('cRepFilComPrem');
-- cRepFilComTrans number:=c.get('cRepFilComTrans');
-- cRepFilComOut number:=c.get('cRepFilComOut');
-- cRepFilTypeNach number:=c.get('cRepFilTypeNach');
-- cRepFilTypeQuit number:=c.get('cRepFilTypeQuit');



Begin



vCnt:=0;
vDt:=trunc(vDatebeg,'MM');

Loop

Execute immediate 'alter table rep_fil_com truncate partition p'||To_Char(vDt,'MMYYYY');
vDt:=ADD_MONTHS(vDt,1);
Exit When vDt>cDateend;

end loop;

/*
delete --+ Index_Combine(b)
from storages.rep_fil_com where daterep>= vDatebeg;
--And (repname='Внешняя Комиссия' or daterep<=vDateend);
commit;
*/
execute immediate 'truncate table storages.rep_fil_com_final reuse storage';


store_and_drop_table_index('storages.rep_fil_com',1);

vDaterep:=vDatebeg;

SesId:=Parallel_Tasks.createnewsession;

loop


vSql:='
declare
 vDaterep date:='''||vDaterep||''';
 cRepFilComPrem number:=c.get(''cRepFilComPrem'');
 cRepFilComTrans number:=c.get(''cRepFilComTrans'');
 cRepFilTypeNach number:=c.get(''cRepFilTypeNach'');
 cRepFilTypeQuit number:=c.get(''cRepFilTypeQuit'');

Begin

DBMS_APPLICATION_INFO.set_module(''rep_fil_com'',vDaterep);

insert into storages.rep_fil_com
Select --+ no_merge ( rm ) use_hash ( mgt )
       rm.agrisn, budgetgroupisn, daterep, buhdeptisn, amountrub,
       clientjuridical, agfilcomission, filisn, rm.comtype,
       decode(agfilcomission,0,Nvl(MgT.tariff,0),agfilcomission) filcomission,
       cRepFilComPrem,
       REPTYPEISN
from (
       Select --+ ordered use_nl ( rm ag ) use_hash ( fc ) no_merge ( rm )
              rm.*,ag.clientjuridical,Nvl(ag.filcomission,0) agfilcomission,ag.calcfilisn FilIsn,
              comtype
       from (
             Select --+ No_Parallel(rm)
                    agrisn,motivgroupisn budgetgroupisn,datequitm daterep,buhdeptisn,sum(amountrub) amountrub,cRepFilTypeQuit REPTYPEISN
             from storages.rep_motivation_quit_p rm
             Where rm.DATEQUITM=vDaterep
               And rm.linecode=''38''
             group by agrisn,motivgroupisn,datequitm,buhdeptisn

             Union all

             Select --+ Index_Combine(rm)
                    agrisn,motivgroupisn budgetgroupisn,trunc(dateval,''MM''),deptisn,sum(amountrub),cRepFilTypeNach REPTYPEISN
             from storages.rep_buh2agr rm
             Where dateval Between vDaterep and add_months(vDaterep,1)-1
               And rm.statcode=38
             group by agrisn,motivgroupisn,trunc(dateval,''MM''),deptisn

            ) rm ,
            storages.repagr ag, storages.cfg_rep_fil_com fc -- fc - таблица настройки департамент, наличие филиала , тип комиссии
       Where rm.AGRISN=ag.agrisn    -- EGAO 27.04.2010 в рамках 14800840003 rm.AGRISN=ag.agrisn(+)
         and ag.ruleisn<>655469916  -- EGAO 27.04.2010 в рамках 14800840003
         and rm.buhdeptisn=fc.deptisn
         and (( ag.calcfilisn is not null and fc.ISFILIAL=1 )
              or (ag.calcfilisn is  null and Nvl(fc.ISFILIAL,0)=0 ))
     ) rm,storages.v_bg_tarif MgT
Where rm.budgetgroupisn=MgT.isn(+)
  And rm.clientjuridical=Mgt.Juridical(+)
  and rm.comtype=mGt.COMTYPE(+)
  and rm.daterep between Mgt.Datebeg(+) and Mgt.dateend(+) ;


insert into storages.rep_fil_com
Select agrisn, budgetgroupisn, daterep, buhdeptisn, amountrub,
       clientjuridical, TRANSFERCOMISSION, filisn, null,
        TRANSFERCOMISSION filcomission, cRepFilComTrans,REPTYPE
from ( Select --+ Ordered Use_Nl(ag)
              rm.*,ag.clientjuridical,Nvl(ag.TRANSFERCOMISSION,0) TRANSFERCOMISSION,Nvl(BFilIsn,ag.filisn) FilIsn
       from ( Select  --+ No_Parallel(rm)
                      agrisn,motivgroupisn budgetgroupisn,datequitm daterep,
                      buhdeptisn,sum(amountrub) amountrub,cRepFilTypeQuit REPTYPE
              from storages.rep_motivation_quit_p rm
              Where rm.DATEQUITM=vDaterep
                And rm.linecode=''38''
              group by agrisn,motivgroupisn,datequitm,buhdeptisn
            ) rm ,storages.repagr ag
       Where rm.AGRISN=ag.agrisn
         and Nvl(ag.TRANSFERCOMISSION,0)<>0 );

 commit;

end;';
 System.Parallel_Tasks.processtask(sesid,vsql);
 vDaterep:=add_months(vDaterep,1);
 DBMS_APPLICATION_INFO.set_module('rep_fil_com','');
 exit when vDaterep>cDateend;

 end loop;
Parallel_Tasks.endsession(sesid);




/*
cfg_rep_fil_com
-- комиссия по сквиованным суммам
Select rm.*,decode(agfilcomission,0,0,MgT.tariff,agfilcomission) filcomission
from
(
Select rm.*,ag.clientjuridical,Nvl(ag.filcomission,0) agfilcomission,ag.filisn
from
(
Select  --+ Index_Combine(rm)
 agrisn,budgetgroupisn,datequitm,buhdeptisn,sum(amountrub)
from rep_motivation_quit rm
Where rm.DATEQUITM between :pDatebeg and :pDateend
And rm.linecode=38
group by agrisn,budgetgroupisn,datequitm,buhdeptisn
) rm ,repagr ag
Where  rm.AGRISN=ag.agrisn(+)

) rm,v_bg_tarif MgT
/*
(

Select x1 budgetgroupisn,decode(x3,1,'N','Y') Juridical,tariff
from
RulTariff rt
where TariffISN=1605820603
) MgT

Where rm.budgetgroupisn=MgT.budgetgroupisn(+)
  And rm.clientjuridical=Mgt.Juridical(+);
*/

/*
-- комиссия по начисленным суммам
Select rm.*,decode(agfilcomission,0,0,MgT.tariff,agfilcomission) filcomission
from
(
Select rm.*,ag.clientjuridical,Nvl(ag.filcomission,0) agfilcomission,ag.filisn
from
(
Select  --+ Index_Combine(rm)
 agrisn,budgetgroupisn,trunc(dateval,'mm') datevalm,buhdeptisn,sum(amountrub)
from repbuh2cond rm
Where rm.DATEval between :pDatebeg and :pDateend
And rm.statcode=38
group by agrisn,budgetgroupisn,trunc(dateval,'mm'),buhdeptisn
) rm ,repagr ag
Where  rm.AGRISN=ag.agrisn(+)

) rm,
(

Select x1 budgetgroupisn,decode(x3,1,'N','Y') Juridical,tariff
from
RulTariff rt
where TariffISN=1605820603
) MgT
Where rm.budgetgroupisn=MgT.budgetgroupisn(+)
  And rm.clientjuridical=Mgt.Juridical(+);
*/

/*
execute immediate 'truncate table tt_rep_bb_com';
Insert Into tt_rep_bb_com
Select * from
(
With Com AS
(
Select --+ Ordered Use_Hash(rb rs) Use_Nl (rb bb bb1)
bb1.Isn BodyIsn,rb.agrisn,rb.structisn,REPRISN,
sum(rb.amountrub)*Max(Nvl(-bb1.damountrub,bb1.camountrub))/Max(Nvl(bb.damountrub,-bb.camountrub)) Comission,
Max(rs.name1) Name1,
Max(rs.name2) Name2,
Max(rs.process) ComType,
rb.dateend
from Rep_fd_body rb,  rep_fd_structure rs,ais.buhbody_t bb, Ais.buhbody_t bb1
where rb.ClassISN in (1292712603,1292398203,1292400303,1292401703,1119216803)--( c.get('c_rbk4') , c.get('c_rbk3'), c.get('c_rbk4'), c.get('c_rbk5'))
and rb.DateBeg between vDateBeg and vDateend
and rs.isn = rb.structisn
and substr(rs.process,1,2) = 'BK'
And  rb.bodyisn=bb.isn
And bb1.headisn=bb.headisn
and bb1.isn<>bb.isn
Group by  bb1.Isn,rb.agrisn,rb.structisn,REPRISN,rb.DATEEND
)
Select --+ Ordered USe_Hash( com agr bb)
com.BodyIsn,
Nvl(Com.agrisn,bb.AgrISn) AgrIsn,
structisn,
nvl(REPRISN,reprdeptisn) REPRISN,
Comission,
Name1,
Name2,
ComType,
 Nvl(agr.BudgetgroupIsn,bb.BudgetgroupIsn) BudgetgroupIsn,
 Nvl(Nvl(AgrPc,BodyPc),1) BgPc,
 DATEEND,
 Nvl(agr.BuhDept,bb.BuhDept) Buhdeptisn
from Com,
(Select --+ Ordered Use_Nl (com rb)
   Rb.AgrIsn,Rb.motivgroupisn BudgetgroupIsn, sum(Amount)/Max(BuhAmount) AgrPc,MAx(DeptIsn) BuhDept
from (Select distinct agrisn from Com) cm,repBuh2cond rb
Where cm.AgrIsn =rb.agrisn
Group by  Rb.AgrIsn,Rb.motivgroupisn
 ) Agr,
(Select --+ Ordered Use_Nl (com rb)
   Rb.BodyIsn,rb.agrisn,Rb.motivgroupisn BudgetgroupIsn,reprdeptisn, sum(Amount)/Max(BuhAmount) BodyPc,MAx(DeptIsn) BuhDept
from (Select distinct BodyIsn from Com) com,repBuh2cond rb
Where com.BodyIsn =rb.BodyIsn
Group by  Rb.BodyIsn,rb.agrisn,Rb.motivgroupisn,reprdeptisn ) Bb
Where Com.agrisn=agr.agrisn(+)
and Com.BodyIsn=bb.bodyIsn(+)
);
Commit;

*/




/*
  SesId:=Parallel_Tasks.createnewsession(pmaxjobcnt=>12);

vMinIsn:=-1;


loop
vLMaxIsn:=Cut_Table('storage_source.reprefund','refundisn',vMinIsn);

 Exit When vLMaxIsn is null;

vSql:='
Begin
Insert into storages.rep_fil_com
select
agrisn, budgetgroupisn, Daterep, Max(buhdept),
       sum(ComF), null, 0, filisn,
       null, 100,''Чужие убытки''
from
(
Select  --+ Ordered Use_Hash(rule r sd) Use_Nl(r ac sb ds t)
r.AgrIsn,
r.refundisn,
MAX(ds.AmountRub) ComF ,
sd.FilIsn,
Add_Months(Trunc(Datepay,''mm''),1)-1 daterep,
r.budgetgroupisn,
Max(r.conddeptisn) buhdept
from

--(           select  Isn,ShortNAme
--             from dicti z
--             Start With z.ISN = 683209116
--            connect by prior z.isn=z.parentisn) RuLe,
storages.reprefund r,DocSum ds,agrclaim ac,subhuman Sb,v_dept sd
Where-- rule.isn=r.ruleisnclaim
--And
r.refundisn>'||vMinIsn||' and r.refundisn<='||vLMAxIsn||'
and r.claimisn=ac.isn
And ac.emplisn=sb.isn(+)
And ds.ClassISN2 IN (c.Get(''amRefundCom''),c.Get(''amRefundCom4''),c.Get(''amRefundCom5''))
 And ds.DocISN IS  NULL And  ds.DocISN2 IS  NULL
 And ds.refundisn=r.refundisn
 And Daterefund between '''||vDatebeg||''' and '''||vDateend||'''
 And Sb.deptIsn=Sd.DeptIsn(+)
group by
r.AgrIsn,
r.refundisn,
sd.FilIsn,
Add_Months(Trunc(Datepay,''mm''),1)-1,
budgetgroupisn
)
Group by  AgrIsn,FilIsn,Daterep,budgetgroupisn;



 COMMIT;

End;';

 System.Parallel_Tasks.processtask(sesid,vsql);

vCnt:=vCnt+1;

vMinIsn:=vLMaxIsn;
DBMS_APPLICATION_INFO.set_module('Update','Updated: '||vCnt*vLoadObjCnt);

end loop;

-- ждем, пока завершатся все джобы
Parallel_Tasks.endsession(sesid);

*/


execute immediate 'truncate table  tt_Long_Isns';

Insert into tt_Long_Isns
Select /*+ Full(t) Parallel(t,32)*/
               Distinct AgrIsn,RepTypeIsn, filisn
        from  storages.rep_fil_com t
        Where t.comclassisn=2336994203/*cRepFilComPrem*/ and comtype<>1304223903
        and daterep Between vDatebeg and cDateend and filcomission>0;
Commit;


  SesId:=Parallel_Tasks.createnewsession;



vMinIsn:=-1;


loop

vLMaxIsn:=Cut_Table('storages.tt_Long_Isns','isn',vMinIsn,pRowCount=>vLoadObjCnt);

 Exit When vLMaxIsn is null;

vSql:='
declare
  cRepFilComOut number:=c.get(''cRepFilComOut'');
  cRepFilComPrem number:=c.get(''cRepFilComPrem'');
Begin
  Insert Into storages.rep_fil_com
  Select --+ Ordered  Index(b X_REPBUH2COND_AGRISN) Use_nl(t b)
         b.agrisn, budgetgroupisn,Trunc(Nvl(datepay,datequit),''mm'') Daterep,
         Max(b.deptisn), Sum(Nvl(gcc2.gcc2(FACTSUM*condpc*buhpc,Factcurrisn,35,datepay),amountrub)),
         null, 0,filisn,
         null, 100,cRepFilComOut,RepTypeIsn
  from ( Select Isn AgrIsn, Attr1 RepTypeIsn,Attr2 filisn
          from  Storages.tt_Long_Isns t
          Where t.isn >'|| vMinIsn||' And t.isn <= '||vLMaxIsn||')  T, storages.repbuh2cond b
  Where t.agrisn=b.agrisn
  ANd b.statcode=20102
  and sagroup=1
  and Nvl(datepay,datequit)>='''||vDatebeg||'''
  --and dateval>=vDatebeg
  group by
  b.agrisn,filisn,--REPRDEPTISN,
  budgetgroupisn,
  Trunc(Nvl(datepay,datequit),''mm''),RepTypeIsn;
  commit;
end;';


 System.Parallel_Tasks.processtask(sesid,vsql);

vCnt:=vCnt+1;

vMinIsn:=vLMaxIsn;
DBMS_APPLICATION_INFO.set_module('rep_fil_com','Updated: '||vCnt*vLoadObjCnt);

end loop;

-- ждем, пока завершатся все джобы
Parallel_Tasks.endsession(sesid);

restore_table_index('storages.rep_fil_com');


--MSerp 05.08.2008. REP_FIL_COM_FINAL


 vMinIsn :=-9e99;
 vCnt :=0;
 vLoadObjCnt :=100000;

store_and_drop_table_index('storages.rep_fil_com_final');

 SesId:=Parallel_Tasks.createnewsession;

SesId:=Parallel_Tasks.createnewsession;

vDt:='01-jan-2002';

Loop


DBMS_APPLICATION_INFO.set_module('rep_fil_com_Final','Process: '||vDt);

vSql:='
Begin
insert into storages.rep_fil_com_final
select
 * from storages.v_rep_fil_com_final v where v.Daterep='''||vDt||''';

Commit;
end;';

 Parallel_Tasks.processtask(sesid,vsql,1);


vDt:=ADD_MONTHS(vDt,1);
Exit When vDt>cDateend;

end loop;

Parallel_Tasks.endsession(sesid);
restore_table_index('storages.rep_fil_com_final');

-- отправим сообщение о том, что таблица изменилась
--rep_message.put(p_recipient => 'COGNOS',p_object => 'REP_FIL_COM');
End;



procedure Rep_Load_Budget(pTaskIsn in number)
Is
begin
Load_Budget('01-jan-2004',Trunc(sysdate,'mm'));
end;



PROCEDURE Load_Budget_Agrs_By_Isns
     (vMinIsn number,vlMaxIsn number,
      pDateBeg Date,
      pDateEnd Date,
      pAgrDateBeg Date )


 Is
  Begin


delete from tt_budget_agrs;

insert into STORAGES.TT_BUDGET_AGRS
( select --+ ordered ordered use_nl(ass  st bsd st1) use_hash(bdp)
         S.AGRISN,
         ASS.AGRID,
         AGRDATEBEG,
         AGRDATEEND,
         SALECHANEL,
         BUDGETGROUPISN,
         DATEVAL,
         CLIENTJURIDICAL,
         EMITISN,
         BEMITISN,
         AGENTCLASSISN,
         AGENTJURIDICAL,
         S.STATCODE,
         nvl(ST1.DESCRIPTION, ST.DESCRIPTION) STATNAME,
         S.DEPTISN,
         AGRDEPTISN,
         BIZFLG,
         CALCBIZFLG,
         REINDEPT,
         AGRCLASSISN,
         AMOUNTRUB,
         AMOUNTUSD,
         SALERGODEPT,
         SALERGO,
         BDP.DEPTISN BDPDEPTISN,
         REISN,
         SAGROUP,
      -- ЦО
         COISN,
      -- ЦП
         CPISN,
         SALERCLASSISN,
         SALERFDEPT,
         SALERFDEPT0,
         SALERCRDEPT,
         SALERCRDEPT0,
         GMISN,
         S.AGRRULEISN -- OD 07.04.2009 8940292103
    from
         ( select --+ index(b x_rep_buh2agr_agr) ordered use_nl(rs ra) push_subq
                  B.AGRISN,
                  MOTIVGROUPISN BUDGETGROUPISN,
                  nvl(RS.STATCODE, B.STATCODE) STATCODE,
                  trunc(DATEVAL, 'MM') DATEVAL,
                  sum(AMOUNTUSD) AMOUNTUSD,
                  sum(AMOUNTRUB) AMOUNTRUB,
                  B.DEPTISN,
                  ( case
                     when B.DEPTISN not in (11414319, 742950000, 1112083803)
                      and RPTGROUPISN in (755075000, 755078500)
                      then 504
                    end ) REINDEPT,
                  B.REISN,
                  SAGROUP,
                  RA.GMISN,  -- определение бизнеса ДП по входящему страхованию - аблигаторы, для всех департаментов кроме дкс, усто, устк
                  RA.RULEISN AGRRULEISN -- OD 07.04.2009 8940292103
             from STORAGES.REP_BUH2AGR B,
                  REP_BUDGET_STATCODE RS, -- в таблице REP_BUDGET_STATCODE статкоды "гиморойные", используемые только для бюджета
                  REPAGR RA
            where B.AGRISN    >  vMINISN
              and B.AGRISN    <= vLMAXISN
              and DATEVAL between PDATEBEG and PDATEEND
              and dateval>=pAgrDateBeg
              and B.SAGROUP in (1, 3)
              and B.STATCODE  <> 60
              and B.SUBACCISN  = RS.SUBACCISN(+)
              and RA.AGRISN(+) = B.AGRISN

              and nvl(RS.CORSUBACCISN(+), B.CORSUBACCISN) = B.CORSUBACCISN
              --(+) MSerp 26.10.2009. Убрал открытый join, т.к. в 10g этот фокус больше не проходит. Если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. Насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
              /*and nvl(RS.ANALITIKISN, 0) = decode(RS.ANALITIKISN, null,0, ( select KS.CLASSISN
                                                                                    from AIS.BUHBODY BB,
                                                                                         AIS.KINDACCSET KS
                                                                                   where BB.ISN        = B.BODYISN
                                                                                     and KS.KINDACCISN = BB.SUBKINDISN
                                                                                     and KS.CLASSISN   = RS.ANALITIKISN ))*/
            -- EGAO 27.10.2009 Глюки начались
            AND CASE
                  WHEN RS.ANALITIKISN(+) IS NULL THEN 1
                  ELSE
                    CASE RS.ANALITIKISN(+)
                      WHEN (SELECT KS.CLASSISN
                            FROM AIS.BUHBODY BB,
                                 AIS.KINDACCSET KS
                            WHERE BB.ISN        = B.BODYISN
                              AND KS.KINDACCISN = BB.SUBKINDISN
                              AND KS.CLASSISN   = RS.ANALITIKISN
                           ) THEN 1
                    END
                END=1


            group by B.AGRISN, MOTIVGROUPISN, nvl(RS.STATCODE, B.STATCODE), trunc(DATEVAL, 'mm'), B.DEPTISN,
                     ( case when B.DEPTISN not in (11414319, 742950000, 1112083803) and RPTGROUPISN in (755075000, 755078500) then 504 end ),
                     B.REISN, SAGROUP, GMISN, RA.RULEISN /*OD 07.04.2009 8940292103*/) S,
         REP_BUDGET_AGR_SALER ASS,
         STORAGES.REP_STATCODE ST,
         ( select distinct
                  STATCODE,
                  DESCRIPTION
             from REP_BUDGET_STATCODE ) ST1,
         V_BG_DEPT BDP
   where S.BUDGETGROUPISN = BDP.ISN(+)
     and S.STATCODE       = to_char(ST.STATCODE(+))
     and S.STATCODE       = ST1.STATCODE(+)
     and S.AGRISN         = ASS.AGRISN(+)
     and DATEVAL between nvl(ASS.SDATEBEG(+), '01-jan-1900')
     and nvl(ASS.SDATEEND(+),'01-jan-3000')
     and S.DEPTISN        = ASS.BUHDEPTISN(+));

/*(
Select
 ass.*
from tt_agr_salers  ass
Where ass.agrIsn>vMinIsn and ass.agrIsn<=vLMaxIsn
and (datebeg is null  and dateend is null or  dateend is null)
) Ass,*/


Insert into rep_budget_agrs
Select * from tt_budget_agrs;


insert into REP_BUDGET
select --+ ordered use_nl(rea)
       SALECHANEL,
       BUDGETGROUPISN,
       DATEVAL,
       T.CLIENTJURIDICAL,
       T.EMITISN,
       T.BEMITISN,
       T.AGENTCLASSISN,
       T.AGENTJURIDICAL,
       STATCODE,
       STATNAME,
       T.DEPTISN,
       AGRDEPTISN,
       T.BIZFLG,
       T.CALCBIZFLG,
       REINDEPT,
       AGRCLASSISN,
       sum(AMOUNTRUB),
       sum(AMOUNTUSD),
       BDPDEPTISN,
       SAGROUP,
       0 ISGOOD,
       REA.CLASSISN,
       SC.ISN,
       COISN,
       CPISN,
       max(GOSALERCLASSISN),
       T.GMISN,
       T.AGRRULEISN -- OD 07.04.2009 8940292103
  from TT_BUDGET_AGRS T,
       REPAGR REA,
       ( select *
           from DICTI
          where PARENTISN = 1366868203 ) SC
 where T.REISN           = REA.AGRISN(+)
   and upper(SALECHANEL) = SC.SHORTNAME(+)
 group by SALECHANEL, BUDGETGROUPISN, DATEVAL, T.CLIENTJURIDICAL, T.EMITISN, T.BEMITISN, T.AGENTCLASSISN, T.AGENTJURIDICAL,
          STATCODE, STATNAME, T.DEPTISN, AGRDEPTISN, T.BIZFLG, T.CALCBIZFLG, REINDEPT, AGRCLASSISN, BDPDEPTISN,
          SAGROUP, REA.CLASSISN, SC.ISN, COISN, CPISN, T.GMISN, T.AGRRULEISN /* OD 07.04.2009 8940292103*/;

Commit;
end;



procedure rep_motiv_dks_make
is
vSql Varchar2(32000);
vCnt number:=0;
vMin rowid:='0';
vMax rowid;
cLoadObjCnt number:=100000;
SesId number;
begin

execute immediate 'truncate table tt_motiv_quit_dks drop storage';
SesId:=PARALLEL_TASKS.createnewsession;
loop


select Max(rid)
into vMax
from
(Select
rowid rid

--from rep_motivation_quit z   sts 24.05.2012 - здесь и ниже: таблица заменена на партиционированный аналог - rep_motivation_quit_p
from rep_motivation_quit_p z

where  rowid>vMin
order by rowid) s
where rownum<=cLoadObjCnt;



exit when vMax is null;


vSql:=
'
begin
REPORT_BUDGET.rep_motiv_dks_insert('''||vMin||''' ,'||cLoadobjcnt||' );
end;';


PARALLEL_TASKS.processtask(sesid,vSql);
vCnt:=vCnt+1;
dbms_application_info.Set_Module('Process',vCnt*cLoadObjCnt);
vMin:=vMax;
end loop;
PARALLEL_TASKS.endsession(sesid);
end;

procedure rep_motiv_dks_insert(vMin rowid,cLoadobjcnt number)
is
begin

delete from tt_rowid;
 insert into tt_rowid(rid)
 select rid from
(Select
rowid rid
from rep_motivation_quit_p z  -- rep_motivation_quit   sts 24.05.2012
where  rowid>vMin
order by rowid) s
where rownum<=cLoadobjcnt;

insert into tt_motiv_quit_dks
select * from (
With M as
(Select--+ ordered use_nl(m)
 agrisn ,bodyisn,motivgroupisn budgetgroupisn,linecode,datequitm,buhdeptisn,
-- sum(amountrub)
 amountrub,reisn
from
  tt_rowid t,
  rep_motivation_quit_p m -- rep_motivation_quit   sts 24.05.2012
where datequitm between '01-jul-2006' and '30-sep-2006'
and buhdeptisn=11414319--(select c.get('Avtodept0') from dual)
and t.rid=m.rowid
--and rownum<0
--group by agrisn ,bodyisn,motivgroupisn ,linecode,datequitm,buhdeptisn,reisn
)


Select  --+ Ordered Use_Nl(m ag d sd1 emp esd fil emt dp sc d1 d2 dcs ) Index(m) Use_Hash(doc)
 dp.dept0name,dp.dept1name,dp.deptname,
 d.shortname Budgetgroup,
 sc.description LineDesc,
 ag.id AgrId,
 ag.bizflg,
 ag.clientjuridical,
  m.amountrub*Nvl(docPc,1) amountrub,
 datequitM,
 null SALERAND,null SALERA, null SALERF,
sd1.shortname Buhdeptisn,
emp.shortname Kurator,
esd.shortname KuratorDept,
fil.shortname Filial,
decode(ag.bizflg,null,Decode(ag.filisn,null,'Ц',null),ag.bizflg) CalcBizFlg,
emt.shortname Emitent,
reag.id ReAgrId,
ag.datebeg,
ag.dateend,
d2.shortname "Продукты с нов. ДСИ",
dcs.id "Номер счета"
from
 m,rep_statcode sc,
dicti d,subdept sd1, subject emp, subdept esd,
subdept fil,subdept emt,repagr ag,
v_bg_with_newdsi bgd,dicti d2,repagr reag,
(select --+ Index_Asc(b X_REPBUHBODY_bodyisn)
  bodyisn,nvl(docisn,docisn2) docisn,
sUM( Buhamount*nvl (DocSumPC,1)
      *decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount) --bpc,
     *decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit))--FactPc
     /Max(buhamount) docPc
     from repbuhbody b
     where bodyisn in (select bodyisn from M)
group by bodyisn,nvl(docisn,docisn2)
) doc, docs dcs,
tt_agr_salers dp
Where  m.agrisn=dp.AgrIsn(+)
And m.linecode =sc.statcode
And m.budgetgroupisn=d.isn(+)
and m.agrisn=ag.agrisn(+)
and m.buhdeptisn=sd1.isn(+)
and ag.emplisn=emp.isn(+)
and ag.deptisn=esd.isn(+)
and nvl(ag.bemitisn,ag.filisn)=fil.isn(+)
and nvl(ag.bemitisn,ag.emitisn)=emt.isn(+)
And trunc(amountrub,2)<>0
and reisn=reag.agrisn(+)
And datequitm between Nvl(dp.Datebeg(+),'01-jan-1900') and Nvl(dp.Dateend(+),'01-jan-3000')
and budgetgroupisn=bgd.Isn(+)
and Nvl(bgd.dsinewisn ,bgd.isn)=d2.isn
and m.bodyisn=doc.bodyisn(+)
and doc.docisn=dcs.isn(+));

commit;








end;



procedure Load_agrs_no_salers(pTaskIsn in number)
is
begin

delete from tt_RowId;
commit;


execute immediate 'truncate table rep_agr_not_salers drop storage';

/*снача грузим из мотивации*/
Insert Into tt_RowId(Isn)
 select --+ Index_FFS(r X_REP_MOTIVATION_QUIT_AGR)
  Distinct Agrisn
 from rep_motivation_quit_p r  -- rep_motivation_quit   sts 24.05.2012
 Where AgrIsn>0
 Minus
 select --+ Index_FFS(r )
  Distinct Agrisn
 from tt_agr_salers r
 Where AgrIsn>0;


Insert Into tt_RowId
Select '0',Agrisn
from
(
Select --+ Ordered Use_Nl(r)
 distinct Agrisn
from tt_RowId t,rep_motivation_quit_p r   -- rep_motivation_quit   sts 24.05.2012
 Where t.isn=r.AgrIsn
Group by Agrisn,DATEQUITM
having sum(AMOUNTRUB)<>0);

delete from tt_rowid Where rId is null;



/* грузим из дебиторки*/
Insert Into tt_RowId(Isn)
  select --+ Index_FFS(r )
  Distinct Agrisn
 from Rep_Buh_Debt_Bc r
 Where AgrIsn>0 and daterep>=add_months(trunc(sysdate,'yyyy'),-12)
 Minus
 select --+ Index_FFS(r )
  Distinct Agrisn
 from tt_agr_salers r
 Where AgrIsn>0
 Minus
 Select Isn from tt_rowId;

 Commit;

Insert into rep_agr_not_salers
(
Select --+ Ordered Use_Nl(ag d1 sd dp)
 ag.id,ag.datebeg,ag.dateend,ag.dept0Isn,ag.ruleisn,
 ag.emitisn,ag.agrisn,dp.filisn

 from tt_rowid t,repagr ag,rep_DEpt dp
 Where t.isn=ag.agrisn
 and ag.emitisn=dp.deptisn(+)
);

commit;



end;





procedure make_REP_BUDGET_AGR_SALER
Is
 vMinIsn Number:=-1;
 vlMaxIsn Number;
 vCnt Number:=0;


 SesId Number;
 vSql Varchar2(4000);
Begin



  SesId:=Parallel_Tasks.createnewsession;

execute immediate 'truncate table REP_BUDGET_AGR_SALER drop storage';

vMinIsn:=0;


loop

vLMaxIsn:=Cut_Table('storages.rep_buh2Agr','agrisn',vMinIsn);

 Exit When vLMaxIsn is null;



vSql:='
storages.report_budget.make_rep_b_agr_saler_by_isn('||vMinIsn ||','||vLMaxIsn ||');
';

 System.Parallel_Tasks.processtask(sesid,vsql);

vCnt:=vCnt+1;

vMinIsn:=vLMaxIsn;
DBMS_APPLICATION_INFO.set_module('Load REP_BUDGET_AGR_SALER','Loaded: '||vCnt*100000);

end loop;

-- ждем, пока завершатся все джобы
Parallel_Tasks.endsession(sesid);




End;


procedure make_REP_B_AGR_SALER_by_isn(pMinIsn number,pMaxIsn number)
is
vMaxIsn Number:=pMaxIsn ;
 vMinIsn Number:=pMinIsn;
Begin

Insert into storages.REP_BUDGET_AGR_SALER
Select --+ Ordered USe_Hash(b)
 a.*
from storages.v_budget_agr_saler a,(Select distinct agrisn,deptisn from storages.rep_buh2Agr where agrisn>vMinIsn and agrisn<=vMaxIsn) b
where a.agrisn>vMinIsn and a.agrisn<=vMaxIsn
and Buhdeptisn=b.deptisn
and a.agrisn=b.agrisn  ;
Commit;
End;


/*procedure Load_to_Ofa (pdatebeg date,pDateend Date)
is
begin

delete from rep_budget_to_ofa where dateval between pdatebeg and pDateend
and statcode <>'48';
commit;

Insert into rep_budget_to_ofa
(coisn, salechanel, salerchanelisn, cpisn,
       budgetgroupisn, clientjuridical, statcode, statname,
       dateval, gosalerclassisn,  deptisn, amountrub,
       amountusd,reinclassisn)
Select S.*


from(
Select --+ Index_Combine(rb)
       coisn, salechanel, salerchanelisn, cpisn,
       budgetgroupisn, clientjuridical, statcode, statname,
       dateval, gosalerclassisn, deptisn,Sum(amountrub) amountrub,
       Sum(amountusd) amountusd,rb.reinclassisn
from rep_budget rb
Where rb.dateval between pdatebeg and pDateend
and statcode <>'48'
group by
      coisn, salechanel, salerchanelisn, cpisn,
       budgetgroupisn, clientjuridical, statcode, statname,
       dateval, gosalerclassisn,  deptisn,rb.reinclassisn) s;
commit;



Insert into rep_budget_to_ofa
(coisn, salechanel, salerchanelisn, cpisn,
       budgetgroupisn, clientjuridical, statcode, statname,
       dateval, gosalerclassisn,  deptisn, amountrub,
       amountusd)
Select S.*

from(
Select --+ Ordered USe_Nl(ass sc)
       coisn, salechanel,sc.isn salerchanelisn, cpisn,
       budgetgroupisn, clientjuridical,'ДЕБ' statcode,'Дебиторско-кредиторская задолженность' statname,
       Daterep dateval, salerclassisn,  db.deptisn,amountrub,
       amountusd
From
(
select --+  Index_Combine(d)
Daterep,
D.AGRISN,
D.DeptIsn,
Sum(BuhAmountRub) AmountRub,
gcc2.gcc2(Sum(BuhAmountRub),35,53,daterep) AmountUsd,
budgetgroupisn
from Rep_Buh_Debt_Bc D
where daterep between pdatebeg and pDateend
and trunc(daterep+1,'q')-1=Daterep
Group by Daterep,
D.AGRISN,
D.DeptIsn,
budgetgroupisn
) db,REP_BUDGET_AGR_SALER ass,(Select * from dicti Where parentisn=1366868203) sc
Where
 Db.agrisn=ass.agrisn(+)
And Daterep between Nvl(ass.SDatebeg(+),'01-jan-1900') and Nvl(ass.SDateend(+),'01-jan-3000')

and db.deptisn=ass.buhdeptisn(+)
and Upper(salechanel)=sc.shortname(+)

) s;
commit;


Insert into rep_budget_to_ofa
(coisn, salechanel, salerchanelisn, cpisn,
       budgetgroupisn, clientjuridical, statcode, statname,
       dateval, gosalerclassisn,  deptisn, amountrub,
       amountusd,sumtype)
Select S.*
from(
Select --+ Ordered USe_Nl(ass sc)
       coisn, salechanel,sc.isn salerchanelisn, cpisn,
       budgetgroupisn, clientjuridical,Nvl(CT.Code,'ВНК') statcode,Nvl(ct.shortName,'Внешняя комиссия') statname,
       Daterep dateval, salerclassisn,  db.deptisn,amountrub,
       amountusd,REPTYPEISN
From
(
select --+  Index_Combine(d)
Daterep,
D.AGRISN,
D.BUHDEPTISN Deptisn,
Sum(AMOUNTRUB) AmountRub,
gcc2.gcc2(Sum(AMOUNTRUB),35,53,daterep) AmountUsd,
budgetgroupisn,
COMTYPE,
REPTYPEISN

from rep_fil_com D
where daterep between pdatebeg and pDateend
and (BUHDEPTISN  not in (select Isn from subdept start with isn=507 connect by prior isn=parentisn) or REPNAME='Внешняя Комиссия')
Group by Daterep,
D.AGRISN,
D.BUHDEPTISN,
budgetgroupisn,
COMTYPE,REPTYPEISN
) db,REP_BUDGET_AGR_SALER ass,(Select * from dicti Where parentisn=1366868203) sc,dicti ct
Where
 Db.agrisn=ass.agrisn(+)
And Daterep between Nvl(ass.SDatebeg(+),'01-jan-1900') and Nvl(ass.SDateend(+),'01-jan-3000')

and db.deptisn=ass.buhdeptisn(+)
and Upper(salechanel)=sc.shortname(+)
and COMTYPE=ct.Isn(+)

--Having Sum(BuhAmountRub)>0
) s;
commit;


Insert into rep_budget_to_ofa
(coisn, salechanel, salerchanelisn, cpisn,
       budgetgroupisn, clientjuridical, statcode, statname,
       dateval, gosalerclassisn,  deptisn, amountrub,
       amountusd,sumtype)
Select S.*
from(
Select --+ Ordered USe_Nl(ass sc)
       coisn, salechanel,sc.isn salerchanelisn, cpisn,
       budgetgroupisn, clientjuridical,db.statcode, st.DESCRIPTION statname,
       DATEQUITM dateval, salerclassisn,  db.deptisn,amountrub,
       amountusd,2
From
(
select --+  Index_Combine(d)
DATEQUITM,
D.AGRISN,
D.BUHDEPTISN Deptisn,
Sum(AMOUNTRUB) AmountRub,
Sum(AMOUNTUSD) AmountUsd,
motivgroupisn budgetgroupisn,
linecode statcode
from rep_motivation_quit D
where DATEQUITM between pdatebeg and pDateend
and linecode<>48
Group by DATEQUITM,
D.AGRISN,
D.BUHDEPTISN,
motivgroupisn,
linecode
) db,REP_BUDGET_AGR_SALER ass,(Select * from dicti Where parentisn=1366868203) sc,rep_statcode st
Where
 Db.agrisn=ass.agrisn(+)
And DATEQUITM between Nvl(ass.SDatebeg(+),'01-jan-1900') and Nvl(ass.SDateend(+),'01-jan-3000')

and db.deptisn=ass.buhdeptisn(+)
and Upper(salechanel)=sc.shortname(+)
and db.statcode=st.statcode

--Having Sum(BuhAmountRub)>0
) s;
commit;







 update rep_budget_to_ofa s
 set
 (coisnofa, conameofa)=(Select
                        coOfa.classisn1 coisnofa,
                        (Select Shortname from dicti Where isn=coOfa.classisn2) conameofa
                       from (select * from dicx where classisn=1368028103) coOfa
                       where s.coisn=coOfa.classisn1),
 (salerchanelisnofa,salechanelofa)=
                       (Select
                        scOfa.classisn1 salerchanelisnofa,
                        (Select Shortname from dicti Where isn=scOfa.classisn2) salechanelofa
                       from (select * from dicx where classisn=1366879103) scOfa
                       where s.salerchanelisn=scofa.classisn1),
 (cpisnofa, cpnameofa)=
                       (Select
                       cpOfa.classisn1 cpisnofa,
                       (Select Shortname from dicti Where isn=cpOfa.classisn2) cpnameofa
                        from (select * from dicx where classisn=1367733203) cpOfa
                       where s.cpisn=cpOfa.classisn1),
 (budgetgroupisnofa,budgetgroupnameofa)=
                       (Select
                       prodOfa.classisn1 budgetgroupisnofa,
                       (Select Shortname from dicti Where isn=prodOfa.classisn2) budgetgroupnameofa
                        from (select * from dicx where classisn=1366782203) prodOfa
                       where s.budgetgroupisn=prodOfa.classisn1)
 where dateval between pdatebeg and pDateend;

 commit;

end;

*/

procedure tt_agr_salers_make_By_Isns (pMinIsn number,pMaxIsn number,pLoadisn Number:=0)
is
  --vtab TTabAgrSalers;
  --vTabLine Storages.TTAGRSALERSLINE;
  vMinIsn Number:= pMinIsn;
  vlMaxIsn number:=pMaxIsn;
  vMinDate date :='01-jan-1900';
  vMaxDate date :='01-jan-3900';
  TYPE TTabNumber IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
  TYPE TTabString IS TABLE OF VARCHAR2(4000) INDEX BY BINARY_INTEGER;
  TYPE TTabDate IS TABLE OF DATE INDEX BY BINARY_INTEGER;

  TabLOADISN   TTabNumber;
  TabAGRISN    TTabNumber;
  TabDEPTISN   TTabNumber;
  TabDEPTSECTISN   TTabNumber;
  TabSALERGO   TTabString;
  TabSALERF    TTabString;
  TabSALERNAME TTabString;
  TabDEPTNAME  TTabString;
  TabDEPT0NAME TTabString;
  TabDEPT1NAME TTabString;
  TabSALERCLASSISN TTabNumber;
  TabDATEBEG  TTabDATE;
  TabDATEEND  TTabDATE;
  TabSALERISN TTabNumber;
  TabAGRSALERCLASSISN TTabNumber;
  TabDEPT0ISN TTabNumber;
  TabDEPT1ISN TTabNumber;
  TabOISN     TTabNumber;
  TabONAME    TTabString;
  TabDOISN    TTabNumber;
  TabDONAME   TTabString;

  Tabsalergoisn                     TTabNumber;
  Tabsalergoclassisn                TTabNumber;
  Tabsalergodept                    TTabNumber;
  Tabsalergodept0isn                TTabNumber;
  Tabsalercrgoisn                   TTabNumber;
  Tabsalercrclassisn                TTabNumber;
  Tabsalercrgodept                  TTabNumber;
  Tabsalercrgodept0isn              TTabNumber;
  Tabsalerfisn                      TTabNumber;
  Tabsalerfclassisn                 TTabNumber;
  Tabsalerfdept                     TTabNumber;
  Tabsalerfdept0isn                 TTabNumber;



begin
  -- sts 16.01.2013 - таблица storages.tt_agr_salers теперь не поддерживается.
  -- под этим именем теперь существует вьюха, заточенная на новую таблицу STORAGES.REP_AGR_SALERS
  -- Поэтому этот загрузчик теперь не нужен. Вставил в первой строке выход из процедуры
  -- Удалять код пока не стал...
  RETURN;


  --MSerp 04.05.2008


/*  KGS 27.09.11 */
-- пример сабжа с кучей пересекающихся мотивационных групп: SubjISN = 760137100


  INSERT INTO storages.tt_agr_salers

  SELECT    pLoadIsn,
            a.agrisn, a.deptisn, a.salera, a.salerf, a.salername, a.DEPTBNAME, a.dept0name,
            a.dept1name, a.valn, a.datebeg, a.dateend, a.subjisn, a.AgrSClass, a.dept0isn, a.dept1isn,
            a.oisn, a.oname, a.doisn, a.doname /*, a.deptsectisn*/



  /*  KGS 27.09.11
  BULK COLLECT INTO
  TabLOADISN,
  TabAGRISN,
  TabDEPTISN,
  TabSALERGO,
  TabSALERF,
  TabSALERNAME,
  TabDEPTNAME,
  TabDEPT0NAME,
  TabDEPT1NAME,
  TabSALERCLASSISN,
  TabDATEBEG,
  TabDATEEND,
  TabSALERISN,
  TabAGRSALERCLASSISN,
  TabDEPT0ISN,
  TabDEPT1ISN,
  TabOISN,
  TabONAME,
  TabDOISN,
  TabDONAME
  */
  FROM (SELECT S.agrisn, S.deptisn, S.salera, S.salerf, S.salername, DEPTBNAME,
               dept0name, dept1name,
               max(oa.valn) KEEP (dense_rank FIRST
                                  ORDER BY CASE
                                             WHEN oa.datebeg IS NOT NULL AND oa.dateend IS NOT NULL THEN 2
                                             ELSE CASE
                                                    WHEN oa.datebeg IS NOT NULL THEN 1
                                                    ELSE CASE
                                                           WHEN oa.dateend IS NOT NULL THEN 1
                                                           ELSE 0
                                                         END
                                                  END
                                           END DESC,
                                           NVL(oa.datebeg, vMinDate) DESC,
                                           NVL(oa.dateend, vMaxDate) DESC
                                 ) AS valn,
               decode(dt,vMinDate,to_date(null),dt)   AS datebeg,
               decode(dte,vMaxDate,to_date(null),dte) AS dateend,
               s.subjisn, s.AgrSClass, s.dept0isn, s.dept1isn,
               s.oisn, s.oname, s.doisn, s.doname
       FROM
       (SELECT a.*,
               first_value(dt) over(partition by agrisn,subjisn, agrsclass order by dt range between 1 following and unbounded following  )-1 dte
        FROM (SELECT DISTINCT  --+ordered use_nl(s oa)
                     s.*,
                    decode(n.n, 1, nvl(s.datebeg, vMinDate),
                                2, nvl(s.dateend, vMaxDate)+1,
                                3, nvl(trunc(oa.datebeg), vMinDate),
                                4, nvl(trunc(oa.dateend), vMaxDate)+1,sysdate+null
                          ) dt
              FROM
              --<Это старый код>
                  (SELECT --+ Use_Nl(s d)
                           S.agrisn, S.deptisn,
                           S.salera,
                           S.salerf,
                           S.salername, DEPTBNAME, dept0name, dept1name,
                           --salerclassisn,
                           datebeg,
                           dateend,
                           subjisn,AgrSClass,dept0isn,dept1isn,
                           d.oisn, d.oname, d.doisn, d.doname
                   FROM (SELECT  --+ Ordered use_Nl(agrs ag sb sd su ar dd) index(sb)
                                DISTINCT
                                Ar.Agrisn,
                                SD.ISN  /*CASE WHEN sD.SHORTNAME LIKE  'СЕКТОР%' THEN SD.pARENTiSN ELSE SD.ISN END*/ DeptIsn, -- OD 01.10.10
                                null SALERAND,
                                   /*
                                   dECODE(salerandrisn,NULL,'N','Y') SALERAND,
                                */
                                --    Case When salerGOisn is  NULL  Then    'N' else 'Y' end SALERA,

                                --    Case When salerfisn is  NULL  Then    'N' else 'Y' end SALERF,
                                Max(decode(dd.code,'SALES_G','Y','N')) over (partition by agrisn) SALERA,
                                Max(decode(dd.code,'SALES_F','Y','N')) over (partition by agrisn) SALERF,
                                Su.ShortName SalerNAme,
                                trunc(ar.datebeg)datebeg,
                                trunc(ar.dateend)dateend,
                                su.isn subjisn,
                                ar.classisn AgrSClass
                         FROM (-- САМОЕ ВАЖНОЕ МЕСТО - ВЫДЕЛЯЕМ 1-го продавца по отделу на период действия.
                               -- В принципе, этого всего можно не делать, если данные в базе АБСОЛЮТНО правильные. ;-)
                               SELECT --+ Ordered use_Nl( a ar ) use_hash(SLR sd) index(ar X_AGRROLE_AGR ) index ( a X_REPAGR_AGR )
                                      DISTINCT First_Value(ar.isn) over (PARTITION BY Ar.Agrisn, trunc(ar.datebeg), trunc(ar.dateend),
                                      SD.ISN -- OD 29.10.2010 /*CASE WHEN sD.SHORTNAME LIKE  'СЕКТОР%' THEN SD.pARENTiSN ELSE SD.ISN END*/
                                       ORDER BY (CASE WHEN ar.Classisn in (1738885603,1738885903,1738886903) then 1 else 0 end ) desc,
                                         decode(ar.classisn,1738886903,1,0),-- кросспродавец имеет преимущество перед ГО, когда они вместе
                                         ar.UPDATED) arisn
                               FROM  repagr a, --EGAO 02.03.2010
                                     agrrole ar,
                                     ( select D.ISN, D.CODE from DICTI D where D.CODE in ('SALES_G', 'SALES_F') ) SLR,
                                     subdept sd
                               WHERE a.agrisn>vMinIsn and ar.agrisn<=vlMaxIsn
                                 AND ar.agrisn=a.agrisn
                                 /* sts 20.07.2012 - old
                                 and ar.classisn  in (1381235003,1398248103,1398248703,1419689703,1419690103,1738885603,
                                                      1738885903,1738886903)
                                 */
                                 and ar.classisn = SLR.ISN
                                 and ar.deptisn is not null
                                 and ar.DeptIsn=sd.isn
                              ) s,
                              agrrole ar,
                              subject su,
                              subdept sd,
                              dicti dd
                         WHERE s.arisn=ar.isn
                           and ar.subjisn=su.isn
                           --       And ar.agrisn=ag.agrisn
                           and  ar.DeptIsn=sd.isn
                           and ar.classisn=dd.isn
                        ) s, storages.rep_dept d
                   Where s.deptisn=d.deptisn
                  )
              --<Это был старый код>
                   S ,
                   ais.obj_attrib oa,
                   (select rownum n from dicti where rownum <=4) n
              where oa.objisn(+)=s.subjisn
                and oa.classisn(+)=1428587803
             )a
       ) s,
       ais.obj_attrib oa
  WHERE dt+ (dte-dt)/2 between nvl(trunc(s.datebeg),vMinDate) and nvl(trunc(s.dateend),vMaxDate)
   AND oa.objisn(+)=s.subjisn
   AND oa.classisn(+)=1428587803
   AND s.dt BETWEEN nvl(trunc(oa.datebeg(+)), vMinDate)  AND nvl(trunc(oa.dateend(+)), vMaxDate)
  GROUP BY S.agrisn, S.deptisn, S.salera, S.salerf, S.salername, s.DEPTBNAME,
        s.dept0name, s.dept1name,decode(dt,vMinDate,to_date(null),dt),
        decode(dte,vMaxDate,to_date(null),dte), s.subjisn, s.AgrSClass, s.dept0isn, s.dept1isn,
        s.oisn, s.oname, s.doisn, s.doname/*, s.deptsectisn*/
   ) a;

/*  KGS 27.09.11
  FORALL i IN TabLoadisn.first..TabLoadIsn.last
  INSERT INTO storages.tt_agr_salers
  VALUES(TabLOADISN(i),
  TabAGRISN(i),
  TabDEPTISN(i),
  TabSALERGO(i),
  TabSALERF(i),
  TabSALERNAME(i),
  TabDEPTNAME(i),
  TabDEPT0NAME(i),
  TabDEPT1NAME(i),
  TabSALERCLASSISN(i),
  TabDATEBEG(i),
  TabDATEEND(i),
  TabSALERISN(i),
  TabAGRSALERCLASSISN(i),
  TabDEPT0ISN(i),
  TabDEPT1ISN(i),
  TabOISN(i),
  TabONAME(i),
  TabDOISN(i),
  TabDONAME(i));

*/
/*  KGS 27.09.11*/
Insert into storages.tt_agr_salersb

Select AgrIsn,
Max(decode(agrsalerclassisn ,1738885603,salerisn)) salergo ,
Max(decode(agrsalerclassisn ,1738885603,deptisn)) ,
Max(decode(agrsalerclassisn ,1738885603,dept0isn)) ,

Max(decode(agrsalerclassisn ,1738886903,salerisn))salercrgo ,
Max(decode(agrsalerclassisn ,1738886903,deptisn)) ,
Max(decode(agrsalerclassisn ,1738886903,dept0isn)) ,

Max(decode(agrsalerclassisn ,1738885903,salerisn))salerf ,
Max(decode(agrsalerclassisn ,1738885903,deptisn) ),
Max(decode(agrsalerclassisn ,1738885903,dept0isn))
from Storages.tt_agr_salers
where agrisn>vMinIsn and agrisn<=vlMaxIsn
and dateend is null
group by agrisn;



Insert into tt_agr_salers_Line
select
    agrisn,
    datebeg  ,
    dateend  ,
    salergoisn ,
    salergoclassisn ,
    salergodept ,
    salergodept0isn ,

    salercrgoisn,
    salercrclassisn ,
    salercrgodept   ,
    salercrgodept0isn ,
    salerfisn,
    salerfclassisn  ,
    salerfdept  ,
    salerfdept0isn
/*  KGS 27.09.11
BULK COLLECT INTO
  Tabagrisn,
  Tabdatebeg,
  Tabdateend,
  Tabsalergoisn,
  Tabsalergoclassisn,
  Tabsalergodept,
  Tabsalergodept0isn,
  Tabsalercrgoisn,
  Tabsalercrclassisn,
  Tabsalercrgodept,
  Tabsalercrgodept0isn,
  Tabsalerfisn,
  Tabsalerfclassisn,
  Tabsalerfdept,
  Tabsalerfdept0isn
  */
    from (
Select --+ Ordered Use_Nl(d1)
Per.Agrisn,Per.db datebeg,Per.De dateend,
Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',salerisn))) keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0)) salergoIsn ,
Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',salerclassisn)))  keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0)) salergoclassisn,
Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',deptisn)))  keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0)) salergodept,
Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',dept0isn)))  keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0))  salergodept0isn,

Max(decode(agrsalerclassisn ,1738886903,salerisn))salercrgoisn  ,
Max(decode(agrsalerclassisn ,1738886903,salerclassisn))salercrclassisn,
Max(decode(agrsalerclassisn ,1738886903,deptisn)) salercrgodept,
Max(decode(agrsalerclassisn ,1738886903,dept0isn)) salercrgodept0isn,

Max(decode(d1.code ,'SALES_F',salerisn))salerfisn ,
Max(decode(d1.code ,'SALES_F',salerclassisn)) salerfclassisn,
Max(decode(d1.code ,'SALES_F',deptisn)) salerfdept,
Max(decode(d1.code ,'SALES_F',dept0isn)) salerfdept0isn

from

 (
with P as
(

Select Distinct Agrisn,Nvl(datebeg,'01-jan-1900') datebeg,Nvl(dateend,'31-dec-3000') dateend
from tt_agr_salers ar
where ar.agrisn>vMinIsn and ar.agrisn<=vlMaxIsn

)
select
*
from
(
select d db, lag(d-1) over(partition by Agrisn order by d desc) de,Agrisn from
(
select distinct agrisn,datebeg  d from P
union
select distinct agrisn,dateend+1 d from P
)X
)X
where de is not null
--and de-db >1

) Per,tt_agr_salers ar,dicti d1
where Per.agrisn=ar.agrisn
and (Per.db between Nvl(datebeg,'01-jan-1900')  and  Nvl(dateend,'31-dec-3000')
or Nvl(datebeg,'01-jan-1900') between Per.db and Per.de)
and ar.AGRSALERCLASSISN=d1.Isn(+)
group by Per.Agrisn,Per.db,Per.De
)
;
/*  KGS 27.09.11
FORALL i IN Tabagrisn.first..Tabagrisn.Last
Insert into tt_agr_salers_Line
VALUES (
  Tabagrisn(i),
  Tabdatebeg(i),
  Tabdateend(i),
  Tabsalergoisn(i),
  Tabsalergoclassisn(i),
  Tabsalergodept(i),
  Tabsalergodept0isn(i),
  Tabsalercrgoisn(i),
  Tabsalercrclassisn(i),
  Tabsalercrgodept(i),
  Tabsalercrgodept0isn(i),
  Tabsalerfisn(i),
  Tabsalerfclassisn(i),
  Tabsalerfdept(i),
  Tabsalerfdept0isn(i));

*/
commit;


end;

procedure RepLoadRefundTotalLoss(pTaskIsn in number)  --EGAO 15.12.2008
is
begin
  LoadRefundTotalLoss(add_months(trunc(sysdate,'yyyy'),-12*4), Trunc(sysdate) );
end;

PROCEDURE LoadRefundTotalLoss(pDateBeg Date, pDateEnd Date )  --EGAO 15.12.2008
IS
  vMinIsn Number:=-1;
  vlMaxIsn Number;
  vCnt Number:=0;
  vLoadObjCnt Number:=100000;

  vSql Varchar2(4000);
  pSesId number;
Begin

  execute immediate 'truncate table tt_rep_refund_totalloss';

  pSesId:=Parallel_Tasks.createnewsession;

  Loop

  /*
      select Max (CLAIMisn)
      into vLMaxIsn
      From
       (Select  --+ Index_Asc (a X_REPREFUND_CLAIM)
          CLAIMisn
        from reprefund a
         where CLAIMisn > vMinIsn
         and rownum <= vLoadObjCnt);
   */

   vLMaxIsn := system.cut_Table('STORAGE_SOURCE.reprefund','ClaimISN', vMinIsn, null, vLoadObjCnt);
   Exit When vLMaxIsn is null;

  vSql:='
  Declare
    vMinIsn number :='||vMinIsn||';
    vMaxIsn number :='||vLMaxIsn||';
    vCnt    number :='||vCnt||';
    vDatebeg date := TO_DATE('''||TO_CHAR(pDatebeg,'DD.MM.YYYY')||''',''DD.MM.YYYY'');
    vDAteend date := TO_DATE('''||TO_CHAR(pDAteend,'DD.MM.YYYY')||''',''DD.MM.YYYY'');

  Begin
    DBMS_APPLICATION_INFO.set_module(''LoadRefundTotalLoss'',''Thread: ''||vCnt);
    storages.REPORT_BUDGET.LoadRefundTotalLossByIsns(vMinIsn, vMaxIsn, vDatebeg, vDAteend);
    Commit;
  end;';

   Parallel_Tasks.processtask(psesid,vsql);

  vCnt:=vCnt+1;

  vMinIsn:=vLMaxIsn;
  DBMS_APPLICATION_INFO.set_module('LoadRefundTotalLoss','Process: '||vCnt*vLoadObjCnt);

  end loop;
  Parallel_Tasks.endsession(psesid);


  DBMS_APPLICATION_INFO.set_module('','');
end;

procedure LoadRefundTotalLossByIsns (pMinIsn Number,pMaxIsn number,pDatebeg date, pDateend date) --EGAO 15.12.2008
is
Begin
  SHARED_SYSTEM.pParam.SetParamD('pDateBeg', pDatebeg);
  SHARED_SYSTEM.pParam.SetParamD('pDateEnd', pDateend);

  delete from tt_rowid;

  insert into tt_rowid(ISN)
  select
  distinct
    ClaimISN
  from
    STORAGE_SOURCE.reprefund rrf
  where rrf.claimisn>pMinIsn AND rrf.claimisn<=pMaxIsn;


  INSERT INTO tt_rep_refund_totalloss
  (
   refundisn,claimid,emplisn,deptisn,refunddept,monthtotalloss,datetotalloss,
   monthsolution,datesolution,solutionperiod,buhdeptisn,status,claimsumrub,
   refundsumrub,claimsumusd,refundsumusd,
   expirelimitations,
   SOLUTIONPERIOD#2,
   budgetgroupisn,dateloss, claimisn
  )
  select V.* from V_tt_rep_refund_totalloss V;

  /* 19.04.2013 - переписал на вьюху
  SELECT
         a.refundisn, a.claimid, a.emplisn, a.deptisn, a.refunddept,a.monthtotalloss,a.datetotalloss,
         a.monthsolution,a.datesolution,a.solutionperiod,a.BUHDEPTISN, a.status, a.claimsumrub,
         a.refundsumrub,a.claimsumusd, a.refundsumusd,a.expirelimitations,
         CASE WHEN a.expirelimitations=1 THEN trunc(a.datesolution)-trunc(a.datetotalloss) END AS SOLUTIONPERIOD#2,
         a.budgetgroupisn, a.dateloss, a.claimisn


  FROM (
        SELECT --+ ordered use_nl ( rrf cl sd2 sd3 )
             rrf.refundisn, rrf.claimisn, cl.id AS claimid, cl.emplisn, cl.deptisn,
             CASE
               WHEN  SD2.SHORTNAME LIKE 'СЕКТОР%'
                 THEN nvl(SD3.ABBREVIATION, SD3.SHORTNAME)
               ELSE nvl(SD2.ABBREVIATION, SD2.SHORTNAME)
             END refunddept,
             TRUNC(rrf.claimdatetotalloss,'MM') AS monthtotalloss,
             trunc(rrf.claimdatetotalloss) AS datetotalloss,
             TRUNC(rrf.datesolution,'MM') AS monthsolution,
             trunc(rrf.datesolution) AS datesolution,
             CASE rrf.status
               WHEN 'R' THEN trunc(rrf.datesolution)
               WHEN 'S' THEN trunc(rrf.datesolution)
               WHEN 'Y' THEN trunc(rrf.datesolution)
               ELSE TO_DATE(NULL)
             END - trunc(rrf.claimdatetotalloss)  AS solutionperiod,
             BUHDEPTISN, rrf.status, rrf.claimsumrub, rrf.refundsumrub,
             rrf.claimsumusd, rrf.refundsumusd, budgetgroupisn, rrf.dateloss,
             CASE
               WHEN rrf.status='Y' AND
                    nvl(rrf.refundsumusd,0)=0 AND
                    rrf.budgetgroupisn=2366826703 AND
                    (trunc(rrf.datesolution)-trunc(rrf.dateloss))>=365*2 THEN 0
               ELSE 1
             END AS expirelimitations
        FROM (SELECT --+ ordered use_nl ( rrf ag) index ( rrf X_REPREFUND_CLAIM )
                     rrf.refundisn,
                     rrf.claimisn,
                     (SELECT --+ index ( t X_QUEUE_OBJ )
                             MIN(t.datesend) KEEP (dense_rank FIRST ORDER BY CASE t.classisn WHEN 1647725903 THEN 1 WHEN 2226668703 THEN 2 END)
                      FROM ais.Queue t
                      WHERE t.objisn=rrf.claimisn
                        AND (t.classisn=1647725903 OR (t.classisn=2226668703 AND t.replyisn=2237388003))
                     ) AS claimdatetotalloss, --EGAO 21.11.2012
                     rrf.daterefund AS datesolution,
                     rrf.conddeptisn AS buhdeptisn,
                     rrf.status,
                     gcc2.gcc2(sum(rrf.claimsumusd), 53, 35,max(nvl(nvl(rrf.rdateval, rrf.dateloss), rrf.dateevent))) claimsumrub,
                     gcc2.gcc2(sum(rrf.refundsumusd), 53, 35,max(nvl(nvl(rrf.rdateval,rrf.dateloss),rrf.dateevent))) refundsumrub,
                     SUM(rrf.claimsumusd)  AS claimsumusd,
                     SUM(rrf.refundsumusd) AS refundsumusd,
                     max(trunc(rrf.dateloss)) AS dateloss, -- EGAO 13.11.2012 в рамках ДИТ-12-4-176886
                     max(rrf.budgetgroupisn) AS  budgetgroupisn-- EGAO 13.11.2012 в рамка ДИТ-12-4-176886
              FROM reprefund rrf,
                   repagr ag
              WHERE rrf.claimisn>pMinIsn AND rrf.claimisn<=pMaxIsn -- гидра
                AND rrf.totalloss='Y'
                AND rrf.agrisn    = ag.agrisn(+)
                AND ag.classisn(+) NOT IN (8746, 35146816)
                AND ( rrf.dateclaim       BETWEEN  pDatebeg  AND pDateEnd
                      or rrf.DATEREG      BETWEEN  pDatebeg  AND pDAteEnd
                      or rrf.datesolution BETWEEN  pDatebeg  AND pDAteEnd
                      or rrf.daterefund   BETWEEN  pDatebeg  AND pDAteEnd
                    )
                    -- по настоянию дмитриева выкидываем сюйверы ДКС
                    AND ( rrf.conddeptisn    <> 11414319 OR rrf.classisn <> 24959616 )
                    and NVL(rrf.nrzu, 'N') = 'N'
              GROUP BY rrf.refundisn, rrf.claimisn, rrf.claimdatetotalloss, rrf.daterefund,
                       rrf.conddeptisn, rrf.status,
                       nvl(rrf.salerdeptisn,rrf.deptisn),
                       nvl(rrf.saleremplisn, rrf.emplisn)

             ) rrf,
             agrclaim cl,
             ais.subdept sd2,
             ais.subdept sd3
        WHERE cl.isn(+) = rrf.claimisn
          AND cl.deptisn  = sd2.isn(+) -- AND rrf.refdeptisn  = sd2.isn(+)
          AND sd2.parentisn = sd3.isn(+)
       ) a;
   */
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


Procedure tt_agr_salers_make_test
 Is
 vMinIsn Number:=-1;
 vlMaxIsn Number;
 vCnt Number:=0;
 vLoadObjCnt Number:=100000;

 vSql Varchar2(32000);
 SesId number;
 vLoadIsn Number;
  Begin

  -- sts 16.01.2013 - а это ваще ХЗ чего за процедура. Может я для тестирования зафигачил, а может и не я...
  -- один фиг - таблица storages.tt_agr_salers теперь не поддерживается (см. коммент к пр-ре tt_agr_salers_make),
  -- а потому убрал...
  RETURN;


/*
Select Max(Loadisn) into vLoadIsn from tt_agr_salers  Where rownum<=1;

Select Nvl(Max(Loadisn),0)
into vCnt
From repagr ag
where  rownum<=1;

If vLoadIsn=vCnt then vCnt:=1; else vCnt:=0; end if;

 If vCnt=0 then
*/
Select  Max(Loadisn) into vLoadisn From repagr where  rownum<=1;
execute Immediate 'truncate table Storages.tt_agr_salers_test';
execute Immediate 'truncate table Storages.tt_agr_salersb_test';
execute Immediate 'truncate table Storages.tt_agr_salers_Line_test';

SesId:=Parallel_Tasks.createnewsession('AgrSalersTest');

Loop
vLMaxIsn:=cut_table('storage_source.repagr','agrisn',vMinIsn);
 Exit When vLMaxIsn is null;

vSql:='
Declare
  vLoadIsn number := '||TO_CHAR(vLoadIsn)||';
  vMinIsn  number := '||TO_CHAR(vMinIsn)||';
  vlMaxIsn number := '||TO_CHAR(vlMaxIsn)||';
  vCnt     number := '||TO_CHAR(vCnt)||';
Begin
DBMS_APPLICATION_INFO.set_module(''tt_agr_salers_test'',''Thread: ''||TO_CHAR(vCnt));
storages.report_Budget.tt_agr_salers_make_By_Isns_test(vMinIsn,vlMaxIsn,vLoadIsn);
end;';
System.Parallel_Tasks.processtask(sesid,vsql);

--storages.report_Budget.tt_agr_salers_make_By_Isns(vMinIsn,vlMaxIsn,vLoadIsn);
vCnt:=vCnt+1;

vMinIsn:=vLMaxIsn;
DBMS_APPLICATION_INFO.set_module('tt_agr_salers_test','Updated: '||vCnt*vLoadObjCnt);

end loop;
--end if;
-- ждем, пока завершатся все джобы
Parallel_Tasks.endsession(sesid);
end;


procedure tt_agr_salers_make_By_Isns_tst (pMinIsn number,pMaxIsn number,pLoadisn Number:=0)
is
  --vtab TTabAgrSalers;
  --vTabLine Storages.TTAGRSALERSLINE;
  vMinIsn Number:= pMinIsn;
  vlMaxIsn number:=pMaxIsn;
  vMinDate date :='01-jan-1900';
  vMaxDate date :='01-jan-3900';
  TYPE TTabNumber IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
  TYPE TTabString IS TABLE OF VARCHAR2(4000) INDEX BY BINARY_INTEGER;
  TYPE TTabDate IS TABLE OF DATE INDEX BY BINARY_INTEGER;

  TabLOADISN   TTabNumber;
  TabAGRISN    TTabNumber;
  TabDEPTISN   TTabNumber;
  TabDEPTSECTISN   TTabNumber;
  TabSALERGO   TTabString;
  TabSALERF    TTabString;
  TabSALERNAME TTabString;
  TabDEPTNAME  TTabString;
  TabDEPT0NAME TTabString;
  TabDEPT1NAME TTabString;
  TabSALERCLASSISN TTabNumber;
  TabDATEBEG  TTabDATE;
  TabDATEEND  TTabDATE;
  TabSALERISN TTabNumber;
  TabAGRSALERCLASSISN TTabNumber;
  TabDEPT0ISN TTabNumber;
  TabDEPT1ISN TTabNumber;
  TabOISN     TTabNumber;
  TabONAME    TTabString;
  TabDOISN    TTabNumber;
  TabDONAME   TTabString;

  Tabsalergoisn                     TTabNumber;
  Tabsalergoclassisn                TTabNumber;
  Tabsalergodept                    TTabNumber;
  Tabsalergodept0isn                TTabNumber;
  Tabsalercrgoisn                   TTabNumber;
  Tabsalercrclassisn                TTabNumber;
  Tabsalercrgodept                  TTabNumber;
  Tabsalercrgodept0isn              TTabNumber;
  Tabsalerfisn                      TTabNumber;
  Tabsalerfclassisn                 TTabNumber;
  Tabsalerfdept                     TTabNumber;
  Tabsalerfdept0isn                 TTabNumber;



begin
  -- sts 16.01.2013 - а это ваще ХЗ чего за процедура. Может я для тестирования зафигачил, а может и не я...
  -- один фиг - таблица storages.tt_agr_salers теперь не поддерживается (см. коммент к пр-ре tt_agr_salers_make),
  -- а потому убрал...
  RETURN;

  --MSerp 04.05.2008
  SELECT    pLoadIsn,
            a.agrisn, a.deptisn, a.salera, a.salerf, a.salername, a.DEPTBNAME, a.dept0name,
            a.dept1name, a.valn, a.datebeg, a.dateend, a.subjisn, a.AgrSClass, a.dept0isn, a.dept1isn,
            a.oisn, a.oname, a.doisn, a.doname/*, a.deptsectisn*/

  BULK COLLECT INTO
  TabLOADISN,
  TabAGRISN,
  TabDEPTISN,
  TabSALERGO,
  TabSALERF,
  TabSALERNAME,
  TabDEPTNAME,
  TabDEPT0NAME,
  TabDEPT1NAME,
  TabSALERCLASSISN,
  TabDATEBEG,
  TabDATEEND,
  TabSALERISN,
  TabAGRSALERCLASSISN,
  TabDEPT0ISN,
  TabDEPT1ISN,
  TabOISN,
  TabONAME,
  TabDOISN,
  TabDONAME
  FROM (SELECT S.agrisn, S.deptisn, S.salera, S.salerf, S.salername, DEPTBNAME,
               dept0name, dept1name,
               max(oa.valn) KEEP (dense_rank FIRST
                                  ORDER BY CASE
                                             WHEN oa.datebeg IS NOT NULL AND oa.dateend IS NOT NULL THEN 2
                                             ELSE CASE
                                                    WHEN oa.datebeg IS NOT NULL THEN 1
                                                    ELSE CASE
                                                           WHEN oa.dateend IS NOT NULL THEN 1
                                                           ELSE 0
                                                         END
                                                  END
                                           END DESC,
                                           NVL(oa.datebeg, vMinDate) DESC,
                                           NVL(oa.dateend, vMaxDate) DESC
                                 ) AS valn,
               decode(dt,vMinDate,to_date(null),dt)   AS datebeg,
               decode(dte,vMaxDate,to_date(null),dte) AS dateend,
               s.subjisn, s.AgrSClass, s.dept0isn, s.dept1isn,
               s.oisn, s.oname, s.doisn, s.doname
       FROM
       (SELECT a.*,
               first_value(dt) over(partition by agrisn,subjisn, agrsclass order by dt range between 1 following and unbounded following  )-1 dte
        FROM (SELECT DISTINCT  --+ordered use_nl(s oa)
                     s.*,
                    decode(n.n, 1, nvl(s.datebeg, vMinDate),
                                2, nvl(s.dateend, vMaxDate)+1,
                                3, nvl(trunc(oa.datebeg), vMinDate),
                                4, nvl(trunc(oa.dateend), vMaxDate)+1,sysdate+null
                          ) dt
              FROM
              --<Это старый код>
                  (SELECT --+ Use_Nl(s d)
                           S.agrisn, S.deptisn,
                           S.salera,
                           S.salerf,
                           S.salername, DEPTBNAME, dept0name, dept1name,
                           --salerclassisn,
                           datebeg,
                           dateend,
                           subjisn,AgrSClass,dept0isn,dept1isn,
                           d.oisn, d.oname, d.doisn, d.doname
                   FROM (SELECT  --+ Ordered use_Nl(agrs ag sb sd su ar dd) index(sb)
                                DISTINCT
                                Ar.Agrisn,
                                SD.ISN  /*CASE WHEN sD.SHORTNAME LIKE  'СЕКТОР%' THEN SD.pARENTiSN ELSE SD.ISN END*/ DeptIsn, -- OD 01.10.10
                                null SALERAND,
                                   /*
                                   dECODE(salerandrisn,NULL,'N','Y') SALERAND,
                                */
                                --    Case When salerGOisn is  NULL  Then    'N' else 'Y' end SALERA,

                                --    Case When salerfisn is  NULL  Then    'N' else 'Y' end SALERF,
                                Max(decode(dd.code,'SALES_G','Y','N')) over (partition by agrisn) SALERA,
                                Max(decode(dd.code,'SALES_F','Y','N')) over (partition by agrisn) SALERF,
                                Su.ShortName SalerNAme,
                                trunc(ar.datebeg)datebeg,
                                trunc(ar.dateend)dateend,
                                su.isn subjisn,
                                ar.classisn AgrSClass
                         FROM (-- САМОЕ ВАЖНОЕ МЕСТО - ВЫДЕЛЯЕМ 1-го продавца по отделу на период действия.
                               -- В принципе, этого всего можно не делать, если данные в базе АБСОЛЮТНО правильные. ;-)
                               SELECT --+ Ordered use_Nl( a ar sd ) index(ar X_AGRROLE_AGR ) index ( a X_REPAGR_AGR )
                                      DISTINCT First_Value(ar.isn) over (PARTITION BY Ar.Agrisn, trunc(ar.datebeg), trunc(ar.dateend),
                                      SD.ISN -- OD 29.10.2010 /*CASE WHEN sD.SHORTNAME LIKE  'СЕКТОР%' THEN SD.pARENTiSN ELSE SD.ISN END*/
                                       ORDER BY (CASE WHEN ar.Classisn in (1738885603,1738885903,1738886903) then 1 else 0 end ) desc,
                                         decode(ar.classisn,1738886903,1,0),-- кросспродавец имеет преимущество перед ГО, когда они вместе
                                         ar.UPDATED) arisn
                               FROM  repagr a, --EGAO 02.03.2010
                                     agrrole ar,subdept sd
                               WHERE a.agrisn>vMinIsn and ar.agrisn<=vlMaxIsn
                                 AND ar.agrisn=a.agrisn
                                 and ar.classisn  in (1381235003,1398248103,1398248703,1419689703,1419690103,1738885603,
                                                      1738885903,1738886903)
                                 and ar.deptisn is not null
                                 and  ar.DeptIsn=sd.isn
                              ) s,
                              agrrole ar,
                              subject su,
                              subdept sd,
                              dicti dd
                         WHERE s.arisn=ar.isn
                           and ar.subjisn=su.isn
                           --       And ar.agrisn=ag.agrisn
                           and  ar.DeptIsn=sd.isn
                           and ar.classisn=dd.isn
                        ) s, storages.rep_dept d
                   Where s.deptisn=d.deptisn
                  )
              --<Это был старый код>
                   S ,
                   ais.obj_attrib oa,
                   (select rownum n from dicti where rownum <=4) n
              where oa.objisn(+)=s.subjisn
                and oa.classisn(+)=1428587803
             )a
       ) s,
       ais.obj_attrib oa
  WHERE dt+ (dte-dt)/2 between nvl(trunc(s.datebeg),vMinDate) and nvl(trunc(s.dateend),vMaxDate)
   AND oa.objisn(+)=s.subjisn
   AND oa.classisn(+)=1428587803
   AND s.dt BETWEEN nvl(trunc(oa.datebeg(+)), vMinDate)  AND nvl(trunc(oa.dateend(+)), vMaxDate)
  GROUP BY S.agrisn, S.deptisn, S.salera, S.salerf, S.salername, s.DEPTBNAME,
        s.dept0name, s.dept1name,decode(dt,vMinDate,to_date(null),dt),
        decode(dte,vMaxDate,to_date(null),dte), s.subjisn, s.AgrSClass, s.dept0isn, s.dept1isn,
        s.oisn, s.oname, s.doisn, s.doname/*, s.deptsectisn*/
   ) a;


  FORALL i IN TabLoadisn.first..TabLoadIsn.last
  INSERT INTO storages.tt_agr_salers_tst
  VALUES(TabLOADISN(i),
  TabAGRISN(i),
  TabDEPTISN(i),
  TabSALERGO(i),
  TabSALERF(i),
  TabSALERNAME(i),
  TabDEPTNAME(i),
  TabDEPT0NAME(i),
  TabDEPT1NAME(i),
  TabSALERCLASSISN(i),
  TabDATEBEG(i),
  TabDATEEND(i),
  TabSALERISN(i),
  TabAGRSALERCLASSISN(i),
  TabDEPT0ISN(i),
  TabDEPT1ISN(i),
  TabOISN(i),
  TabONAME(i),
  TabDOISN(i),
  TabDONAME(i));



Insert into storages.tt_agr_salersb_tst
Select AgrIsn,
Max(decode(agrsalerclassisn ,1738885603,salerisn)) salergo ,
Max(decode(agrsalerclassisn ,1738885603,deptisn)) ,
Max(decode(agrsalerclassisn ,1738885603,dept0isn)) ,

Max(decode(agrsalerclassisn ,1738886903,salerisn))salercrgo ,
Max(decode(agrsalerclassisn ,1738886903,deptisn)) ,
Max(decode(agrsalerclassisn ,1738886903,dept0isn)) ,

Max(decode(agrsalerclassisn ,1738885903,salerisn))salerf ,
Max(decode(agrsalerclassisn ,1738885903,deptisn) ),
Max(decode(agrsalerclassisn ,1738885903,dept0isn))
from Storages.tt_agr_salers
where agrisn>vMinIsn and agrisn<=vlMaxIsn
and dateend is null
group by agrisn;




select
    agrisn,
    datebeg  ,
    dateend  ,
    salergoisn ,
    salergoclassisn ,
    salergodept ,
    salergodept0isn ,

    salercrgoisn,
    salercrclassisn ,
    salercrgodept   ,
    salercrgodept0isn ,
    salerfisn,
    salerfclassisn  ,
    salerfdept  ,
    salerfdept0isn
BULK COLLECT INTO
  Tabagrisn,
  Tabdatebeg,
  Tabdateend,
  Tabsalergoisn,
  Tabsalergoclassisn,
  Tabsalergodept,
  Tabsalergodept0isn,
  Tabsalercrgoisn,
  Tabsalercrclassisn,
  Tabsalercrgodept,
  Tabsalercrgodept0isn,
  Tabsalerfisn,
  Tabsalerfclassisn,
  Tabsalerfdept,
  Tabsalerfdept0isn
    from (
Select --+ Ordered Use_Nl(d1)
Per.Agrisn,Per.db datebeg,Per.De dateend,
Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',salerisn))) keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0)) salergoIsn ,
Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',salerclassisn)))  keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0)) salergoclassisn,
Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',deptisn)))  keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0)) salergodept,
Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',dept0isn)))  keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0))  salergodept0isn,

Max(decode(agrsalerclassisn ,1738886903,salerisn))salercrgoisn  ,
Max(decode(agrsalerclassisn ,1738886903,salerclassisn))salercrclassisn,
Max(decode(agrsalerclassisn ,1738886903,deptisn)) salercrgodept,
Max(decode(agrsalerclassisn ,1738886903,dept0isn)) salercrgodept0isn,

Max(decode(d1.code ,'SALES_F',salerisn))salerfisn ,
Max(decode(d1.code ,'SALES_F',salerclassisn)) salerfclassisn,
Max(decode(d1.code ,'SALES_F',deptisn)) salerfdept,
Max(decode(d1.code ,'SALES_F',dept0isn)) salerfdept0isn

from

 (
with P as
(

Select Distinct Agrisn,Nvl(datebeg,'01-jan-1900') datebeg,Nvl(dateend,'31-dec-3000') dateend
from tt_agr_salers ar
where ar.agrisn>vMinIsn and ar.agrisn<=vlMaxIsn

)
select
*
from
(
select d db, lag(d-1) over(partition by Agrisn order by d desc) de,Agrisn from
(
select distinct agrisn,datebeg  d from P
union
select distinct agrisn,dateend+1 d from P
)X
)X
where de is not null
--and de-db >1

) Per,tt_agr_salers ar,dicti d1
where Per.agrisn=ar.agrisn
and (Per.db between Nvl(datebeg,'01-jan-1900')  and  Nvl(dateend,'31-dec-3000')
or Nvl(datebeg,'01-jan-1900') between Per.db and Per.de)
and ar.AGRSALERCLASSISN=d1.Isn(+)
group by Per.Agrisn,Per.db,Per.De
)
;

FORALL i IN Tabagrisn.first..Tabagrisn.Last
Insert into tt_agr_salers_Line_tst
VALUES (
  Tabagrisn(i),
  Tabdatebeg(i),
  Tabdateend(i),
  Tabsalergoisn(i),
  Tabsalergoclassisn(i),
  Tabsalergodept(i),
  Tabsalergodept0isn(i),
  Tabsalercrgoisn(i),
  Tabsalercrclassisn(i),
  Tabsalercrgodept(i),
  Tabsalercrgodept0isn(i),
  Tabsalerfisn(i),
  Tabsalerfclassisn(i),
  Tabsalerfdept(i),
  Tabsalerfdept0isn(i));


commit;
end;


END;
