CREATE OR REPLACE PACKAGE "STORAGES"."REP_STAT" 
  IS



/* единственно, что тут живое - full_REPBUHSUMMARY -
отчет "премия, комиссия, убытки" , выгружается вместе с резервами из Storage_Admin

upd: sts 14.09.2011 - еще живое - St_RepBuhSummary_Load
*/


procedure full_REPBUHSUMMARY
(pLoadIsn number:=0, pDateRep IN DATE := SYSDATE);


procedure set_buhdate_to_repagr
(pLoadIsn number:=0);


procedure full_REPCONDSUMMARY
(pLoadIsn number:=0);

procedure add_turist_to_condsumm;

procedure full_REPCONDSUMMARY_AGR;

procedure St_RepBuhSummary_Load (
  pDateFrom date, -- дата начала периода (включительно)
  pDateTo date    -- дата окончания периода (включительно)
);

END; -- Package spec

CREATE OR REPLACE PACKAGE BODY "STORAGES"."REP_STAT" 
IS


cObjCnt Number:=100000;
cNullDate Date:=To_Date('01013000','DDMMYYYY');


procedure Ins_REPSUM2COND_By_TT;



procedure full_REPBUHSUMMARY
(pLoadIsn number:=0, pDateRep IN DATE := SYSDATE)
 Is
  vMinIsn Number;
  vMaxIsn Number;
  vLMaxIsn Number;
  vCnt Number;


  SesId Number;
  vSql VarChar2(4000);


  vDb date:='01-jan-2002';
 Begin


 execute immediate 'Truncate table REPBUHSUMMARY  ';
 execute immediate 'Truncate table REPSUM2AGR  ';
 execute immediate 'Truncate table repbuhmeasure  ';

store_and_drop_table_index('REPSUM2AGR',1);
store_and_drop_table_index('REPBUHSUMMARY',1);

--SesId:=Parallel_Tasks.createnewsession(pMaxJobCnt => 1);

   vMinIsn:=-1;
   vCnt:=0;




/*

  Loop
/*
  -- ОПРЕДЕЛИЛИ ГРАНИЦЫ ОКНА ПО RepBuh2Cond
        Select  Max(BodyIsn)
        Into vLMaxIsn
        From
         (
              Select --+ Index_Asc (a X_REPBUH2COND_BodyIsn)
               BODYISN
              From RepBuh2Cond a
              Where BODYISN>vMinIsn
              And RowNum<=cObjCnt
         );

   Exit When vLMaxIsn is Null;

/
   -- АГРЕГИРУЕМ ПО ОКНУ И ПИШЕМ РЕЗУЛЬТАТ В REPSUM2AGR

  Exit When  vDb>pDateRep/*EGAO 28.09.2012 sysdate/ ;

DBMS_APPLICATION_INFO.set_module('FULL REPSUM2AGR ','Applied : '||vDb);

execute immediate 'truncate table TT_REPSUM2AGR';

 vSql:='Begin



 DBMS_APPLICATION_INFO.set_module('' REPSUM2AGR '','''||vDb||'-'||add_months(vDb,1)||''');
          Insert  Into TT_REPSUM2AGR
          (
          Select Seq_Reports.NEXTVAL,null,S.*
          From
          (
               Select  --+ Full(a) Parallel(a,32)
                   /*Deptisn/
                   ( case
                      when A.DEPTISN in (511, 519, 520, 505, 507, 508, 742950000, 2024845003)
                       then A.DEPTISN
                      else DEPT0ISN
                     end ) DEPT0ISN, -- OD 17.02.09  8512966003
                   StatCode,Subaccisn,rptgroupisn,
                   '''||vDb||''' dateval ,
                   trunc(nvl(datebeg,'''||cNullDate||'''),''MM'') datebeg,null,Nvl(REPRDEPTISN,0),
                   Nvl(OBJREGION,0),Nvl(BIZFLG,0),Nvl(agrisn,0),bodyisn,
                   Nvl(Sum(AMOUNTRUB),0) amount,Decode(A.DeptIsn,23735116/*медики/,0,Nvl(CondIsn,0)),
                   Max(RefundIsn),SAGROUP
               From RepBuh2Cond a, REP_DEPT B
               where A.DEPTISN = B.DEPTISN(+)
                 and dateval >= '''||vDb||''' and dateval<'''||add_months(vDb,1)||'''
              Group by ( case
                          when A.DEPTISN in (511, 519, 520, 505, 507, 508, 742950000, 2024845003)
                           then A.DEPTISN
                          else DEPT0ISN
                         end )/*Deptisn  -- OD 17.02.09 8512966003/,StatCode,Subaccisn,rptgroupisn,trunc(nvl(datebeg,'''||cNullDate||'''),''MM''),Nvl(REPRDEPTISN,0),
                   Nvl(OBJREGION,0),Nvl(BIZFLG,0),Nvl(agrisn,0),bodyisn,Decode(A.DeptIsn,23735116/*медики/,0,Nvl(CondIsn,0)),SAGROUP
          )S)  ;


Insert  into REPBUHSUMMARY a
(a.isn, a.deptisn, a.statcode, a.subaccisn, a.rptgroupisn,
       a.dateval, a.buhamount, a.reprdeptisn, a.bizflg)
Select Seq_Reports.NEXTVAL,S.*
from(
Select --+ Full(a) Parallel(a,32) Oredered
 DeptIsn,
 StatCode,Subaccisn,a.rptgroupisn,'''||vDb||''' dateval,Sum(BuhAMOUNT) amount,REPRDEPTISN,a.BIZFLG
From TT_REPSUM2AGR a
Where  statcode in (Select statcode from rep_statcode where grp in (''Входящее перестрахование'',''Прямое страхование''))
Group by Deptisn ,StatCode,Subaccisn,rptgroupisn,REPRDEPTISN,a.BIZFLG)s ;


Insert into REPSUM2AGR (select  /*+ Full(a) Parallel(a,32) / * from tt_REPSUM2AGR A);

        Commit;
       end;';
 -- Parallel_Tasks.processtask(sesid,vsql);

EXECUTE IMMEDIATE vSql;
vDb:=add_months(vDb,1);



end loop;

-- ~45-55 мин

Parallel_Tasks.endsession(sesid);



restore_table_index('REPSUM2AGR');
restore_table_index('REPBUHSUMMARY');



/*

vMinIsn:=0;
vCnt:=0;
  Loop



  -- ОПРЕДЕЛИЛИ ГРАНИЦЫ ОКНА ПО REPSUM2AGR
       Select Max(Isn)
       Into vLMaxIsn
       From
       (
              Select --+ Index_Asc (a X_REPSUM2AGR)
               Isn
              From REPSUM2AGR a
              Where Isn>=vMinIsn
              And RowNum<=cObjCnt
       );

    Exit When vLMaxIsn Is Null;

           Merge --+ Index (d X_REPBUHSUMMARY_F5)
            into REPBUHSUMMARY D
           Using
            (Select --+ Index (a X_REPSUM2AGR)
              Deptisn,StatCode,Subaccisn,rptgroupisn,dateval,datebeg,REPRDEPTISN,OBJREGION,BIZFLG,Sum(BUHAMOUNT) amount
             From REPSUM2AGR    a
             Where Isn Between vMinIsn And vLMaxIsn and sagroup=1
              Group by Deptisn,StatCode,Subaccisn,rptgroupisn,dateval,datebeg,REPRDEPTISN,OBJREGION,BIZFLG
             )S
             On
              (
               D.deptisn=S.deptisn
           AND D.statcode=S.statcode
           AND D.subaccisn=S.subaccisn
           AND D.rptgroupisn=S.rptgroupisn
           AND D.dateval=S.dateval
           AND D.datebeg=S.datebeg
           AND D.REPRDEPTISN=S.REPRDEPTISN
           AND D.OBJREGION=S.OBJREGION
           AND D.BIZFLG =S.BIZFLG)

        WHEN MATCHED THEN UPDATE SET D.buhamount = D.buhamount + S.amount
        WHEN NOT MATCHED THEN INSERT (isn,  deptisn, statcode, subaccisn, rptgroupisn, dateval,datebeg,REPRDEPTISN,OBJREGION,BIZFLG,buhamount)
             Values
             (Seq_Reports.NEXTVAL, S.deptisn, S.statcode, S.subaccisn, S.rptgroupisn, S.dateval,S.datebeg,S.REPRDEPTISN,S.OBJREGION,S.BIZFLG,S.amount);
    commit;
vMinIsn:=vLMaxIsn+1;
vCnt:=vCnt+1;
DBMS_APPLICATION_INFO.set_module('FULL_REPBUHSUMMARY','REPBUHSUMMARY Applied : '||vCnt*cObjCnt);
end loop;
*/

--EXPORT_DATA.export_to_owb_by_FLD('repbuhsummary','Isn');
--EXPORT_DATA.export_to_owb_by_FLD('repsum2agr','AgrIsn');


/*

--заполнили repbuhmeasure
Insert Into repbuhmeasure (Isn,deptisn, rptgroupisn, reprdeptisn, objregion, bizflg)

 (Select Seq_Reports.NEXTVAL,s.*
  From
  (
  SELECT Distinct  deptisn, rptgroupisn, reprdeptisn, objregion, bizflg
  FROM repbuhsummary
  )S);
commit;


 Update
  repbuhsummary a
 Set
  MEASUREISN= (Select Isn
               from repbuhmeasure b
               Where B.deptisn=A.deptisn
               And B.rptgroupisn=A.rptgroupisn
               And B.reprdeptisn=A.reprdeptisn
               And B.objregion=A.objregion
               And B.bizflg=A.bizflg);
 commit;



-- ~ 35 мин

SesId:=Parallel_Tasks.createnewsession;

vCnt:=0;
vMinIsn:=0;
  Loop



  -- ОПРЕДЕЛИЛИ ГРАНИЦЫ ОКНА ПО REPSUM2AGR
       Select  Max(Isn)
       Into vLMaxIsn
       From
       (
              Select --+ Index_Asc (a X_REPSUM2AGR)
               Isn
              From REPSUM2AGR a
              Where Isn>=vMinIsn
              And RowNum<=cObjCnt
       );
    Exit When vLMaxIsn Is Null;

vSql:=' Begin
 Update
   REPSUM2AGR b
  Set
     SUMISN=(Select --+ Index (a X_REPBUHSUMMARY_F5)
                  Isn
             From REPBUHSUMMARY A
             where
                 A.deptisn=B.deptisn
             AND A.statcode=B.statcode
             AND A.subaccisn=B.subaccisn
             AND A.rptgroupisn=B.rptgroupisn
             AND A.dateval=B.dateval
             AND A.datebeg=B.datebeg
             AND A.REPRDEPTISN=B.REPRDEPTISN
             AND A.OBJREGION=B.OBJREGION
             AND A.BIZFLG =B.BIZFLG)
  Where isn between '||vMinIsn||' and '||vLMaxIsn||';
Commit;
end;';
Parallel_Tasks.processtask(sesid,vSql);
vCnt:=vCnt+1;
DBMS_APPLICATION_INFO.set_module('FULL_REPBUHSUMMARY','UPD REPSUM2AGR : '||vCnt*cObjCnt);
 vMinIsn:=vLMaxIsn+1;
end loop;

Parallel_Tasks.endsession(sesid);
/*
 Update
  REPBUHSUMMARY A
 Set
  AgrCnt=   (Select Cnt
             From
                (Select SumIsn,Count(*) cnt
                 From
               (
                 Select SumIsn,AgrIsn,Sum(BuhAmount)
                 from REPSUM2AGR
                 group by SumIsn,AgrIsn
                 Having Sum(BuhAmount)<>0
                )
                group by SumIsn)
             Where SumIsn=a.isn)   ;
commit;
*/

  -- загрузка таблицы ST_REPBUHSUMMARY начиная с начала года по прошлый месяц от текущей даты
  vDb := add_months(trunc(pDateRep/*EGAO 28.09.2012 sysdate*/, 'mm'), -1);
  St_RepBuhSummary_Load(trunc(vDb, 'y'), vDb);
end;



procedure set_buhdate_to_repagr
(pLoadIsn number:=0)
Is
Type TTDate is Table of Date;

  Agrs TNesTable;
  TDate TTDate;
  cStepCnt number:=30000;
  vCnt number;


  vMinIsn Number;
  vMaxIsn Number;
  vLMaxIsn Number;



Begin


   Select Min(AgrIsn)-1e-30 into vMinIsn from REPSUM2AGR;
   Select Max(AgrIsn) into vMaxIsn from REPSUM2AGR;

execute immediate 'Truncate table TT_SUM_AGR';
vCnt:=0;
  Loop

   Exit When vMinIsn>=vMaxIsn;

  -- ОПРЕДЕЛИЛИ ГРАНИЦЫ ОКНА ПО REPSUM2AGR
        Select  Max(AgrIsn)
        Into vLMaxIsn
        From
         (
              Select --+ Index_Asc (a X_REPSUM2AGR_AGR)
               AgrIsn
              From REPSUM2AGR a
              Where AgrIsn>vMinIsn and AgrIsn<=vMaxIsn
              And RowNum<=cObjCnt*2
         );




Select --+ Index (b X_REPSUM2AGR_AGR) Use_Hash(a b)  Index (a X_REPBUHSUMMARY)
  b.agrisn,min(a.dateval)
bulk collect into Agrs,TDate
from REPBUHSUMMARY a,REPSUM2AGR b
Where B.AgrIsn>vMinIsn and B.AgrIsn<=vLMaxIsn
   And  a.isn=b.sumisn
Group by b.agrisn;


      ForAll i in Agrs.First..Agrs.Last

           Update
             RepAgr S
           Set
              S.DATEBUH=TDate(i)
             Where S.AgrIsn=Agrs(i);
   Commit;
vMinIsn:=vLMaxIsn;
vCnt:=vCnt+1;
DBMS_APPLICATION_INFO.set_module('FULL_REPBUHSUMMARY','UPD REPAGR : '||vCnt*cObjCnt*2);

 end loop;

end;





procedure full_REPCONDSUMMARY
(pLoadIsn number:=0)
 Is
  vMinIsn Number;
  vMaxIsn Number;
  vLMaxIsn Number;
  vCnt Number;


 Begin

 execute immediate 'Truncate table REPSUM2COND reuse storage';
 execute immediate 'Truncate table TT_SUM_AGR';

   Select Min(AgrIsn)-1e-30 into vMinIsn from REPSUM2AGR;
   Select Max(AgrIsn) into vMaxIsn from REPSUM2AGR;


vCnt:=0;



  Loop

   Exit When vMinIsn>=vMaxIsn;

  -- ОПРЕДЕЛИЛИ ГРАНИЦЫ ОКНА ПО REPSUM2AGR
        Select  Max(AgrIsn)
        Into vLMaxIsn
        From
         (
              Select --+ Index_Asc (a X_REPSUM2AGR_AGR)
               AgrIsn
              From REPSUM2AGR a
              Where AgrIsn>vMinIsn and AgrIsn<=vMaxIsn
              And RowNum<=cObjCnt
         );


        execute immediate 'Truncate table TT_REPCOND reuse storage';


       -- заполнили буфер
         Insert Into TT_REPCOND
         (Select --+ Index (b X_RepCond2_condisn)
           B.isn, B.condisn, datebeg, dateend, agrisn, addisn, addsign, objisn,
           parentobjisn, limitrub, agrdatebeg, agrdateend, newaddsign,
           quantity,measureisn,BuhDate,PREMRUB
          from
          (Select --+ Index (a X_REPSUM2AGR_AGR) Index (b X_repbuhsummary)   Use_hash(a b)
            b.measureisn,a.condisn,Min(a.dateval) BuhDate
           from  REPSUM2AGR a,repbuhsummary b
           Where  A.AgrIsn>vMinIsn and A.AgrIsn<=vLMaxIsn
            And   A.sumisn=b.isn
            And   A.CondIsn<>0
            Group By b.measureisn,a.condisn
           ) A, RepCond B
          Where  A.condisn=B.condisn

          );

        -- в отдельный список, где condisn=0
          Insert Into TT_SUM_AGR
          (Select --+ Index (a X_REPSUM2AGR_AGR) Index (b X_repbuhsummary) Use_hash(a b)
            b.measureisn,a.agrisn,Min(a.dateval) BuhDate
           from  REPSUM2AGR a,repbuhsummary b
           Where  A.AgrIsn>vMinIsn and A.AgrIsn<=vLMaxIsn
            And   A.sumisn=b.isn
            And   A.CondIsn=0
            And   A.AgrIsn<>0
          Group by b.measureisn,a.agrisn
           );


         Commit;
          vMinIsn:=vLMaxIsn;

   -- АГРЕГИРУЕМ ПО ОКНУ И ПИШЕМ РЕЗУЛЬТАТ В REPSUM2COND
       Ins_REPSUM2COND_By_TT;

vCnt:=vCnt+1;
DBMS_APPLICATION_INFO.set_module('full_REPCONDSUMMARY','REPSUM2COND AGREGATE: '||vCnt*cObjCnt);

end loop;

vCnt:=0;
execute immediate 'Truncate table TT_REPCOND reuse storage';

-- теперь идем по договорам, где condisn=0
  For cur in (select RowNum,A.* from TT_SUM_AGR A ) loop
       -- заполнили буфер
         Insert Into TT_REPCOND
         (Select --+ Index (A X_RepCond2_Agr)
           isn, condisn, datebeg, dateend, agrisn, addisn, addsign, objisn,
           parentobjisn, limitrub, agrdatebeg, agrdateend, newaddsign,
           quantity,Cur.sumisn,Cur.BuhDate,PREMRUB
          from  RepCond A
          Where  A.AgrIsn=Cur.AgrIsn
          );
          vCnt:=vCnt+Sql%RowCount;
         Commit;


  If vCnt>=cObjCnt Then
   -- АГРЕГИРУЕМ ПО ОКНУ И ПИШЕМ РЕЗУЛЬТАТ В REPSUM2COND
         Ins_REPSUM2COND_By_TT;

         vCnt:=0;
         execute immediate 'Truncate table TT_REPCOND reuse storage';
 end if;

 DBMS_APPLICATION_INFO.set_module('full_REPCONDSUMMARY','CondIsn=0 Step: '||Cur.Rownum);
 end loop;

 -- и по остаткам в буфере
  Ins_REPSUM2COND_By_TT;
  execute immediate 'Truncate table TT_REPCOND reuse storage';

  add_turist_to_condsumm;

end;



procedure add_turist_to_condsumm
 Is
  vMinIsn Number;
  vMaxIsn Number;
  vLMaxIsn Number;
  vCnt Number;
  vTurGr0 number;
  vTurGr1 Number;


  Begin

--   set_buhdate_to_repagr;


   -- наполняем буфер
   Select Min(Isn) into vMinIsn from REPAGR;
   Select Max(Isn) into vMaxIsn from REPAGR;

execute immediate 'Truncate table TT_TUR_NO_PAY reuse storage';

vCnt:=0;
  Loop

   Exit When vMinIsn>=vMaxIsn;

  -- ОПРЕДЕЛИЛИ ГРАНИЦЫ ОКНА ПО REPAGR
        Select  Max(Isn)
        Into vLMaxIsn
        From
         (
              Select --+ Index_Asc (a X_REPAGR)
               Isn
              From REPAGR a
              Where Isn between vMinIsn and vMaxIsn
                And A.ruledept=707480016--c.Get('PrivDept')
                And A.Status='В'
                And RowNum<=cObjCnt
         );

       Insert Into TT_TUR_NO_PAY (Isn,agrisn, deptisn, repdeptisn, objregion, bizflg,RuleIsn)
       Select --+ Index (A_RepAgr ) Index (b X_REPCOND2_AGR)
        Seq_Reports.NEXTVAL, A.agrisn, A.ruledept, Nvl(A.FILISN,0), 0 /*!!!!*/, Nvl(A.bizflg,0),Nvl(b.RISKRULEISN,0)
       From RepAgr a,RepCond b
       Where A.Isn between vMinIsn and vLMaxIsn
        And  a.datebuh is null
        And A.ruledept=707480016--c.Get('PrivDept')
        And A.Status='В'
        And A.agrisn=b.agrisn;
     commit;

      vMinIsn:=vLMaxIsn+1;
 vCnt:=vCnt+1;
DBMS_APPLICATION_INFO.set_module('full_REPCONDSUMMARY','Turist Applied: '||vCnt*cObjCnt);
 end loop;

 -- проставляем учетную группу

  select Max(Nvl(groupisn,0))
  into vTurGr0
  from
   (select level lv, isn
    from ais.rule
    start with isn = 13310616
    connect by prior parentisn = isn
    order by lv) r,Rep_tt_rules2groups t
  where r.isn = t.ruleisn
   and nvl (t.param,0) =0
   And RowNum<=1;


  select Max(Nvl(groupisn,0))
  into vTurGr1
  from
   (select level lv, isn
    from ais.rule
    start with isn = 13310616
    connect by prior parentisn = isn
    order by lv) r, rep_tt_rules2groups t
  where r.isn = t.ruleisn
   and nvl (t.param,1) = 1
   And RowNum<=1;


-- туристы

vCnt:=0;
--Страхование туристов. Россия
 Loop
       Update --+ Index (a X_TT_TUR_NO_PAY_RULE_PRT)
        TT_TUR_NO_PAY a
       Set
        RPTGROUPISN=vTurGr1
       Where       RuleIsn = 13310616
               AND RPTGROUPISN=0
               AND Exists (Select AgrIsn From REP_AGRTUR b Where b.AGRISN=A.AgrIsn and isrussia=1 )
               AND RowNum<=cObjCnt;
         Exit When SQL%RowCount<=0;
          commit;
vCnt:=vCnt+1;
DBMS_APPLICATION_INFO.set_module('full_REPCONDSUMMARY','Turist Updt: '||vCnt*cObjCnt);
end loop;

--Страхование туристов. Не россия Россия (все остальные)
vCnt:=0;
Loop

Update  --+ Index (a X_TT_TUR_NO_PAY_RULE_PRT)
  TT_TUR_NO_PAY a
Set
 RPTGROUPISN=vTurGr0
Where  RuleIsn = 13310616
AND RPTGROUPISN=0
AND RowNum<=cObjCnt;
 Exit When SQL%RowCount<=0;
 commit;
vCnt:=vCnt+1;
DBMS_APPLICATION_INFO.set_module('full_REPCONDSUMMARY','Turist Updt: '||vCnt*cObjCnt);

end loop;

-- по правилу
execute immediate 'truncate table TT_RULE_RPNGRP reuse storage';

for cur in (Select Isn from ais.rule) loop

   select Max(Nvl(groupisn,0))
   into vTurGr0
   from (select level lv, isn
   from rule
   start with isn =Cur.Isn
   connect by prior parentisn = isn
   ) r, rep_tt_rules2groups t
  where r.isn = t.ruleisn
  And       param is null
  and  RowNum=1;

       If (vTurGr0>0) then
               Insert into TT_RULE_RPNGRP Values
               (Cur.ISN,vTurGr0,null);
       end if;

 end loop;
commit;

  For Cur In (Select groupisn,ruleisn
              from TT_RULE_RPNGRP b ) Loop


 Loop

       Update --+ Index (a X_TT_TUR_NO_PAY_RULE_PRT)
       TT_TUR_NO_PAY a
        Set
         rptgroupisn=Cur.groupisn
       Where RuleIsn=Cur.RuleIsn
       AND  rptgroupisn=0
       AND RowNum<=cObjCnt;
  Exit When Sql%RowCount=0;
 Commit;
end loop;

end loop;
 -- проставляем sumisn

   Select Min(Isn) into vMinIsn from TT_TUR_NO_PAY;
   Select Max(Isn) into vMaxIsn from TT_TUR_NO_PAY;
vCnt:=0;
  Loop

   Exit When vMinIsn>=vMaxIsn;

  -- ОПРЕДЕЛИЛИ ГРАНИЦЫ ОКНА ПО TT_TUR_NO_PAY
        Select  Max(Isn)
        Into vLMaxIsn
        From
         (
              Select --+ Index_Asc (a X_TT_TUR_NO_PAY)
               Isn
              From TT_TUR_NO_PAY a
              Where Isn between vMinIsn and vMaxIsn
              And RowNum<=cObjCnt
         );


Update  --+ Index (a X_TT_TUR_NO_PAY)
 TT_TUR_NO_PAY a
Set
 SumIsn=(Select Isn
         From repbuhmeasure b
         where A.deptisn=B.deptisn
          And A.rptgroupisn=B.rptgroupisn
          And A.REPDEPTISN=B.reprdeptisn
          And A.objregion=B.objregion
          And A.bizflg=B.bizflg )
 Where Isn Between vMinIsn and vLMaxIsn;
Commit;

-- а вдруг появились новые аналитики?
 Loop

 -- а есть ли?
    Select --+ Index (a X_TT_TUR_NO_PAY)
       Nvl(Max(Isn),0)
    Into vTurGr0
    from TT_TUR_NO_PAY
    Where Isn Between vMinIsn and vLMaxIsn
    And SumIsn Is Null
    ANd Rownum<=1;

   Exit When vTurGr0=0;

  -- если есть - заносим в measure

  Select Seq_Reports.NEXTVAL into vTurGr1 from dual;

          Insert Into repbuhmeasure
          (isn, deptisn, rptgroupisn, reprdeptisn, objregion, bizflg)
          Select vTurGr1, deptisn,rptgroupisn, repdeptisn, objregion, bizflg
          from TT_TUR_NO_PAY
          Where Isn=vTurGr0;
commit;

-- обновимся
Update  --+ Index (a X_TT_TUR_NO_PAY)
 TT_TUR_NO_PAY a
Set
 SumIsn=(Select Isn
         From repbuhmeasure b
         where A.deptisn=B.deptisn
          And A.rptgroupisn=B.rptgroupisn
          And A.REPDEPTISN=B.reprdeptisn
          And A.objregion=B.objregion
          And A.bizflg=B.bizflg )
 Where Isn Between vMinIsn and vLMaxIsn
  And SumIsn is null;
   commit;

end loop;

vMinIsn:=vLMaxIsn+1;
vCnt:=vCnt+1;
DBMS_APPLICATION_INFO.set_module('full_REPCONDSUMMARY','SetSum Updt: '||vCnt*cObjCnt);

end loop;

-- загоняем это дело


vCnt:=0;
execute immediate 'Truncate table TT_REPCOND reuse storage';

  For cur in (select Distinct A.sumisn,A.Agrisn from TT_TUR_NO_PAY A ) loop
       -- заполнили буфер
         Insert Into TT_REPCOND
         (Select --+ Index (A X_RepCond2_Agr)
           isn, condisn, datebeg, dateend, agrisn, addisn, addsign, objisn,
           parentobjisn, limitrub, agrdatebeg, agrdateend, newaddsign,
           quantity,Cur.Sumisn,null,PREMRUB
          from  RepCond A
          Where  A.AgrIsn=Cur.AgrIsn
          );
          vCnt:=vCnt+Sql%RowCount;
         Commit;
  If vCnt>=cObjCnt Then
         Ins_REPSUM2COND_By_TT;
         vCnt:=0;
         execute immediate 'Truncate table TT_REPCOND reuse storage';
 end if;

-- DBMS_APPLICATION_INFO.set_module('full_REPCONDSUMMARY','Turist Step: '||Cur.RowCount);
end loop;
 -- и по остаткам в буфере
 Ins_REPSUM2COND_By_TT;



end;



procedure Ins_REPSUM2COND_By_TT
Is
Begin
         Insert Into REPSUM2COND
         (
          Select Seq_Reports.NEXTVAL,condisn, datebeg, dateend, agrisn, addisn, addsign, objisn,
                 parentobjisn, limitrub, agrdatebeg, agrdateend, newaddsign,
                 quantity, sumisn, buhdate, premrub
          FROM tt_repcond
         );
      Commit;

end;


procedure full_REPCONDSUMMARY_AGR
 Is
 Type TTDate is Table of Date;
  TDate TTDate;
  vMinIsn Number;
  vMaxIsn Number;
  vLMaxIsn Number;
  vCnt Number;
Begin
Execute Immediate 'Truncate Table repcondsummary reuse storage';
--  Идем по sumisn
  For Cur In (Select * From repbuhmeasure) Loop

    delete from TT_CONDS;
  -- в буфер по одному sumisn
    Insert Into TT_CONDS
    (
    Select --+ Index(a X_repsum2cond_sum)
        SumIsn,Trunc(BuhDate,'Q') BuhDate, Trunc(Greatest(Greatest(Nvl(addsign,DATEBEG),Datebeg)),'MM') DB,
        Trunc(Least(Nvl(a.newaddsign,DATEEND),DATEEND),'MM') De,
        AgrIsn, Addisn,newaddsign,parentobjisn,OBJISN,Nvl(Quantity,0) Quantity,
        Nvl(LIMITRUB,0) LIMITRUB,Nvl(PREMRUB,0) PREMRUB
        from repsum2cond a
        Where A.sumIsn=Cur.Isn
     );
     vCnt:=Sql%RowCount;
   Commit;

If vCnt>0 Then

          If Cur.DeptIsn=509 /*AvtoDept*/ Then

 -- Те, которые начались
                Insert Into repcondsummary
                 (Select Seq_Reports.NEXTVAL,s.*
                  From
                 ( Select SumIsn,Db,BuhDate,0,Count(Distinct AgrIsn),
                   Sum(limitrub),Sum(premrub),Sum(Quantity)
                   From  TT_CONDS
                   Where AgrIsn=AddIsn
                   Group by  SumIsn,Db,BuhDate
                  )S);
                  Commit;
 -- Те, которые закончились
                Insert Into repcondsummary
                 (Select Seq_Reports.NEXTVAL,s.*
                  From
                 ( Select SumIsn,De,BuhDate,1,Count(Distinct AgrIsn),
                   Sum(limitrub),Sum(premrub),Sum(Quantity)
                   From  TT_CONDS
                   Where NewAddSign Is Null
                   Group By SumIsn,De,BuhDate
                  )S);
                Commit;
 -- Те, которые изменились
                Insert Into repcondsummary
                 (Select Seq_Reports.NEXTVAL,s.*
                  From
                 ( Select SumIsn,Db,BuhDate,2,Count(Distinct AgrIsn),
                   Sum(limitrub),Sum(premrub),Sum(Quantity)
                   From  TT_CONDS
                   Where AgrIsn<>AddIsn
                   Group by  SumIsn,Db,BuhDate
                  )S);

            Commit;
     ELSIF Cur.DeptIsn=707480016 /*PrivDept*/ Then

 -- Те, которые начались
                Insert Into repcondsummary
                 (Select Seq_Reports.NEXTVAL,s.*
                  From
                 ( Select SumIsn,Db,BuhDate,0,Count(Distinct AgrIsn),
                   Sum(limitrub),Sum(premrub),Count( Distinct Decode(Nvl(ParentObjIsn,0),0,ObjIsn,Null))
                   From  TT_CONDS
                   Where AgrIsn=AddIsn
                   Group by  SumIsn,Db,BuhDate
                  )S);
                Commit;
 -- Те, которые закончились
                Insert Into repcondsummary
                 (Select Seq_Reports.NEXTVAL,s.*
                  From
                 ( Select SumIsn,De,BuhDate,1,Count(Distinct AgrIsn),
                   Sum(limitrub),Sum(premrub),Count( Distinct Decode(Nvl(ParentObjIsn,0),0,ObjIsn,Null))
                   From  TT_CONDS
                   Where NewAddSign Is Null
                   Group By SumIsn,De,BuhDate
                  )S);
                 Commit;
 -- Те, которые изменились
                Insert Into repcondsummary
                 (Select Seq_Reports.NEXTVAL,s.*
                  From
                 ( Select SumIsn,Db,BuhDate,2,Count(Distinct AgrIsn),
                   Sum(limitrub),Sum(premrub),Count( Distinct Decode(Nvl(ParentObjIsn,0),0,ObjIsn,Null))
                   From  TT_CONDS
                   Where AgrIsn<>AddIsn
                   Group by  SumIsn,Db,BuhDate
                  )S);

            Commit;
     ELSE
 -- Те, которые начались
                Insert Into repcondsummary
                 (Select Seq_Reports.NEXTVAL,s.*
                  From
                 ( Select SumIsn,Db,BuhDate,0,Count(Distinct AgrIsn),
                   Sum(limitrub),Sum(premrub),Count( Distinct ObjIsn)
                   From  TT_CONDS
                   Where AgrIsn=AddIsn
                   Group by  SumIsn,Db,BuhDate
                  )S);
                 Commit;
 -- Те, которые закончились
                Insert Into repcondsummary
                 (Select Seq_Reports.NEXTVAL,s.*
                  From
                 ( Select SumIsn,De,BuhDate,1,Count(Distinct AgrIsn),
                   Sum(limitrub),Sum(premrub),Count( Distinct ObjIsn)
                   From  TT_CONDS
                   Where NewAddSign Is Null
                   Group By SumIsn,De,BuhDate
                  )S);
                Commit;
 -- Те, которые изменились
                Insert Into repcondsummary
                 (Select Seq_Reports.NEXTVAL,s.*
                  From
                 ( Select SumIsn,Db,BuhDate,2,Count(Distinct AgrIsn),
                   Sum(limitrub),Sum(premrub),Count( Distinct ObjIsn)
                   From  TT_CONDS
                   Where AgrIsn<>AddIsn
                   Group by  SumIsn,Db,BuhDate
                  )S);

            Commit;


 End if;

end if;


end loop;


end;

-- загрузка начинается с 01.01.2002
procedure St_RepBuhSummary_Load(
  pDateFrom date, -- дата начала периода (включительно)
  pDateTo date    -- дата окончания периода (включительно)
)
as
 vDateTo date := add_months(trunc(pDateTo, 'mm'), 1); -- первый день следующего месяца от pDateTo
 SesId number;
 vSql  varChar2(4000);
 vDb   date;
begin
 if pDateFrom > pDateTo then return; end if;

 delete from ST_REPBUHSUMMARY r where r.DateVal >= pDateFrom and r.DateVal < vDateTo;
 commit;

 SesId:=Parallel_Tasks.createnewsession;

 system.store_and_drop_table_index('STORAGES.ST_REPBUHSUMMARY', 0);

--1
 DBMS_APPLICATION_INFO.set_module('FULL REPSUM2AGR ch1', 'Applied : '||vDb);
 vSql:='
 begin
  DBMS_APPLICATION_INFO.set_module(''FULL REPSUM2AGR ch1'', ''' || pDateFrom || ' - ' || pDateTo || ''');
   --- Убытки из отчета "треугольник", также, как их грузят в РПНУ
  insert into ST_REPBUHSUMMARY (
    Select --+ ordered use_nl(r bb) use_hash(s4)  Parallel (r 10)
          r.statCode,
          nvl(r.rptgroupisn, 0) rptgroupisn,
          Trunc(r.dateval, ''MM'') Dateval,
          Null Qdatebeg,
          Null QdateEnd,
          Null QAddateEnd,
          trunc(Least(nvl(r.dateevent, r.dateval), r.Dateval), ''Q'') Qdaterefund,
          Sum(buhamountrub) amount,
          bb.subaccisn,
          s4.sagroup
     from
       storages.rep_ref_triangle r,
       ais.buhbody_t bb,
       subacc4dept s4
    where
      r.dateval >= to_date('''||pDateFrom||''') and r.dateval < to_date('''||vDateTo||''')
      and bb.isn(+) = r.bodyisn
      and s4.subaccisn(+) = bb.subaccisn
      and r.statcode in (220,24)
      and r.buhdeptisn <> 1002858925
      and nvl (r.rptgroupisn,0) not in (1162286003, 756169500)
    group by
      r.statCode,
      nvl(r.rptgroupisn, 0),
      trunc(r.dateval,''MM''),
      trunc(Least(nvl(r.dateevent, r.dateval), r.Dateval), ''Q''),
      bb.subaccisn,
      s4.sagroup);
  commit;
 end;';

 Parallel_Tasks.processtask(sesid, vsql);

 Parallel_Tasks.endsession(sesid);
 SesId:=Parallel_Tasks.createnewsession;

--2
 vDb:= pDateFrom;

 Loop
  Exit When vDb >= vDateTo;

  vSql:='
   Begin
    DBMS_APPLICATION_INFO.set_module(''FULL REPSUM2AGR ch2'', ''' || vDb || ''');

    Insert into ST_REPBUHSUMMARY
    ( Select --+ Ordered USe_Nl(ag ar ad ac)
             a.StatCode,
             nVL(a.rptgroupisn, 0) rptgroupisn,
             a.dateval,
             Trunc(ag.datebeg, ''Q'') Qdatebeg,
             Trunc(ag.dateEnd, ''Q'') QdateEnd,
             Trunc(ad.dateEnd, ''Q'') QAddateEnd,
             Trunc(trunc(greatest(nvl(dateclaim, datereg),
             nvl(dateevent, nvl(dateclaim, datereg)))), ''Q'') Qdaterefund,
             Sum(amountRub) amount,
             a.subaccisn,
             a.sagroup
        From ( Select --+ Index_combine (a) No_Parallel(a)
                      StatCode,
                      a.rptgroupisn,
                      Trunc(a.dateval, ''MM'') Dateval,
                      AgrIsn,
                      decode(statcode, 221,AddIsn, 241, AddIsn, null) AddIsn,
                      RefundIsn,
                      a.subaccisn,
                      a.sagroup,
                      Sum(AMOUNTRub) amountRub
                 From storages.repbuh2cond a
                Where a.dateval >= to_date('''||vDb||''') and a.dateval < to_date('''||add_months(vDb, 1)||''')
                  /* and sagroup=1  sts 10.08.2011 - убрал, т.к. поле sagroup теперь сохраняется в таблице */
                  and statcode in ( Select statcode
                                      from storages.rep_statcode
                                     where grp in (''Входящее перестрахование'',
                                                   ''Прямое страхование''))
                  and statcode not in (220,24)
                group by StatCode, a.rptgroupisn, AgrIsn,
                         decode(statcode, 221, AddIsn, 241, AddIsn, null), RefundIsn,
                         a.subaccisn, a.sagroup,
                         Trunc(a.dateval, ''MM'')
             ) a,
             agreement ag,
             agreement ad,
             agrrefund ar,
             agrclaim ac
       Where a.agrisn=ag.isn(+)
         and a.refundisn=ar.isn(+)
         and a.addisn=ad.isn(+)
         and ar.claimisn=ac.isn(+)
       Group by a.StatCode, nvl(a.rptgroupisn,0), a.dateval,
                Trunc(ag.datebeg, ''Q''), Trunc(ag.dateEnd, ''Q''), Trunc(ad.dateEnd, ''Q''),
                Trunc(trunc(greatest(nvl(dateclaim, datereg),
                nvl(dateevent,nvl(dateclaim, datereg)))), ''Q''),
                a.subaccisn, a.sagroup
    );
    Commit;
   end;';

  Parallel_Tasks.processtask(sesid,vsql);
  vDb:= add_months(vDb, 1);
  DBMS_APPLICATION_INFO.set_module('FULL REPSUM2AGR ch2', 'Applied : '||vDb);
end loop;

Parallel_Tasks.endsession(sesid);
SesId:=Parallel_Tasks.createnewsession;

--3
 vDb:= pDateFrom;

 Loop
  Exit When vDb >= vDateTo;

  vSql:='
   Begin
    DBMS_APPLICATION_INFO.set_module(''FULL REPSUM2AGR ch3'', ''' || vDb || ''');

    Insert into ST_REPBUHSUMMARY
    ( Select --+ Index(a) Ordered use_nl(ag ar ad)
             StatCode,
             nvl(a.rptgroupisn, 0) rptgroupisn,
             a.dateval,
             Null Qdatebeg,
             Null QdateEnd,
             Null QAddateEnd,
             Null Qdaterefund,
             amountRub,
             a.subaccisn,
             a.sagroup
       From ( Select --+ Index_combine(a)
                     StatCode,
                     nvl(a.rptgroupisn, 0) rptgroupisn,
                     Trunc(a.dateval, ''MM'') Dateval,
                     a.subaccisn,
                     a.sagroup,
                     Sum(amountrub) amountRub
                From storages.rep_buh2agr a
               Where a.dateval >= to_date('''||vDb||''') and a.dateval < to_date('''||add_months(vDb, 1)||''')
                 /* and sagroup=1  sts 10.08.2011 - убрал, т.к. поле sagroup теперь сохраняется в таблице */
                 and statcode in ( Select statcode
                                     from storages.rep_statcode
                                    where grp in (''Исходящее перестрахование''))
               group by StatCode, nvl(a.rptgroupisn, 0), a.subaccisn, a.sagroup, Trunc(a.dateval, ''MM'')) a
     );
   Commit;
  end;';

 Parallel_Tasks.processtask(sesid, vsql);
 vDb := add_months(vDb,1);
 DBMS_APPLICATION_INFO.set_module('FULL REPSUM2AGR ch3', 'Applied : '||vDb);
end loop;

Parallel_Tasks.endsession(sesid);
SesId:=Parallel_Tasks.createnewsession;


vDb:= pDateFrom;

 Loop
  Exit When vDb >= vDateTo;

  vSql:='
  begin
    DBMS_APPLICATION_INFO.set_module(''FULL REPSUM2AGR ch4'', ''' || vDb || ''');

    insert into ST_REPBUHSUMMARY
    Select a.StatCode,
           a.rptgroupisn,
           a.datevalM,
           Null Qdatebeg,
           Null QdateEnd,
           Null QAddateEnd,
           Null Qdaterefund,
           a.amountRub,
           a.subaccisn,
           a.sagroup
      From ( Select --+ Index_combine (a) No_Parallel(a)
                    a.StatCode,
                    Nvl(a.rptgroupisn, 0) rptgroupisn,
                    Trunc(a.Dateval, ''mm'') datevalM,
                    a.subaccisn,
                    a.sagroup,
                    Sum(a.AMOUNTRub) amountRub
              From storages.repbuh2cond a
              Where a.dateval >= to_date('''||vDb||''') and a.dateval < to_date('''||add_months(vDb, 1)||''')
                --and a.sagroup=1   sts 10.08.2011 - убрал, т.к. поле sagroup теперь сохраняется в таблице
                and a.statcode=608
              group by
                a.StatCode,
                nvl(a.rptgroupisn, 0),
                Trunc(a.Dateval, ''mm''),
                a.subaccisn,
                a.sagroup,
                Trunc(a.Dateval, ''mm'')) a;
    commit;
  end;';

 Parallel_Tasks.processtask(sesid, vsql);
 vDb := add_months(vDb,1);
 DBMS_APPLICATION_INFO.set_module('FULL REPSUM2AGR ch4', 'Applied : '||vDb);

 end loop;

 Parallel_Tasks.endsession(sesid);

 system.restore_table_index('STORAGES.ST_REPBUHSUMMARY', 0);
 commit;
end;

END;