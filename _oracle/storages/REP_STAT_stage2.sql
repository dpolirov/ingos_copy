CREATE OR REPLACE PACKAGE "STORAGES"."REP_STAT" 
IS

procedure full_REPBUHSUMMARY
(pLoadIsn number:=0, pDateRep IN DATE := SYSDATE);

procedure St_RepBuhSummary_Load (
  pDateFrom date, -- дата начала периода (включительно)
  pDateTo date    -- дата окончания периода (включительно)
);

END; -- Package spec

CREATE OR REPLACE PACKAGE BODY "STORAGES"."REP_STAT" 
IS

cObjCnt Number:=100000;
cNullDate Date:=To_Date('01013000','DDMMYYYY');

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

  -- загрузка таблицы ST_REPBUHSUMMARY начиная с начала года по прошлый месяц от текущей даты
  vDb := add_months(trunc(pDateRep/*EGAO 28.09.2012 sysdate*/, 'mm'), -1);
  St_RepBuhSummary_Load(trunc(vDb, 'y'), vDb);
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