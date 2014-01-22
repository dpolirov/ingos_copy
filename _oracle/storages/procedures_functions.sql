
  CREATE OR REPLACE FUNCTION "STORAGES"."GCC2P" (pAmount in number,pCurIn in number, pCurOut in number, pDate in date) return number PARALLEL_ENABLE

 is
  r number;
begin
 R:=gcc2.gcc2(pAmount,pCurIn , pCurOut, pDate);
 return r;
end;

  CREATE OR REPLACE FUNCTION "STORAGES"."GETAGRREINFO" (AISN NUMBER, RESISN NUMBER:=NULL,
          oisn number:=0, risn number:=0) return varchar2 PARALLEL_ENABLE is
s varchar2(4000);
p number;
pr number;
isum number;
pq number;
xper number;
begin

s:=null; xper:=0;
for r in (select --+ use_nl(a x s)
              a.isn aisn, x.isn xisn,a.datebeg, a.ruleisn,
              getcrosscover(insuredsum,a.currisn,53,a.datebeg) insuredsum,
              getcrosscover(limitsum,a.currisn,53,a.datebeg) limitsum,
              x.sectisn,x.xpc,nvl(reinspc,0) reinspc,nvl(sharepc,100) sharepc,s.secttype,s.currisn,
              s.limiteverymode,s.optionalcode,x.ISCALC,x.LIMITUSD
         from STORAGES.REP_AGRX x,agreement a, resection s
         where  x.objisn=oisn and x.riskisn=risn and x.agrisn=aisn
               and    a.isn=x.agrisn and x.sectisn=s.isn

                        order by nvl(to_char(s.orderno),s.secttype||s.id)) loop
 if s is null then
  s:=Substr('Share:'||r.sharepc||' F:'||r.reinspc,1,255);
  p:=r.reinspc; xper:=p;
  if oisn>0 or risn>0 then

     If Nvl(r.ISCALC,1)=0 then
      r.insuredsum:=r.LIMITUSD;
     else
      r.insuredsum:=getisum2(r.aisn,53,r.datebeg,oisn,risn,r.limiteverymode,r.xisn);
     end if;
    --raise_application_error(-20005,'isum'||r.insuredsum);
    r.limitsum:=0;
  elsif r.limiteverymode='Y' then
    ---r.insuredsum:=getisum2(r.aisn,53,r.datebeg,0,0,'Y');
                      SELECT max(SUM(limitsum))
                      INTO r.insuredsum
                      FROM(
                      SELECT connect_by_root(objisn) AS prnobj, a.limitsum
                      FROM (SELECT gcc2.gcc2(nvl(x.limiteverysum, x.limitsum), b.currisn, 53, r.datebeg) AS limitsum, b.objisn, b.parentobjisn
                            FROM agrcond x, repcond b WHERE b.condisn=x.isn AND x.agrisn=r.aisn) a
                      START WITH a.parentobjisn IS NULL
                      CONNECT BY PRIOR a.objisn=a.parentobjisn
                      )
                      GROUP BY prnobj;







    r.limitsum:=0;
  end if;
  if r.ruleisn=34053716 then -- тех.риски
   if nvl(r.insuredsum,0)>0 and nvl(risn,0)=0 then r.limitsum:=0; end if;
  end if;
  isum:=((r.insuredsum+r.limitsum)*r.sharepc/100)*((100-r.reinspc)/100);
--  dbms_output.put_line(isum);
  if isum=0 then return 0; end if; --BAG!!!
 end if;
--  raise_application_error(-20010,'debug: '||isum);
 if r.secttype='QS' then
  select retention,getcrosscover(limitsum,r.currisn,53,sysdate) into pq,pr
   from recond where sectisn=r.sectisn;
  if isum>pr then
   pq:=round((pr*100/isum),4)*pq/100;
  else
   pr:=isum;
  end if;
  if (nvl(r.xpc,0)=0) and (isum > 0) then
    -- p:=(100-p)*(round(pr*100/isum,2)-pq)/100;
    p:=(100-xper)*(round(pr*100/isum,4)-pq)/100;
  else
     p:=nvl(r.xpc,0);
  end if;
  if (p > 0) then
    s:=Substr(s||' QS:'||p,1,255);
    xper:=xper+p;
    -- isum:=isum*((100-p)/100);
    isum:=isum*pq/100;
  end if;
 end if;
 if r.secttype='SP' then
   select getcrosscover(prioritysum,r.currisn,53,sysdate),
          getcrosscover(limitsum,r.currisn,53,sysdate)
          into pr,pq from recond where sectisn=r.sectisn;
--  raise_application_error(-20010,'debug: isum:'||isum||' pr+pq:'||(pr+pq));
   if isum>pr+pq then
     pr:=isum-pq;
   end if;
   if (nvl(r.xpc,0)=0) and (isum > 0) then
     p:=(100-xper)*round((isum-pr)*100/isum,4)/100;
-- p:=round((isum-pr)*100/isum,2);
   else
     p:=nvl(r.xpc,0);
   end if;
   if (p > 0) then
     s:=Substr(s||' SP:'||p,1,255);
     xper:=xper+p;
     if (isum > 0) then
       isum:=isum*((100-round((isum-pr)*100/isum,2))/100);
     end if;
   end if;
 end if;
 if r.secttype='XL' then
  select max(getcrosscover(limitsum+prioritysum,r.currisn,53,sysdate))
      into pr from recond where sectisn=r.sectisn;
     dbms_output.put_line('0isum =  '||isum);
  if pr<isum and r.optionalcode is null then
   -- s:=s||' XL:'||round(100-(isum-pr)*100/isum,2);
   -- isum:=isum*round(100-(isum-pr)*100/isum,2)/100;
   dbms_output.put_line('r.xpc= '||r.xpc);
   dbms_output.put_line('isum =  '||isum);
   if (nvl(r.xpc,0)=0) and (isum > 0) then
    p:=(100-xper)*round(100-(isum-pr)*100/isum,4)/100;
   -- p:=round(100-(isum-pr)*100/isum,2);
   else
     p:=nvl(r.xpc,0);
   end if;
   if (p > 0) then
     s:=Substr(s||' XL:'||p,1,255);
     xper:=xper+p;
     if (isum > 0) then
       isum:=isum*((100-round((isum-pr)*100/isum,2))/100);
     end if;
   end if;
  else
   if nvl(r.xpc,0)=0 then
    p:=100-xper;
   else
    p:=r.xpc;
   end if;
   if (p > 0) then
     s:=s||' XL:'||p;
   end if;
  end if;
 end if;
-- dbms_output.put_line(isum);
 if resisn=r.sectisn then
  If P<0 Then p:=0; end if;
  return p;
 end if;
end loop;
return S;
/* exception when others then raise_application_error(-20000,aisn||' '||resisn); */
end;

  CREATE OR REPLACE FUNCTION "STORAGES"."INIT_PARTITION_BY_KEY" 
  ( pTableName varchar2,
    pKey varchar2,
    pClear number:=1,
    pCompress NUMBER := 1)
  RETURN  varchar2 IS
--
-- To modify this template, edit file FUNC.TXT in TEMPLATE 
-- directory of SQL Navigator
--
-- Purpose: Briefly explain the functionality of the function
--
-- MODIFICATION HISTORY
-- Person      Date    Comments
-- ---------   ------  -------------------------------------------       
  vPart varchar2(250);
   -- Declare program variables as shown above
BEGIN 
vPart:=GET_TABLE_VALUE_PARTITITON(pTableName,pKey);

 if  (vPart is not null and pClear=1)then
  Execute immediate 'Alter TAble '||pTableName||' truncate partition '||vPart;
 end if;

 If (vPart is  null) Then
  Execute Immediate 'Alter Table '||pTableName||' add Partition p'||pKey||' Values ('||pKey||')'||CASE pCompress WHEN 1 THEN ' compress' END;
  vPart:='p'||pKey;
 end if;

return vPart;
END;


  CREATE OR REPLACE PROCEDURE "STORAGES"."INSERT_AGRRE" (pLoadIsn Number,pLoadAgrx Number:=1)
Is
 vMinIsn Number:=-1;
 vlMaxIsn Number;
 vCnt Number:=0;
 vLoadObjCnt Number:=1000;
 SesId Number;
 vSql Varchar2(4000);
Begin
  Execute Immediate 'truncate table Rep_AgrRe reuse storage';


   -- все перестрахованные в формуляре прямого дог-ра
  Insert Into Rep_AgrRe(isn, agrisn, subjisn, sharepc, outcompc, datebeg, dateend,Loadisn,OLDSHAREPC, Agrxinsuredsum, agrdatebeg, agrdateend)
  Select --+ ordered use_nl ( s ra ) no_merge ( s ) index ( ra X_REPAGR_AGR )
         Seq_Reports.NEXTVAL,s.agrisn,s.subjisn, s.sharepc,
    decode(s.BasePC,0,0,Least(100,Greatest(0,100*s.ComBasePC/s.BasePC))),s.Datebeg,s.Dateend,pLoadIsn,s.sharepc*100,0
    , ra.datebeg, ra.dateend
  From
    (select a.agrisn, A.subjisn,
       nvl(sum(Decode(sumclassisn,427,0,A.sharepc/100)),0) sharepc,
       nvl(sum(Decode(sumclassisn,427,0,A.base)),0) BasePC,
       nvl(sum(Decode(sumclassisn,427,A.base,0)),0) ComBasePC,
       Min(Datebeg) Datebeg,Max(Dateend) Dateend
     from Ais.agrrole a
     where 1=1 --EGAO 27.11.2012 A.orderno>0
       and A.classisn=435 -- ПЕРЕСТРАХОВЩИК
       and nvl(A.sumclassisn,0) in (8133016,414,427) -- 0, ПРЕМИЯ, КОМИССИЯ
       and A.sharepc<>0
       and A.calcflg='Y'
--       and (nvl(A.sumclassisn,0)=427 or instr(upper(A.formula),'SI')>0)
     group by a.agrisn,A.subjisn) S, repagr ra
     WHERE s.agrisn=ra.agrisn(+);

  Commit;
/*
    -- Туристы
  Merge Into Rep_AgrRe D
  Using (select --+   ordered Use_Nl ( r t1 a) Index(a x_repagr_agr)
           R.agrisn,t1.subjisn,Max(t1.reinspc) SHAREPC,Min(r.Datebeg) Datebeg,Max(r.Dateend) Dateend,
           Max( T1.REINSCOMMI) outcompc
         from finance.tourreinsurers t1, Ais.agrrole r, repagr a
         where A.AgrIsn=r.agrisn
           ANd A.RULEDEPT=707480016 -- c.get('PrivDept') Управление страхования путешествующих
           and r.subjisn=t1.subjisn
         Group by R.agrisn,t1.subjisn ) S
  On (D.agrisn=S.agrisn and D.subjisn=S.subjisn)
  WHEN MATCHED THEN Update Set D.SHAREPC=S.SHAREPC,D.DATEBEG=S.DATEBEG,D.Dateend=S.Dateend,D.outcompc=S.outcompc
  WHEN NOT MATCHED THEN INSERT (d.Isn,D.AgrIsn, D.subjisn,D.SHAREPC,D.outcompc,D.DATEBEG,D.Dateend)
         VALUES (Seq_Reports.NEXTVAL,S.AgrIsn, S.subjisn,S.SHAREPC,S.outcompc,S.datebeg,S.Dateend);
  commit;

     -- Туристы шенгеН
  Merge Into Rep_AgrRe D
  Using (select a.agrisn,5618616 subjisn, 100/3 SHAREPC, 90/3 outcompc
         from REP_AGRTUR a
         where a.isshengen=1) S
  On (D.agrisn=S.agrisn)
  WHEN MATCHED THEN
    Update Set D.SHAREPC=S.SHAREPC,D.subjisn=S.subjisn,D.OUTCOMPC=S.OUTCOMPC
  WHEN NOT MATCHED THEN
    INSERT (D.Isn,D.AgrIsn, D.subjisn,D.SHAREPC,D.OUTCOMPC)
    VALUES (Seq_Reports.NEXTVAL,S.AgrIsn, S.subjisn,S.SHAREPC,S.OUTCOMPC);

  Commit;

  */
-- execute Immediate 'truncate Table Rep_AgrX';
  If pLoadAgrx=1 then
--- А БЛОКИРНУ КА Я ЭТУ МУТЬ С ДОГОВОРАМИ ПОКРЫТИЯ КАСКО, ВРЕМЕННО, ПОГЛЯДИМ ;-)
--delete from rep_agrx where isn in (Select isn from ais.agrx);

    execute Immediate 'truncate Table Rep_AgrX';

    --{EGAO 13.03.2012 Добавил join с repagr, т.к. для входящих облигаторов вместо значения поле objisn из agrx надо брать 0.
    -- Для входящих облигаторов в этом поле указывается isn секции этого облигатора.
    /*Insert Into Rep_AgrX(isn,
                         agrisn,
                         sectisn,
                         objisn,
                         riskisn,
                         reisn,
                         xpc,
                         datebeg,
                         dateend,
                         iscalc,
                         limitusd,
                         loadisn,
                         gr,
                         grp,
                         norig,
                         shareneorig
                         )
    Select min(isn) isn, agrisn,sectisn,objisn,riskisn,reisn,xpc,DateBeg,DateEnd,1,0,pLoadIsn,gr,grp,1 norig, nvl(shareneorig,100) AS shareneorig
    From ais.agrx
    group by agrisn,sectisn,objisn,riskisn,reisn,xpc,DateBeg,DateEnd,gr,grp,nvl(shareneorig,100);*/
    Insert Into Rep_AgrX(isn,
                         agrisn,
                         sectisn,
                         objisn,
                         riskisn,
                         reisn,
                         xpc,
                         datebeg,
                         dateend,
                         iscalc,
                         limitusd,
                         loadisn,
                         gr,
                         grp,
                         norig,
                         shareneorig
                         )
    SELECT /*+  Full(x) Parallel(x,32) ordered use_nl ( x ra ) index ( ra X_REPAGR_AGR )*/
           min(x.isn) isn, x.agrisn,x.sectisn,
           CASE WHEN NVL(ra.classisn,0)=9020 THEN 0 ELSE x.objisn END AS objisn,
           x.riskisn,x.reisn,x.xpc,x.DateBeg,x.DateEnd,1,0,-1 AS pLoadIsn,x.gr,x.grp,1 norig, nvl(x.shareneorig,100) AS shareneorig
    From ais.agrx x, repagr ra
    WHERE ra.agrisn(+)=x.agrisn
    group by x.agrisn,x.sectisn,CASE WHEN NVL(ra.classisn,0)=9020 THEN 0 ELSE x.objisn END,x.riskisn,x.reisn,x.xpc,x.DateBeg,x.DateEnd,x.gr,x.grp,nvl(x.shareneorig,100);

    Commit;
  end if;
  SesId:=Parallel_Tasks.createnewsession();

  vMinIsn:=-1;

  loop
  vLMaxIsn:=cut_table(PTABLENAME=>'storages.Rep_AgrX', PCOLUMNNAME=>'isn', PMINISN=>vMinIsn,  PROWCOUNT=>vLoadObjCnt);

  /*
    select Max (Agrisn) into vLMaxIsn
    From
     (Select /*+ Index_Asc (a X_REP_AGRX_AGR) / Agrisn
      from Rep_AgrX a
      where Agrisn > vMinIsn
        and rownum <= vLoadObjCnt);
*/
    Exit When vLMaxIsn is null;

    vSql:='Begin
      storages.INSERT_AGRRE_BY_ISNS('|| vMinIsn||','||vLMaxIsn||','||pLoadIsn||');
      Commit;
      End;';

    System.Parallel_Tasks.processtask(sesid,vsql);

    Select /*+ Index (a X_REP_AGRX) */ vCnt+Count(*) into vCnt
    from Rep_AgrX a
    where Isn>vMinIsn and Isn<=vLMaxIsn;

    vCnt:=vCnt+1;

    vMinIsn:=vLMaxIsn;
    DBMS_APPLICATION_INFO.set_module('Load Agrre','Loaded: '||vCnt);
  end loop;
-- ждем, пока завершатся все джобы
  Parallel_Tasks.endsession(sesid);
 -- EXPORT_DATA.export_to_owb_by_FLD('rep_agrre','AgrIsn');
  DBMS_APPLICATION_INFO.set_module('','');
END; -- Procedure


  CREATE OR REPLACE PROCEDURE "STORAGES"."INSERT_AGRRE_BY_ISNS" (vMinIsn number, vMaxIsn Number,pLoadIsn Number) IS
BEGIN

  delete from tt_RowId;
  Insert Into tt_RowId(Isn)
  Select /*+ Index(x) */ Isn
  from Rep_AgrX x
  where Isn>vMinIsn and Isn<=vMaxIsn;

  INSERT_AGRRE_BY_TT_ROWID(pLoadIsn);
END; -- Procedure

  CREATE OR REPLACE PROCEDURE "STORAGES"."INSERT_AGRRE_BY_TT_ROWID" (pLoadIsn number,pIsFull Number:=1) IS
  p number;
  S number;
  L Number;


pOld Number;
vOldRate Number;
  isum number;

  xper number:=0;
  vAgr Number:=0;
--  vObj Number:=0;
--  vRisk Number:=0;
--  db date;
--  de date;
  com1 Number;
  com2 Number;
  vRate Number;
  vDept0Isn Number;
  vSharePc Number:=0;
  vDpremsum number;
  vFlatPrem number;
  vEpi number;
  vAgrxInsuredSum NUMBER := 0; -- EGAO 12.08.2011
  vAgrxInsuredCurrDate DATE; -- EGAO 16.08.2011


  vCnt number:=1;

  TYPE TAgrxInsuredRec IS RECORD(
    InsuredSum NUMBER,
    CurrDate DATE
  );

  AgrxInsuredRec TAgrxInsuredRec;


  TYPE TAgrxInsuredTab IS TABLE OF TAgrxInsuredRec INDEX BY VARCHAR2(100);
  AgrxInsuredTab TAgrxInsuredTab;
BEGIN
  If pIsFull=0 then
    delete from Rep_AgrRe Where agrisn In (select Isn from tt_rowId);
    commit;

    Insert Into Rep_AgrRe
      (isn, agrisn, subjisn, sharepc, outcompc, datebeg, dateend)
    Select Seq_Reports.NEXTVAL, agrisn, subjisn, sharepc,
      decode(BasePC,0,0,Least (100,Greatest (0,100*ComBasePC/BasePC))),trunc(Datebeg),trunc(Dateend)
    From
      (select a.agrisn,A.subjisn,
         nvl (sum (Decode(sumclassisn,427,0,A.sharepc)),0) sharepc,
         nvl (sum (Decode(sumclassisn,427,0,A.base)),0) BasePC,
         nvl (sum (Decode(sumclassisn,427,A.base,0)),0) ComBasePC,
         Min(Datebeg) Datebeg,Max(Dateend) Dateend
       from tt_rowId t, Ais.agrrole a
       where t.isn=a.agrisn
         And A.orderno>0
         and A.classisn=435 -- ПЕРЕСТРАХОВЩИК
         and nvl(A.sumclassisn,0) in (0,414,427)
         and A.sharepc<>0
         and A.calcflg='Y'
         and (nvl(A.sumclassisn,0)=427 or instr(upper(A.formula),'SI')>0)
       group by a.agrisn, A.subjisn) S;
    Commit;

    -- Туристы
    Merge Into Rep_AgrRe D
    Using (select --+   ordered Use_Nl ( r t1 a) Index(a x_repagr_agr)
             R.agrisn, t1.subjisn, Max(t1.reinspc) SHAREPC, Min(trunc(r.Datebeg)) Datebeg,
             Max(trunc(r.Dateend)) Dateend, Max(T1.REINSCOMMI) outcompc
           from tt_rowId t,Ais.agrrole r,repagr a,finance.tourreinsurers t1
           where t.isn=r.agrisn
             And A.AgrIsn= r.agrisn
             ANd A.RULEDEPT=707480016 -- c.get('PrivDept')
             and r.subjisn = t1.subjisn
           Group by R.agrisn,t1.subjisn ) S
    On (D.agrisn=S.agrisn and D.subjisn=S.subjisn)
    WHEN MATCHED THEN Update Set D.SHAREPC=S.SHAREPC,D.DATEBEG=S.DATEBEG,D.Dateend=S.Dateend,D.outcompc=S.outcompc
    WHEN NOT MATCHED THEN INSERT (d.Isn,D.AgrIsn, D.subjisn,D.SHAREPC,D.outcompc,D.DATEBEG,D.Dateend)
      VALUES (Seq_Reports.NEXTVAL,S.AgrIsn, S.subjisn,S.SHAREPC,S.outcompc,S.datebeg,S.Dateend);
    commit;

     -- Туристы шенгеН
    Merge Into Rep_AgrRe D
    Using (select a.agrisn,5618616 subjisn, 100/3 SHAREPC, 90/3 outcompc
           from tt_rowId t,REP_AGRTUR a
           where a.isshengen=1 and t.isn=a.agrisn) S
    On (D.agrisn=S.agrisn)
    WHEN MATCHED THEN Update Set D.SHAREPC=S.SHAREPC,D.subjisn=S.subjisn,D.OUTCOMPC=S.OUTCOMPC
    WHEN NOT MATCHED THEN INSERT (D.Isn,D.AgrIsn, D.subjisn,D.SHAREPC,D.OUTCOMPC)
      VALUES (Seq_Reports.NEXTVAL,S.AgrIsn, S.subjisn,S.SHAREPC,S.OUTCOMPC);

    Commit;
  end if;

  xper:=0;
  for R in
    (select --+ ordered USE_NL (x  A A1 S) Index (x X_Rep_AgrX)
       a.isn aisn, x.isn xisn,trunc(a.datebeg) AS datebeg/*EGAO 20.10.2010 a.datebeg*/,trunc(a.dateend) AS dateend/*EGAO 20.10.2010 a.dateend*/,
       a.insuredsum, s.Rate, a1.classisn reclassisn, a1.deptisn redeptisn,
       a.limitsum,a1.datebase,a.currisn agrcurrisn,
       x.sectisn,x.xpc,nvl(a.reinspc,0) reinspc,nvl(a.sharepc,100) sharepc,s.secttype,/*s.currisn,*/
       s.limiteverymode,s.optionalcode,x.objisn,x.riskisn,x.reisn,
       Case When a1.datebase='I'and a.datebeg between s.DateBeg and s.DateEnd then a.datebeg
       else   greatest(s.datebeg,a.datebeg) end xDateBeg,
       Case When a1.datebase='I'and a.datebeg between s.DateBeg and s.DateEnd then a.dateend
       else  Least(s.dateend,a.dateend) end  xDateEnd,

       s.datebeg sDatebeg,
       s.dateEnd sDateEnd,
       x.ISCALC,x.limitusd,
       A1.clientisn SubjIsn,
       s.currisn sCurrIsn,
       s.premiumtype,
       1 nOrig,
       x.shareneorig,
       a1.id AS reid -- EGAO 27.03.2012
     from tt_rowId t, Rep_AgrX x, ais.agreement a, ais.agreement a1, ais.resection S
     where t.isn=X.Isn
       and a.isn=x.agrisn and x.sectisn=s.isn and a1.isn=x.reisn
       and a1.status in ('В','Д','Щ','С')
     order by a.isn, nvl(x.objisn,0),nvl(x.riskisn,0), nvl(to_char(s.orderno),s.secttype||s.id))
  loop


  DBMS_APPLICATION_INFO.set_module('Load Agrre By Isns','Loading: '||vCnt);
  vCnt:=vCnt+1;
      Begin
         vRate:=0;
         vSharePc:=0;
         vDpremsum:=0;
         vFlatPrem:=0;
         p:=0;



        SELECT Nvl(Least(100, Sum(s1)/Sum(S2))/100,0), -- доля размещения секции с учетом размещения каждого лейра (с учетом пропорции лейров)
             Nvl(gcc2.gcc2( Sum(Epi),r.scurrisn,53,r.sdatebeg),0),
             Nvl(Sum(Rate),0),
             Nvl(Sum(Dpremsum),0),
             Nvl(Sum(FlatPrem),0)
        Into vSharePc,vEpi,vRate,vDpremsum,vFlatPrem
        From
          (SELECT
             Max(decode(Nvl(rc.Rate,0),0,Decode(Nvl(rc.depospremsum,0),0,1,rc.depospremsum),rc.Rate))*Sum(sp.sharepc) s1,
             Max(decode(Nvl(rc.Rate,0),0,Decode(Nvl(rc.depospremsum,0),0,1,rc.depospremsum),rc.Rate)) s2,
             Max(Epi) Epi,
             Max(rc.Rate) Rate,
             gcc2.gcc2(Max(Case When r.reclassisn<>9018 and Nvl(r.rate,0)>0 then rc.depospremsum else 0 end),R.SCurrIsn,53,r.sDatebeg) Dpremsum,
             gcc2.gcc2(Max( Case When r.reclassisn=9018 Or Nvl(r.rate,0)=0 then  rc.depospremsum else 0 end),R.SCurrIsn,53,r.sDatebeg) FlatPrem

           FROM recond rc, RESUBJPERIOD sp
           WHERE rc.SectIsn= r.sectisn
             And sp.CONDISN=rc.isn
             and sp.parentisn is null
           group by rc.isn);




      Com1:=0;

      if r.secttype='QS' then --1

        select RECOMMISS,OVRCOMMISS into Com1, Com2
        from Ais.recond
        where sectisn=r.sectisn;






         If vRate=0 then -- если собс удержание не указанно, считаем долю в дог-ре(p)
              select
                 gcc2.gcc2(limitsum,r.scurrisn/*r.currisn*/,r.agrcurrisn,r.sdatebeg)
              into L from recond where sectisn=r.sectisn;



                 if r.ObjIsn>0 or r.riskisn>0 then
                   r.insuredsum:=getisum2(r.aisn,r.agrcurrisn,r.datebeg,r.ObjIsn,r.riskisn,r.limiteverymode,r.sectisn);
                   r.limitsum:=0;
                elsif r.limiteverymode='Y' then
                      ----EGAO 10.03.2011 r.insuredsum:=getisum2(r.aisn,r.agrcurrisn,r.datebeg,0,0,'Y');
                      SELECT max(SUM(limitsum))
                      INTO r.insuredsum
                      FROM(
                      SELECT connect_by_root(objisn) AS prnobj, a.limitsum
                      FROM (SELECT gcc2.gcc2(nvl(x.limiteverysum, x.limitsum), b.currisn, r.agrcurrisn, r.datebeg) AS limitsum, b.objisn, b.parentobjisn
                            FROM agrcond x, repcond b WHERE b.condisn=x.isn AND x.agrisn=r.aisn) a
                      START WITH a.parentobjisn IS NULL
                      CONNECT BY PRIOR a.objisn=a.parentobjisn
                      )
                      GROUP BY prnobj;

                      r.limitsum:=0;
                end if;

                isum:=((Nvl(r.insuredsum,0)+Nvl(r.limitsum,0))*Nvl(r.sharepc,100)/100);

                 --dbms_output.put_line(l);
                 --dbms_output.put_line(isum);


                  if isum>0 then
                      p:=Least(1,l/isum)*vSharePc; -- если L=null - то секция бесконечная
                      vRate:=P;
                  else p:=0; end if;


              If Nvl(p,0)=0 then -- если для квоты что-то не получилось - она равна доли размещения
              p:=vSharePc;
             end if;

          else
             p:=(1-vRate/100); -- если собс удержание  указанно, считаем доля (p) = 1 - собс удержание
             r.NOrig:=1;

           end if;

      else Com2:=0;
      END IF;


 if r.secttype='SP' then -- рассчет доли для SP - всегда считаем
   select gcc2.gcc2(prioritysum,r.scurrisn/*r.currisn*/,r.agrcurrisn,r.sdatebeg),
          gcc2.gcc2(limitsum,r.scurrisn/*r.currisn*/,r.agrcurrisn,r.sdatebeg)
          into S,L from recond where sectisn=r.sectisn;
/*
dbms_output.put_line('r.currisn '||r.currisn);
dbms_output.put_line('agrcurrisn '||r.agrcurrisn);
dbms_output.put_line('sdatebeg '||r.sdatebeg);

dbms_output.put_line('s '||s);
dbms_output.put_line('l '||l);
*/
          if r.ObjIsn>0 or r.riskisn>0 then
           r.insuredsum:=getisum2(r.aisn,r.agrcurrisn,r.datebeg,r.ObjIsn,r.riskisn,r.limiteverymode,r.sectisn);
           r.limitsum:=0;
          elsif r.limiteverymode='Y' then
              --EGAO 10.03.2011 r.insuredsum:=getisum2(r.aisn,r.agrcurrisn,r.datebeg,0,0,'Y');
              SELECT max(SUM(limitsum))
              INTO r.insuredsum
              FROM(
              SELECT connect_by_root(objisn) AS prnobj, a.limitsum
              FROM (SELECT gcc2.gcc2(nvl(x.limiteverysum, x.limitsum), b.currisn, r.agrcurrisn, r.datebeg) AS limitsum, b.objisn, b.parentobjisn
                    FROM agrcond x, repcond b WHERE b.condisn=x.isn AND x.agrisn=r.aisn) a
              START WITH a.parentobjisn IS NULL
              CONNECT BY PRIOR a.objisn=a.parentobjisn
              )
              GROUP BY prnobj;


              r.limitsum:=0;
          end if;

          isum:=((r.insuredsum+r.limitsum)*r.sharepc/100);

--dbms_output.put_line('isum '||isum);

--   If isum>0 and (s is null or L is null) then
  --     p:=vSharePc/100;-- если L=null - то секция бесконечная

   if isum>0 then
       p:=Least(Greatest((isum-S)/isum,0),( L-S)/isum)*vSharePc;-- если L=null - то секция бесконечная
   else p:=0; end if;

 end if;


      if r.secttype='XL' then --1


           If vRate>0 then -- and (r.Datebase='I' or R.reclassisn=9018) then -- указанна ставка перерасчета
             p:=(vRate/100)*(vSharePc);
           end if;
           --els
           if vEpi>0 and vFlatPrem>0 then -- если есть Epi и указанна флэт-премия
             p:=0;--(vSharePc/100)*(vFlatPrem/vEpi);

            elsIf (Nvl(vEpi,0)=0) and Nvl(vFlatPrem,0)>0 then -- если нет Epi и указанна флэт-премия, то считаем его
              Select --+ Ordered Use_Nl(rc) index ( rc X_REPCOND_AGR )
                Decode(Sum(rc.premusd*NORIG),0,1,Sum(rc.premusd*NORIG))
              Into vEpi
             from rep_agrx ax, repcond rc
             Where ax.sectisn=r.sectisn
                 and rc.agrisn=ax.agrisn
                 and rc.newaddisn is null
                 and (ax.ObjIsn=0 or Rc.PARENtObjIsn=ax.ObjIsn or Rc.ObjIsn=ax.ObjIsn )
                 and ( ax.RiskIsn=0 or Rc.PARENTRISKISN=ax.RiskIsn or Rc.RiskIsn=ax.RiskIsn );

               p:=0;--(vSharePc/100)*(vFlatPrem/vEpi);

            end if;
--         end if;

      end if;


-- старый метод рассчета доли
 vOldRate:=1;
      If nvl(r.xpc,0)=0 Then
        begin
          pOld:=Storages.GetAgrReInfo(r.aisn,r.sectisn,r.ObjIsn,r.riskisn);
        exception when others then
          --dbms_output.put_line('Agrisn '||r.aisn||'Sectisn '||r.sectisn);
          raise;
        end;
     ELSE
        pOld:=r.xpc;
      end if; --nvl(r.xpc,0)=0


      if r.secttype='XL' then --1
        Select Decode(nvl(r.Rate/100,1),0,1,nvl(r.Rate/100,1)) into vOldRate
        From Dual;

        If r.reclassisn=9018 and nvl(r.Rate,0)=0 then
          Select Getcrosscover(Sum(rc.depospremsum),R.SCurrIsn,53,r.xDatebeg) Into vOldRate
          from recond rc
          where Sectisn=r.sectisn;

          Select /*+ Ordered Use_Nl(rc) index ( rc X_REPCOND_AGR) */
            vOldRate/Decode(Sum(rc.premusd),0,1,Sum(rc.premusd)) Into vOldRate
          from rep_agrx ax, repcond rc
          Where ax.sectisn=r.sectisn
            and rc.agrisn=ax.agrisn
            and rc.newaddisn is null
            and (Nvl(Rc.PARENtObjIsn,Rc.ObjIsn)=ax.ObjIsn or ax.ObjIsn=0)
            and (Nvl(Rc.PARENTRISKISN,Rc.RiskIsn)=ax.RiskIsn or ax.RiskIsn=0);

          If vOldRate>1 then vOldRate:=1; end if;

          If Nvl(vOldRate,0)>0 Then
             pOld:=100*vSharePc;
          end if;
        end if;
      end if;

-- старый метод рассчета доли






      select /*+ Index(r X_REPCOND_AGR)*/ max(agrisn) into vAgr
      from repcond r
      where agrisn=r.aisn and rownum=1; -- проверка на ретроцессию или договор без кондов

      select nvl(max(isn),0) into vDept0Isn
      from subdept
      where parentisn=0
      start with isn=r.redeptisn connect by prior parentisn=isn;

      --{EGAO 12.08.2011
      IF AgrxInsuredTab.Exists(to_char(r.xisn)) THEN
        vAgrxInsuredSum :=  AgrxInsuredTab(to_char(r.xisn)).InsuredSum;
        vAgrxInsuredCurrDate := AgrxInsuredTab(to_char(r.xisn)).CurrDate;
      ELSE
        pparam.Clear;
        pparam.SetParamN('AgrxIsn',r.xisn);

        SELECT NVL(MAX(a.InsuredSum),0), MAX(a.currdate)
        INTO vAgrxInsuredSum, vAgrxInsuredCurrDate
        FROM v_agrxinsuredsum a;

        AgrxInsuredRec.InsuredSum := vAgrxInsuredSum;
        AgrxInsuredRec.CurrDate := vAgrxInsuredCurrDate;
        AgrxInsuredTab(to_char(r.xisn)) := AgrxInsuredRec;

      END IF;
      --}

      if (vAgr is null) or (r.ObjIsn=0 and r.RiskIsn=0) then
        Insert Into Rep_AgrRe
         (isn,loadisn,agrisn, objisn, riskisn, sectisn, reisn, sharepc, datebase, datebeg, dateend, condisn, datebegx, dateendx,
          agrxisn, OUTCOMPC, OUTCOMPC1, reclassisn, redept0isn, SubjIsn, RESHAREPC, DEPPREMUSD,
          Rate,premiumtype,EPI,NORIG,FLATPREMUSD,sDatebeg,sDateEnd,secttype,OLDSHAREPC, AgrxInsuredSum, shareneorig, AgrxInsuredCurrDate, sectcurrisn,
          agrdatebeg, agrdateend, reid)
        Values
         (Seq_Reports.NEXTVAL, pLoadIsn,r.aisn, r.objisn, r.riskisn, r.sectisn, r.reisn,  p, r.datebase, r.datebeg, r.dateend, null,
          r.Xdatebeg, r.Xdateend, r.xisn, Com1, Com2, r.reclassisn, vDept0Isn, r.SubjIsn, vSharePc, vDpremsum,
          r.rate,r.premiumtype,vEPI,r.nOrig,vFLATPREM,r.sDatebeg,r.sDateEnd,r.secttype,pOld*vOldRate, vAgrxInsuredSum, r.shareneorig, vAgrxInsuredCurrDate, r.scurrisn,
          r.datebeg, r.dateend, r.reid);
      else

       /*kgs 13.07.2012 чтобы правильно попадать в индексы делаем 3 инсерта, иначе тупо долго на договорах с больши числом кондов */

       IF (r.ObjIsn=0) and  (r.RiskIsn>0)  then /* все объекты не все риски */
         Insert Into Rep_AgrRe
         (isn, loadisn,agrisn, objisn, riskisn, sectisn, reisn, sharepc, datebase, datebeg, dateend, condisn, datebegx, dateendx,
          agrxisn, OUTCOMPC, OUTCOMPC1, reclassisn, redept0isn, SubjIsn, RESHAREPC, DEPPREMUSD,
          Rate,premiumtype,EPI,NORIG,FLATPREMUSD,sDatebeg,sDateEnd,secttype,OLDSHAREPC,AgrxInsuredSum, shareneorig, AgrxInsuredCurrDate, sectcurrisn,
          agrdatebeg, agrdateend, reid)
        select Seq_Reports.NEXTVAL,pLoadIsn,S.*
        From
          (Select --+ Use_Concat(rc)
             Rc.agrisn, r.objisn, r.riskisn, r.sectisn, r.reisn, p, r.datebase,
             Nvl(rc.Datebeg,r.datebeg), Nvl(rc.Dateend,r.dateend), rc.condisn, r.Xdatebeg, r.Xdateend, r.xisn,
             com1, com2, r.reclassisn, vDept0Isn,r.SubjIsn,vSharePc,vDpremsum,r.rate,r.premiumtype,
             vEPI,r.nOrig,vFLATPREM,r.sDatebeg,r.sDateEnd,r.secttype,pOld*vOldRate, vAgrxInsuredSum, r.shareneorig, vAgrxInsuredCurrDate, r.scurrisn,
             r.datebeg, r.dateend, r.reid
           from repcond rc
           where Nvl(Rc.AgrIsn,0) = r.aisn -- чтобы в индекс по договору не попадать
--             and  (Rc.ObjIsn=r.ObjIsn or Rc.ParentObjIsn=r.ObjIsn or r.ObjIsn=0 )
             and (Rc.PARENTRISKISN=r.RiskIsn Or Rc.RiskIsn=r.RiskIsn)
--             and rc.newaddisn is null /*KGS 20.06.2011*/
                  ) S;
         end if;    --(r.ObjIsn=0) and  (r.RiskIsn>0)

       IF (r.ObjIsn>0) and  (r.RiskIsn=0)  then /* все риски не все  объекты */
         Insert Into Rep_AgrRe
         (isn, loadisn,agrisn, objisn, riskisn, sectisn, reisn, sharepc, datebase, datebeg, dateend, condisn, datebegx, dateendx,
          agrxisn, OUTCOMPC, OUTCOMPC1, reclassisn, redept0isn, SubjIsn, RESHAREPC, DEPPREMUSD,
          Rate,premiumtype,EPI,NORIG,FLATPREMUSD,sDatebeg,sDateEnd,secttype,OLDSHAREPC,AgrxInsuredSum, shareneorig, AgrxInsuredCurrDate, sectcurrisn,
          agrdatebeg, agrdateend, reid)
        select Seq_Reports.NEXTVAL,pLoadIsn,S.*
        From
          (Select --+ Use_Concat(rc)
             Rc.agrisn, r.objisn, r.riskisn, r.sectisn, r.reisn, p, r.datebase,
             Nvl(rc.Datebeg,r.datebeg), Nvl(rc.Dateend,r.dateend), rc.condisn, r.Xdatebeg, r.Xdateend, r.xisn,
             com1, com2, r.reclassisn, vDept0Isn,r.SubjIsn,vSharePc,vDpremsum,r.rate,r.premiumtype,
             vEPI,r.nOrig,vFLATPREM,r.sDatebeg,r.sDateEnd,r.secttype,pOld*vOldRate, vAgrxInsuredSum, r.shareneorig, vAgrxInsuredCurrDate, r.scurrisn,
             r.datebeg, r.dateend, r.reid
           from repcond rc
           where Nvl(Rc.AgrIsn,0) = r.aisn -- чтобы в индекс по договору не попадать
             and  (Rc.ObjIsn=r.ObjIsn or Rc.ParentObjIsn=r.ObjIsn  )
--             and (Rc.PARENTRISKISN=r.RiskIsn Or Rc.RiskIsn=r.RiskIsn)
--             and rc.newaddisn is null /*KGS 20.06.2011*/
                  ) S;
         end if;    --(r.ObjIsn>0) and  (r.RiskIsn=0)


       IF (r.ObjIsn>0) and  (r.RiskIsn>0)  then /* не все риски не все  объекты */
         Insert Into Rep_AgrRe
         (isn, loadisn,agrisn, objisn, riskisn, sectisn, reisn, sharepc, datebase, datebeg, dateend, condisn, datebegx, dateendx,
          agrxisn, OUTCOMPC, OUTCOMPC1, reclassisn, redept0isn, SubjIsn, RESHAREPC, DEPPREMUSD,
          Rate,premiumtype,EPI,NORIG,FLATPREMUSD,sDatebeg,sDateEnd,secttype,OLDSHAREPC,AgrxInsuredSum, shareneorig, AgrxInsuredCurrDate, sectcurrisn,
          agrdatebeg, agrdateend, reid)
        select Seq_Reports.NEXTVAL,pLoadIsn,S.*
        From
          (Select --+ Use_Concat(rc)
             Rc.agrisn, r.objisn, r.riskisn, r.sectisn, r.reisn, p, r.datebase,
             Nvl(rc.Datebeg,r.datebeg), Nvl(rc.Dateend,r.dateend), rc.condisn, r.Xdatebeg, r.Xdateend, r.xisn,
             com1, com2, r.reclassisn, vDept0Isn,r.SubjIsn,vSharePc,vDpremsum,r.rate,r.premiumtype,
             vEPI,r.nOrig,vFLATPREM,r.sDatebeg,r.sDateEnd,r.secttype,pOld*vOldRate, vAgrxInsuredSum, r.shareneorig, vAgrxInsuredCurrDate, r.scurrisn,
             r.datebeg, r.dateend, r.reid
           from repcond rc
           where Nvl(Rc.AgrIsn,0) = r.aisn -- чтобы в индекс по договору не попадать
             and  (Rc.ObjIsn=r.ObjIsn or Rc.ParentObjIsn=r.ObjIsn  )
              and (Rc.PARENTRISKISN=r.RiskIsn Or Rc.RiskIsn=r.RiskIsn)
--             and rc.newaddisn is null /*KGS 20.06.2011*/
                  ) S;
         end if;    --(r.ObjIsn>0) and  (r.RiskIsn=0)



      end if;
    exception When No_Data_Found THen
      Null;
    end;
--end if; --BAG!!! ---33
  end loop;
  commit;
END; -- Procedure


  CREATE OR REPLACE PROCEDURE "STORAGES"."MAKE_REPBUH2RESECTION" is
  sesId NUMBER;
  vSQL VARCHAR2(4000);
  vCnt NUMBER := 0;
  vMaxIsn NUMBER := -1;
  vMinIsn NUMBER := -1;
  vLoadObjCnt NUMBER := 10000;
BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE storages.repbuh2resection REUSE STORAGE';
  store_and_drop_table_index('storages.repbuh2resection');
  sesId:=Parallel_Tasks.createnewsession();
  LOOP
    SELECT max (agrisn)
    INTO vMaxIsn
    FROM (
          SELECT --+ index (b X_REP_AGRRE_AGR)
                 agrIsn
          FROM rep_agrre b
          WHERE agrisn > vMinIsn
            AND ROWNUM <= vLoadObjCnt
         );
    IF (vMaxIsn IS NULL) THEN EXIT; END IF;
    
    vSql:='declare
              vMinIsn number :='||vMinIsn||';
              vMaxIsn number :='||vMaxIsn||';
              vCnt    number :='||vCnt||';
           begin
             dbms_application_info.set_module(''repbuh2resection'',''Thread: ''||vCnt);
             storages.make_repbuh2resection_by_isn(vMinIsn, vMaxIsn);
           end;';
    System.Parallel_Tasks.processtask(sesid,vsql);
    vCnt:=vCnt+1;
    vMinIsn:=vMaxIsn;
    DBMS_APPLICATION_INFO.set_module('repbuh2resection','Applied: '||vCnt*vLoadObjCnt);
  END LOOP;
  Parallel_Tasks.endsession(sesid);
  restore_table_index('storages.repbuh2resection');
END;


  CREATE OR REPLACE PROCEDURE "STORAGES"."MAKE_REPBUH2RESECTION_BY_ISN" (pMinagrIsn IN NUMBER, pMaxAgrIsn IN NUMBER)
IS 
BEGIN
  pparam.clear;
  pparam.SetParamN('minagrisn', pMinagrIsn);
  pparam.SetParamN('maxagrisn', pMaxAgrIsn);
  
  DELETE FROM tt_repbuh2resection;
  
  INSERT INTO tt_repbuh2resection(
        agrisn, bodyisn, buhcurrisn, buhamount, buhamountrub, buhamountusd,
        amount, amountrub, amountusd, statcode, dateval, sagroup, rptgroupisn,
        motivgroupisn, rptclass, rptclassisn, budgetgroupisn, refundisn,
        refundextisn, reisn, sectisn, sectpc, shareneorig, reinsuranceperiodpc, reagrclassisn, buhisn
        )
  SELECT agrisn, bodyisn, buhcurrisn, buhamount, buhamountrub, buhamountusd,
         amount, amountrub, amountusd, statcode, dateval, sagroup, rptgroupisn,
         motivgroupisn, rptclass, rptclassisn, budgetgroupisn, refundisn,
         refundextisn, reisn, sectisn, sectpc, shareneorig, reinsuranceperiodpc, reagrclassisn, buhisn
  FROM v_repbuh2resection a;

  INSERT INTO repbuh2resection(
   isn, agrisn, bodyisn, buhcurrisn, buhamount, buhamountrub, buhamountusd,
   amount, amountrub, amountusd, statcode, dateval, sagroup, rptgroupisn,
   motivgroupisn, rptclass, rptclassisn, budgetgroupisn, refundisn,
   refundextisn, reisn, sectisn, sectpc, shareneorig, reinsuranceperiodpc, reagrclassisn, buhisn
   )
  SELECT seq_re.nextval, agrisn, bodyisn, buhcurrisn, buhamount, buhamountrub, buhamountusd,
        amount, amountrub, amountusd, statcode, dateval, sagroup, rptgroupisn,
        motivgroupisn, rptclass, rptclassisn, budgetgroupisn, refundisn,
        refundextisn, reisn, sectisn, sectpc, shareneorig, reinsuranceperiodpc, reagrclassisn, buhisn
  FROM tt_repbuh2resection a;

  COMMIT;

END;


  CREATE OR REPLACE PROCEDURE "STORAGES"."MAKE_REPBUHRE2DIRECTANALYTICS" 
IS
  sesId NUMBER;
  vSQL VARCHAR2(4000);
  vCnt NUMBER := 0;
  vMaxIsn NUMBER := -1;
  vMinIsn NUMBER := -1;
  vLoadObjCnt NUMBER := 50000;
BEGIN
  EXECUTE IMMEDIATE 'truncate table repbuhre2directanalytics reuse storage';
  EXECUTE IMMEDIATE 'truncate table storages.tt_buhre2directanalytics';
  EXECUTE IMMEDIATE 'truncate table storages.tt_pay_re'; -- доля перестраховщиков в выплатах
  --dbms_lock.sleep(10);
  store_and_drop_table_index('storages.repbuhre2directanalytics',1);


  DBMS_APPLICATION_INFO.set_module('repbuhre2directanalytics','fill tt_pay_re');
  -- заполнение tt_pay_re
  INSERT INTO tt_pay_re(bodyisn,
                        dateval,
                        refundisn,
                        claimcurrisn,
                        reamount,
                        repc,
                        refundextisn,
                        dateloss)
  SELECT --+ parallel ( r 32 )
    bodyisn,
    MAX(r.dateval),
    r.refundisn,
    max(r.claimcurrisn),
    sum(reamount) AS reamount, Sum(-reamount)/SUM(gcc2p(gcc2p(r.buhamount,r.BUHCURRISN,r.refundcurrisn,r.dateval),r.refundcurrisn,r.claimcurrisn,r.repdateloss)) RePc,
    r.refundextisn,
    MIN(r.repdateloss) AS dateloss
  FROM storage_source.rep_refund_payments_re r
  GROUP BY r.bodyisn,r.refundisn, r.refundextisn
  HAVING SUM(buhamount)<>0;

  --отсаживаем исх. проводки с нерасшифрованной УГ
  DBMS_APPLICATION_INFO.set_module('repbuhre2directanalytics','fill tt_buhre2directanalytics');
  INSERT  INTO tt_buhre2directanalytics(
    bodyisn, 
    dateval, 
    statcode, 
    sagroup,
    amount, amountusd, amountrub,  
    buhcurrisn, 
    buhamount,buhamountusd,buhamountrub,
    DeptIsn, 
    subaccisn
    )
    SELECT --+ full ( a ) parallel ( a 32 )
           a.bodyisn,
           MAX(a.dateval) AS dateval,
           MAX(a.statcode) AS statcode,
           MAX(a.sagroup) AS sagroup,
           SUM(a.amount) AS amount,
           SUM(a.amountusd) AS amountusd,
           SUM(a.amountrub) AS amountrub,
           MAX(a.buhcurrisn) AS currisn,
           MAX(a.buhamount) AS buhamount,
           MAX(a.buhamountusd) AS buhamountusd,
           MAX(a.buhamountrub) AS buhamountrub,
           max(a.deptisn) AS deptisn,
           max(a.subaccisn) AS subaccisn
    FROM repbuh2cond a
    WHERE a.statcode IN (27,33,35, 351, 924)
      AND a.agrclassisn IN (9018, 9058)
      AND a.refundisn IS NULL
      AND a.sagroup IN (1, 3)
     -- KGS. Типа все надо мазать на прямые договоры не смотря не на что
     -- AND a.rptgroupisn=0
    GROUP BY a.bodyisn;

  COMMIT;

  --пытаемся проставить УГ
  vMaxIsn := -1;
  vMinIsn := -1;
  vCnt := 0;
  vLoadObjCnt := 100;
  sesId:=Parallel_Tasks.createnewsession();
  LOOP
    vMaxIsn:=Cut_Table('tt_buhre2directanalytics','bodyisn',vMinIsn,pRowCount=>vLoadObjCnt);
    EXIT WHEN vMaxIsn IS NULL;

    vSql:='declare
              vMinIsn number :='||vMinIsn||';
              vMaxIsn number :='||vMaxIsn||';
              vCnt    number :='||vCnt||';
           begin
             pparam.clear;
             pparam.SetParamN(''MinIsn'', vMinIsn);
             pparam.SetParamN(''MaxIsn'', vMaxIsn);
             DBMS_APPLICATION_INFO.SET_MODULE(''repbuhre2directanalytics'',''Thread#''||vCNT);
             
             delete from storages.tt_repbuhre2directanalytics;


            insert into storages.tt_repbuhre2directanalytics
            select bodyisn,
                   dateval,statcode,sagroup,buhcurrisn,buhamount,buhamountusd,buhamountrub,
                   amount,amountusd,amountrub,agrisn,docsumisn,directpc,rptgroupisn,motivgroupisn,
                   deptisn,reisn,rptclassisn,rptclass,budgetgroupisn,subaccisn,sectisn
            from storages.v_repbuhre2directanalytics a;
            
            INSERT INTO storages.tt_repbuhre2directanalytics
            SELECT --+ use_hash ( x ) no_merge( x ) index ( a X_REPBUH2COND_BODYISN )
                   a.bodyisn,
                   max(a.dateval),
                   max(a.statcode),
                   max(a.sagroup),
                   MAX(a.buhcurrisn),
                   MAX(a.buhamount),
                   MAX(a.buhamountusd),
                   MAX(a.buhamountrub),
                   SUM(a.amount)*MAX(x.pc),
                   SUM(a.amountusd)*MAX(x.pc),
                   SUM(a.amountrub)*MAX(x.pc),
                   a.agrisn,
                   a.docsumisn,
                   1,
                   a.rptgroupisn,
                   a.motivgroupisn,
                   a.DeptIsn,
                   a.agrisn,
                   a.rptclassisn,
                   a.rptclass,
                   a.budgetgroupisn,
                   a.subaccisn,
                   to_number(null)
            FROM  (SELECT a.bodyisn, 1-CASE WHEN sum(a.amount)=0 THEN 0 ELSE sum(a.amount)/MAX(a.buhamount) END  AS pc
                   from storages.tt_repbuhre2directanalytics a 
                   GROUP BY a.bodyisn
                   HAVING CASE WHEN sum(a.amount)=0 THEN 0 ELSE round(sum(a.amount)/MAX(a.buhamount),4) END<>1 
                   UNION ALL
                   SELECT a.bodyisn, 1 AS pc
                   FROM storages.tt_buhre2directanalytics a, (SELECT DISTINCT bodyisn FROM storages.tt_repbuhre2directanalytics) b 
                   WHERE a.bodyisn>pparam.GetParamN(''MinIsn'') AND a.bodyisn<=pparam.GetParamN(''MaxIsn'')
                     AND b.bodyisn(+)=a.bodyisn AND b.bodyisn IS NULL 
                  ) x, 
                  repbuh2cond a 
            WHERE a.bodyisn>pparam.GetParamN(''MinIsn'') AND a.bodyisn<=pparam.GetParamN(''MaxIsn'')
              AND x.bodyisn=a.bodyisn
            GROUP BY a.bodyisn,a.agrisn,a.docsumisn,a.rptgroupisn,a.motivgroupisn,
                     a.DeptIsn,a.rptclassisn,a.rptclass,a.budgetgroupisn,a.subaccisn;

            insert into repbuhre2directanalytics(
              bodyisn,
              dateval,statcode,sagroup,buhcurrisn,buhamount,buhamountusd,
              buhamountrub,amount,amountusd,amountrub,agrisn,docsumisn,
              directpc,rptgroupisn,motivgroupisn,deptisn,reisn,
              rptclassisn,rptclass,budgetgroupisn,subaccisn,sectisn
            ) 
            select bodyisn,dateval,statcode,sagroup,buhcurrisn,
                   buhamount,buhamountusd,buhamountrub,amount,amountusd,
                   amountrub,agrisn,docsumisn,directpc,rptgroupisn,
                   motivgroupisn,deptisn,reisn,rptclassisn,rptclass,budgetgroupisn,subaccisn,sectisn 
            from storages.tt_repbuhre2directanalytics;
            commit;
           end;';
    System.Parallel_Tasks.processtask(sesid,vsql);

    vCnt:=vCnt+1;

    vMinIsn:=vMaxIsn;
    DBMS_APPLICATION_INFO.set_module('repbuhre2directanalytics','Applied: '||vCnt*vLoadObjCnt);

  END LOOP;

  Parallel_Tasks.endsession(sesid);

  --проводки, УГ которых беруются из repbuh2cond как есть
  INSERT  INTO repbuhre2directanalytics(bodyisn,
                                          dateval,
                                          statcode,
                                          sagroup,
                                          buhcurrisn,
                                          buhamount,
                                          buhamountusd,
                                          buhamountrub,
                                          amount,
                                          amountusd,
                                          amountrub,
                                          agrisn,
                                          docsumisn,
                                          directpc,
                                          rptgroupisn,
                                          motivgroupisn,
                                          DeptIsn,
                                          reisn, -- ??? что в этом случае ведь нет прмого договора (пока так) KGS - так всегда и было - значит правильно
                                          rptclassisn,
                                          rptclass,
                                          budgetgroupisn,
                                          subaccisn


                                          )
  SELECT --+ full ( a ) parallel ( a 32 )
         a.bodyisn,
         max(a.dateval),
         max(a.statcode),
         max(a.sagroup),
         MAX(a.buhcurrisn),
         MAX(a.buhamount) AS buhamount,
         MAX(a.buhamountusd) AS buhamountusd,
         MAX(a.buhamountrub) AS buhamountrub,

         SUM(a.amount) AS amount,
         SUM(a.amountusd) AS amountusd,
         SUM(a.amountrub) AS amountrub,

         a.agrisn,
         a.docsumisn,
         1,
         a.rptgroupisn,
         a.motivgroupisn,
         a.DeptIsn,
         a.agrisn,
         a.rptclassisn,
         a.rptclass,
         a.budgetgroupisn,
         a.subaccisn


  FROM  repbuh2cond a
  WHERE a.statcode IN (27,33,35, 351, 924)
    AND NOT (nvl(a.agrclassisn,0) IN (9018, 9058) AND a.refundisn IS NULL
    -- KGS См выше аналогичный комментарий
    --AND a.rptgroupisn=0
    )
    AND a.sagroup IN (1, 3)
  GROUP BY a.bodyisn,
           a.agrisn,
           a.docsumisn,
           a.rptgroupisn,
           a.motivgroupisn,
           a.DeptIsn,
           a.rptclassisn,
           a.rptclass,
           a.budgetgroupisn,
           a.subaccisn
           ;
  COMMIT;

  restore_table_index('storages.repbuhre2directanalytics');
END;


  CREATE OR REPLACE PROCEDURE "STORAGES"."MAKE_REPBUHRE2RESECTION" 
IS
  vMinIsn number:=-1;
  vMaxIsn number;

  vSql varchar2(4000);
  SesId Number;
  vLoadObjCnt number:=1000;
  vCnt number:=0;
BEGIN
  /*DELETE \*+ FULL ( a )*\ FROM storages.repbuhre2resection a;
  COMMIT;*/
  EXECUTE IMMEDIATE 'truncate table storages.repbuhre2resection reuse storage';

  store_and_drop_table_index('storages.repbuhre2resection',1);
  SesId:=Parallel_Tasks.createnewsession();

  LOOP
    vMaxIsn:=Cut_Table('ais.reaccsum','reaccisn',vMinIsn,pRowCount=>vLoadObjCnt);
    EXIT WHEN vMaxIsn IS NULL;
    vSql:= 'DECLARE
              vMinIsn number :='||vMinIsn||';
              vMaxIsn number :='||vMaxIsn||';
              vCnt    number :='||vCnt||';
            BEGIN
              DBMS_APPLICATION_INFO.SET_MODULE(''repbuhre2resection'',''Precess#''||vCNT);

              pparam.SetParamN(''MinIsn'', vMinIsn);
              pparam.SetParamN(''MaxIsn'', vMaxIsn);

              INSERT INTO storages.repbuhre2resection(
                                            bodyisn,dateval,statcode,subaccisn,buhcurrisn,
                                            buhdeptisn,buhamount,amount,docsumisn,docisn,
                                            dssubjisn,dscurrisn,dsclassisn,dsclassisn2,docsumpc,
                                            sectisn,secttype,sectdatebeg,sectdateend,sectcurrisn,sectpc,
                                            reaccisn,reaccdatebeg,reaccdateend,agrisn,agrclassisn,datebase,agrdatebeg,
                                            agrdateend, isrevaluation)  
              SELECT bodyisn,dateval,statcode,subaccisn,buhcurrisn,
                     buhdeptisn,buhamount,amount,docsumisn,docisn,
                     dssubjisn,dscurrisn,dsclassisn,dsclassisn2,docsumpc,
                     sectisn,secttype,sectdatebeg,sectdateend,sectcurrisn,sectpc,
                     reaccisn,reaccdatebeg,reaccdateend,agrisn,agrclassisn,datebase,agrdatebeg,
                     agrdateend, isrevaluation 
              FROM storages.v_repbuhre2resection;
              COMMIT;

           END;';
    System.Parallel_Tasks.processtask(sesid,vsql);
    vCnt:=vCnt+1;
    vMinIsn:=vMaxIsn;
    DBMS_APPLICATION_INFO.set_module('repbuhre2resection','Applied: '||vCnt*vLoadObjCnt);
  END LOOP;
  -- ждем, пока завершатся все джобы
  Parallel_Tasks.endsession(sesid);

  -- создаем индексы для указанной секции
  restore_table_index('storages.repbuhre2resection');
END;


  CREATE OR REPLACE PROCEDURE "STORAGES"."P_STORAGELOAD_SENDMAIL" (
                            vSTAGE varchar2 -- BE - начало EN - конец загрузки
                            )
as
begin

case vSTAGE
 when 'BE'
  then
   for i in (Select 'storage_info@ingos.ru' MAILADDR from dual
            /*select MAILADDR from V_STORAGELOAD_SENDMAIL*/ ) loop
    ais.smtp.Init(i.MAILADDR);
    ais.smtp.WrtM(490, i.MAILADDR, '<TR FONT face=Arial size=8>Уважаемые коллеги,</FONT><BR>', 1, 'Загрузка хранилища');
    ais.smtp.WrtM(490, i.MAILADDR, '<TR FONT face=Arial size=8>Запущена выгрузка Хранилища данных на сервере OLAP.</FONT><BR><BR>', 1, 'Тема');
    ais.smtp.WrtM(490, i.MAILADDR, '<TR FONT face=Arial size=8 style=bold><B>Просьба Приостановить проведение любых регламентных работ на сервере (о возможности возобновления будет сообщено дополнительно).</B></FONT><BR><BR>', 1, 'Тема');
    ais.smtp.WrtM(490, i.MAILADDR, '<TR FONT face=Arial size=3>С уважением,</FONT>', 4, 'Тема');
    ais.smtp.WrtM(490, i.MAILADDR, '<TR FONT face=Arial size=3>Отдел разработки отчетности.</FONT>', 4, 'Тема');
    ais.smtp.ClsM;
   end loop;
 when 'EN'
  then
   for i in (  Select 'storage_info@ingos.ru' MAILADDR from dual
       /* select MAILADDR from V_STORAGELOAD_SENDMAIL */) loop
    ais.smtp.Init(i.MAILADDR);
    ais.smtp.WrtM(490, i.MAILADDR, '<TR FONT face=Arial size=8>Уважаемые коллеги,</FONT><BR>', 1, 'Загрузка хранилища');
    ais.smtp.WrtM(490, i.MAILADDR, '<TR FONT face=Arial size=8>Выгрузка Хранилища данных на сервере OLAP закончена.</FONT><BR><BR>', 1, 'Тема');
    ais.smtp.WrtM(490, i.MAILADDR, '<TR FONT face=Arial size=3>С уважением,</FONT>', 4, 'Тема');
    ais.smtp.WrtM(490, i.MAILADDR, '<TR FONT face=Arial size=3>Отдел разработки отчетности.</FONT>', 4, 'Тема');
    ais.smtp.ClsM;
   end loop;
 else null;
end case;

null;
END;

  CREATE OR REPLACE PROCEDURE "STORAGES"."REPLOAD_U" 
   (
  pIsn In number,
  pLastisnloaded  In  number:=Null,
  pLastrundate  In date:=Null,
  pLastenddate  In date:=Null
   )
IS
JId number;
vSql Varchar2(4000);
vnullData Date:=to_date('01.01.1900','dd.mm.yyyy');

BEGIN

vSql:=' Begin
  Update
   Repload
  Set
   Lastisnloaded=Decode('||Nvl(pLastisnloaded,0)||',0,Lastisnloaded,'||Nvl(pLastisnloaded,0)||'),
   Lastrundate=Decode('''||Nvl(pLastrundate,vnullData)||''','''||vnullData||''',Lastrundate,'''||Nvl(pLastrundate,vnullData)||'''),
   Lastenddate=Decode('''||Nvl(pLastenddate,vnullData)||''','''||vnullData||''',Lastenddate,'''||Nvl(pLastenddate,vnullData)||''')
 Where Isn='||pIsn||';
Commit;
end;';


--dbms_output.put_line(substr(vSql,1,255));
--dbms_output.put_line(substr(vSql,255,255));
Dbms_Job.submit(JId,vSql,SYSDATE,null);
Commit;


END; -- Procedure

  CREATE OR REPLACE PROCEDURE "STORAGES"."SET_AGR_BUHDATE_BY_BUH" 
-- Красюков
IS
Begin
update
 Report_BuhBody_List A
Set
 agrbuhdate= (Case
              When (StatCode = 38 and DeptIsn = 707480016 and agrbuhdate is Null) Then
                nvl (A.Agrdatebeg,A.datepay)
              When (StatCode = 38 and DeptIsn = 707480016 and agrbuhdate is not Null) Then
                agrbuhdate
              When Exists(SELECT 1
                            FROM AgrClause ac
                            WHERE ac.Agrisn=a.AgrIsn
                             and ac.classisn=742561400 --c.get('clsValidAfterPayment')
                             and RowNum<=1) Then
                     (select min (datepay)
                      from docsum
                      where parentisn = docsumisn
                       and discr = 'F')
              When (Months_Between(AgrDateEnd,AgrDAteBeg)<13)
               And AgrStatus  Not in ('Д','Щ')
               ANd DeptIsn<>505
               Then Greatest(Decode(DeptIsn,11414319,Nvl(adddatebeg,AgrDatebeg),Nvl(Nvl(addsign,addDateBeg),AgrDatebeg)),Nvl(AddDatebeg,Agrdatebeg))
              Else ais.Get_Agr_BuhDate(nvl (addisn,agrisn), docsumisn, deptisn, statcode)
             end)
Where Statcode in (38,34,32,99,221,241)
      And nvl (AgrClassIsn,0)not in (9020,9058)
      And deptisn  not in (0,504,1002858925) and sagroup in (1,3);

update
 Report_BuhBody_List A
Set
 agrbuhdate= (Case
                   When Exists(SELECT 1
                            FROM AgrClause ac
                            WHERE ac.Agrisn=a.AgrIsn
                             and ac.classisn=742561400 --c.get('clsValidAfterPayment')
                             and RowNum<=1) Then
                     (select min (datepay)
                      from docsum
                      where parentisn = docsumisn
                       and discr = 'F')
              When (Months_Between(AgrDateEnd,AgrDAteBeg)<13)
               And AgrStatus Not in ('Д','Щ')
               ANd DeptIsn<>505
               Then Greatest(Decode(DeptIsn,11414319,Nvl(adddatebeg,AgrDatebeg),Nvl(Nvl(addsign,addDateBeg),AgrDatebeg)),Nvl(AddDatebeg,Agrdatebeg))
              Else ais.Get_Agr_BuhDate(nvl (addisn,agrisn), docsumisn, deptisn, statcode)

                 end)
Where Statcode in (38,34,32,99,221,241)
      And nvl (AgrClassIsn,0)not in (9020,9058)
      And deptisn  not in (0,504,707480016,1002858925)
      And agrbuhdate is Null  and sagroup in (1,3);
end;

  CREATE OR REPLACE PROCEDURE "STORAGES"."SET_BUDGETROUPISN_BY_COND" 
   IS
BEGIN
  -----------------------------------------
  -- ЛБ согласно страховому продукту (DICX)
  -----------------------------------------
/*
  UPDATE STORAGES.REP_COND_LIST a
   SET (BUDGETGROUPISN, motivgroupisn) = (SELECT MAX(Dx.classisn2),MAX(Dx.classisn2)
                                          FROM dicX dx
                                           WHERE Classisn=2366708603 -- отношение "ПРОДУКТ ДОГОВОРА=>ЛИНИЯ БИЗНЕСА (NEW)" (classisn1 - продукт, classisn2 - линия)
                                          AND (a.ruleisnagr in (Select Isn from dicti Start With Isn=classisn1 connect By Prior Isn=PArentIsn)));

KGS 11.05.2012
*/

/* код страшный, однако: от продукта идем вверх по дереву. Для каждого узла ищем настройку в DicX

Берем первую не пустую и ближайшую по уровню.
 */

  UPDATE STORAGES.REP_COND_LIST a
   SET (BUDGETGROUPISN, motivgroupisn )=
              (
              select
             Max( (SELECT MAX(Dx.classisn2)
                   FROM dicX dx
                   WHERE Classisn=2366708603
                     and ClassIsn1=D.Isn ) )  keep (dense_rank First Order by Decode((
                                                                    SELECT MAX(Dx.classisn2)
                                                                    FROM dicX dx
                                                                    WHERE Classisn=2366708603
                                                                      and ClassIsn1=D.Isn
                                                                     ),null,1,0),Level),
             Max( (SELECT MAX(Dx.classisn2)
                   FROM dicX dx
                   WHERE Classisn=2366708603
                     and ClassIsn1=D.Isn ) )  keep (dense_rank First Order by Decode((
                                                                    SELECT MAX(Dx.classisn2)
                                                                    FROM dicX dx
                                                                    WHERE Classisn=2366708603
                                                                      and ClassIsn1=D.Isn
                                                                     ),null,1,0),Level)

       from dicti d
      Start With Isn=A.ruleisnagr
      connect By Prior PArentIsn=Isn
      );


   -----------------------------------
   -- ЛБ согласно детализации продукта
   -----------------------------------
   --  ФГУП "КОСМИЧЕСКАЯ СВЯЗЬ"
  UPDATE STORAGES.REP_COND_LIST A
  SET BUDGETGROUPISN=2366821803, -- КОСМ. СТРАХ-Е ФГУП КОСМ-Я СВЯЗЬ
      motivgroupisn =2366821803
  Where a.ruleisnagr IN (SELECT d.isn
                         FROM dicti d
                         START WITH d.isn IN(683213616, 36626416)
                         CONNECT BY PRIOR d.isn=d.parentisn
                        )
    and a.AgrIsn In (Select  Agrisn  from agrext Where x1 =2314945103 );

  --  Агропромышленное страхование


  UPDATE STORAGES.REP_COND_LIST A
  SET BUDGETGROUPISN=2366843303, -- Страхование сельскохозяйственных рисков
      motivgroupisn =2366843303
  Where a.ruleisnagr IN (SELECT d.isn
                         FROM dicti d
                         START WITH d.isn=1104369003 -- ИМУЩЕСТВО ЮР. ЛИЦ
                         CONNECT BY PRIOR d.isn=d.parentisn
                        )
    and a.AgrIsn In (Select  Agrisn from agrext Where x1 =2933455203);

  --{ EGAO 06.03.2013
  /*-- KGR 11.03.2011  ДИТ-10-4-121942

  -- теперь все сельхозриски в " кроме растиениводства"
  UPDATE STORAGES.REP_COND_LIST A
  SET BUDGETGROUPISN=3290674303,
      motivgroupisn =3290674303
  Where motivgroupisn=2366843303;


-- и в "растиениводство" только тех, где растения
   UPDATE STORAGES.REP_COND_LIST A
  SET BUDGETGROUPISN=3290673703,
      motivgroupisn =3290673703
  Where motivgroupisn=3290674303 and
  objclassisn in ( SELECT d.isn
                         FROM dicti d
                         START WITH d.isn=2798546103 -- растения
                         CONNECT BY PRIOR d.isn=d.parentisn);


  -- KGR 11.03.2011  ДИТ-10-4-121942*/
  UPDATE STORAGES.REP_COND_LIST A
  SET (a.BUDGETGROUPISN, a.motivgroupisn)=(
                                           SELECT  
                                                 CASE (SELECT COUNT(1) FROM dicti d
                                                       WHERE d.isn=2798546103 -- растения
                                                       START WITH d.isn=a.objclassisn
                                                       CONNECT BY d.isn= PRIOR d.parentisn)
                                                   WHEN 1 THEN 3290673703     
                                                   ELSE 3290674303
                                                 END,   
                                                 CASE (SELECT COUNT(1) FROM dicti d
                                                       WHERE d.isn=2798546103 -- растения
                                                       START WITH d.isn=a.objclassisn
                                                       CONNECT BY d.isn= PRIOR d.parentisn)
                                                   WHEN 1 THEN 3290673703     
                                                   ELSE 3290674303
                                                 END
                                           FROM dual 
                                          ) 
  Where a.motivgroupisn=2366843303;
  
  --} EGAO 06.03.2013


  -------------------------------------
  -- ЛБ согласно условиям договоров ДКС
  -------------------------------------
  -- поправки на ДСАГО и АВТО НС

  UPDATE STORAGES.REP_COND_LIST A
  SET BUDGETGROUPISN =   CASE  (Select Nvl(Max(d.Isn),0)
                                from dicti d
                                Where D.isn in (2041,2066)
                               Start With d.isn= a.rptclassIsn
                               connect by prior parentisn=ISN
                               )
                          WHEN 2041 THEN 2366827803 -- АВТО НС
                          WHEN 2066 THEN 2366827703 -- ДСАГО
                       END,
      motivgroupisn  =  CASE (Select Nvl(Max(d.Isn),0)
                                from dicti d
                                Where D.isn in (2041,2066)
                               Start With d.isn= a.rptclassIsn
                               connect by prior parentisn=ISN
                               )
                          WHEN 2041 THEN 2366827803 -- АВТО НС
                          WHEN 2066 THEN 2366827703 -- ДСАГО
                        END
  WHERE a.BUDGETGROUPISN=2366826703 -- АВТОКАСКО
    AND a.rptclassisn IN (select ISN from dicti
                          start with isn in (2041, 2066)
                          Connect By Prior isn=Parentisn );

  --------------------------------------
  -- ЛБ согласно согласно программам ДМС
  --------------------------------------
  UPDATE STORAGES.REP_COND_LIST A
  SET a.budgetgroupisn= CASE
                          WHEN a.datebeg < TO_DATE('01.07.2006','DD.MM.YYYY') THEN
                            CASE a.bizflg
                              WHEN 'Ф' THEN
                                CASE
                                  WHEN (SELECT MAX(1)
                                        FROM dicti d
                                        WHERE d.isn IN (SELECT x.isn
                                                        FROM dicti x
                                                        START WITH x.isn=686160416
                                                        CONNECT BY PRIOR x.isn = x.parentisn AND x.isn<>840792401
                                                       )
                                        START WITH d.isn=a.ruleisnagr
                                        CONNECT BY PRIOR d.parentisn=d.isn
                                       ) IS NOT NULL THEN  2366837603 -- специальное дмс
                                  WHEN (SELECT MAX(1)
                                        FROM dicti d
                                        WHERE d.isn = 840792401
                                        START WITH d.isn=a.ruleisnagr
                                        CONNECT BY PRIOR d.parentisn=d.isn
                                       ) IS NOT NULL THEN  2366837703 -- стандартное дмс
                                END
                              ELSE
                                CASE
                                  WHEN (SELECT MAX(1)
                                        FROM dicti d
                                        WHERE d.isn IN (2317815203,3358966203)--EGAO 30.08.2011 в рамках 24383164503 (1500758503, 2293897603, 2317815203)
                                        START WITH d.isn = a.riskclassisn -- депозитные программа ДМС
                                        CONNECT BY PRIOR d.parentisn=d.isn
                                       ) IS NOT NULL THEN 2366837603 -- специальное дмс
                                  ELSE 2366837703 -- стандартное дмс
                                END
                            END
                          ELSE
                            CASE NVL((SELECT --+ index ( ext X_AGREXT_AGR )
                                      MAX(CASE ext.x1 WHEN 1083092225 THEN 1 WHEN 1083092025 THEN 2  WHEN 2513069503 THEN 2 END )
                                      FROM agrext ext
                                      WHERE ext.agrisn=a.agrisn
                                        AND ext.classisn=1071774425
                                     ),2)
                              WHEN 1 THEN 2366837603 -- ограниченный -> специальное дмс
                              WHEN 2 THEN
                                CASE
                                  WHEN (SELECT MAX(1)
                                        FROM dicti d
                                        WHERE d.isn IN (2317815203,3358966203)--EGAO 30.08.2011 в рамках 24383164503 (1500758503, 2293897603, 2317815203)
                                        START WITH d.isn=a.riskclassisn -- депозитные программа ДМС
                                        CONNECT BY PRIOR d.parentisn=d.isn
                                       ) IS NOT NULL THEN 2366837603 -- специальное дмс
                                  ELSE 2366837703 -- стандартное дмс
                                END
                            END
                        END,
      a.motivgroupisn = CASE
                          WHEN a.datebeg < TO_DATE('01.07.2006','DD.MM.YYYY') THEN
                            CASE a.bizflg
                              WHEN 'Ф' THEN
                                CASE
                                  WHEN (SELECT MAX(1)
                                        FROM dicti d
                                        WHERE d.isn IN (SELECT x.isn
                                                        FROM dicti x
                                                        START WITH x.isn=686160416
                                                        CONNECT BY PRIOR x.isn = x.parentisn AND x.isn<>840792401
                                                       )
                                        START WITH d.isn=a.ruleisnagr
                                        CONNECT BY PRIOR d.parentisn=d.isn
                                       ) IS NOT NULL THEN  2366837603 -- специальное дмс
                                  WHEN (SELECT MAX(1)
                                        FROM dicti d
                                        WHERE d.isn = 840792401
                                        START WITH d.isn=a.ruleisnagr
                                        CONNECT BY PRIOR d.parentisn=d.isn
                                       ) IS NOT NULL THEN  2366837703 -- стандартное дмс
                                END
                              ELSE
                                CASE
                                  WHEN (SELECT MAX(1)
                                        FROM dicti d
                                        WHERE d.isn IN (2317815203,3358966203)--EGAO 30.08.2011 в рамках 24383164503 (1500758503, 2293897603, 2317815203)
                                        START WITH d.isn = a.riskclassisn -- депозитные программа ДМС
                                        CONNECT BY PRIOR d.parentisn=d.isn
                                       ) IS NOT NULL THEN 2366837603 -- специальное дмс
                                  ELSE 2366837703 -- стандартное дмс
                                END
                            END

                          ELSE
                            CASE NVL((SELECT --+ index ( ext X_AGREXT_AGR )
                                      MAX(CASE ext.x1 WHEN 1083092225 THEN 1 WHEN 1083092025 THEN 2  WHEN 2513069503 THEN 2 END )
                                      FROM agrext ext
                                      WHERE ext.agrisn=a.agrisn
                                        AND ext.classisn=1071774425
                                     ),2)
                              WHEN 1 THEN 2366837603 -- ограниченный -> специальное дмс
                              WHEN 2 THEN
                                CASE
                                  WHEN (SELECT MAX(1)
                                        FROM dicti d
                                        WHERE d.isn IN (2317815203,3358966203)--EGAO 30.08.2011 в рамках 24383164503 (1500758503, 2293897603, 2317815203)
                                        START WITH d.isn=a.riskclassisn -- депозитные программа ДМС
                                        CONNECT BY PRIOR d.parentisn=d.isn
                                       ) IS NOT NULL THEN 2366837603 -- специальное дмс
                                  ELSE 2366837703 -- стандартное дмс
                                END
                            END

                        END

  WHERE a.ruleisnagr IN (SELECT isn
                         FROM dicti d
                         START WITH d.isn=686160416 -- продукты ДМС
                         CONNECT BY PRIOR d.isn = d.parentisn AND d.isn NOT IN (48654516, 1804413003, 2967557403, 2968741503, 2969117103, 2969118503)
                        )
/*KGS 29.13.2011 ЛБ "дмс (будь здоров)" не трогаем*/  and Nvl(a.motivgroupisn,0)<>2964494503 ;
END; -- Procedure


  CREATE OR REPLACE PROCEDURE "STORAGES"."SET_REFUND_BUDGETROUPISN_NEW" (vMinIsn number,vMaxIsn Number)
   IS
BEGIN

   /*Update --+index(a x_reprefund)
   RepRefund a
    Set
     BUDGETGROUPISN= --Nvl(
     (Select Nvl(Max(Dx.classisn1),0)

   from dicX dx
   Where Classisn=1604770503
   And  (classisn2=a.ruleisnagr
      Or a.ruleisnagr in (Select Isn from dicti Start With Isn=classisn2 connect By Prior Isn=PArentIsn)))
      --,
--      Case
--                     When  RptClassIsn in (Select Isn  from dicti Start With Isn=2051 connect by prior isn=ParentIsn) then 1343924403 --      Прочее имущественное страхование    Нет признаков в АИС
--                     When  RptClassIsn in (Select Isn  from dicti Start With Isn=2066 connect by prior isn=ParentIsn)  then 1343933603 -- 2,9 Гражданская ответственность (кроме тех. рисков, международных программ и 2.10,2.11 )    42
--                     end)
Where  Isn > vMinIsn and Isn<=vMaxIsn
ANd Rptgroupisn not in (747779200,755075000,755078500);
--commit;


-- поправки на ДОСАГО

   Update --+ Use_Nl(a) index(a x_reprefund)
   RepRefund a
    Set
     BUDGETGROUPISN=  1343940203
      Where  Isn > vMinIsn and Isn<=vMaxIsn And BUDGETGROUPISN=1343935803
      and exists (select 'a' from repbuh2cond b
     where b.BUDGETGROUPISN=1343940203 and b.agrisn=a.agrisn and rownum<=1)

\*      agrisn in (
     select --+ Index_combine(b X_REPBUH2COND_BUDGETGROUP)
     distinct agrisn
     from repbuh2cond b
     where BUDGETGROUPISN=1343940203
     ) *\;*/

  -- EGAO 17.11.2008 предыдущий вариант в комментариях выше

  --1. изменить линию бизнеса у тех договоров,
  --   у которых детализация продукта страхования ФГУП "КОСМИЧЕСКАЯ СВЯЗЬ" (select Agrisn from agrext...)
  UPDATE --+ use_hash ( a ) index ( a X_REPREFUND )
         RepRefund a
  SET a.BUDGETGROUPISN=2366821803, -- КОСМ. СТРАХ-Е ФГУП КОСМ-Я СВЯЗЬ
      a.motivgroupisn =2366821803
  WHERE
       a.Isn > vMinIsn AND a.Isn<=vMaxIsn -- условие гидры
   AND     BUDGETGROUPISN IS NULL
    AND a.ruleisnagr IN (SELECT d.isn
                         FROM dicti d
                         START WITH d.isn IN(683213616, 36626416)
                         CONNECT BY PRIOR d.isn=d.parentisn
                        )
    AND a.AgrIsn IN (SELECT  Agrisn  FROM agrext WHERE x1 =2314945103 )
    AND a.Rptgroupisn NOT IN (747779200,755075000,755078500); -- условие из варианта в комментариях;

  --  Агропромышленное страхование
  UPDATE --+ use_hash ( a ) index ( a X_REPREFUND )
         RepRefund a
  SET BUDGETGROUPISN=2366843303, -- Страхование сельскохозяйственных рисков
      motivgroupisn =2366843303
  WHERE
        a.Isn > vMinIsn AND a.Isn<=vMaxIsn -- oneiaea aea?u
  AND   BUDGETGROUPISN IS NULL
    AND a.ruleisnagr IN (SELECT d.isn
                         FROM dicti d
                         START WITH d.isn=1104369003 -- ИМУЩЕСТВО ЮР. ЛИЦ
                         CONNECT BY PRIOR d.isn=d.parentisn
                        )
    AND a.AgrIsn IN (SELECT  Agrisn  FROM agrext WHERE x1 =2933455203 )

    AND a.Rptgroupisn NOT IN (747779200,755075000,755078500); -- условие из варианта в комментариях;



   /*--2. изменить линию бизнеса у тех договоров, у которых детализация продукта страхования ФИКСИРОВАННОЕ ПОКРЫТИЕ (select Agrisn from agrext...)
   --   и соответствующие прдукты страхования
   UPDATE --+ use_hash ( a ) index ( a X_REPREFUND )
          RepRefund a
   Set BUDGETGROUPISN=2366837603, -- ФИКСИРОВАННЫЙ РИСК (ДМС)
       motivgroupisn =2366837603
   WHERE BUDGETGROUPISN IS NULL
     AND a.ruleisnagr IN (SELECT d.isn
                          FROM dicti d
                          START WITH d.isn IN(738374400)
                          CONNECT BY PRIOR d.isn=d.parentisn)
     AND AgrIsn IN (SELECT  Agrisn  FROM agrext Where x1 =1083092225 )
     AND a.Isn > vMinIsn AND a.Isn<=vMaxIsn -- условие гидры
     AND a.Rptgroupisn NOT IN (747779200,755075000,755078500); -- условие из варианта в комментариях




   --3. изменить линию бизнеса у тех договоров, у которых детализация продукта страхования РИСКОВЫЙ (select Agrisn from agrext...)
   --   и соответствующие прдукты страхования
   UPDATE --+ use_hash ( a ) index ( a X_REPREFUND )
          RepRefund a
   SET BUDGETGROUPISN=2366837703, -- РИСКОВОЕ СТРАХОВАНИЕ (ДМС)
       motivgroupisn =2366837703
   WHERE BUDGETGROUPISN is NULL
     AND a.ruleisnagr IN (SELECT d.isn
                          FROM dicti d
                          START WITH d.isn IN(738374400)
                          CONNECT BY PRIOR d.isn=d.parentisn
                         )
     AND AgrIsn IN (SELECT  Agrisn  FROM agrext WHERE x1 =1083092025 )
     AND a.Isn > vMinIsn AND a.Isn<=vMaxIsn -- условие гидры
     AND a.Rptgroupisn NOT IN (747779200,755075000,755078500); -- условие из варианта в комментариях*/

   -- продукты ДМС EGAO 12.02.2009
   UPDATE Reprefund a
   SET a.budgetgroupisn= CASE
                            WHEN a.agrdatebeg < TO_DATE('01.07.2006','DD.MM.YYYY') THEN
                              CASE (SELECT b.bizflg FROM repagr b WHERE b.agrisn=a.agrisn)
                                WHEN 'Ф' THEN
                                  CASE
                                     WHEN (SELECT MAX(1)
                                           FROM dicti d
                                           WHERE d.isn IN (SELECT x.isn
                                                           FROM dicti x
                                                           START WITH x.isn=686160416
                                                           CONNECT BY PRIOR x.isn = x.parentisn AND x.isn<>840792401
                                                          )
                                           START WITH d.isn=a.ruleisnagr
                                           CONNECT BY PRIOR d.parentisn=d.isn
                                          ) IS NOT NULL THEN  2366837603 -- специальное дмс
                                     WHEN (SELECT MAX(1)
                                           FROM dicti d
                                           WHERE d.isn = 840792401
                                           START WITH d.isn=a.ruleisnagr
                                           CONNECT BY PRIOR d.parentisn=d.isn
                                          ) IS NOT NULL THEN  2366837703 -- стандартное дмс
                                  END
                                ELSE
                                  CASE
                                    WHEN (SELECT MAX(1)
                                          FROM dicti d
                                          WHERE d.isn IN (2317815203,3358966203)--EGAO 30.08.2011 в рамках 24383164503 (1500758503, 2293897603, 2317815203)
                                          START WITH d.isn = a.riskclassisn -- депозитные программа ДМС
                                          CONNECT BY PRIOR d.parentisn=d.isn
                                         ) IS NOT NULL THEN 2366837603 -- специальное дмс
                                    ELSE 2366837703 -- стандартное дмс
                                  END
                              END
                            ELSE
                              CASE NVL((SELECT --+ index ( ext X_AGREXT_AGR )
                                        MAX(CASE ext.x1 WHEN 1083092225 THEN 1 WHEN 1083092025 THEN 2  WHEN 2513069503 THEN 2 END )
                                        FROM agrext ext
                                        WHERE ext.agrisn=a.agrisn
                                          AND ext.classisn=1071774425
                                       ),2)
                                WHEN 1 THEN 2366837603 -- ограниченный -> специальное дмс
                                WHEN 2 THEN
                                  CASE
                                    WHEN (SELECT MAX(1)
                                          FROM dicti d
                                          WHERE d.isn IN (2317815203,3358966203)--EGAO 30.08.2011 в рамках 24383164503 (1500758503, 2293897603, 2317815203)
                                          START WITH d.isn=a.riskclassisn -- депозитные программа ДМС
                                          CONNECT BY PRIOR d.parentisn=d.isn
                                         ) IS NOT NULL THEN 2366837603 -- специальное дмс
                                    ELSE 2366837703 -- стандартное дмс
                                  END
                              END
                          END,
        a.motivgroupisn = CASE
                            WHEN a.agrdatebeg < TO_DATE('01.07.2006','DD.MM.YYYY') THEN
                              CASE (SELECT b.bizflg FROM repagr b WHERE b.agrisn=a.agrisn)
                                WHEN 'Ф' THEN
                                  CASE
                                     WHEN (SELECT MAX(1)
                                           FROM dicti d
                                           WHERE d.isn IN (SELECT x.isn
                                                           FROM dicti x
                                                           START WITH x.isn=686160416
                                                           CONNECT BY PRIOR x.isn = x.parentisn AND x.isn<>840792401
                                                          )
                                           START WITH d.isn=a.ruleisnagr
                                           CONNECT BY PRIOR d.parentisn=d.isn
                                          ) IS NOT NULL THEN  2366837603 -- специальное дмс
                                     WHEN (SELECT MAX(1)
                                           FROM dicti d
                                           WHERE d.isn = 840792401
                                           START WITH d.isn=a.ruleisnagr
                                           CONNECT BY PRIOR d.parentisn=d.isn
                                          ) IS NOT NULL THEN  2366837703 -- стандартное дмс
                                  END
                                ELSE
                                  CASE
                                    WHEN (SELECT MAX(1)
                                          FROM dicti d
                                          WHERE d.isn IN (2317815203,3358966203)--EGAO 30.08.2011 в рамках 24383164503 (1500758503, 2293897603, 2317815203)
                                          START WITH d.isn = a.riskclassisn -- программа ДМС
                                          CONNECT BY PRIOR d.parentisn=d.isn
                                         ) IS NOT NULL THEN 2366837603 -- специальное дмс
                                    ELSE 2366837703 -- стандартное дмс
                                  END
                              END
                            ELSE
                              CASE NVL((SELECT --+ index ( ext X_AGREXT_AGR )
                                        MAX(CASE ext.x1 WHEN 1083092225 THEN 1 WHEN 1083092025 THEN 2  WHEN 2513069503 THEN 2 END )
                                        FROM agrext ext
                                        WHERE ext.agrisn=a.agrisn
                                          AND ext.classisn=1071774425
                                       ),2)
                                WHEN 1 THEN 2366837603 -- ограниченный -> специальное дмс
                                WHEN 2 THEN
                                  CASE
                                    WHEN (SELECT MAX(1)
                                          FROM dicti d
                                          WHERE d.isn IN (2317815203,3358966203)--EGAO 30.08.2011 в рамках 24383164503 (1500758503, 2293897603, 2317815203)
                                          START WITH d.isn=a.riskclassisn
                                          CONNECT BY PRIOR d.parentisn=d.isn
                                         ) IS NOT NULL THEN 2366837603 -- специальное дмс
                                    ELSE 2366837703 -- стандартное дмс
                                  END
                              END
                          END
   WHERE

        a.Isn > vMinIsn AND a.Isn<=vMaxIsn -- условие гидры
  AND a.ruleisnagr IN (SELECT isn
                          FROM dicti d
                          START WITH d.isn=686160416 -- продукты ДМС
                          CONNECT BY PRIOR d.isn = d.parentisn AND d.isn NOT IN (48654516, 1804413003, 2967557403, 2968741503, 2969117103, 2969118503)
                         )
       AND a.Rptgroupisn NOT IN (747779200,755075000,755078500); -- условие из варианта в комментариях

   -- устанавливаем значение линии бизнеса для продуктов договоров
   UPDATE --+ index ( a X_REPREFUND )
          RepRefund a
   SET BUDGETGROUPISN= (SELECT MAX(Dx.classisn2)
                        FROM dicX dx
                        WHERE Classisn=2366708603 -- отношение "ПРОДУКТ ДОГОВОРА=>ЛИНИЯ БИЗНЕСА (NEW)" (classisn1 - продукт, classisn2 - линия)
                          AND (a.ruleisnagr in (Select Isn from dicti Start With Isn=classisn1 connect By Prior Isn=PArentIsn))),
       motivgroupisn = (SELECT MAX(Dx.classisn2)
                        FROM dicX dx
                        WHERE Classisn=2366708603 -- отношение "ПРОДУКТ ДОГОВОРА=>ЛИНИЯ БИЗНЕСА (NEW)" (classisn1 - продукт, classisn2 - линия)
                          AND (a.ruleisnagr in (Select Isn from dicti Start With Isn=classisn1 connect By Prior Isn=PArentIsn)))
   WHERE BUDGETGROUPISN IS NULL
     AND a.Isn > vMinIsn AND a.Isn<=vMaxIsn -- условие гидры
     AND a.Rptgroupisn NOT IN (747779200,755075000,755078500); -- условие из варианта в комментариях



   -- поправки на ДСАГО и АВТО НС
   UPDATE --+ index ( a X_REPREFUND )
          RepRefund a
   SET BUDGETGROUPISN =  CASE a.rptclassisn
                           WHEN 2041 THEN 2366827803 -- АВТО НС
                           WHEN 2066 THEN 2366827703 -- ДСАГО
                        END,
       motivgroupisn  =  CASE a.rptclassisn
                           WHEN 2041 THEN 2366827803 -- АВТО НС
                           WHEN 2066 THEN 2366827703 -- ДСАГО
                        END
   WHERE a.BUDGETGROUPISN=2366826703 -- АВТОКАСКО
     AND a.rptclassisn IN (2041, 2066)
     AND a.Isn > vMinIsn AND a.Isn<=vMaxIsn -- условие гидры
     AND a.Rptgroupisn NOT IN (747779200,755075000,755078500); -- условие из варианта в комментариях



END; -- Procedure

  CREATE OR REPLACE PROCEDURE "STORAGES"."SET_RPTCLASS" 
   IS

BEGIN


          Update --+ Index_Asc(a x_repbuh2cond)
                tt_repBuh2Cond A
              Set
               (RptClassIsn,RptClass)=(Select
                           (Case When rptgroupisn in (755075000,755078500)
                            or AgrClassisn = 9058 /*kgs 19.07.12 письмо Дмитревской*/
                            Then
                           (select classisn2 from dicx where classisn = 49680116
                            and classisn1 = ruleisnagr) Else RptClassIsn End) RptClassIsn,
                            Nvl(
                            Nvl(
                            (Select
                            decode (D.Isn, 818752900, 818752900, 1162286003,
                            -- MSerp 30.06.2008  было 1162286003, заменил на 57687916
              57687916,


                            decode (d.parentisn,747778500, decode (nvl (rptclassisn,0),0,2051), d.filterisn))
                            From Dicti D Where Isn=RptGroupIsn),
                            (select max (isn) from dicti where parentisn = 2004
                             start with isn = Nvl(rptclassisn,
                           Case When rptgroupisn in (755075000,755078500)
                           or AgrClassisn = 9058 /*kgs 19.07.12 письмо Дмитревской*/
                           Then
                           (select classisn2 from dicx where classisn = 49680116
                            and classisn1 = ruleisnagr) end
                             )
                             connect by prior parentisn = isn)),
                       decode (deptisn,505,2051,519,2051,520,2066,11413819,2051,23735116,2041,
                       691616516,2041,707480016,2041,742950000,2066)
                       ) RptClass
                              From Dual);





END; -- Procedure

  CREATE OR REPLACE PROCEDURE "STORAGES"."SET_RPTGROUPISN_BY_BUH" 
IS

 vParam Number;
 vGroupIsn number;

 vTurGr0 number;
 vTurGr1 number;

 vCnt     number:=0;
 vCommitCount number:=300000;

FinishRPMDate Date := to_date ('28-03-2003','dd-mm-yyyy');
BEGIN

-- правило 1 - по списку договоров
--РИСКИ АРН
   Update 
    Report_BuhBody_List a
    Set
     RPTGROUPISN=755075000
    Where AGRISN IN (870705616, 870663416, 870783416, 870736216, 870657416, 870660716,
          870655816, 870658316, 870730616, 870783216, 870787016, 870655716);

-- правило 2 - по значению   classisn



--ОСТАЛЬНОЕ ВХОДЯЩЕЕ ОБЛИГАТОРНОЕ ПЕРЕСТР.
  Update
     Report_BuhBody_List a
    Set
     RPTGROUPISN=755078500
   Where Nvl(RPTGROUPISN,0)=0
   AND (AGRCLASSISN =9020 --c.get ('agrInOblig')
     Or DeptIsn = 504 and AGRDISCR = 'Г');

--EGAO 05.05.2013 в рамках ДИТ-13-2-199598
UPDATE report_buhbody_list a
SET a.rptgroupisn=755078500
WHERE nvl(a.rptgroupisn,0)=0
  AND a.agrclassisn=8746 -- ВХОДЯЩИЙ ФАКУЛЬТАТИВ
  AND a.ruleisnagr IN (SELECT isn FROM dicti d start WITH d.isn=36628616 CONNECT BY PRIOR d.isn=d.parentisn)--ОГОНЬ И ПР: СТРАХОВАНИЕ ТУРИСТОВ
  AND trunc(a.dateval)>=to_date('01.01.2013','dd.mm.yyyy');

-- правило 3 - по фиксированным сабсчетам





     Update
        Report_BuhBody_List a
       Set
       RPTGROUPISN=Decode(sagroup,3,
       Nvl(( Select --+ Index(sa)
        rptgroupisn
       from ais.subacc4dept sa
       Where  rptgroupisn is not null AND SUBACCISN =(
           Select /*+ index ( bb X_REPBUHBODY_AGR ) */ distinct First_Value(subaccisn) over (order by Abs(BuhamountUsd ) desc)
           from repbuhbody bb
            Where bb.agrisn=a.agrisn and bb.statcode=a.statcode and bb.sagroup<>3)),0),

       Nvl((Select rptgroupisn from ais.subacc4dept Where rptgroupisn is not null AND SUBACCISN=A.SUBACCISN),0))

       Where   Nvl(RPTGROUPISN,0)=0;






-- Грузы

-- Выставочные
    Select max (groupisn)
    into vGroupIsn
    from rep_tt_rules2groups
    where deptisn = 505--c.get ('CargoDept')
      And Param=2;



     Update
      Report_BuhBody_List a
       Set
       RPTGROUPISN=vGroupIsn
      Where
          DEPTISN=505--c.get ('CargoDept')
      AND RULEISNAGR=46260916 --c.get('agrCrgExhibition')
      AND Nvl(RPTGROUPISN,0)=0 ;


-- Морем и больше чем 1
    Select max (groupisn)
    into vGroupIsn
    from rep_tt_rules2groups
    where deptisn =505 --c.get ('CargoDept')
      And Param=1;

     Update
       Report_BuhBody_List a
       Set
       RPTGROUPISN=vGroupIsn
        where  DEPTISN=505--c.get ('CargoDept')
           AND Nvl(RPTGROUPISN,0)=0
           And AgrIsn In (Select AgrIsn From Rep_AgrCargo Where  sea=1 or More1=1);


-- Все остальные грузы
    Select max (groupisn)
    into vGroupIsn
    from rep_tt_rules2groups
    where deptisn = 505
      And Param=0;

     Update
      Report_BuhBody_List a
       Set
       RPTGROUPISN=vGroupIsn
      Where
          DEPTISN=505  -- CargoDept
      AND Nvl(RPTGROUPISN,0)=0;



-- Правило 4, по группе







/* согласно задаче 4396689303
-- Перестрахование, морем

     Update
      Report_BuhBody_List a
       Set
       RPTGROUPISN=755040800
      Where
          DEPTISN=c.get ('ReInsDept')  -- CargoDept
      AND Nvl(RPTGROUPISN,0)=0
      And RULEISNAGR IN (  select ISN
                           from dicti
                           start with isn =36625016
                           connect by prior  isn=parentisn);

*/

--EGAO 27.02.2012 в рамках ДИТ-12-1-160925
  UPDATE report_buhbody_list a
  SET a.rptgroupisn=755064900
  WHERE a.ruleisnagr IN (SELECT d.isn FROM dicti d start WITH d.isn=37504416 CONNECT BY PRIOR d.isn=d.parentisn) -- КОМПЛЕКСНОЕ ИПОТЕЧНОЕ СТРАХОВАНИЕ
    AND a.dateval<to_date('01.01.2012','dd.mm.yyyy')
    AND nvl(a.rptgroupisn,0)=0;

END;



  CREATE OR REPLACE PROCEDURE "STORAGES"."SET_RPTGROUPISN_BY_COND" 
IS

 vParam Number;
 vGroupIsn number;

 vTurGr0 number;
 vTurGr1 number;

 vCnt     number:=0;
 vCommitCount number:=300000;

FinishRPMDate Date := to_date ('28-03-2003','dd-mm-yyyy');
BEGIN

-- Правило 4, по группе


--cначала исключения из правил


vParam:=0;

  select Max(Nvl(groupisn,0))
  into vTurGr0 --747777700
  from
   (select level lv, isn
    from ais.rule
    start with isn = 13310616
    connect by prior parentisn = isn
    order by lv) r, ais.tt_rules2groups t
  where r.isn = t.ruleisn
   and nvl (t.param,vParam) = vParam
   And RowNum<=1;


vParam:=1;
  select Max(Nvl(groupisn,0))
  into vTurGr1 --747777600
  from
   (select level lv, isn
    from ais.rule
    start with isn = 13310616
    connect by prior parentisn = isn
    order by lv) r, ais.tt_rules2groups t
  where r.isn = t.ruleisn
   and t.param = vParam
   And RowNum<=1;


-- туристы, программа А


--Страхование туристов. Россия
       Update  --+  INDEX_COMBINE (a  X_tt_RepBuh2Cond_Dept X_tt_RepBuh2Cond_Rule ) Use_Hash(a)
        REP_COND_LIST a
       Set
        RPTGROUPISN=vTurGr1
       Where  -- DeptIsn = 707480016--c.get ('PrivDept')
               --And
               RuleIsn = 13310616
               AND Nvl(RPTGROUPISN,0)=0
               AND A.AGRISN In ( Select AgrIsn From REP_AGRTUR b Where isrussia=1);
--Страхование туристов. Не россия Россия (все остальные)

Update --+ INDEX_COMBINE (a  X_tt_RepBuh2Cond_RPTGROUPISN X_tt_RepBuh2Cond_RULE X_tt_RepBuh2Cond_DEPT)
  REP_COND_LIST a
Set
 RPTGROUPISN=vTurGr0
Where   --DeptIsn = 707480016--c.get ('PrivDept')
--And
RuleIsn = 13310616
AND Nvl(RPTGROUPISN,0)=0;


-- все остальное


       Update
       REP_COND_LIST a
        Set
         rptgroupisn=Nvl((Select groupisn  from TT_RULE_RPNGRP Where RuleIsn=A.RuleIsn and TYPERULE='COND'),0)
       Where rptgroupisn=0;


/* KGS 29.12.12. 41528597003 Согласно письму ДАР убираем
-- поправляем
       Update--+ Index (a  X_tt_RepBuh2Cond_RPTGROUPISN)
       REP_COND_LIST a
        Set
         rptgroupisn=755103500
       Where --DeptIsn=507 --c.get ('FireDept')
       --And
       rptgroupisn=747778900
       AND ObjClassIsn not in (213816,213916,214016,214116,33757016);


*/


/*KGS 29.12.12. 41528597003 Согласно письму ДАР добавляем*/

       Update--+ Index (a  X_tt_RepBuh2Cond_RPTGROUPISN)
       REP_COND_LIST a
        Set
         rptgroupisn=747778900
       Where
       RuleIsnAgr in (
3443048403,--  АУДИТОРЫ
3443049203,--  ВРАЧИ
3443048903,--  РИЭЛТОРЫ
3442999203, --  НОТАРИУС
3442996603, --  АРБИТРАЖНЫЕ УПРАВЛЯЮЩИЕ
3443050203,--  ЮРИСТЫ И АДВОКАТЫ
2904000603, --  ОТВЕТСТВЕННОСТЬ СТРОИТЕЛЕЙ (СРО)
2904001203, --  ОТВЕТСТВЕННОСТЬ ПРОЕКТИРОВЩИКОВ (СРО)
2904001603) -- ОТВЕТСТВЕННОСТЬ ИЗЫСКАТЕЛЕЙ (СРО)
;





/* KGS 27.12.2011 УГ 757271000 отменили. Вместо нее 3540195803. Настройки в DicX поменял

       Update--+ INDEX (a X_tt_RepBuh2Cond_RPTGROUPISN)
       REP_COND_LIST a
        Set
         rptgroupisn=747778800
       Where  rptgroupisn=757271000
       AND DateBeg >= FinishRPMDate;
*/


-- СТРАХОВЫЕ ПРОДУКТЫ
       Update--+ Index (a X_tt_RepBuh2Cond_RPTGROUPISN )
       REP_COND_LIST a
        Set
         rptgroupisn=Nvl((Select groupisn From TT_RULE_RPNGRP Where A.RuleIsnAgr=RuleIsn and TYPERULE='AGR'),0)
       Where rptgroupisn=0 ;

END;