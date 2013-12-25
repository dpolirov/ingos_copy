CREATE OR REPLACE PACKAGE "STORAGES"."REPORT_RZU" IS


/*ОТЧЕТ РЗУ, построение LoadReserve*/

/*построение списка убытков для РЗУ*/
Procedure Load_Refunds
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pMinIsn In Number:=0,
 pMaxIsn In Number:=0
 );



/*Загрузка платежей по убыткам*/
Procedure Load_Payments_Buh2Cond
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pMinIsn In Number:=0,
 pMaxIsn In Number:=0);


/*Загрузка старых платежей по убыткам + "не форматных" платежей */
Procedure Load_Payments_DocSum
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pRefundIsn In Number:=0);



/* загрузка истории изменения заявленной суммы и простановка суммы РЗУ в рублях*/
Procedure Load_History
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pRefundIsn In Number:=0);


/* доля престраховщиков в РЗУ*/
Procedure Load_RzuReinsOut
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pRefundIsn In Number:=0);


/* сборка конечного отчета из всех инфоструктур*/

Procedure Load_RZU
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pRefundIsn In Number:=0);


 /* процедура - построитель*/
Procedure LoadReserve
(
pLoadIsn Number,
pDateRep Date,
pStage Number:=0,
pDeptIsn IN Number := 0,
pRefundIsn In Number:=0);



/* активная загрузка на дату*/
Function GetActiveLoad(pDaterep date) return Number;


/* отчет "РЗУ" , используется для вывода в АИС*/
function get_rzu(pDeptIsn number,pDaterep Date) return report_2c.TCur;

-- EGAO 18.09.2009
--PROCEDURE Load_ResRzuRefundExt(pdaterep IN date,ploadisn IN NUMBER:= NULL);
PROCEDURE Load_resrzure(pdaterep IN date,ploadisn IN NUMBER:= NULL);
PROCEDURE Load_resrzure_ByIsn(pMinIsn number, pMaxIsn number, ploadisn IN NUMBER);

--EGAO 28.09.2012
PROCEDURE Load_ResRzuRe_Subj(pdaterep IN date, ploadisn IN NUMBER := NULL);

--EGAO 24.06.2013
PROCEDURE LoadStateReimbursementAgr(ploadisn IN NUMBER := NULL);

End;

CREATE OR REPLACE PACKAGE BODY "STORAGES"."REPORT_RZU" IS

AvtoDept0    constant Number := c.get('AvtoDept0');
MedicDept    constant Number := c.get('MedicDept');
TechRiskDept constant Number := 511;
ReInsDept    constant Number := 504;
AviaDept     constant Number := 508;
PrivDept     constant Number := 707480016;
CarrierDept  constant Number := 742950000;
DateMethodChanged constant Date:= to_date ('31-12-2002','dd-mm-yyyy');
vLocalCurr        constant Number := c.get ('LocalCurr');

Procedure Load_Refunds
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pMinIsn In Number:=0,
 pMaxIsn In Number:=0
 ) IS
Begin
  insert into tt_resrzurefund
   (isn, daterep, loadisn, refundisn,
    agrisn, condisn, currisn, claimsum, dateloss, dateclaim,
    subjisn, dateevent, deptisn, agrdatebeg, rptclassisn, lossshare,
    claimisn, datereg, emplisn, objisn, parentobjisn, franchdeducted,
    rptgroupisn, conddeptisn, sharepc, isrevaluation,LIMITSUM,LIMITCURRISN,BUDGETGROUPISN,Daterefund,rein,calcBizFlg,rptdateloss,
    riskclassisn, rptclass, agrruleisn, ruleisn , objclassisn, clientjuridical,
    clientorgformisn, agrclassisn, refundid, refundclassisn, claimcurrisn, clientisn
    --{EGAO 24.06.2013
    ,parentobjclassisn
    ,issub
    --}EGAO 24.06.2013 
    ,claimsumfull -- EGAO 20.11.2013
  )
  select
  seq_reports.NextVal, pDateRep, pLoadIsn,S.*
  From(
  Select --+ Ordered USe_Nl( r ag)
    refundisn,
    Max(Decode(agrextisn,null,agrisn,ragrisn)) agrisn,
    Max(Decode(agrextisn,null,condisn,null)) CondIsn,
    Max(currisn),

    Sum(
    case when  nvl(claimsum,0) -
            decode (nvl (conddeptisn,deptisn),519,0,520,0,MedicDept,0,nvl (FranchDed,0)) <
            decode (nvl (conddeptisn,deptisn),AvtoDept0,0,nvl (refundsum,0))
            then
                case when refundsum is not null then
                        decode(nvl(conddeptisn,deptisn),AvtoDept0, greatest(0, nvl(refundsum,0)- nvl(FranchDed,0)), nvl(refundsum,0)) --EGAO 03.06.2011 письмо от Дмитревской nvl(refundsum,0)
                        when nvl(dateevent,datereg)>'01-sep-2006'
                             or nvl(datereg,dateevent)>'01-sep-2006'
                             or nvl (conddeptisn,deptisn) in (519, 520,MedicDept)
                         then 0
                        else
                         nvl(claimsum,0) end
            else case when (nvl(dateevent,datereg)>'01-sep-2006'
                             or nvl(datereg,dateevent)>'01-sep-2006')
                            and  nvl (conddeptisn,deptisn) not in (MedicDept)
                            then nvl(claimsum,0) - nvl (FranchDed,0)

                        else nvl(claimsum,0) end
                end)  claimsum,
    Min(dateloss) dateloss,
    Min(trunc (greatest (nvl (dateclaim,datereg),nvl (dateevent,nvl (dateclaim,datereg))))) dateclaim,
    Max(subjisn),
    Min(dateevent),
    Nvl(Max(deptisn),0),
    Min(agrdatebeg),
    Nvl(Max(rptclassisn),Nvl(Max(RptClass),2051)),
    Max(lossshare),
    Max(claimisn),
    Min(datereg),
    Max(Decode(agrextisn,null,emplisn,0)),
    Max(Decode(agrextisn,null,objisn,0)),
    Max( Decode(agrextisn,null,parentobjisn,0)),
    Sum(nvl (FranchDed,0)),
    Nvl(Max(rptgroupisn),0),
    Max(nvl (conddeptisn,GET_DEPT0ISN(DeptIsn))),
    Max(nvl (lossshare,100)/100),
    Max(isrevaluation),
    Sum(LIMITSUm),
    Max(LIMITCURRISN),
    Max(BUDGETGROUPISN),
    Max(Daterefund),
    Nvl( Max(agrclassisn),0),
    Max(calcBizFlg),
    Min(REPDATELOSS),

    Nvl( Max(riskclassisn),0),
    Nvl( Max(RptClass),2051),
    Nvl( Max(RULEISNAGR),0) agrruleisn,

    Nvl(Max(RiskRULEISN),0) RULEISN ,
    Nvl(Max(OBJCLASSISN),0),

    Nvl(Max(clientjuridical),'N'),
    NVL(MAX(clientorgformisn),0) AS clientorgformisn, -- EGAO 24.06.2013 0 CLIENTORGFORMISN,
    Nvl( Max(agrclassisn),0) agrclassisn
    ,MAX(decode(agrextisn,NULL,refundid)) AS refundid
    ,MAX(decode(agrextisn,NULL,classisn)) AS refundclassisn
    ,MAX(claimcurrisn) AS claimcurrisn
    ,MAX(clientisn) AS clientisn
    --{EGAO 24.06.2013
    ,Nvl(Max(parentobjclassisn),0)
    ,MAX(IsSub) AS IsSub
    --}EGAO 24.06.2013
    ,SUM(s.claimsum) AS claimsumfull -- EGAO 20.11.2013
 From
 (
  Select --+ Ordered Use_Nl(ag ext sb) index ( r X_REPREFUND_REFUNDISN )
   r.*,/*ag.classisn agrclassisn,*/ ag.clientjuridical, ag.clientisn,
    Greatest(Decode(rfranchsum,null,
        nvl (Gcc2.Gcc2(decode (nvl (franchtype,'Б'),'Б',decode (franchtariff,null,franchsum)),franchcurrisn,r.currisn,pDaterep),0)+
    nvl (claimsum*decode (nvl (franchtype,'Б'),'Б',franchtariff),0)/100, --franchdeducted
  Gcc2.Gcc2(greatest(rfranchsum,0),rfranchcurrisn,r.currisn,pDaterep)),0) FranchDed,
  calcBizFlg,

                            Nvl(Nvl((Select  decode (D.Isn, 818752900, 818752900, 1162286003,
                             57687916,decode (d.parentisn,747778500, decode (nvl (rptclassisn,0),0,2051), d.filterisn))
                            From Dicti D Where Isn=RptGroupIsn),
                            (select max (isn) from dicti where parentisn = 2004
                             start with isn = Nvl(rptclassisn,
                           Case When rptgroupisn in (755075000,755078500) or AgrClassisn = 9058 /*kgs 19.07.12 письмо Дмитревской*/ Then
                           (select classisn2 from dicx where classisn = 49680116
                            and classisn1 = Ag.RULEISN) end
                             ) connect by prior parentisn = isn)),
                       decode (r.deptisn,505,2051,519,2051,520,2066,11413819,2051,23735116,2041,
                       691616516,2041,707480016,2041,742950000,2066)
                       ) RptClass

  --{EGAO 24.06.2013
  ,decode(ext.agrisn,null,0,1) AS IsSub
  ,sb.orgformisn AS clientorgformisn
  --}EGAO 24.06.2013
  from reprefund r ,repagr ag, rzustatereimbursementagr ext, repSubject sb
  where
  r.refundisn>pMinIsn AND r.refundisn<=pMaxIsn AND -- гидра
  -- дата заявления должна быть до отчетной даты

  trunc (greatest (
          nvl (dateclaim,datereg),nvl (dateevent,nvl (dateclaim,datereg)),Decode(nvl (conddeptisn,r.deptisn),MedicDept,Datereg,nvl (dateclaim,datereg)))) <= pDateRep

  -- признаная сумма больше 0 или заявленная-франшиза(которая в дог-ре) больше 0
    and
    greatest (nvl (claimsum,0)-decode (conddeptisn,519,0,520,0,MedicDept,0,
    Decode(rfranchsum,null,nvl (franchdeducted,0),0)),
      decode (condDeptIsn,AvtoDept0,0,MedicDept,0,nvl (refundsum,0))) > 0

     -- статус не урегулирован или дата урегулирования больше отчетной
    and (r.status = 'N' and claimstatus = 'N' or r.status in ('N','Y') and claimstatus in ('N','Y')
    and decode(nvl (conddeptisn,r.deptisn),MedicDept,nvl (datesolution,daterefund), -- у медиков берем только datesolution
     trunc (least (nvl (daterefund,datesolution),nvl (datesolution,daterefund)))) > pDateRep)

    -- жизнь убираем
    and nvl (r.firmisn,492) = 492
    and Nvl(conddeptisn,0)<>1002858925

    -- тестовые убытки убираем
    and not (nvl (upper (claimid),'A') like '%TEST%')
    And NVL(NRZU,'N')='N'
    and r.agrisn=ag.agrisn(+)
    --{EGAO 24.06.2013
    AND ext.agrisn(+)=r.agrisn
    AND ext.loadisn(+)=ploadisn
    and ag.clientisn=sb.isn(+)
    --}EGAO 24.06.2013
   )S
   Group by RefundIsn) S
  where claimsum>0 ;
  commit;


  /* проставляем INSTYPEISN в буффер*/
  --EGAO 24.06.2013 со слов Гоши уже не нужно set_rzu_instype(pMinIsn, pMaxIsn);
  commit;
End;

Procedure Load_Payments_Buh2Cond
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pMinIsn In Number:=0,
 pMaxIsn In Number:=0) IS
Begin

  insert into tt_resrzurefund_payments
    (isn, DateRep, LoadIsn, DeptIsn, refundisn, bodyisn, dateval, statcode, buhcurrisn, buhamount, buhamountrub)
  select --+ use_nl (r b) ordered
    SEQ_REPORTS.NextVal, pDateRep, pLoadIsn,S.*
  From
  (
  Select --+ Ordered USe_Nl(r b) index ( b X_REPBUH2COND_REFUNDISN ) index ( r X_TT_RESRZUREFUND_REFUNDISN )
        nvl (conddeptisn,0) conddeptisn,
        b.refundisn,
        b.bodyisn,
        max(b.dateval) AS dateval,
        max(b.statcode) AS statcode,
        max(b.buhcurrisn) AS buhcurrisn,
        Sum(b.amount) ,
        Sum(b.amountrub)
  from   tt_resrzurefund r, repbuh2cond b
  WHERE r.refundisn>pMinIsn AND r.refundisn<=pMaxIsn
    AND r.refundisn = b.refundisn
    and b.statcode in (220,24)
    and b.dateval <= pDateRep
    and sagroup in (1,3,2)
   Group by  nvl (conddeptisn,0) ,
             b.refundisn,
             b.bodyisn/*,
             b.dateval,
             b.statcode,
             b.buhcurrisn*/
  )S;
  commit;
End;

Procedure Load_Payments_DocSum
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pRefundIsn In Number:=0) IS
vDateStart Date := to_date ('01-01-2002','dd-mm-yyyy');
Begin

  delete from tt_resrzurefund_payments;
/*  where daterep = pDateRep
--    and deptisn <> MedicDept
    and docsumisn is not null
    and (pDeptIsn = 0 or pDeptIsn = deptisn)
    and (pRefundIsn=0 or RefundIsn=pRefundIsn);--and loadisn = pLoadIsn
  commit;
*/
  insert into tt_resrzurefund_payments
    (isn, DateRep, LoadIsn, DeptIsn, refundisn, docsumisn, dateval, buhcurrisn, buhamount, buhamountrub)
  select --+ use_hash (r d) ordered
    SEQ_REPORTS.NextVal, pDateRep, pLoadIsn, nvl (conddeptisn,0), d.refundisn, d.docsumisn, datepay, d.currisn,
--    decode (d.discr,'F',1,'P',-1)*amount, decode (d.discr,'F',1,'P',-1)*amountrub
    amount, amountrub
  from  tt_resrzurefund r, repdocsum d
  where
--            r.daterep = pDateRep
--         and r.loadisn = pLoadIsn
--    and nvl (r.conddeptisn,0) <> MedicDept
--    ANd
    r.refundisn = d.refundisn
    and d.classisn = 445
    and d.discr = 'F'
    and d.datepay <= pDateRep
    and (nvl (d.statcode,0) = 0 or nvl (d.dateval,vDateStart-1) < vDateStart);
--    and (pDeptIsn = 0 or pDeptIsn = conddeptisn)
--    and R.RefundIsn=pRefundIsn;
--    and (pRefundIsn=0 or R.RefundIsn=pRefundIsn);
  commit;
End;

Procedure Load_Payments_Medic
(pDateRep IN Date,
 pLoadIsn IN Number,
 pRefundIsn In Number:=0) IS
Begin
  delete from resrzurefund_payments
  where daterep = pDateRep
    and deptisn = MedicDept
    and (pRefundIsn=0 or RefundIsn=pRefundIsn);
  commit;
  insert into resrzurefund_payments
    (isn, DateRep, LoadIsn, DeptIsn, refundisn, docsumisn, dateval, buhcurrisn, buhamount, buhamountrub)
  select --+ use_nl (r d) index (d X_REPDOCSUM_REFUND) ordered
    SEQ_REPORTS.NextVal, pDateRep, pLoadIsn, MedicDept, d.refundisn, d.docsumisn,
    decode (d.discr,'F',d.datepay,'P',nvl (d.dateval,pDateRep+1)), d.currisn,
    decode (d.discr,'F',1,'P',-1)*amount, decode (d.discr,'F',1,'P',-1)*amountrub
  from resrzurefund r, repdocsum d
  where r.daterep = pDateRep
    and r.loadisn = pLoadIsn
    and r.conddeptisn = MedicDept
    and r.refundisn = d.refundisn
    and d.classisn = 445
    and (d.docisn2 is null and d.discr = 'F' and d.datepay > pDateRep)
    and (pRefundIsn=0 or R.RefundIsn=pRefundIsn);
--         d.docisn2 is null and d.discr = 'P' and d.datepay >= add_months (pDateRep,-12) and d.dateval is null);
--         d.docisn2 is not null and d.discr = 'P' and d.dateval > pDateRep);
  commit;
End;



Procedure Load_History
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pRefundIsn In Number:=0) IS
vDate Date := pDateRep+1;
Begin
  --Простановка признака переоценки
  --{EGAO 11.03.2013
  /*update tt_resrzurefund r
  set isrevaluation = 1
  where --r.daterep = pDateRep
--    and r.loadisn = pLoadIsn
--    and
     nvl (isrevaluation,-1) = -1
    and (pDeptIsn = 0 or pDeptIsn = conddeptisn)
    and (pRefundIsn=0 or R.RefundIsn=pRefundIsn)
    and exists (select isn
     from tt_resrzurefund_payments
     where refundisn = r.refundisn
     and daterep = pDateRep
     and loadisn = pLoadIsn
     and buhcurrisn <> 35)
    and not exists
    (select isn
     from tt_resrzurefund_payments
     where refundisn = r.refundisn
     and daterep = pDateRep
     and loadisn = pLoadIsn
     and buhcurrisn = 35);
--    and nvl (conddeptisn,0) <> MedicDept;*/
  update tt_resrzurefund r
  set r.isrevaluation = 1
  where 1=1
    AND nvl (r.isrevaluation,-1) = -1
    and (pDeptIsn = 0 or pDeptIsn = r.conddeptisn)
    and (pRefundIsn=0 or R.RefundIsn=pRefundIsn)
    and r.refundisn IN (
                        select x.refundisn
                        from tt_resrzurefund_payments x
                        where 1=1
                          and x.daterep = pDateRep
                           and x.loadisn = pLoadIsn
                        GROUP BY x.refundisn
                        HAVING COUNT(decode(x.buhcurrisn,35,1))=0 AND  COUNT(decode(x.buhcurrisn,35,NULL,1))<>0
                       );
  --}EGAO 11.03.2013                     
  commit;
/*
  delete from resrzurefund_hist
  where daterep = pDateRep
    and (pDeptIsn = 0 or pDeptIsn = deptisn)
    and (pRefundIsn=0 or RefundIsn=pRefundIsn);
  commit;
*/


/*delete from tt_resrzurefund_hist;

  insert into tt_resrzurefund_hist (isn, daterep, loadisn, refundisn, datebeg, diff, deptisn)
  select SEQ_REPORTS.NextVal, pDateRep, pLoadIsn, refundisn, recdatebeg,
    claimsum-nvl (LAG (claimsum) OVER (PARTITION BY refundisn ORDER BY recdatebeg),0) diff,
    deptisn
  from (select --+ use_Nl (r h) ordered
    r.refundisn, r.conddeptisn deptisn, greatest (h.recdatebeg,r.dateclaim) recdatebeg,





    decode (sign (h.recdateend-vDate),1,r.claimsum,

case when  nvl(h.claimsum,0) -
            decode (nvl (r.conddeptisn,r.deptisn),519,0,520,0,MedicDept,0,nvl (r.franchdeducted,0)) <
            decode (nvl (r.conddeptisn,r.deptisn),AvtoDept0,0,nvl (h.refundsum,0))
            then
                case when h.refundsum is not null then
                        nvl(h.refundsum,0)
                        when nvl(h.dateevent,h.datereg)>'01-sep-2006'
                             or nvl(h.datereg,h.dateevent)>'01-sep-2006'
                             or nvl (r.conddeptisn,r.deptisn) in (519, 520,MedicDept)
                         then 0
                        else
                         nvl(h.claimsum,0) end
            else case when (nvl(h.dateevent,h.datereg)>'01-sep-2006'
                             or nvl(h.datereg,h.dateevent)>'01-sep-2006')
                            and  nvl (r.conddeptisn,r.deptisn) not in (MedicDept)
                            then nvl(h.claimsum,0) - nvl (r.franchdeducted,0)

                        else nvl(h.claimsum,0) end
                end




\*
     nvl (decode (sign (nvl (h.claimsum,0)-
     decode (r.conddeptisn,519,0,520,0,medicdept,0,nvl (r.franchdeducted,0))-

     decode (r.condDeptIsn,AvtoDept0,0,nvl (h.refundsum,0))),-1,h.refundsum,h.claimsum-
    -- вот такая врезка. убытки до 01.09.2006 - франшизу не вычитаем, после - вычитаем
           (case When nvl (r.dateclaim,r.datereg)>'01-sep-2006'
           and conddeptisn<>medicdept then nvl (r.franchdeducted,0) else 0 end)),0)
*\
           )
           --)
      claimsum







  from tt_resrzurefund r, reprefund_hist h
  where
  --r.daterep = pDateRep
--    and r.loadisn = pLoadIsn
--    and
    nvl (r.conddeptisn,0) <> MedicDept
    and nvl (isrevaluation,0) < 1
    and r.currisn <> 35
  --  and (pDeptIsn = 0 or pDeptIsn = conddeptisn)
    and r.refundisn = h.refundisn
    and r.currisn = h.currisn
  --  and (pRefundIsn=0 or  R.RefundIsn=pRefundIsn)
    and not exists (select --+ index (h X_REPREFUND_HIST_REFUND)
    isn from reprefund_hist
    where refundisn = h.refundisn
      and recdatebeg > h.recdatebeg
      and (currisn <> h.currisn or recdatebeg <= greatest (DateMethodChanged,r.dateclaim))));
  commit;

  update tt_resrzurefund r set
   (revalcode, claimsumrub) =
   (select 'RUR' revalcode, sum (gcc2.gcc2 (diff,r.currisn,35,greatest (DateMethodChanged,least (pDateRep,datebeg))))
    from tt_resrzurefund_hist
    where refundisn = r.refundisn
      and daterep = pDateRep)
  where
  -- r.daterep = pDateRep
--    and r.loadisn = pLoadIsn
--    and
     nvl (r.conddeptisn,0) <> MedicDept
    and nvl (isrevaluation,0) < 1
    and r.currisn <> 35;
  --  and (pDeptIsn = 0 or pDeptIsn = conddeptisn)
    --and (pRefundIsn=0 or RefundIsn=pRefundIsn);
  commit;*/

  update tt_resrzurefund r set
   (revalcode, claimsumrub) =
   (select decode (currisn,35,'RUB','USD'),
    gcc2.gcc2(claimsum,currisn,35,decode (isrevaluation,1,pDateRep,greatest (DateMethodChanged,r.dateclaim)))
    from dual)
  where r.daterep = pDateRep
    and r.loadisn = pLoadIsn
--    and nvl (r.conddeptisn,0) <> MedicDept
    and r.claimsumrub is null
    and (pDeptIsn = 0 or pDeptIsn = conddeptisn)
    and (pRefundIsn=0 or RefundIsn=pRefundIsn);
  commit;
End;


Procedure Load_RzuReinsOut
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pRefundIsn In Number:=0) IS
Begin
  --{EGAO 26.06.2013
  /*delete \*+ index ( a X_RESRZUREFUND_RE_DATEREP )*\ from resrzurefund_re a
  where a.daterep = pDateRep
   and (pDeptIsn = 0 or pDeptIsn = a.deptisn)
   and (pRefundIsn=0 or a.RefundIsn=pRefundIsn);-- and loadisn = pLoadIsn;*/
  DELETE /*+ index ( a X_RESRZUREFUND_RE_LOAD )*/
         FROM resrzurefund_re a
  WHERE a.loadisn=pLoadIsn
   and (pDeptIsn = 0 or pDeptIsn = a.deptisn)
   and (pRefundIsn=0 or a.RefundIsn=pRefundIsn);
  --}EGAO 26.06.2013
  commit;
/*
  insert into resrzurefund_re
  (isn, daterep, loadisn, deptisn, refundisn, agrxisn, sectisn, reisn, reagrdeptisn, reagrclassisn, sharepc, omitted,
  refundextisn,SUBJISN,sharepcNew)
  select SEQ_REPORTS.NextVal, daterep, loadisn, conddeptisn, refundisn, agrxisn, sectisn, reisn, reagrdeptisn, reagrclassisn,
    decode (secttype, 'XL', xref*decode (nvl (resumfull,0),0,1/resumcnt,resum/resumfull), null, sharepc, xref) sharepc,
    nvl (decode (reisn, null, 'N',
      decode (reagrclassisn, 9018, 'N', 55232916, 'N',
        decode (deptisn, TechRiskDept, 'N', ReInsDept, 'N', AviaDept, 'N',
          decode (ais.getdept (reagrdeptisn),ReInsDept,'Y','N')))),'Y'),
    refundextisn,SUBJISN,
    (Select  Sum(re.xref)from storage_source.reprefund_re_New re where re.refundisn=r.refundisn
    and Nvl(re.agrxisn,0)=Nvl(re.agrxisn,0))

    from (select --+ use_hash (r re) ordered
    r.daterep, r.loadisn, r.conddeptisn, r.refundisn, r.conddeptisn deptisn,refundextisn,
    agrxisn, sectisn, secttype, reisn, reagrdeptisn, reagrclassisn, resum*RefPc resum, xref, re.SHAREPC,
    SUM (resum*RefPc) OVER (PARTITION BY r.refundisn, agrxisn) resumfull,
    COUNT (*) OVER (PARTITION BY r.refundisn, agrxisn) resumcnt,
    re.SUBJISN
  from tt_resrzurefund r, reprefund_re re
  where --r.loadisn = pLoadIsn
--    and r.daterep = pDateRep
--    and
     r.refundisn = re.refundisn) r;
--    and (pDeptIsn = 0 or pDeptIsn = r.conddeptisn)
--   and (pRefundIsn=0 or R.RefundIsn=pRefundIsn));
commit;
*/

  insert into resrzurefund_re
  (isn, daterep, loadisn, deptisn, refundisn, agrxisn, sectisn, reisn, reagrdeptisn, reagrclassisn, sharepc, omitted,
  refundextisn,SUBJISN,SECTTYPE)
  select
    SEQ_REPORTS.NextVal, daterep, loadisn, conddeptisn, refundisn, agrxisn, sectisn, reisn, reagrdeptisn, reagrclassisn,
 xref sharepc,
'N',
    refundextisn,SUBJISN,SECTTYPE
    from (
    select --+ use_hash (r re) ordered Full(r) Parallel(r,32)
    r.daterep, r.loadisn, r.conddeptisn, r.refundisn, r.conddeptisn deptisn,r.refundextisn,
    agrxisn, sectisn, secttype, reisn, reagrdeptisn, reagrclassisn,  XREF,
    re.SUBJISN
  from tt_resrzurefund r,
 storage_source.reprefund_re_New re
  where --r.loadisn = pLoadIsn
--    and r.daterep = pDateRep
--    and
     r.refundisn = re.refundisn
     AND nvl(r.refundextisn,0)=NVL(re.refundextisn,0)
     ) r;

commit;

null;


End;

Procedure Load_RZU
(pDateRep IN Date,
 pLoadIsn IN Number,
 pDeptIsn IN Number := 0,
 pRefundIsn In Number:=0) IS

 vPart    Varchar2(150);
 vColList Varchar2(32000);



Begin
/* delete from resrzufull
  where daterep = pDateRep
    and (pDeptIsn = 0 or pDeptIsn = deptisn )
    and (pRefundIsn=0 or RefundIsn=pRefundIsn);-- and loadisn = pLoadIsn;
  commit;*/

--vPart:=INIT_PARTITION_BY_KEY('resrzufull',pLoadIsn);
vPart:=INIT_PARTITION_BY_KEY(pTableName => 'resrzufull',pKey =>pLoadIsn,pCompress => 0);

 if pDeptIsn in (0, ReInsDept, CarrierDept) and (pRefundIsn = 0) then
  insert into resrzufull (isn, loadisn, daterep, docisn, deptisn, rptgroupisn, agrisn, currisn,
    id, currcode, code, revalcode, claimdate, claimsum, rzucurr, rzufull, agrid, insuredsum, rzu, rzuoutrsbu, rzuact,emplname,rein,
    rzuoutmsfo, emplisn, agrcurrisn, condpc, clientisn 
    --{EGAO 24.06.2013
    ,agrruleisn
    ,rptclass
    ,clientjuridical
    ,clientorgformisn
    ,issub
    --}EGAO 24.06.2013
    ,claimsumfull -- EGAO 20.11.2013
    )
  select --+ use_nl (m d c a emp ext sb ) ordered
         SEQ_REPORTS.NextVal, pLoadIsn, pDateRep, docisn,
         --{EGAO 29.08.2013
         /*(select isn from subdept where parentisn = 0
              start with isn = m.deptisn
              connect by prior parentisn = isn)*/
         nvl(
             (
              SELECT --+ index ( bc X_REPBUH2COND_AGRISN )
              MAX(bc.deptisn) KEEP (dense_rank LAST 
                                 ORDER BY SUM(decode(bc.statcode,34,bc.amountrub)) NULLS FIRST,
                                          abs(SUM(decode(bc.statcode,24,bc.amountrub))) NULLS FIRST, 
                                          SUM(decode(bc.statcode,34,null,24,null,1)) NULLS FIRST)
              FROM repbuh2cond bc
              WHERE bc.agrisn=m.agrisn
              GROUP BY deptisn
             ),
             --{EGAO 20.09.2013
             /*(select isn from subdept where parentisn = 0
              start with isn = m.deptisn
              connect by prior parentisn = isn)*/
              ReInsDept
              --}EGAO 20.09.2013
              ) deptisn,
         --}EGAO 29.08.2013
         rptgroupisn, m.agrisn, m.currisn, d.id, c.code, c.code, decode (c.isn,35,'RUB','USD'),
         m.dateloss, m.claimsum, m.claimsum, m.claimsum*1.03, a.id, a.insuredsum,
         gcc2.gcc2 (m.claimsum*1.03,m.currisn,35,pDateRep),
         gcc2.gcc2 (m.reinspc*1.03,m.currisn,35,pDateRep),
         gcc2.gcc2 (m.claimsum,m.currisn,35,pDateRep),
         emp.shortname,1,
         gcc2.gcc2 (m.reinspc,m.currisn,35,pDateRep),-- EGAO 26.07.2011 gcc2.gcc2 (m.reinspc*1.03,m.currisn,35,pDateRep),
         d.userown,            -- EGAO 11.08.2008
         nvl(a.currisn, c.isn) -- EGAO 11.08.2008
         ,1 -- EGAO 07.06.2011
         ,a.clientisn -- EGAO 24.04.2013
         --{EGAO 24.06.2013
         ,NVL(m.ruleisnagr,0)
         ,NVL(Nvl(Nvl((Select  decode (D.Isn, 818752900, 818752900, 1162286003, 57687916,decode (d.parentisn,747778500, decode (nvl (m.rptclassisn,0),0,2051), d.filterisn))
                   From Dicti D 
                   Where Isn=m.RptGroupIsn
                  ),
                  (select max (isn) 
                   from dicti 
                   where parentisn = 2004
                   start with isn = Nvl(m.rptclassisn,
                                        Case When m.rptgroupisn in (755075000,755078500) or nvl(a.classisn,0) = 9058 /*kgs 19.07.12 письмо Дмитревской*/ Then
                                          (select classisn2 
                                           from dicx 
                                           where classisn = 49680116 and classisn1 = m.ruleisnagr
                                          ) 
                                        END)
                   connect by prior parentisn = isn)
                 ),
                 (SELECT /*+ index ( bc X_REPBUH2COND_AGRISN ) */ MAX(bc.rptclass) 
                  FROM repbuh2cond bc 
                  WHERE bc.agrisn=m.agrisn
                 )
             ),0) RptClass,
             nvl(sb.juridical,'N'),
             NVL(sb.orgformisn,0) AS clientorgformisn,
             decode(ext.agrisn,null,0,1) AS IsSub
         --}EGAO 24.06.2013
         ,m.claimsum -- EGAO 20.11.2013
  from reprzumemo m, docs d, currency c, agreement a,subject emp, rzustatereimbursementagr ext, repSubject sb
  where dateloss <= pDateRep
    and not exists (select 0 from resrzumemo_storned s where s.docisn = m.docisn)
    and m.docisn = d.isn
    and m.currisn = c.isn
    and m.agrisn = a.isn (+)
    and (pDeptIsn = 0 or pDeptIsn = (select isn from subdept where parentisn = 0
    start with isn = m.deptisn
    connect by prior parentisn = isn))
  and d.userown=emp.Isn(+)
  --{EGAO 24.06.2013
    AND ext.agrisn(+)=m.agrisn
    AND ext.loadisn(+)=ploadisn
    and a.clientisn=sb.isn(+)
  --}EGAO 24.06.2013
  ;
  commit;
 end if;




  insert into resrzufull(
    isn, loadisn, daterep, refundisn, deptisn, rptgroupisn,
    agrisn, id, objname, emplname, currcode,
    claimdate, claimsum, rzucurr, rzufull,
    agrid, insuredsum, shortname, code, rzu, claimisn,  revalcode,
    rzuact, currisn, dateloss,budgetgroupisn,datereg,
    daterefund,dateevent,rein,calcbizflg,rzuoutrsbu, rzuoutmsfo,repdateloss,
    emplisn, objisn, subjisn, agrcurrisn,instypeisn,
    refundextisn, refundagrisn, condisn,
    rzuoutpc,
    condpc, recalculationtype, insuredsumcurrisn, refundid, refundclassisn, riskclassisn, agrruleisn, claimcurrisn, clientisn, lossshare
    --{EGAO 24.06.2013
    ,rptclass
    ,ruleisn
    ,parentobjclassisn
    ,objclassisn
    ,issub
    ,clientjuridical
    ,clientorgformisn
    ,rptclassisn
    --}EGAO 24.06.2013
    ,claimsumfull -- EGAO 20.11.2013
    )
  SELECT --+ ordered use_nl ( r c e s rt o a cr ) no_merge ( r ) use_hash ( xl )
         seq_reports.nextval, pLoadIsn, pDateRep, r.refundisn, r.deptisn, r.rptgroupisn,
         r.agrisn, c.id id, o.name objname,
         e.shortname emplname, cr.code currcode,
         r.claimdate, r.claimsum*r.condpc,
         r.rzucurr*condpc, r.rzufull*r.condpc,
         a.id agrid, r.limitsum*r.condpc, s.shortname, rt.code, r.rzu*r.condpc, r.claimisn,  r.revalcode,
         r.rzuact*r.condpc, r.currisn, r.dateloss, r.budgetgroupisn, r.Datereg,
         r.Daterefund, r.Dateevent, r.rein, r.calcBizFlg, 0 AS rzuoutrsbu,0 AS rzuoutmsfo, r.rptdateloss,
         c.emplisn, -- EGAO 11.08.2008
         o.isn,     -- EGAO 11.08.2008
         c.subjisn, -- EGAO 11.08.2008
         cr.isn,    -- EGAO 11.08.2008
         r.instypeisn,
         r.refundextisn, r.refundagrisn, r.condisn,
         decode(nvl(r.claimrzuact,0),0,1/r.claimcnt,r.rzuact*r.condpc/r.claimrzuact),
         r.condpc,
         --{EGAO 31.10.2012
         /*max(r.recalculationtype) over (PARTITION BY r.claimisn)*/
         nvl2(xl.claimisn,1,0) AS recalculationtype
         --}
         ,r.limitcurrisn, r.refundid, r.refundclassisn, r.riskclassisn, agrruleisn, r.claimcurrisn, r.clientisn, r.lossshare
         --{EGAO 24.06.2013
         ,r.rptclass
         ,r.ruleisn
         ,r.parentobjclassisn
         ,r.objclassisn
         ,r.issub
         ,r.clientjuridical
         ,r.clientorgformisn
         ,r.rptclassisn
         --}EGAO 24.06.2013
         , r.claimsumfull*r.condpc -- 20.11.2013 
  FROM (
        select --+ use_nl (r rf) ordered index ( rf X_REPREFUND_REFUNDISN )
               r.refundisn, r.deptisn, r.rptgroupisn, r.agrisn,
               r.claimdate, r.claimsum, r.rzucurr, r.rzufull, r.limitsum, r.rzu, r.claimisn,  r.revalcode, r.rzuact,
               r.currisn, r.dateloss, r.budgetgroupisn,r.Datereg, r.Daterefund, r.Dateevent, r.rein,
               r.calcBizFlg, r.rptdateloss, r.objisn, r.INSTYPEISN, rf.agrextisn AS refundextisn,
               rf.agrisn AS refundagrisn, rf.condisn,
               NVL(rf.condpc,0) AS condpc,
               SUM(r.rzuact*NVL(rf.condpc,0)) over (PARTITION BY r.claimisn) AS claimrzuact,
               COUNT(1) over (PARTITION BY r.claimisn) AS claimcnt
               --{EGAO 31.10.2012
               /*(SELECT \*+ index ( re X_REP_AGRRE_AGR )*\
                       COUNT(1)
                FROM  rep_agrre re
                WHERE re.agrisn=rf.agrisn
                 AND (re.objisn=0 OR  re.condisn=rf.condisn)
                 AND (re.riskisn=0 OR re.condisn=rf.condisn)
                 AND re.secttype IN ('XL', 'RX')
                 AND ROWNUM<=1
               ) AS recalculationtype -- EGAO 26.07.2011*/
               --}
               ,r.limitcurrisn, r.refundid, r.refundclassisn, r.riskclassisn, r.agrruleisn, r.claimcurrisn, r.clientisn, r.lossshare
               --{EGAO 24.06.2013
               ,r.rptclass
               ,r.ruleisn
               ,r.parentobjclassisn
               ,r.objclassisn
               ,r.issub
               ,r.clientjuridical
               ,r.clientorgformisn
               ,r.RPTCLASSISN
               --}EGAO 24.06.2013
               ,r.claimsumfull -- EGAO 20.11.2013  
        FROM (SELECT  r.refundisn, r.deptisn, r.rptgroupisn,
                      r.agrisn,
                      r.claimdate, r.claimsum*nvl (r.sharepc,1) claimsum,
                      r.claimsum*nvl (r.sharepc,1)-r.paidsum rzucurr,
                      (r.claimsum*nvl (r.sharepc,1)-r.paidsum)*1.03 rzufull,
                      r.LIMITSUM,
                      1.03*r.rzu rzu,
                      r.claimisn,  r.revalcode, r.rzuact,
                      r.currisn, r.dateloss,r.budgetgroupisn,r.Datereg, r.Daterefund, r.Dateevent, r.rein,
                      r.calcBizFlg,
                      r.rptdateloss,
                      r.objisn,
                      r.INSTYPEISN,
                      r.limitcurrisn, refundid, r.refundclassisn, r.riskclassisn, r.agrruleisn, r.claimcurrisn, r.clientisn, r.lossshare
                      --{EGAO 24.06.2013
                      ,r.rptclass
                      ,r.ruleisn
                      ,r.parentobjclassisn
                      ,r.objclassisn
                      ,r.issub
                      ,r.clientjuridical
                      ,r.clientorgformisn
                      ,r.RPTCLASSISN
                      --}EGAO 24.06.2013
                      ,r.claimsumfull*nvl (r.sharepc,1) AS claimsumfull -- EGAO 20.11.2013                     
              from (select --+ use_nl (r d) index (d X_RESRZUREFUND_PAYMENTS_REFUND) ordered full ( r ) parallel ( r 32 )
                          r.refundisn,
                          greatest (r.dateclaim,r.dateevent) claimdate, r.claimsum AS claimsum ,
                          r.currisn,
                          r.rptgroupisn, r.conddeptisn deptisn,
                          Max(r.sharepc) sharepc,
                          r.revalcode,
                          nvl (r.dateevent,r.dateloss) dateloss,
                          -nvl (sum (gcc2.gcc2 (d.buhamount, d.buhcurrisn, r.currisn,decode (r.isrevaluation,1,pDateRep,d.dateval))),0) paidsum,
                          r.claimsumrub*r.sharepc+
                          nvl (sum (decode (r.isrevaluation, 1,
                                    gcc2.gcc2 (1, r.currisn, 35, pDateRep)*
                                    gcc2.gcc2 (d.buhamount, d.buhcurrisn, r.currisn, d.dateval),
                                    gcc2.gcc2 (d.buhamount, d.buhcurrisn, 35, pDateRep))),0) rzu,
                          gcc2.gcc2 (1, r.currisn, 35, pDateRep)*
                                        (r.claimsum*r.sharepc+
                                         nvl (sum (gcc2.gcc2 (d.buhamount, d.buhcurrisn, r.currisn, d.dateval)),0)) rzuact,
                          r.limitsum,r.budgetgroupisn,
                          r.ClaimIsn,r.ObjIsn,r.AgrIsn,r.Datereg,r.Daterefund,r.Dateevent,
                          Decode(r.rein,8746,1,35146816,1,9020,1) rein,
                          r.calcBizFlg,
                          MAX(r.rptdateloss) rptdateloss,
                          r.instypeisn
                          , MAX(r.limitcurrisn) AS limitcurrisn, MAX(r.refundid) AS refundid, MAX(r.refundclassisn) AS refundclassisn 
                          ,MAX(r.riskclassisn) AS riskclassisn, MAX(r.agrruleisn) AS agrruleisn
                          ,MAX(r.claimcurrisn) AS claimcurrisn
                          ,MAX(r.clientisn) AS clientisn
                          ,MAX(r.lossshare) AS lossshare
                          --{EGAO 24.06.2013
                          ,r.rptclass
                          ,r.ruleisn
                          ,r.parentobjclassisn
                          ,r.objclassisn
                          ,max(r.issub) AS issub
                          ,max(r.clientjuridical) AS clientjuridical
                          ,max(r.clientorgformisn) AS clientorgformisn
                          ,r.RPTCLASSISN
                          --}EGAO 24.06.2013
                          ,MAX(r.claimsumfull) AS claimsumfull -- EGAO 20.11.2013
                    from tt_resrzurefund r, tt_resrzurefund_payments d
                    WHERE r.claimsumrub*r.sharepc > 0
                      and r.refundisn = d.refundisn (+)
                    group by r.refundisn, greatest (r.dateclaim,r.dateevent),
                             r.claimsum, r.currisn, r.rptgroupisn, r.conddeptisn,
                             r.sharepc, r.claimsumrub, r.revalcode,
                             nvl (r.dateevent,r.dateloss),
                             r.LIMITSUM,r.BUDGETGROUPISN,r.ClaimIsn,r.ObjIsn,r.AgrIsn,r.Datereg,
                             r.Daterefund,r.Dateevent,Decode(r.rein,8746,1,35146816,1,9020,1) ,calcBizFlg,
                             INSTYPEISN
                             --{EGAO 24.06.2013
                             ,r.rptclass
                             ,r.ruleisn
                             ,r.parentobjclassisn
                             ,r.objclassisn
                             ,r.RPTCLASSISN
                             --}EGAO 24.06.2013
                    having
                   (round(decode (r.conddeptisn,PrivDept,0.95,1)*r.claimsum*nvl (r.sharepc,1) +
                    nvl (sum (gcc2.gcc2 (d.buhamount, d.buhcurrisn, r.currisn, decode (r.isrevaluation,1,pDateRep,d.dateval))),0),2) > 0)
                    and (round(r.claimsumrub*r.sharepc+decode (r.conddeptisn,PrivDept,1.05,1)*
                        nvl (sum (decode (r.isrevaluation, 1,
                        gcc2.gcc2 (1, r.currisn, 35, pDateRep)*
                        gcc2.gcc2 (d.buhamount, d.buhcurrisn, r.currisn, d.dateval),
                        gcc2.gcc2 (d.buhamount, d.buhcurrisn, 35, pDateRep))),0),2) > 0)
                  ) r
             ) r,
             reprefund rf
        WHERE rf.refundisn=r.refundisn
  ) r,  agrclaim c, subject e, subject s, currency rt,
  agrobject o,   agreement a, currency cr,
  tt_resrzureclaim2rxxlsection xl -- EGAO 31.10.2012
  WHERE r.currisn = rt.isn(+)
    AND r.claimisn = c.isn (+)
    AND c.emplisn = e.isn (+)
    AND c.subjisn = s.isn (+)
    AND r.objisn  = o.isn (+)
    AND r.agrisn = a.isn (+)
    AND a.currisn = cr.isn (+)
    AND r.claimisn=xl.claimisn(+)
  ;
  commit;

  REBULD_TABLE_INDEX('storages.resrzufull',pPartititon=>vPart,pIsBitmap=>1);

--{EGAO 20.11.2013 Теперь доля считается здесь: make_refundpayment_re (вызывается в report_rzu.LoadReserve 
/*-- проставляем новую долю
Load_resrzure(pdaterep, ploadisn);

--доля перестраховщиков в РЗУ по РСБУ
UPDATE \*+ Full(rr)  Parallel(rr,32) *\
storages.resrzufull rr
SET rzuoutrsbu=CASE
                 WHEN rr.rzu=0 OR rr.rzuact=0 OR nvl(rr.rzuoutpc,0)=0 THEN 0
                 ELSE rr.rzuoutpc*NVL((SELECT \*+ index ( x X_RESRZURE_CLAIM )*\
                                              x.rzuout
                                       FROM resrzure x
                                       WHERE x.loadisn=rr.loadisn
                                         AND x.claimisn=rr.claimisn),0)*rr.rzu/rr.rzuact
               END
WHERE rr.loadisn=pLoadisn AND Nvl(refundisn,0)>0
  AND rr.recalculationtype=1 -- EGAO 26.07.2011
;
--{ EGAO 26.07.2011 --доля перестраховщиков в РЗУ по РСБУ
UPDATE  \*+ Full(rr)  Parallel(rr,32) *\
storages.resrzufull rr
SET rzuoutrsbu=NVL((SELECT \*+ index ( x X_RESRZURE_REFUND )*\ x.rzuout
                    FROM resrzure x
                    WHERE x.loadisn=rr.loadisn
                      AND x.refundisn=rr.refundisn
                      AND NVL(x.refundextisn,0)=NVL(rr.refundextisn,0)),0)
WHERE rr.loadisn=pLoadisn AND Nvl(refundisn,0)>0
  AND rr.recalculationtype=0
;
--}

--доля перестраховщиков в РЗУ по МСФО
UPDATE \*+ Full(rr)  Parallel(rr,32) *\
storages.resrzufull rr
SET rr.rzuoutmsfo=CASE
                   WHEN NVL(rr.rzuact,0)=0 OR NVL(rr.rzu,0)=0 OR nvl(rr.rzuoutrsbu,0)=0 THEN 0
                   ELSE rr.rzuoutrsbu*rr.rzuact/rr.rzu
                 END
WHERE rr.loadisn=pLoadisn AND Nvl(refundisn,0)>0
;



COMMIT;
*/
--}EGAO 20.11.2013


/*-- EGAO 18.09.2009 Сумма перестрахования убытков, относящихся к услугам длговоров ДМС
Load_ResRzuRefundExt(pdaterep, ploadisn);

-- vColList:=storages.REP_COGNOS_UTILS.get_not_null_columns_list ('storages.resrzufull_cube');

Execute Immediate 'truncate table storages.resrzufull_cube';
 execute immediate
 'insert into storages.resrzufull_cube
 select * from
    (select
        daterep,
        deptisn,
        rptgroupisn,
        currisn,
        decode(rein,1,24,220) statcode,
        sum(rzu)rzu,
        sum(rzuout)rzuout
     from
        resrzufull
     where
        loadisn = (select report_rzu.getactiveload(daterep) from dual)
     group by
        daterep,
        deptisn,
        rptgroupisn,
        currisn,
        rein
     )
 ';
-- отправляем сообщение о том, что таблицу надо перегрузить
rep_message.put(p_recipient => 'COGNOS', p_object => 'REP_RES_RZU_FULL');*/

commit;

End;




Procedure LoadReserve
(
pLoadIsn Number,
pDateRep Date,
pStage Number:=0,
pDeptIsn IN Number := 0,
pRefundIsn In Number:=0)
IS
vStage Number:=pStage;
sesid number;
vSql varchar(4000);

vMinIsn number:=-9999;
vMaxIsn number;
vLoadObjCnt number:=100000;
vPayObjCnt NUMBER := 5000;
vCnt number:=0;
vXLRXObjCnt NUMBER := 1000;

BEGIN

EXECUTE IMMEDIATE 'truncate TABLE tt_resrzurefund'; -- для гидры
EXECUTE IMMEDIATE 'truncate TABLE tt_resrzurefund_payments'; -- для гидры
EXECUTE IMMEDIATE 'truncate TABLE tt_resrzureclaim2rxxlsection'; -- EGAO 31.12.2012


IF (pStage=0) and (pRefundIsn = 0) Then
Select Nvl(Max(Lastisnloaded),pStage)
Into vStage
from Repload
Where Isn=pLoadIsn;
end if;

    DBMS_APPLICATION_INFO.set_module('Rzu','');


  replog_i (pLoadIsn, 'LoadRZU', 'LoadResrve',pAction=>'Begin');


  if (vStage = 0) then

    replog_i (pLoadIsn, 'LoadRZU', 'Prepare',pAction=>'Begin');

    if (pRefundIsn = 0) then
--     sesid:=parallel_tasks.createnewsession;

--      vSql:='Begin  REPORT_BUH_STORAGE_NEW.setrefundrptgroup; end;';
--    parallel_tasks.processtask(sesid,vsql);

      REPORT_BUH_STORAGE_NEW.loadrzumemo(ploadisn);
--      parallel_tasks.processtask(sesid,vsql);


--     parallel_tasks.endsession(sesid);
    else
     delete from tt_rowid;
     insert into tt_rowid (isn) values (pRefundIsn);
     commit;
     REPORT_BUH_STORAGE_NEW.LoadRefund_By_TT_RowId(pLoadIsn, 0);
     REPORT_BUH_STORAGE_NEW.LoadRepRefund_Hist_By_TT_RowId(pLoadIsn, 0);
     REPORT_BUH_STORAGE_NEW.setrefundrptgroup (pRefundIsn);
    end if;
    RepLoad_U(pLoadIsn,pLastisnloaded=>1);
    replog_i (pLoadIsn, 'LoadRZU', 'Prepare',pAction=>'End');
  end if;


  if (vStage <= 1) THEN
    --{EGAO 24.06.2013
    replog_i (pLoadIsn, 'LoadRZU', 'LoadStateReimbursementAgr',pAction=>'Begin');
    LoadStateReimbursementAgr(ploadisn);
    replog_i (pLoadIsn, 'LoadRZU', 'LoadStateReimbursementAgr',pAction=>'End');
    --}EGAO 24.06.2013
    
    replog_i (pLoadIsn, 'LoadRZU', 'Load_Refunds',pAction=>'Begin');

    --==
    --начало гидры
    --==

    sesid:=parallel_tasks.createnewsession;
    vMinIsn := -1;
    vCnt := 0;
    LOOP

      vMaxIsn:=Cut_Table('storage_source.reprefund','refundisn',vMinIsn,pRowCount=>vLoadObjCnt);

      EXIT WHEN vMaxIsn IS NULL;

      vSql:= 'DECLARE
                vMinIsn number :='||vMinIsn||';
                vMaxIsn number :='||vMaxIsn||';
                vLoadIsn number :='||pLoadIsn||';
                vDeptIsn number :='||pDeptIsn||';
                vDateRep DATE:= TO_DATE('''||to_char(pDateRep,'dd.mm.yyyy')||''',''dd.mm.yyyy'');
                vCnt    number :='||vCnt||';
              BEGIN
                DBMS_APPLICATION_INFO.SET_MODULE(''RZU. Refund loading....'',''Precess#''||vCNT);

                report_rzu.Load_Refunds (vDateRep, vLoadIsn, vDeptIsn, vMinIsn, vMaxIsn);

                COMMIT;
             END;';

      System.Parallel_Tasks.processtask(sesid,vsql);

      vCnt:=vCnt+1;

      vMinIsn:=vMaxIsn;
      DBMS_APPLICATION_INFO.set_module('RZU loading...','Applied: '||vCnt*vLoadObjCnt);

    END LOOP;

    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);
    --==
    --конец гидры
    --==


    --Load_Refunds (pDateRep, pLoadIsn, pDeptIsn, pRefundIsn);

    --{EGAO 31.10.2012
    sesid:=parallel_tasks.createnewsession;
    vMinIsn := -1;
    vCnt := 0;
    LOOP

      vMaxIsn:=Cut_Table('storages.tt_resrzurefund','claimisn',vMinIsn,pRowCount=>vXLRXObjCnt);

      EXIT WHEN vMaxIsn IS NULL;

      vSql:= 'DECLARE
                vMinIsn number :='||vMinIsn||';
                vMaxIsn number :='||vMaxIsn||';
                vLoadIsn number :='||pLoadIsn||';
                vCnt    number :='||vCnt||';
              BEGIN
                DBMS_APPLICATION_INFO.SET_MODULE(''RZU. XL-RX info loading....'',''Precess#''||vCNT);

                INSERT INTO tt_resrzureclaim2rxxlsection(claimisn,loadisn)
                SELECT a.claimisn, vLoadIsn
                FROM (SELECT /*+ index ( a X_TT_RESRZUREFUND_CLAIM )*/
                             DISTINCT claimisn
                      FROM tt_resrzurefund a
                      WHERE a.claimisn>vMinIsn and a.claimisn<=vMaxIsn
                     ) a,
                     table(Ais.reinsn.refundretbl_Olap(a.claimisn)) x,
                     resection s
                WHERE s.isn=x.sectisn AND s.secttype IN (''XL'', ''RX'')
                GROUP BY a.claimisn
                HAVING SUM(x.xpc)>0;


                COMMIT;
             END;';

      System.Parallel_Tasks.processtask(sesid,vsql);

      vCnt:=vCnt+1;

      vMinIsn:=vMaxIsn;
      DBMS_APPLICATION_INFO.set_module('RZU loading...','Applied: '||vCnt*vXLRXObjCnt);

    END LOOP;

    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);
    --}конец EGAO 31.10.2012


    RepLoad_U(pLoadIsn,pLastisnloaded=>2);
    replog_i (pLoadIsn, 'LoadRZU', 'Load_Refunds',pAction=>'End');
  end if;



  --EGAO 24.11.2010 в рамках ДИТ-10-4-122685
  --закомментировал кусок кода if (vStage <= 2) then...end if
  /*if (vStage <= 2) then
    replog_i (pLoadIsn, 'LoadRZU', 'Load_Payments_DocSum',pAction=>'Begin');

    Load_Payments_DocSum (pDateRep, pLoadIsn, pDeptIsn, pRefundIsn);
--    Load_Payments_Medic (pDateRep, pLoadIsn);

    RepLoad_U(pLoadIsn,pLastisnloaded=>4);
    replog_i (pLoadIsn, 'LoadRZU', 'Load_Payments_DocSum',pAction=>'End');
  end if;*/


  if (vStage <= 3) then
    replog_i (pLoadIsn, 'LoadRZU', 'Load_Payments_Buh2Cond',pAction=>'Begin');

    --==
    --начало гидры
    --==

    sesid:=parallel_tasks.createnewsession;
    vMinIsn := -1;
    vCnt := 0;
    LOOP

      vMaxIsn:=Cut_Table('storages.tt_resrzurefund','refundisn',vMinIsn,pRowCount=>vPayObjCnt);

      EXIT WHEN vMaxIsn IS NULL;

      vSql:= 'DECLARE
                vMinIsn number :='||vMinIsn||';
                vMaxIsn number :='||vMaxIsn||';
                vLoadIsn number :='||pLoadIsn||';
                vDeptIsn number :='||pDeptIsn||';
                vDateRep DATE:= TO_DATE('''||to_char(pDateRep,'dd.mm.yyyy')||''',''dd.mm.yyyy'');
                vCnt    number :='||vCnt||';
              BEGIN
                DBMS_APPLICATION_INFO.SET_MODULE(''RZU. payment loading...'',''Precess#''||vCNT);

                report_rzu.Load_Payments_Buh2Cond (vDateRep, vLoadIsn, vDeptIsn, vMinIsn, vMaxIsn);

                COMMIT;
             END;';

      System.Parallel_Tasks.processtask(sesid,vsql);

      vCnt:=vCnt+1;

      vMinIsn:=vMaxIsn;
      DBMS_APPLICATION_INFO.set_module('RZU loading...','Applied: '||vCnt*vPayObjCnt);

    END LOOP;

    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);
    --==
    --конец гидры
    --==


    --Load_Payments_Buh2Cond (pDateRep, pLoadIsn, pDeptIsn, pRefundIsn);

    RepLoad_U(pLoadIsn,pLastisnloaded=>3);
    replog_i (pLoadIsn, 'LoadRZU', 'Load_Payments_Buh2Cond',pAction=>'End');
  end if;



  if (vStage <= 4) then
    replog_i (pLoadIsn, 'LoadRZU', 'Load_History',pAction=>'Begin');

    Load_History (pDateRep, pLoadIsn, pDeptIsn, pRefundIsn);

    RepLoad_U(pLoadIsn,pLastisnloaded=>5);
    replog_i (pLoadIsn, 'LoadRZU', 'Load_History',pAction=>'End');
  end if;


  if (vStage <= 5) then
    replog_i (pLoadIsn, 'LoadRZU', 'Load_RzuReinsOut',pAction=>'Begin');

    Load_RzuReinsOut (pDateRep, pLoadIsn, pDeptIsn, pRefundIsn);

    RepLoad_U(pLoadIsn,pLastisnloaded=>6);
    replog_i (pLoadIsn, 'LoadRZU', 'Load_RzuReinsOut',pAction=>'End');
  end if;

  if (vStage <= 6) then
    replog_i (pLoadIsn, 'LoadRZU', 'Load_RZU',pAction=>'Begin');

    Load_RZU (pDateRep, pLoadIsn, pDeptIsn, pRefundIsn);

    RepLoad_U(pLoadIsn,pLastisnloaded=>7);
    replog_i (pLoadIsn, 'LoadRZU', 'Load_RZU',pAction=>'End');
  end if;

  IF vStage <=7 THEN
    replog_i (pLoadIsn, 'LoadRZU', 'ResRzuRe_Subj',pAction=>'Begin');
    Load_ResRzuRe_Subj(pDateRep, pLoadIsn);

    RepLoad_U(pLoadIsn,pLastisnloaded=>8);
    replog_i (pLoadIsn, 'LoadRZU', 'ResRzuRe_Subj',pAction=>'End');
  END IF;

  --{EGAO 01.11.2013
  IF vStage <=8 THEN
    replog_i (pLoadIsn, 'LoadRZU', 'make_refundpayment_re',pAction=>'Begin');
    make_refundpayment_re(ploadisn);
    
    DELETE FROM tt_refundpayment_re4resrzufull a WHERE a.loadisn=ploadisn;
    INSERT INTO tt_refundpayment_re4resrzufull(loadisn, refundisn,refundextisn,reshare)
    SELECT /*+ full ( x ) parallel ( x 32 ) */
           x.loadisn, x.refundisn, x.refundextisn, LEAST(1,greatest(0,nvl(SUM(x.reshare),0))) AS reshare
    FROM refundpayment_re x
    WHERE x.loadisn=ploadisn
      AND x.buhtype=2
    GROUP BY x.loadisn, x.daterep, x.refundisn, x.refundextisn
    HAVING LEAST(1,greatest(0,nvl(SUM(x.reshare),0)))<>0;
    
    UPDATE resrzufull a
    SET (a.rzuoutrsbu, a.rzuoutmsfo)= (SELECT --+ index ( t X_TT_REFUNDPAYMENT_RE4RESRZU )
                                              a.rzu*t.reshare, a.rzuact*t.reshare
                                       FROM tt_refundpayment_re4resrzufull t
                                       WHERE t.loadisn=a.loadisn
                                         AND t.refundisn=a.refundisn
                                         AND nvl(t.refundextisn,0)=nvl(a.refundextisn,0))
    WHERE a.loadisn=ploadisn
      AND a.refundisn IS NOT NULL; -- исключили меморандумы                              
    
    COMMIT;
    
    
    RepLoad_U(pLoadIsn,pLastisnloaded=>9);
    replog_i (pLoadIsn, 'LoadRZU', 'make_refundpayment_re',pAction=>'End');  
  END IF;  
  --}EGAO 01.11.2013

commit;



  -- экспорт данных для РДТЕХ
--  EXPORT_DATA.export_to_owb_by_loadisn(ploadisn,'resrzufull');



end;


Function GetActiveLoad(pDaterep date) return Number
 Is
  vLoad Number;
Begin

  Select Min(isn)
  Into vLoad
  from repload
  Where PROCISN=12 and Daterep=pDaterep and classIsn=1;

 Return vLoad;
end;


function get_rzu(pDeptIsn number,pDaterep Date) return report_2c.TCur
Is
 vCur report_2c.TCur;
begin

Open vCur For
Select * from (
select
Daterep "Дата отчета",
(Select nvl(sd.abbreviation,sd.shortname) from subdept sd Where Isn=DeptIsn) "Департамент" ,
(Select d.code||' '||d.Shortname from dicti d Where Isn=rptgroupisn) "Учетная группа",
r.id "№ убытка",
DATELOSSclm "Дата убытка",
r.dateevent "Дата претензии",
r.DATECLAIM "Дата заявления убытка",
r.objname "Объект убытка",
r.emplname "Куратор",
r.agrId "№ договора",
r.shortname "Страхователь",
r.insuredsum "Страх. сумма",
r.currcode "Валюта договора",
r.code "Валюта убытка",
r.claimSUM "Заявл. сумма",
rzu "РЗУ (Руб)",
r.rzucurr "РЗУ (Валюта)",
rzuoutrsbu "РЗУ,перестр.(Руб)",
decode (rein,1,rzu)  "РЗУ,входящее(Руб)",
decode (rein,1,rzuoutrsbu) "РЗУ,входящее,перестр.(Руб)",
decode (nvl (rzu,0),0,0,rzuoutrsbu/rzu) "Доля перестр.%",
extid "Внешний номер",
calcbizflg "Признак бизнеса"

from
(


Select --+ Index_Combine(rf x_resrzufull_Daterep ) ordered Use_Nl( rf ag ac)
      rf.*,ac.extid,ac.dateclaim,ac.dateloss DATELOSSclm,ac.datereg  dateregclm
from resrzufull rf,agrclaim ac,repagr ag
Where   rf.LoadISn = (Select storages.report_rzu.getactiveload(pDaterep) From dual)
And (rf.Deptisn in (Select Isn from subdept start with isn=pDeptIsn connect by prior isn=Parentisn))
And rf.claimisn=ac.isn(+)
And rf.refundisn is not null
and rf.agrisn=ag.agrisn(+)


)   r
Union all


Select --+ Ordered Index_Combine(rf x_resrzufull_Daterep ) ordered Use_Nl( rf ds sb ag)
Daterep "Дата отчета",
(Select nvl(sd.abbreviation,sd.shortname) from subdept sd Where Isn=rf.DeptIsn) "Департамент" ,
(Select d.code||' '||d.Shortname from dicti d Where Isn=rptgroupisn) "Учетная группа",
Substr(conc(rf.id||'; '),1,255) "№ убытка",
null "Дата убытка",
null "Дата претензии",
null "Дата заявления убытка",
null "Объект убытка",
Max(emplname) "Куратор",
Max(Ag. id) "№ договора",
Max(sb.shortname) "Страхователь",
null "Страх. сумма",
null "Валюта договора",
rf.CODE "Валюта убытка",
Sum(claimSUM) "Заявл. сумма",
Sum(rzu) "РЗУ (Руб)",
Sum(rzucurr) "РЗУ (Валюта)",
Sum(rzuoutrsbu) "РЗУ,перестр.(Руб)",
null  "РЗУ,входящее(Руб)",
null "РЗУ,входящее,перестр.(Руб)",
null "Доля перестр.%",
null"Внешний номер",
null "Признак бизнеса"
from resrzufull rf,docs ds,subject sb,repagr ag
Where   rf.LoadISn = (Select storages.report_rzu.getactiveload(pDaterep) From dual)
And ( rf.Deptisn in (Select Isn from subdept start with isn=pDeptIsn connect by prior isn=Parentisn))
And refundisn is null
and rf.docisn=ds.isn
and ds.recisn=sb.Isn(+)
and rf.agrisn=ag.agrisn(+)
group by daterep,rf.deptisn,ds.recisn,rf.code,rptgroupisn,rf.agrisn
)
Order by "Учетная группа","Дата убытка";
return vCur;
end;

/*-- EGAO 18.09.2009
PROCEDURE Load_ResRzuRefundExt(pdaterep IN date, ploadisn IN NUMBER := NULL)
IS
  vMinIsn     number:=-9999;
  vMaxIsn     number;
  vSql        varchar2(4000);
  SesId       Number;
  vLoadIsn NUMBER := pLoadIsn;
  vLoadObjCnt number:=100000;
  vCnt        number:=0;
  vPart    Varchar2(150);
BEGIN
  IF vloadisn IS NULL THEN
    vloadisn:=GetActiveLoad(pDaterep => pdaterep);
  END IF;
  IF vloadisn IS NOT NULL THEN
    SesId:=Parallel_Tasks.createnewsession;
    vPart:=INIT_PARTITION_BY_KEY('resrzurefundext',vLoadIsn);
    LOOP
      vMaxIsn:=Cut_Table('storages.resrzufull','refundisn',vMinIsn,pRowCount=>vLoadObjCnt);
      EXIT WHEN vMaxIsn IS NULL;
      vSql:=' declare
                vMinIsn   number :='||vMinIsn||';
                vMaxIsn   number :='||vMaxIsn||';
                vCnt      number := '||vCnt||';
                vLoadisn  number := '||vloadisn||';
                vDateRep  Date   :=TO_DATE('''||TO_CHAR(pDateRep,'dd.mm.yyyy')||''',''dd.mm.yyyy'');
              Begin

                DBMS_APPLICATION_INFO.set_module(''fill resrzurefundext'',''THREAD: ''||vCnt);

                INSERT INTO resrzurefundext(refundisn, refundextisn, rzuout, daterep, loadisn)
                SELECT a.refundisn, a.agrextisn, a.rzuout, vDateRep, a.loadisn
                FROM (
                      SELECT --+ ordered use_nl ( rr a ) index ( rr X_EG_RESRZUFULL_PRT_REFUND )
                             a.refundisn, a.agrextisn,
                             (Rzu/rzucurr)* (NVL(rr.CLAIMSUM*a.condpc*(Select  Least(sum(XREF),100)/100 XREF
                                              from storage_source.reprefund_re_New r
                                              Where r.refundisn=rr.refundisn AND nvl(r.refundextisn,0)=nvl(a.agrextisn,0)),0)
                                              -- доля в убытке
                                              -
                                             Nvl((  -- сумма в выплате в валюте претензии
                                              Select Sum(gcc2.gcc2(r.reamount,claimcurrisn,rr.currisn,rr.REPDATELOSS))
                                              from storage_source.rep_refund_payments_re r
                                              where dateval<=rr.daterep and r.refundisn=rr.refundisn AND nvl(r.refundextisn,0)=nvl(a.agrextisn,0)),0)) as rzuout,

                             rr.loadisn
                      FROM resrzufull rr, storage_source.reprefund a
                      WHERE rr.Loadisn=vLoadisn
                        and Nvl(rr.rzucurr,0)<>0
                        AND nvl(rr.rzu,0)<>0
                        and rr.refundisn>vMinIsn and rr.refundisn<=vMaxIsn
                        AND a.refundisn=rr.refundisn
                     ) a
                WHERE NVL(a.rzuout,0)<>0;

                COMMIT;
             End;';
      System.Parallel_Tasks.processtask(sesid,vsql);
      vCnt:=vCnt+1;
      vMinIsn:=vMaxIsn;
      DBMS_APPLICATION_INFO.set_module('fill resrzurefundext','daterep: '||to_char(pdaterep,'dd.mm.yyyy'));
    END LOOP;

    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);
  END IF;
END;*/


PROCEDURE Load_resrzure(pdaterep IN date, ploadisn IN NUMBER := NULL)
IS
  vMinIsn     NUMBER :=-9999;
  vMaxIsn     NUMBER;
  vSql        VARCHAR2 (4000);
  SesId       NUMBER ;
  vLoadIsn    NUMBER := NVL(pLoadIsn, report_rzu.GetActiveLoad(pDaterep => pdaterep));
  vLoadObjCnt NUMBER :=5000;
  vCnt        NUMBER :=0;
  vPart       VARCHAR2(30);
BEGIN
  IF vloadisn IS NULL THEN
    RETURN;
  END IF;

  SesId:=Parallel_Tasks.createnewsession(pMaxJobCnt => 5);
  vPart:=INIT_PARTITION_BY_KEY('storages.resrzure',vLoadIsn);

  LOOP

    SELECT max (claimisn)
    into  vMaxIsn
    FROM (
          Select --+ index (b X_EG_RESRZUFULL_CLAIMISN)
                 b.claimisn
          from resrzufull b
          WHERE b.claimisn > vMinIsn and b.loadisn=vLoadIsn
            and rownum <= vLoadObjCnt
         );

    EXIT WHEN vMaxIsn IS NULL;
    vSql:=' declare
              vMinIsn   number :='||vMinIsn||';
              vMaxIsn   number :='||vMaxIsn||';
              vCnt      number := '||vCnt||';
              vLoadisn  number := '||vloadisn||';
              vDateRep  Date   :=TO_DATE('''||TO_CHAR(SYSDATE,'dd.mm.yyyy')||''',''dd.mm.yyyy'');
            Begin

              DBMS_APPLICATION_INFO.set_module(''fill resrzure'',''THREAD: ''||vCnt);
              report_rzu.Load_resrzure_ByIsn(vMinIsn, vMaxIsn, vloadisn);


             COMMIT;
           End;';
    System.Parallel_Tasks.processtask(sesid,vsql);
    vCnt:=vCnt+1;
    vMinIsn:=vMaxIsn;
    DBMS_APPLICATION_INFO.set_module('fill resrzure','daterep: '||to_char(pdaterep,'dd.mm.yyyy'));
  END LOOP;

  -- ждем, пока завершатся все джобы
  Parallel_Tasks.endsession(sesid);

  INSERT INTO resrzure(loadisn,claimisn,refundisn,refundextisn,rzuout,actualdate)
  SELECT ploadisn,
         a.claimisn,
         a.refundisn,
         a.refundextisn,
         (a.Rzu/a.rzucurr)*(NVL(a.claimsum*(Select  Least(sum(XREF),100)/100 XREF
                                                                          from storage_source.reprefund_re_New r
                                                                           Where r.refundisn=a.refundisn AND nvl(r.refundextisn,0)=nvl(a.refundextisn,0)),0)
                                                                          -- доля в убытке
                                                                          -
                                                                          /*Nvl((  -- сумма в выплате в валюте претензии
                                                                               SELECT Sum(gcc2.gcc2(r.reamount,claimcurrisn,a.currisn,a.repdateloss))
                                                                               from storage_source.rep_refund_payments_re r
                                                                               where dateval<=a.daterep and r.refundisn=a.refundisn AND nvl(r.refundextisn,0)=nvl(a.refundextisn,0)),0)*/
                                                                          Nvl((  -- сумма в выплате в валюте претензии
                                                                               SELECT --+ index ( r X_REFUND_PAYMENT_RE_REFUND )
                                                                                      SUM(r.rzureamount)
                                                                               from storage_source.rep_refund_payment_re_new r
                                                                               where r.dateval<=a.daterep and r.refundisn=a.refundisn AND nvl(r.agrextisn,0)=nvl(a.refundextisn,0)),0)

                                                                               ),
         trunc(SYSDATE)
  FROM resrzufull a
  WHERE a.loadisn=ploadisn
    and Nvl(a.rzufull,0)<>0
    AND nvl(a.rzu,0)<>0
    AND a.recalculationtype=0;
  COMMIT;


END;

PROCEDURE Load_resrzure_ByIsn(pMinIsn number, pMaxIsn number, ploadisn IN NUMBER)
IS
  vDateEvent  date := to_date('01.09.2006','dd.mm.yyyy');
BEGIN

  INSERT INTO resrzure(claimisn, rzuout, actualdate, loadisn)
  SELECT a.claimisn,
         SUM((/*decode(a.status,'N',a.claimsum*a.lossshare,a.claimsum)*/a.claimsum*a.lossshare*a.xref-a.paidsum)*rate),
         trunc(SYSDATE),
         MAX(a.loadisn)
  FROM (
        SELECT --+ index ( rf X_REPREFUND_CLAIM ) no_merge( rr ) ordered use_nl ( rr rf )
               rr.claimisn,
               rr.loadisn,
               rf.status,
               CASE rf.status
                 WHEN 'N' THEN
                   CASE
                     when  nvl(claimsum,0) - decode (nvl (conddeptisn,deptisn),519,0,
                                                       520,0,MedicDept,0,
                                                       nvl (Greatest(Decode(rfranchsum,null,
                                                                              nvl (Gcc2.Gcc2(decode(nvl(franchtype,'Б'),'Б',decode (franchtariff,null,franchsum)),franchcurrisn,currisn,rr.daterep),0)+
                                                                              nvl (claimsum*decode(nvl (franchtype,'Б'),'Б',franchtariff),0)/100,
                                                                              Gcc2.Gcc2(greatest(rfranchsum,0),rfranchcurrisn,currisn,rr.daterep)),0),0)) <
                                                                              decode (nvl (conddeptisn,deptisn),AvtoDept0,0,nvl (refundsum,0)) then
                       case
                         when refundsum is not null THEN decode(nvl(conddeptisn,deptisn),AvtoDept0, greatest(0, nvl(refundsum,0)- nvl (Greatest(Decode(rfranchsum,null,
                                                                                                                                                                  nvl (Gcc2.Gcc2(decode(nvl(franchtype,'Б'),'Б',decode (franchtariff,null,franchsum)),franchcurrisn,currisn,rr.daterep),0)+
                                                                                                                                                                  nvl (claimsum*decode(nvl (franchtype,'Б'),'Б',franchtariff),0)/100,
                                                                                                                                                                  Gcc2.Gcc2(greatest(rfranchsum,0),rfranchcurrisn,currisn,rr.daterep)),0),0)
                                                                                                            ), nvl(refundsum,0)
                                                               ) --EGAO 03.06.2011 письмо от Дмитревской nvl(refundsum,0)
                         when nvl(dateevent,datereg)>vDateEvent
                                       or nvl(datereg,dateevent)>vDateEvent
                                       or nvl (conddeptisn,deptisn) in (519, 520,MedicDept)
                                  then 0
                         ELSE nvl(claimsum,0)
                       end
                   else
                     case when (nvl(dateevent,datereg)>vDateEvent or nvl(datereg,dateevent)>vDateEvent)
                                and  nvl (conddeptisn,deptisn) not in (MedicDept) then nvl(claimsum,0) - nvl (Greatest(Decode(rfranchsum,null,
                                                                                         nvl (Gcc2.Gcc2(decode (nvl (franchtype,'Б'),'Б',decode (franchtariff,null,franchsum)),franchcurrisn,currisn,rr.daterep),0)+
                                                                                         nvl (claimsum*decode (nvl (franchtype,'Б'),'Б',franchtariff),0)/100,
                                                                                         Gcc2.Gcc2(greatest(rfranchsum,0),rfranchcurrisn,currisn,rr.daterep)),0),0)

                          else nvl(claimsum,0)
                     END
                   END
                 WHEN 'Y' THEN NVL(rf.refundsum,0)
               END AS claimsum,
               NVL((Select  Least(sum(r.xref),100)/100
                    from storage_source.reprefund_re_New r
                    Where r.refundisn=rf.refundisn
                      AND nvl(r.refundextisn,0)=nvl(rf.agrextisn,0)
                   ),0) AS xref,
               /*Nvl((
                    Select Sum(gcc2.gcc2(r.reamount,r.claimcurrisn,rf.currisn,rf.REPDATELOSS))
                    from storage_source.rep_refund_payments_re r
                    where r.dateval<=rr.daterep
                      and r.refundisn=rf.refundisn
                      AND nvl(r.refundextisn,0)=nvl(rf.agrextisn,0)),0)*/
                Nvl((
                    Select --+ index ( r X_REFUND_PAYMENT_RE_REFUND )
                           Sum(r.rzureamount)
                    from storage_source.rep_refund_payment_re_new r
                    where r.dateval<=rr.daterep
                      and r.refundisn=rf.refundisn
                      AND nvl(r.agrextisn,0)=nvl(rf.agrextisn,0)),0) AS paidsum, -- сумма в выплате в валюте претензии
               CASE WHEN rf.currisn=vLocalCurr THEN 1 ELSE gcc2.gcc2(1,rf.currisn,vLocalCurr,rr.daterep) END AS rate,
               nvl(rf.lossshare,100)/100 AS lossshare
        FROM (
              SELECT --+ index ( rr X_EG_RESRZUFULL_CLAIMISN )
                     DISTINCT rr.claimisn, rr.loadisn, rr.daterep
              FROM resrzufull rr
              WHERE rr.claimisn > pMinIsn AND rr.claimisn <= pMaxIsn
                AND rr.Loadisn=pLoadisn
                AND rr.recalculationtype=1 -- EGAO 26.07.2011
             ) rr,
             reprefund rf
        WHERE rf.claimisn=rr.claimisn
          and rf.status in ('Y','N')
          and NVL(rf.nrzu,'N')='N'
       ) a
  GROUP BY a.claimisn
  HAVING SUM(a.claimsum*a.xref*a.lossshare-a.paidsum)<>0;

 COMMIT;

END;

PROCEDURE Load_ResRzuRe_Subj(pdaterep IN date, ploadisn IN NUMBER := NULL)
IS
  vPart       VARCHAR2(30);
  vloadisn    NUMBER := NVL(pLoadIsn, report_rzu.GetActiveLoad(pDaterep => pdaterep));
BEGIN
  IF vloadisn IS NULL THEN
    RETURN;
  END IF;

  vPart:=INIT_PARTITION_BY_KEY('storages.ResRzuRe_Subj',vloadisn);
  vPart:=INIT_PARTITION_BY_KEY('storages.rzusection2subj',vloadisn);


  Insert Into  rzusection2subj
  SELECT vloadisn,subjisn, sectisn, condisn, sharepc, condsharepc, condrate,
         sum (condrate/condcnt)over( partition by sectisn) sectrate,
         sectcondcnt,
         prioritysum,limitsum,currisn,
         min(prioritysum) over( partition by sectisn)
  FROM (
        select --+ use_nl (p c rs) ordered
               p.subjisn,
               p.sectisn,
               p.condisn,
               p.sharepc,
               sum (p.sharepc) over (partition by p.condisn) condsharepc,
               c.rate condrate,
               count (*)over( partition by c.isn  )  condcnt,
               count (distinct c.isn)over( partition by p.sectisn  )  sectcondcnt,
               prioritysum,limitsum,rs.currisn
        from ais.resubjperiod p, ais.recond c,resection rs
        where not exists (select isn from resubjperiod where condisn = p.CondIsn and parentisn = p.isn)
          and p.sharepc > 0
          and p.condisn = c.isn
          and c.sectisn=rs.isn
       );

  -- рзу по not XL, но сехция
  insert into ResRzuRe_Subj
  select SEQ_REPORTS.NextVal AS isn,
         s.subjisn,
         r.daterep,
         r.loadisn,
         r.agrisn,
         r.sectisn,
         decode (r.fullreinspc,0,1/cntall,r.rzuout/r.fullreinspc)*r.reinspc AS reamount,
         sharepc/condsharepc*decode (nvl (sectrate,0),0,1/sectcondcnt,condrate/sectrate) subjpc,
         r.deptisn,
         r.refundisn,
         null,
         null,
         RPTGROUPISN,
         ruleisn
  from (select --+ use_nl (r rr a) index_combine (r) ordered
              r.daterep,
              r.loadisn,
              r.deptisn,
              r.refundisn,
              r.agrisn,
              r.rzuoutrsbu AS rzuout,
              rr.sectisn,
              rr.sharepc AS reinspc,
              nvl (sum (rr.sharepc) over (partition by r.refundisn),0) AS fullreinspc,
              COUNT(1) over (partition by r.refundisn) AS cntall,
              Secttype, r.RPTGROUPISN, a.ruleisn
        from resrzufull r, resrzurefund_re rr, agreement a
        where  r.loadisn=vloadisn
          and r.refundisn is not null
          and r.rzuoutrsbu<>0
          and r.refundisn = rr.refundisn
          AND nvl(r.refundextisn,0)=nvl(rr.refundextisn,0)
          and r.loadisn = rr.LoadIsn
          and rr.omitted = 'N'
          and r.agrisn = a.isn
        ) r, rzusection2subj s
        where  r.sectisn > 0
          and Nvl(Secttype,'QS')<>'XL'
          And r.sectisn=s.sectisn(+)
          AND r.loadisn=s.loadisn(+) ;


  -- рзу по долям из agrrole
  INSERT INTO ResRzuRe_Subj
  select seq_reports.nextval AS isn,
         r.subjisn,
         r.daterep,
         r.loadisn,
         r.agrisn,
         r.sectisn,
         decode(r.fullreinspc,0,1/cntall,r.reinspc/r.fullreinspc)*r.rzuout AS reamount,
         1 AS subjpc,
         r.deptisn,
         r.refundisn,
         null,
         null,
         r.rptgroupisn,
         r.ruleisn
  from (select --+ use_nl (r rr a) index_combine (r) ordered
              r.daterep,
              r.loadisn,
              rr.subjisn,
              r.deptisn,
              r.refundisn,
              r.agrisn, r.rzuoutrsbu rzuout,
              rr.sectisn,
              rr.sharepc reinspc,
              nvl (sum (rr.sharepc) over (partition by r.refundisn),0) fullreinspc,
              COUNT(1) over (partition by r.refundisn) AS cntall,
              r.rptgroupisn,
              a.ruleisn
        from resrzufull r, resrzurefund_re rr, agreement a  --Морин М.А. 19.10.2010 УГ   RPTGROUPISN, ruleisnagr
        where r.loadisn= vLoadIsn
          and r.refundisn is not null
          and r.rzuoutrsbu <> 0
          and r.refundisn = rr.refundisn
          AND nvl(r.refundextisn,0)=nvl(rr.refundextisn,0)
          and r.loadisn = rr.LoadIsn
          and rr.omitted = 'N'
          and r.agrisn = a.isn
       )r
  Where  Nvl(sectisn ,0)<=0 ;



  -- рзу по  XL
  insert into ResRzuRe_Subj
  select seq_reports.nextval isn,
         r.subjisn,
         r.daterep,
         r.loadisn,
         r.agrisn,
         r.sectisn,
         decode (r.fullreinspc,0,0,r.rzuout*r.reinspc/r.fullreinspc) AS reamount,
         r.LayerKoef*r.sharepc/sum(r.LayerKoef*r.condsharepc/r.condcnt) over (partition by r.sectisn, r.refundisn) AS subjpc,
         r.deptisn,
         r.refundisn,
         null,
         null,
         r.rptgroupisn,
         r.ruleisn
  from
      (
        Select  r.*,s.sharepc,S.condsharepc,s.subjisn,
                s.condrate,s.prioritysum,s.limitsum,MINPRIORITYSUM,
                greatest(0,Least(gcc2.gcc2(s.prioritysum+s.limitsum,s.sectcurrisn,claimcurrisn,repdateloss),
                sectclaimsum+gcc2.gcc2(MINPRIORITYSUM,s.sectcurrisn,claimcurrisn,repdateloss))-gcc2.gcc2(prioritysum,s.sectcurrisn,claimcurrisn,repdateloss))
                /sectclaimsum LayerKoef,
                count(*) over (partition by condisn, refundisn) condcnt
        from (
              select --+ use_nl (r rr a) index_combine (r) ordered
                      r.daterep, r.loadisn, r.deptisn, r.refundisn, r.agrisn, r.rzuoutrsbu/*EGAO 04.08.2011 r.rzuoutNew*/ rzuout, rr.sectisn,
                      rr.sharepc reinspc, nvl (sum (rr.sharepc) over (partition by r.refundisn),0) fullreinspc,
                      Secttype,r.claimsum,r.currisn,
                      sum(gcc2.gcc2(r.claimsum,r.currisn,ac.currisn,repdateloss)*rr.SHAREPC/100) over (partition by r.claimisn,rr.sectisn) sectclaimsum,
                      repdateloss,ac.currisn claimcurrisn, r.RPTGROUPISN, a.ruleisn
              from resrzufull r, resrzurefund_re rr,agrclaim ac, agreement a
              where  r.loadisn=vloadisn
                and r.refundisn is not null
                and r.rzuoutrsbu <> 0
                and r.refundisn = rr.refundisn
                AND nvl(r.refundextisn,0)=nvl(rr.refundextisn,0)
                and r.loadisn = rr.LoadIsn
                and rr.omitted = 'N'
                and r.claimisn=ac.isn
                and r.agrisn = a.isn  --Морин М.А. 19.10.2010 УГ   RPTGROUPISN, ruleisnagr
             ) r, rzusection2subj s
        where  r.sectisn > 0
          and Nvl(Secttype,'QS')='XL'
          And r.sectisn=s.sectisn(+)
          AND r.loadisn=s.loadisn(+)
      ) r;


  -- рзу по  XL
  insert into ResRzuRe_Subj
  select seq_reports.nextval AS isn,
         null AS subjisn,
         daterep,
         loadisn,
         agrisn,
         sectisn,
         decode (fullreinspc,0,0,rzuout*reinspc/fullreinspc) AS reamount,
         1 AS subjpc,
         deptisn,
         refundisn,
         null,
         null,
         rptgroupisn,
         ruleisn
  FROM (
        select --+ use_nl (r a) index_combine (r) ordered
                r.daterep,
                r.loadisn,
                r.deptisn,
                r.refundisn,
                r.agrisn,
                r.rzuoutrsbu AS rzuout,
                null AS sectisn,
                100 AS reinspc,
                100 AS fullreinspc,
                null AS secttype,
                r.rptgroupisn,
                a.ruleisn
        from resrzufull r, agreement a
        where  r.loadisn=vloadisn
        and r.refundisn is  null
        and r.rzuoutrsbu<>0
        and r.agrisn = a.isn(+)
       );


  delete from ResRzuRe_Subj a where a.loadisn=vloadisn AND nvl(a.reamount*a.subjpc,0)=0;


  --убытки ДМС

  INSERT INTO ResRzuRe_Subj
  SELECT seq_reports.nextval AS isn,
         s.subjisn,
         r.daterep,
         r.loadisn,
         r.agrisn,
         r.sectisn,
         decode (r.fullreinspc,0,0,r.rzuout*r.reinspc/r.fullreinspc) AS reamount,
         s.sharepc/s.condsharepc*decode (nvl (s.sectrate,0),0,1/s.sectcondcnt,s.condrate/s.sectrate) AS subjpc,
         r.deptisn,
         r.refundisn,
         null,
         null,
         rptgroupisn,
         ruleisn
  FROM (
        SELECT --+ ordered use_nl ( r rf rr a)
               r.daterep, r.loadisn, r.deptisn, r.refundisn, rf.agrisn, r.rzuoutrsbu AS rzuout,
               rr.sectisn, rr.sharepc reinspc,  rr.secttype,
               nvl (sum (rr.sharepc) over (partition by rf.refundisn, nvl(rf.agrextisn,0)),0) fullreinspc,
               r.RPTGROUPISN, a.ruleisn

        FROM resrzufull r,
             reprefund rf,
             storage_source.reprefund_re_New rr,
             agreement a   --Морин М.А. 19.10.2010 УГ   RPTGROUPISN, ruleisnagr
        WHERE r.loadisn=vloadisn
          AND r.deptisn=23735116
          AND rf.refundisn=r.refundisn
          AND NVL(rf.agrextisn,0)=NVL(r.refundextisn,0)
          AND rr.refundisn=rf.refundisn
          AND nvl(rr.refundextisn,0)=nvl(rf.agrextisn,0)
          AND r.agrisn = a.isn
       ) r,
       rzusection2subj s
  WHERE r.sectisn > 0
  and Nvl(r.Secttype,'QS') IN ('QS', 'SP')
  And r.sectisn=s.sectisn(+)
  AND r.loadisn=s.loadisn(+);

  INSERT INTO ResRzuRe_Subj
  SELECT seq_reports.nextval AS isn,
         to_number(null) AS subjisn,
         r.daterep,
         r.loadisn,
         r.agrisn,
         r.sectisn,
         decode (r.cntall,0,0,r.rzuout*1/r.cntall) AS reamount,
         1 AS subjpc,
         r.deptisn,
         r.refundisn,
         null,
         null,
         rptgroupisn,
         ruleisn
  FROM (
        SELECT --+ ordered use_nl ( r rf rr a)
               r.daterep, r.loadisn, r.deptisn, r.refundisn, r.agrisn, r.rzuoutrsbu AS rzuout,
               to_number(null) AS sectisn, 0 reinspc,  to_char(null) AS secttype,
               COUNT(1) over (partition by r.refundisn) AS cntall,
               r.RPTGROUPISN, a.ruleisn
        FROM resrzufull r,
             agreement a
        WHERE r.loadisn=vLoadIsn
          AND r.refundisn IS NOT NULL -- !!!
          and r.rzuoutrsbu<>0
          AND r.agrisn = a.isn
          AND NOT EXISTS (SELECT 'x' FROM ResRzuRe_Subj x WHERE x.loadisn=r.loadisn AND x.refundisn=r.refundisn)
       ) r;





  UPDATE ResRzuRe_Subj
  SET code=substr (Rep_Reins_MFCode (agrisn, sectisn),1,3)
  WHERE loadisn=vloadisn;

  COMMIT;

END;

PROCEDURE LoadStateReimbursementAgr(pLoadIsn NUMBER)
IS
  vMinIsn     number:=-9999;
  vMaxIsn     number;
  vSql        varchar2(4000);
  SesId       Number;
  vLoadObjCnt number:=100000;
  vCnt        number:=1;
  vPart       VARCHAR2(150);
BEGIN
  vPart:=INIT_PARTITION_BY_KEY(pTableName => 'storages.rzustatereimbursementagr',pKey =>pLoadIsn,pCompress => 1);  

  SesId:=Parallel_Tasks.createnewsession();
  vMinIsn:=-1;

  LOOP

    vMaxIsn:=Cut_Table('ais.agrext','agrisn',vMinIsn,pRowCount=>vLoadObjCnt);

    EXIT WHEN vMaxIsn IS NULL;
    vSql:=' declare
              vMinIsn number :='||vMinIsn||';
              vMaxIsn number :='||vMaxIsn||';
              vCnt    number :='||vCnt||';
              vLoadIsn number :='||pLoadIsn||';
            Begin
              DBMS_APPLICATION_INFO.set_module(''rzustatereimbursementagr'',''Thread# ''||vCnt);

              INSERT INTO rzustatereimbursementagr (loadisn, agrisn)
              SELECT --+ index ( ext X_AGREXT_AGR )
                     distinct vLoadIsn, ext.agrisn
              FROM ais.agrext ext
              WHERE ext.agrisn > vMinIsn and ext.agrisn <= vMaxIsn
                AND ext.classisn=1071774425
                AND ext.x1=1283168203;

            COMMIT;
          End;';

    System.Parallel_Tasks.processtask(sesid,vsql);

    vCnt:=vCnt+1;

    vMinIsn:=vMaxIsn;
    SYS.DBMS_APPLICATION_INFO.Set_Module ('rzu. fill rzustatereimbursementagr',vCnt*vLoadObjCnt);

  END LOOP;

  -- ждем, пока завершатся все джобы
  Parallel_Tasks.endsession(sesid);


END;



End;
