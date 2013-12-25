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

/* процедура - построитель*/
Procedure LoadReserve
(
pLoadIsn Number,
pDateRep Date,
pStage Number:=0,
pDeptIsn IN Number := 0,
pRefundIsn In Number:=0);

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

end;

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
