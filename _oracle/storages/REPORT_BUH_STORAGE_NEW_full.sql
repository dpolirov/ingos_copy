CREATE OR REPLACE PACKAGE "STORAGES"."REPORT_BUH_STORAGE_NEW" IS


LoadIsn Number;
vDateStage DATE := trunc(SYSDATE,'mm')-1;

/*в пакете много не используемомго кода

используются

LoadBuh2Cond
LoadBuh2Cond_By_List
LoadBuh2cond_By_Isns - загрузка repbuh2cond


SetRefundRptGroup
SetRefundRptGroup_By_Isns - простановка УГ для убытков (вызывается послепостроения repbuh2cond)

LoadDocSumm_WO_Buhbody - загрузка доксумм без проводок по убыткам для РЗУ (вызывается при загрузке repbuh2cond)

Create_Agr_Analitiks
CREATE_AGR_ANALITIKS_By_Isns - загрузка REP_AGR_ANALITIKS - отдельная витрина, строится  из report_admin


LoadRZUMemo - загрузка меморандумов для РЗУ, вызывается при построении РЗУ

REP_LONGAGR
rep_longagr_by_Isns - загрузка инфоструктур по длинным договорам для построения Repbuh2cond и РНП
вызывается при загрузке хранилища
 */




Procedure LoadBuh_By_List(IsFull Number:=0); -- по буферу тянем в RepBuhBody

Procedure LoadBuh2Cond
(pLoadIsn IN Number);

Procedure LoadBuh2Cond_BY_DATE
(
pLoadIsn IN Number,
pDatebeg DATE

);



Procedure LoadBuh2Cond_By_List
(pLoadIsn IN Number,
 pIsFull in Number:=1,
 pCommitEveryPut number:=0);

Procedure LoadRefund_By_TT_RowId
(pLoadIsn IN Number,
 IsFull in Number:=1);

procedure LoadRepRefund_Hist_By_TT_RowId
(  pLoadIsn   in   Number,
   IsFull number:=1);

Procedure LoadRefund
(  pLoadIsn   in   Number,
   pRunType in Number:=Report_Storage.cpReRun
 );



procedure SetRefundRptGroup
(pRefundIsn IN Number := 0);




Procedure LoadRepBuh
(  pLoadIsn   in   Number,
   pRunType in Number:=Report_Storage.cpReRun
 );

procedure LoadDocSumm_WO_Buhbody
(  pLoadIsn   in   Number);
-- Загрузка сумм по убыткам не привязанным к проводкам



procedure LoadRZUMemo
-- запускать после простановки учетных групп в repbuh2cond
(  pLoadIsn   in   Number);

procedure LoadAgrRefundExt
(  pLoadIsn   in   Number);



Procedure LoadRepBuh_By_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number
);


Procedure LoadRepBuh_By_Hist_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number
);


Procedure LoadBuh2cond_By_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number
);



Procedure LoadRefund_By_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number
);



procedure Create_Agr_Analitiks;


Procedure CREATE_AGR_ANALITIKS_By_Isns
(
vMinIsn Number,
vMaxIsn Number
);


procedure SetRefundRptGroup_By_Isns (vMinIsn number,vLMaxIsn Number);


Procedure LoadBuhQuit_By_List (IsFull Number:=0);
procedure Set_Body_Dept_Isn_By_List(pMinIsn Number, pMaxIsn Number);



Procedure LoadRepBuh_MakeHist (pLoadIsn   in   Number);



procedure LoadReprefund_re(pLoadisn Number);


PROCEDURE REP_LONGAGR
( pLoadIsn number:=0);

procedure rep_longagr_by_Isns(pLoadIsn number,vMinIsn number,vMaxIsn number);


procedure get_log_to_reload_repbuh2cond;
Procedure LoadBuh2Cond_BY_LOGS
(
pLoadIsn IN Number
);


Procedure LoadBuh2cond_By_Log_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number

);


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


procedure CreateClaimHistBuffer
(
pLoadIsn Number
);




---------------------------------------------
Procedure LoadBuh_By_List (IsFull Number:=0)
  IS
    Vrc Number;
    vBlockIsn number;
Begin



 vrc:=0;

  If IsFull=0 Then

    select seq_rep_block.NEXTVAL into vBlockIsn from dual;
     RepLog_i (LoadIsn, 'InsertRepBuh', 'Delete',pAction => 'Begin', pBlockIsn=>vBlockIsn);
      DELETE /*+ INDEX(A X_REPBUHBODY_HEAD)*/FROM RepBuhBody a
            WHERE HeadIsn In (Select Distinct HISTBODYISN from TT_BODY_LIST);
     vrc := SQL%ROWCount;

      RepLog_i (LoadIsn, 'InsertRepBuh', 'Delete', pAction => 'End',pobjCount => vrc, pBlockIsn=>vBlockIsn);
  end if;

      select seq_rep_block.NEXTVAL into vBlockIsn from dual;
      RepLog_i (LoadIsn, 'InsertRepBuh', 'Insert', pAction => 'Begin', pBlockIsn=>vBlockIsn);

/*
    insert  into RepBuhBody (Isn, LoadIsn, DeptIsn, StatCode, ClassIsn, BodyIsn, DateVal, CurrIsn, DeptIsnBuh,
     SubjIsn, SubAccIsn, BuhAmount, BuhAmountRub, DocSumIsn, DatePayLast, AgrIsn, DeptIsnAn, ReprDeptIsn,
     BizFlg, ParentIsn, AddIsn, RefundIsn, DocsumPC, BuhQuitBodyIsn, BuhQuitBodyCnt, AccCurrIsn, QuitDebetIsn,
     QuitCreditIsn, QuitDateVal, DateQuit, QuitCurrIsn, QuitDebetSubAccIsn, QuitCreditSubAccIsn,
     QuitDebetBuhAmount, QuitCreditBuhAmount, AmountClosedQuit, AmountClosingQuit, FullAmountClosingQuit,
     BuhQuitAmount, BuhQuitPartAmount, BuhQuitDate, FactIsn, BuhQuitIsn, BuhHeadFid, BuhAmountUSD,
     OPRISN,OPRDEPTISN, DatePay, AgrBuhDate, DOCSUMSUBJ,HeadIsn,docISn,AddSign,AddDateBeg,DocIsn2,SaGroup,
     CORSUBACCISN)
    select --+ use_nl (b d dp s d2 a aa r f b1 b2 bb bb1 opr) ordered
     Seq_Reports.NextVal, LoadIsn, b.Deptisn, b.StatCode, b.ClassIsn,
     b.bodyisn, b.dateval, b.currisn, b.DeptIsnBuh,  b.SubjIsn, b.subaccisn,
     b.buhamount, b.buhamountrub, b.docsumisn, b.datepaylast,Nvl(aa.isn,0) Agrisn,
     b.DeptIsnAn, b.ReprDeptIsn, b.BizFlg, b.ParentIsn,
     a.isn AddIsn, r.isn Refundisn,
     decode (nvl (fullamountdoc,0),0,decode (docsumcnt,0,0+null,1/docsumcnt),b.amountdoc/fullamountdoc) docsumpc,
     b.BuhQuitBodyIsn, b.BuhQuitBodyCnt, s.CurrIsn AccCurrIsn,
     b1.isn QuitDebetIsn, b2.isn QuitCreditIsn,
     nvl (nvl (b1.dateval, b2.dateval),f.DatePay) QuitDateVal,
     nvl (b1.datequit, b2.datequit) DateQuit,
     nvl (f.CurrIsn,nvl (b1.CurrIsn, b2.CurrIsn)) QuitCurrIsn,
     b1.SubAccIsn QuitDebetSubAccIsn,
     b2.SubAccIsn QuitCreditSubAccIsn,
     nvl (b1.damount, -b1.camount) QuitDebetBuhAmount,
     nvl (b2.camount, -b2.damount) QuitCreditBuhAmount,
     f.amount AmountClosedQuit,
     f.amountdoc AmountClosingQuit,
     SUM (abs (f.amountdoc)) OVER (PARTITION BY b.docsumisn, nvl (bb1.isn,bb.isn)) AS FullAmountClosingQuit,
     nvl (bb.damount,-bb.camount) BuhQuitAmount,
     nvl (bb1.damount,-bb1.camount) BuhQuitPartAmount,
     bb1.datequit BuhQuitDate,
     f.isn FactIsn, bb1.isn BuhQuitIsn,
     b.BuhHeadFid, b.BuhAmountUsd,B.OprIsn,Opr.CLASSISN1 OprDeptIsn,
     b.DatePay, decode (b.statcode,38,decode (b.deptisn,707480016,dp.signed)),
     F.SubjIsn,
     B.HeadIsn,
     b.docisn,
     A.DateSign,
     A.Datebeg,
     b.DocIsn2,
     Sagroup,
     bb.subaccisn CorSubAccIsn

--     decode (decode (b.statcode,38,1,34,1),1,decode (nvl (a.isn,aa.isn),null,null,Ais.Get_Agr_BuhDate (nvl (a.isn,aa.isn), b.DocSumIsn))) AgrBuhDate
    from (select --+ use_nl (r b pc pd h) ordered
    --Поля из report_body_list
     r.Deptisn, r.StatCode, r.ClassIsn,
    --Поля проводки
     b.isn BodyIsn, b.dateval DateVal, b.currisn CurrIsn, h.fid BuhHeadFid,
     b.deptisn DeptIsnBuh, b.SubjIsn, b.SubAccIsn, b.parentisn,H.Isn HeadIsn,
     nvl (b.camount, -b.damount) BuhAmount,
     nvl (b.camountrub, -b.damountrub) BuhAmountRub,
     nvl (b.camountusd, -b.damountusd) BuhAmountUsd,
     --Поля аналитики
     AIS.BuhKind_Utils.GetDeptFromKindAcc (b.SubKindIsn) DeptIsnAn,
     AIS.BuhKind_Utils.GetReprDeptFromKindAcc (b.SubKindIsn) ReprDeptIsn,
/*     (select max (decode (classisn, BizCenter, 'Ц', BizFil, 'Ф'))
     from kindaccset where kindaccisn = b.SubKindIsn and kindisn = KindBiz) Null BizFlg,
     --Поля плановой доксуммы
     nvl (pc.isn, pd.isn) DocSumIsn,
     decode (pc.isn,null,pd.DatePay,pc.DatePay) DatePay,
     decode (pc.isn,null,pd.DatePayLast,pc.DatePayLast) DatePayLast,
     nvl (nvl (pc.AgrIsn, pd.agrisn), b.AgrIsn) AgrIsn,
     nvl (pc.RefundIsn,pd.RefundIsn) RefundIsn,
     gcc2.gcc2(nvl (pc.AmountDoc, pd.AmountDoc),nvl(pc.DOCCURRISN,pd.DOCCURRISN),b.currisn,b.dateval) AmountDoc,
     nvl (pc.DocIsn, pd.DocIsn) DocIsn,
     nvl (pc.DocIsn2, pd.DocIsn2) DocIsn2,
     B.oprisn,
     SUM (gcc2.gcc2(nvl (pc.AmountDoc, pd.AmountDoc),nvl(pc.DOCCURRISN,pd.DOCCURRISN),b.currisn,b.dateval)) OVER (PARTITION BY b.isn) AS FullAmountDoc,
     COUNT (*) OVER (PARTITION BY b.isn) AS DocSumCnt,
     --Поля корреспонденции
     (select max (isn)
      from ais.buhbody_t
      where headisn = b.headisn
        and status = 'А'
        and decode (b.damount,null,damount,camount) is not null) BuhQuitBodyIsn,
     (select count (*)
      from ais.buhbody_t
      where headisn = b.headisn
        and status = 'А'
        and decode (b.damount,null,damount,camount) is not null) BuhQuitBodyCnt,
        Decode(pc.Isn,null,Pd.CreditIsn,Pc.DebetIsn) PdsBuhQuitIsn,
        r.sagroup
    from TT_BODY_LIST r, ais.buhbody_t b, docsum pc, docsum pd, ais.buhhead_t h
    where r.bodyisn = b.isn
      and b.isn = pc.creditisn (+)
      and b.isn = pd.debetisn (+)
      and pc.discr (+) between 'F' and 'P'
      and pd.discr (+) between 'F' and 'P'
      and b.headisn = h.isn
    ) b, docs d, docs dp, ais.subacc s, docs d2, agreement a, agreement aa, agrrefund r, docsum f, ais.buhbody_t b1, ais.buhbody_t b2, ais.buhbody_t bb, ais.buhbody_t bb1,
      (select x.* from dicx x,dicti d where x.classisn = c.get('xDeptReprOper')
        And d.isn=x.classisn1 and Nvl(d.active,'S')<>'S') Opr
    where b.docisn = d.isn (+)
      and d.parentisn = dp.isn (+)
      and d.accisn =  s.isn (+)
      and b.docisn2 = d2.isn (+)
      and b.refundisn = a.isn (+)
      and b.agrisn = aa.isn (+)
      and b.refundisn = r.isn (+)
      and b.docsumisn = f.parentisn (+)
      and f.discr (+) = 'F'
      and f.status (+) is null
      and f.amount (+) <> 0
      and f.debetisn = b1.isn (+)
      and f.creditisn = b2.isn (+)
      and b.BuhQuitBodyIsn = bb.isn (+)
      and (bb.isn = bb1.isn
        or bb.isn = bb1.parentisn
        and bb1.status = 'А'
        and bb1.oprisn = cAutoPartQuit)
        And Opr.CLASSISN2(+)= B.OprIsn;


  /*  insert into RepBuhBody (Isn, LoadIsn, DeptIsn, StatCode, ClassIsn, BodyIsn, DateVal, CurrIsn, DeptIsnBuh,
     SubjIsn, SubAccIsn, BuhAmount, BuhAmountRub, DocSumIsn, DatePayLast, AgrIsn, DeptIsnAn, ReprDeptIsn,
     BizFlg, ParentIsn, AddIsn, RefundIsn, DocsumPC, BuhQuitBodyIsn, BuhQuitBodyCnt, AccCurrIsn, QuitDebetIsn,
     QuitCreditIsn, QuitDateVal, DateQuit, QuitCurrIsn, QuitDebetSubAccIsn, QuitCreditSubAccIsn,
     QuitDebetBuhAmount, QuitCreditBuhAmount, AmountClosedQuit, AmountClosingQuit, FullAmountClosingQuit,
     BuhQuitAmount, BuhQuitPartAmount, BuhQuitDate, FactIsn, BuhQuitIsn, BuhHeadFID, BuhAmountUSD)
    select Seq_Reports.NextVal, LoadIsn, DeptIsn, StatCode, ClassIsn, BodyIsn, DateVal, CurrIsn, DeptIsnBuh,
     SubjIsn, SubAccIsn, BuhAmount, BuhAmountRub, DocSumIsn, DatePayLast, AgrIsn, DeptIsnAn, ReprDeptIsn,
     BizFlg, ParentIsn, AddIsn, RefundIsn, DocsumPC, BuhQuitBodyIsn, BuhQuitBodyCnt, AccCurrIsn, QuitDebetIsn,
     QuitCreditIsn, QuitDateVal, DateQuit, QuitCurrIsn, QuitDebetSubAccIsn, QuitCreditSubAccIsn,
     QuitDebetBuhAmount, QuitCreditBuhAmount, AmountClosedQuit, AmountClosingQuit, FullAmountClosingQuit,
     decode (HaveFact,1,BuhQuitAmountQueIsn,BuhQuitAmount), BuhQuitPartAmount, BuhQuitDate, FactIsn, BuhQuitIsn,
     BuhHeadFID, BuhAmountUSD
    from (select --+ use_nl (b d s a aa r f bb) ordered
     b.Deptisn, b.StatCode, b.ClassIsn, b.BuhHeadFID,
     b.bodyisn, b.dateval, b.currisn, b.DeptIsnBuh,  b.SubjIsn, b.subaccisn,
     b.buhamount, b.buhamountrub, b.docsumisn, b.datepaylast, aa.isn,
     b.DeptIsnAn, b.ReprDeptIsn, b.BizFlg, b.ParentIsn,
     a.isn AddIsn, r.isn Refundisn,
     decode (nvl (fullamountdoc,0),0,decode (docsumcnt,0,0+null,1/docsumcnt),b.amountdoc/fullamountdoc) docsumpc,
     b.BuhQuitBodyIsn, b.BuhQuitBodyCnt, s.CurrIsn AccCurrIsn,
     f.FactIsn, f.QuitDebetIsn, f.QuitCreditIsn, f.QuitDateVal, f.DateQuit, f.QuitCurrIsn,
     f.QuitDebetSubAccIsn, f.QuitCreditSubAccIsn, f.QuitDebetBuhAmount, f.QuitCreditBuhAmount,
     f.AmountClosedQuit, f.AmountClosingQuit, f.QueIsn1, f.QueIsn2,
     bb.BuhQuitIsn, bb.BuhQuitAmount, bb.BuhQuitAmountQueIsn, bb.BuhQuitPartAmount, bb1.datequit BuhQuitDate,
     MAX (decode (bb.queisn,f.QueIsn1,1,f.QueIsn2,1,0)) OVER (PARTITION BY b.DocSumIsn, f.FactIsn) HaveFact,
     SUM (abs (f.amountdoc)) OVER (PARTITION BY b.docsumisn, bb.isn) AS FullAmountClosingQuit
--     decode (decode (b.statcode,38,1,34,1),1,decode (nvl (a.isn,aa.isn),null,null,Ais.Get_Agr_BuhDate (nvl (a.isn,aa.isn), b.DocSumIsn))) AgrBuhDate
    from (select --+ use_nl (r b pc pd) index (r) ordered
    --Поля из report_body_list
     r.Deptisn, r.StatCode, r.ClassIsn,
    --Поля проводки
     b.isn BodyIsn, b.dateval DateVal, b.currisn CurrIsn,
     b.deptisn DeptIsnBuh, b.SubjIsn, b.SubAccIsn, b.parentisn,
     nvl (b.camount, -b.damount) BuhAmount,
     nvl (b.camountrub, -b.damountrub) BuhAmountRub,
     nvl (b.camountusd, -b.damountusd) BuhAmountUsd,
     h.fid BuhHeadFID,
     --Поля аналитики
     AIS.BuhKind_Utils.GetDeptFromKindAcc (b.SubKindIsn) DeptIsnAn,
     AIS.BuhKind_Utils.GetReprDeptFromKindAcc (b.SubKindIsn) ReprDeptIsn,
     (select max (decode (classisn, BizCenter, 'Ц', BizFil, 'Ф'))
     from kindaccset where kindaccisn = b.SubKindIsn and kindisn = KindBiz) BizFlg,
     --Поля плановой доксуммы
     nvl (pc.isn, pd.isn) DocSumIsn,
     decode (pc.isn,null,pd.DatePayLast,pc.DatePayLast) DatePayLast,
     nvl (nvl (pc.AgrIsn, pd.agrisn), b.AgrIsn) AgrIsn,
     nvl (pc.RefundIsn,pd.RefundIsn) RefundIsn,
     nvl (pc.AmountDoc, pd.AmountDoc) AmountDoc,
     nvl (pc.DocIsn, pd.DocIsn) DocIsn,
     nvl (pc.DocIsn2, pd.DocIsn2) DocIsn2,
     SUM (nvl (pc.AmountDoc, pd.AmountDoc)) OVER (PARTITION BY b.isn) AS FullAmountDoc,
     COUNT (*) OVER (PARTITION BY b.isn) AS DocSumCnt,
     --Поля корреспонденции
     (select max (isn)
      from ais.buhbody_t
      where headisn = b.headisn
        and status = 'А'
        and decode (b.damount,null,damount,camount) is not null) BuhQuitBodyIsn,
     (select count (*)
      from ais.buhbody_t
      where headisn = b.headisn
        and status = 'А'
        and decode (b.damount,null,damount,camount) is not null) BuhQuitBodyCnt
    from report_body_list r, ais.buhbody_t b, docsum pc, docsum pd, ais.buhhead_t h
    where r.bodyisn = b.isn
      and b.isn = pc.creditisn (+)
      and b.isn = pd.debetisn (+)
      and pc.discr (+) between 'F' and 'P'
      and pd.discr (+) between 'F' and 'P'
      and b.headisn = h.isn
    ) b, docs d, ais.subacc s, agreement a, agreement aa, agrrefund r,
     (select --+ use_nl (f b1 b2 d1 d2) index (f X_DOCSUM_PARENT) ordered
       f.isn FactIsn,
       f.parentisn,
       b1.isn QuitDebetIsn, b2.isn QuitCreditIsn,
       nvl (nvl (b1.dateval, b2.dateval),f.DatePay) QuitDateVal,
       nvl (b1.datequit, b2.datequit) DateQuit,
       nvl (nvl (b1.CurrIsn, b2.CurrIsn),f.CurrIsn) QuitCurrIsn,
       b1.SubAccIsn QuitDebetSubAccIsn,
       b2.SubAccIsn QuitCreditSubAccIsn,
       nvl (b1.damount, -b1.camount) QuitDebetBuhAmount,
       nvl (b2.camount, -b2.damount) QuitCreditBuhAmount,
       f.amount AmountClosedQuit,
       f.amountdoc AmountClosingQuit,
       d1.queisn queisn1, d2.queisn queisn2
      from docsum f, ais.buhbody_t b1, ais.buhbody_t b2, ais.docgrp d1, ais.docgrp d2
      where b.docsumisn = f.parentisn
        and f.discr = 'F'
        and f.status is null
        and f.amount <> 0
        and f.debetisn = b1.isn (+)
        and f.creditisn = b2.isn (+)
        and b1.groupisn = d1.isn (+)
        and b2.groupisn = d2.isn (+)
      union all
      select isn, isn, debetisn, creditisn, null, null, currisn, 0, 0, 0, 0, 0, -remainder, 0, 0
      from docsum
      where isn = b.docsumisn
       and remainder <> 0
      ) f,
     (select --+ use_nl (b1 b2 d) ordered
       b.BuhQuitBodyIsn BodyIsn,
       b2.isn BuhQuitIsn,
       SUM (nvl (b2.damount,-b2.camount)) OVER () BuhQuitAmount,
       SUM (nvl (b2.damount,-b2.camount)) OVER () BuhQuitAmountQueIsn,
       nvl (b2.damount,-b2.camount) BuhQuitPartAmount,
       b2.datequit BuhQuitDate, nvl (d.queisn,0) QueIsn
      from ais.buhbody_t b1, ais.buhbody_t b2, ais.docgrp d
      where b.BuhQuitBodyIsn = b1.isn
      and (b1.isn = b2.isn
        or b1.isn = b2.parentisn
        and b2.status = 'А'
        and b2.oprisn = cAutoPartQuit)
      and b2.groupisn = d.isn (+)) bb
    where b.docisn = d.isn (+)
      and d.accisn =  s.isn (+)
      and b.refundisn = a.isn (+)
      and b.agrisn = aa.isn (+)
      and b.refundisn = r.isn (+)
      and b.docsumisn = f.parentisn (+)
      and b.BuhQuitBodyIsn = bb.bodyisn (+))
    where (HaveFact = 0 or Queisn = QueIsn1 or QueIsn = f.QueIsn2);
    */
      vrc := SQL%ROWCount;
      RepLog_i (LoadIsn, 'InsertRepBuh','Insert', pAction => 'End',pobjCount => vrc, pBlockIsn=>vBlockIsn);


            --         LoadBuhQuit_By_List(IsFull);
End;


Procedure LoadBuhQuit_By_List (IsFull Number:=0)
  IS
    Vrc Number;
    vBlockIsn number;
Begin



 vrc:=0;

  If IsFull=0 Then
    select seq_rep_block.NEXTVAL into vBlockIsn from dual;
     RepLog_i (LoadIsn, 'InsertRepBuhQuit', 'Delete',pAction => 'Begin', pBlockIsn=>vBlockIsn);
      DELETE /*+ INDEX(A X_RepBuhQuit_HEAD)*/FROM RepBuhQuit a
            WHERE HeadIsn In (Select Distinct HISTBODYISN from TT_BODY_LIST);
     vrc := SQL%ROWCount;

      RepLog_i (LoadIsn, 'InsertRepBuhQuit', 'Delete', pAction => 'End',pobjCount => vrc, pBlockIsn=>vBlockIsn);
  end if;

      select seq_rep_block.NEXTVAL into vBlockIsn from dual;
      RepLog_i (LoadIsn, 'InsertRepBuhQuit', 'Insert', pAction => 'Begin', pBlockIsn=>vBlockIsn);


insert  into RepBuhQuit
Select --+ use_nl (bbq) ordered
  Seq_Reports.NextVal,Loadisn,B.*,bbq.dateval QuitDateval,0
from
(
Select --+ use_nl (qd1  qd2) ordered
 B.*,
 Nvl(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2),0) quitSum,
Nvl(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2),1)/decode(Nvl((Sum(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2)) over (Partition by bodyisn,BuhQuitIsn)),1),0,1,
Nvl((Sum(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2)) over (Partition by bodyIsn,BuhQuitIsn)),1)) quitPc,
Decode(Nvl(qd1.REFISN,qd2.ObjIsn),null,null,
(Select Nvl(Max('Y'),'N') from docsum where (creditisn=Nvl(qd1.REFISN,qd2.ObjIsn) or debetisn=Nvl(qd1.REFISN,qd2.ObjIsn)) and discr='F'  )) Fact,
Nvl(qd1.REFISN,qd2.ObjIsn) QuitBodyIsn
from(
 Select --+ use_nl (r bb bb1 dg) ordered
     b.*,
    decode( nvl (bb1.damount,-bb1.camount),0,0,nvl (bb1.damount,-bb1.camount)/ nvl (bb.damount,-bb.camount)) BuhPc,
    decode(nvl (bb.damount,-bb.camount),0,0,BuhAmount/nvl (bb.damount,-bb.camount)) BuhQuitPc,
     bb1.datequit BuhQuitDate,
     bb1.groupisn,
     bb1.isn BuhQuitIsn,
     dg.queisn,
     bb.subaccisn CorSubAccIsn

 from
 (
 select --+ use_nl (r b pc pd h) ordered
    --Поля из report_body_list
     r.Deptisn, r.StatCode, r.ClassIsn,
    --Поля проводки
     b.isn BodyIsn, b.dateval DateVal, b.currisn CurrIsn, h.fid BuhHeadFid,
     b.deptisn DeptIsnBuh, b.SubjIsn, b.SubAccIsn, b.parentisn,H.Isn HeadIsn,
     nvl (b.camount, -b.damount) BuhAmount,
     nvl (b.camountrub, -b.damountrub) BuhAmountRub,
     nvl (b.camountusd, -b.damountusd) BuhAmountUsd,
     --Поля аналитики
     B.oprisn,
     --Поля корреспонденции
     (select max (isn)
      from ais.buhbody_t
      where headisn = b.headisn
        and status = 'А'
        and decode (b.damount,null,damount,camount) is not null) BuhQuitBodyIsn,
     (select count (*)
      from ais.buhbody_t
      where headisn = b.headisn
        and status = 'А'
        and decode (b.damount,null,damount,camount) is not null) BuhQuitBodyCnt,
        r.sagroup
    from TT_BODY_LIST r, ais.buhbody_t b,  ais.buhhead_t h
    where r.bodyisn = b.isn
      and b.headisn = h.isn

) b,ais.buhbody_t bb, ais.buhbody_t bb1,ais.DOCGRP dg

    where b.BuhQuitBodyIsn = bb.isn (+)
       and (bb.isn = bb1.isn
        or bb.isn = bb1.parentisn
        and bb1.status = 'А'
        and bb1.oprisn = c.get('oPartQuit'))
    And  bb1.groupisn=dg.isn(+)

    ) b,ais.quedecode qd1,ais.quedecode qd2
        Where b.queisn=qd1.queisn(+)
        And  b.BuhQuitIsn=qd1.ObjIsn(+)

        and b.queisn=qd2.queisn(+)
        And  b.BuhQuitIsn=qd2.REFISN(+)
) b,ais.buhbody_t bbq
Where b.QuitBodyIsn=bbq.isn(+);

      vrc := SQL%ROWCount;
      RepLog_i (LoadIsn, 'InsertRepBuhQuit','Insert', pAction => 'End',pobjCount => vrc, pBlockIsn=>vBlockIsn);
End;



---------------------------------------------
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

/*

delete --+ Ordered Use_Nl(a)
from tt_add_info a
Where (agrisn ,dsdatebeg,dsdateend) in
(select distinct agrisn,Nvl(dsdatebeg,'01-jan-1900'),Nvl(dsdateend,'01-jan-3000')
from Report_BuhBody_List Where statcode not in (220,24) )
and Loadisn<>pLoadIsn;

*/


/*--\* sagrp=4

Insert into tt_add_info
SELECT agrisn,
       addisn,
       newaddisn,
       Nvl( (SELECT \*+ index ( bb X_REPBUHBODY_AGR ) *\ Min (dateval) from repbuhbody bb Where bb.agrisn=s.agrisn and bb.addisn=s.Newaddisn and statcode in (38,34,27,221,241,27)),
            (select Datesign from agreement ag where ag.PREMIUMSUM= 0 and ag.STATUS='В' and ag.isn=s.Newaddisn))
       newadddateval,
       addsign,
       addbeg,
       decode(AddPrem,0,1/AddCnt,PremUsd/AddPrem) NEWADDPC,
       pLoadisn,
       dsdatebeg,dsdateend
FROM ( Select S.*,
              sum(PremUsd) Over (partition by addisn) AddPrem,
              count(*) Over (partition by addisn) AddCnt
       FROM ( Select  --+ no_merge ( b ) ordered use_nl ( b rc ) index ( rc X_REPCOND_AGR )
                     rc.agrisn,
                     addisn,
                     newaddisn,
                     Nvl(Sum(decode(rc.premUsd,null,gcc2.gcc2(rc.premiumsum,rc.premcurrisn,53,sysdate),rc.premUsd)
                         * ( (Least(dsdateend,rc.dateend)-Greatest(dsdatebeg,rc.datebeg)+1)/(rc.dateend-rc.datebeg+1) )
                           \*про рата действия конда в рамках доксуммы*\
                             ),0) PremUsd,
                     addsign,
                     addbeg,
                     Min(ADDPREMIUMSUM) ADDPREMIUMSUM,
                     Max(ADDSTATUS)ADDSTATUS,
                     dsdatebeg,
                     dsdateend
              FROM (select distinct agrisn,
                                    Nvl(dsdatebeg,'01-jan-1900') dsdatebeg,
                                    Nvl(dsdateend,'01-jan-3000') dsdateend
                    from Report_BuhBody_List b
                    Where statcode not in (220,24)
                    \*
                    and not exists (select 'a' from tt_add_info  t
                                    where b.agrisn=t.agrisn
                                    and t.loadisn=pLoadisn
                                    and t.dsdatebeg=Nvl(b.dsdatebeg,'01-jan-1900')
                                    and t.dsdateend=Nvl(b.dsdateend,'01-jan-3000')
                                    and rownum<=1)
                    *\
                   ) b, repcond rc
              Where b.agrisn=rc.agrisn
                and nvl(decode(rc.premUsd,null,gcc2.gcc2(rc.premiumsum,rc.premcurrisn,53,sysdate),rc.premUsd),0) >=0
                AND( dsdatebeg between rc.datebeg and rc.dateEnd
                     or  rc.datebeg  between dsdatebeg and dsdateend)
              group BY rc.agrisn, addisn,  newaddisn,  AddSign,  addbeg, dsdatebeg,dsdateend
            ) S
     ) S
Where decode(AddPrem,0,1/AddCnt,PremUsd/AddPrem)>0;


-- удаляем переходы на адендум без кондов
Update
 TT_ADD_INFO A
Set
A.NEWADDISN=NULL,
A.newadddateval=Null
WHERE  A.NEWADDISN IN
(SELECT B.NEWADDISN FROM TT_ADD_INFO B WHERE  B.NEWADDISN IS NOT NULL
    Minus
 SELECT B.ADDISN FROM TT_ADD_INFO B);



\*
delete from tt_add_info a where not exists(select 'a' from tt_add_info b where b.addisn=a.newaddisn)
and a.newaddisn is not null;
*\




-- по адендумам без кондов  перенаправляем проводку на "договор"
Update Report_BuhBody_List
Set
Addisn=agrisn
Where (Addisn,Nvl(dsdatebeg,'01-jan-1900'),Nvl(dsdateend,'01-jan-3000')) in (

-- список адендумов , у которых есть начисления
Select Addisn,Nvl(dsdatebeg,'01-jan-1900'),Nvl(dsdateend,'01-jan-3000')
from Report_BuhBody_List b
Where statcode not in (220,24) and statcode in (select statcode from rep_statcode Where grp in ('Прямое страхование','Входящее перестрахование'))
and agrisn<>addisn
Minus
-- список адендумов по этим договорам, у которых есть конды
Select --+ Ordered Use_Hash(ad)
 Addisn,Nvl(dsdatebeg,'01-jan-1900'),Nvl(dsdateend,'01-jan-3000')
from tt_add_info ad
Where agrisn in (select distinct agrisn from Report_BuhBody_List Where statcode not in (220,24) )
--and loadisn=pLoadIsn

)
and Refundisn is null;


--*\ sagrp4


--\* sagrp4

Loop
\* генеририуем фиктивные проводки *\


\* немного о поле vDocsumIsn. В обычной проводке доксумма начислятся на 1 адендум, поэтому, когда у доксуммы
есть дата начала или окончания мы отбирем те конды адендума, которые действуют в этот период и она однозначно
идентифицирует список кондов и проводку для последующей сборки. Для фиктивных проводок мы нарушаем заповедь
"одна доксумма - один адендум "- фиктивная проводка с этой доксуммой "начисляется" на все дерево сторнирующих адендумов
поэтому вводим "виртуальный" исн, дабы вернутся к "одна доксумма - один адендум ". При сборке бухтуконда эта виртуальная
фигня уйдет в никуда - по ней будет сделанна только связь с пачкой кондов и все

2,decode(dsdatebeg,null,decode(dsdateend,null,Nvl(b.addisn,b.agrisn),Nvl(vDocsumIsn,DocsumIsn)),Nvl(vDocsumIsn,DocsumIsn))
если у доксуммы есть дата начала или дата конца - связь по vDocsumIsn
нет - по Nvl(b.addisn,b.agrisn)
*\



-- это список проводок сторно-переноса, т.е. если адендум чего-то кому-то отдает,то эта же сумма идет
-- на его отдающую часть, только со знаком " - " (мажем не навсе конды адендума, а на "отдающие")
-- заменяем DateVal на дату первого начисления принимающего адендума, ставим Sagroup -4 , тип загрузки - 3
-- потом должны нагенерить слой кондов для 3-го типа, сбрасываем bodyisn в null  и пишем bodyisn в parentisn


-- Datequit наследуем, дабы определять сумму сквитованную в отчетах




insert into Report_BuhBody_List (isn, dpc, bpc, fpc, bodyisn, agrisn, addisn,
      statcode, deptisn, classisn, subaccisn, currisn, BuhAmount, BuhAmountRub,BUHAMOUNTUSD, DateVal,Datequit,
       DocSumIsn,  ReprDeptIsn,SubjIsn,BodySubjIsn,REFUNDPC,Sagroup,
      ParentIsn,Loadtype,RPTGROUPISN,isrevaluation,AGRCLASSISN,agrdiscr,RULEISNAGR,dsdatebeg,dsdateend,vDocsumIsn)
Select
      null isn,-newaddpc*dpc dpc, bpc, fpc, -1 bodyisn, agrisn, addisn,
      statcode, deptisn, classisn, subaccisn, currisn, BuhAmount, BuhAmountRub,BUHAMOUNTUSD,
      newadddateval DateVal, Datequit, DocSumIsn, ReprDeptIsn,SubjIsn,BodySubjIsn,1 REFUNDPC,4 Sagroup,
      decode(Nvl(bodyisn,-1),-1,ParentIsn,bodyisn)ParentIsn, 3 Loadtype,RPTGROUPISN,
      isrevaluation,AGRCLASSISN,agrdiscr,RULEISNAGR,dsdatebeg,dsdateend,vDocsumIsn
from (
Select --+ Ordered USe_Hash(ad)
 b.*,ad.newadddateval,newaddpc
from
(
Select --+ Ordered Use_Nl(ad)
bodyisn,DocSumIsn, Max(dsdatebeg) dsdatebeg,Max(dsdateend) dsdateend,
Sum(BuhAmountRub*dpc*bpc*fpc)/Max(BuhAmountRub) dpc,1 bpc,1 fpc, Max(agrisn) agrisn, Max(addisn) addisn,
Max(statcode) statcode, Max(deptisn) deptisn, Max(classisn) classisn, Max(subaccisn) subaccisn,
Max(currisn) currisn, Max(BuhAmount) BuhAmount, Max(BuhAmountRub) BuhAmountRub,Max(BUHAMOUNTUSD) BUHAMOUNTUSD,
       Max( ReprDeptIsn) ReprDeptIsn, Max(SubjIsn) SubjIsn,Max(BodySubjIsn) BodySubjIsn,Max(ParentIsn) ParentIsn,
       Max(RPTGROUPISN)RPTGROUPISN,
Max(isrevaluation) isrevaluation,
Max(AGRCLASSISN) AGRCLASSISN,
Max(agrdiscr) agrdiscr,
Max(RULEISNAGR) RULEISNAGR,
Max(vDocsumIsn) vDocsumIsn,
Max(Datequit) Datequit


from Report_BuhBody_List b
Where statcode not in (220,24)
and b.stat is null -- метка для цикла
group by bodyisn,docsumisn

) b,tt_add_info ad
Where b.addisn=ad.addisn
and ad.newadddateval is not null -- новый адендум должен быть "инициализирован" или вообще "Быть"
and Nvl(ad.addisn,0)<>Nvl(ad.newaddisn,0)
and dpc<>0 -- защита от зацикливаний - бывает однако
and Nvl(b.dsdatebeg,'01-jan-1900')=ad.dsdatebeg
and Nvl(b.dsdateend,'01-jan-3000')=ad.dsdateend
--and ad.loadisn=pLoadisn
);


Update Report_BuhBody_List set stat=1 Where stat is null ; -- пометили как обработанные в цикле 1 раз

-- это список проводок сторно-переноса, т.е. если адендум чего-то кому-то отдает,то эта же сумма идет
-- на его принимающий адендум, только с коэфициентом переноса
-- заменяем DateVal на дату первого начисления принимающего адендума, ставим Sagroup -4 ,

insert into Report_BuhBody_List (isn, dpc, bpc, fpc, bodyisn, agrisn, addisn,
      statcode, deptisn, classisn, subaccisn, currisn, BuhAmount, BuhAmountRub,BUHAMOUNTUSD, DateVal, Datequit,
       DocSumIsn,  ReprDeptIsn,SubjIsn,BodySubjIsn,REFUNDPC,Sagroup,
      ParentIsn,RPTGROUPISN,isrevaluation,AGRCLASSISN,agrdiscr,RULEISNAGR,dsdatebeg,dsdateend,vDocSumIsn)
Select
      null isn,newaddpc*dpc dpc, bpc, fpc, -1 bodyisn, agrisn,newaddisn addisn,
      statcode, deptisn, classisn, subaccisn, currisn, BuhAmount, BuhAmountRub,BUHAMOUNTUSD,
      newadddateval DateVal,Datequit, DocSumIsn, ReprDeptIsn,SubjIsn,BodySubjIsn,1 REFUNDPC,4 Sagroup,
      decode(Nvl(bodyisn,-1),-1,ParentIsn,bodyisn)ParentIsn,RPTGROUPISN,
      isrevaluation,AGRCLASSISN,agrdiscr,RULEISNAGR,dsdatebeg,dsdateend,Seq_Reports.NEXTVAL
from (
Select --+ Ordered USe_Hash(ad)
 b.*,ad.newaddisn,newadddateval,ad.newaddpc
from

(
Select
bodyisn,DocSumIsn,Max(dsdatebeg) dsdatebeg,Max(dsdateend) dsdateend,
Sum(BuhAmountRub*dpc*bpc*fpc)/Max(BuhAmountRub) dpc,1 bpc,1 fpc, Max(agrisn) agrisn, Max(addisn) addisn,
Max(statcode) statcode, Max(deptisn) deptisn, Max(classisn) classisn, Max(subaccisn) subaccisn,
Max(currisn) currisn, Max(BuhAmount) BuhAmount, Max(BuhAmountRub) BuhAmountRub,Max(BUHAMOUNTUSD) BUHAMOUNTUSD,
       Max( ReprDeptIsn) ReprDeptIsn, Max(SubjIsn) SubjIsn,Max(BodySubjIsn) BodySubjIsn,Max(ParentIsn) ParentIsn,
       Max(RPTGROUPISN) RPTGROUPISN,
 Max(isrevaluation) isrevaluation,
Max(AGRCLASSISN) AGRCLASSISN,
Max(agrdiscr) agrdiscr,
Max(RULEISNAGR) RULEISNAGR,
Max(Datequit) Datequit


from Report_BuhBody_List b
Where statcode not in (220,24)
and stat=1 and Loadtype is null -- флаг цикла+ не переносим ранее сгенерированные
group by bodyisn,docsumisn
)
 b,tt_add_info ad
Where b.addisn=ad.addisn
and ad.newadddateval is not null -- новый адендум должен быть "инициализирован"
and Nvl(ad.addisn,0)<>Nvl(ad.newaddisn,0) -- защита от зацикливаний - бывает однако
and dpc<>0
and Nvl(b.dsdatebeg,'01-jan-1900')=ad.dsdatebeg
and Nvl(b.dsdateend,'01-jan-3000')=ad.dsdateend
--and ad.loadisn=pLoadisn
);

exit when sql%rowcount<=0;


Update Report_BuhBody_List set stat=2 Where stat =1 ; -- пометили как обработанные в цикле 2 раз

end loop;


--*\ sagrp4*/

  --EGAO 02.03.2011 активные аддендумы договоров
  /*INSERT INTO tt_add_info(agrisn, addisn)
  SELECT x.agrisn, a.isn
  FROM (SELECT DISTINCT x.agrisn FROM Report_BuhBody_List x WHERE x.statcode NOT IN (220,24)) x,
       agreement a
  WHERE ((a.isn = x.agrisn AND (a.discr = 'Д' OR a.discr = 'Г')) OR
         (a.parentisn = x.agrisn AND a.discr = 'А' AND CASE NVL(a.id,'X')
                                                         WHEN 'П' THEN
                                                           CASE WHEN (SELECT COUNT(1)
                                                                         FROM repcond rc
                                                                         WHERE rc.agrisn=x.agrisn
                                                                           AND rc.addisn=a.parentisn
                                                                           AND rownum<=1
                                                                        )=1 THEN 1
                                                                   ELSE 0
                                                           END
                                                         ELSE 1
                                                       END=1))
    AND ((a.status='В' AND  a.datesign<=vDateStage) OR
         (SELECT COUNT(1) FROM repbuhbody bb WHERE bb.agrisn=x.agrisn AND nvl(bb.addisn, bb.agrisn)=a.isn AND nvl(bb.amount,0)<>0 AND ROWNUM<=1)<>0
        );*/

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



  /*
    KGS 11.01.2013 в связи с выносом части медицинских кондов из repcond теперь надо лазить в другие таблицы (код выше)

    select --+ ordered use_nl (b r c ag) index (c X_REPCOND_COND) no_merge( b ) index ( r X_REPREFUND_REFUNDISN ) index ( ag X_REPAGR_AGR )
    1, b.refundisn, c.condisn,
     c.isn,
     r.condpc,
     ag.datebeg agrdatebeg,ag.dateend agrdateend,
     c.datebeg, c.dateend,
     c.riskruleisn,
     ag.ruleisn agrruleisn,
     ag.classisn agrclassisn, ag.comission agrcomission,ag.discr agrdiscr,
     c.objclassisn,c.rptclassisn,nvl(r.agrisn,C.AgrIsn),
    C.OBJREGION,
    C.OBJCOUNTRY,
    c.Riskclassisn,RISKPRNCLASSISN,c.CLIENTISN,c.AgrOldDateEnd,Nvl(c.OBJPRNCLASSISN,C.ObjClassIsn) PARENTOBJCLASSISN,
    AgrEXTISN,addsign,  addbeg,c.objisn,Nvl(c.parentobjisn,c.objisn) PARENTOBJISN,
    r.classisn,-- EGAO 02.03.2011
    ag.bizflg,
    decode(nvl(c.premusd,0),0,gcc2.gcc2(c.premiumsum,c.premcurrisn,53,sysdate-1),c.premusd) -- EGAO 15.03.2011
    ,c.premagr -- EGAO 07.09.2011
    ,C.CARRPTCLASS
    from (select distinct refundisn from Report_BuhBody_List
    where  refundisn is not null) b, reprefund r, repcond c,repagr ag
    where b.refundisn = r.refundisn
--      and r.condisn is not null
      and r.condisn = c.condisn(+)
      and r.agrisn=ag.agrisn(+);
*/
 --   commit;


   update --+ use_hash (b)
    Report_BuhBody_List b set
      loadtype = 1
    where refundisn In (select bodyisn from REP_COND_LIST);
 --  Commit;




-- льем на все конды - тем где 1 конд с премией 0 или адендум , где все конды с премией 0 - хрен с ними,
-- льем на них с фиктивным condPC, иначе проблемы классификации.
-- конды с отрицательной премией не учитываем

/* ВОТ ЭТО  - МЕНЯЕМ  - ТУТ Должны появится активные слои кондов по ДОГОВОРАМ!*/


  --...потом привязанные к аддендумам (договор - это тоже адендум)
    /*insert into Rep_buh2cond_list
    (loadtype, bodyisn, condisn, repcondisn, condpc, datebeg, dateend,
     datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
     objisn,PARENTOBJISN
     )


    Select * from (
    select 2 loadtype, Nvl(DocsumIsn,addisn) Bodyisn,  condisn, repcondisn,
     decode(nvl(Trunc(addprem,2),0),0,1/addcnt,premusd/addprem) CondPc,
     agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn, agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
     CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
     objisn, PARENTOBJISN
    from
    (

    select addisn,condisn, isn repcondisn, premusd,
    agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
    agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
    OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
    SUM (premusd) OVER (PARTITION BY Nvl(DocsumIsn,addisn)) as AddPrem,
    Count(*) OVER (PARTITION BY Nvl(DocsumIsn,addisn)) as AddCnt,
    CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
    DocsumIsn,objisn,PARENTOBJISN
    from
    (
    select --+  ordered USe_Nl(b c) Index(C X_REPCOND_ADDISN) no_merge ( b )
     c.addisn , c.newaddisn,DocsumIsn,
     agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
     agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,c.AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,Nvl(c.OBJPRNCLASSISN,C.ObjClassIsn) PARENTOBJCLASSISN,
     max (isn) isn, max (condisn) condisn,
     sum (Decode(Nvl(premusd,0),0,gcc2.gcc2(c.premiumsum,c.premcurrisn,53,sysdate-1),premusd)
     * ( (Least(dsdateend,c.dateend)-Greatest(dsdatebeg,c.datebeg)+1)/(c.dateend-c.datebeg+1) )) premusd,


     Max(c.CLIENTISN) Clientisn ,Max(AgrOldDateEnd) AgrOldDateEnd,Max(addsign) addsign,Max(addbeg) addbeg,
     Decode(IsDks,1,objisn,null) objisn,  Decode(IsDks,1,Nvl(c.parentobjisn,c.objisn),null)   PARENTOBJISN

    from (select  Nvl(addisn,agrisn) addisn,Nvl(dsdatebeg,'01-jan-1900')dsdatebeg,Nvl(dsdateend,'01-jan-3000')dsdateend,
     decode(dsdatebeg,null,decode(dsdateend,null,null,vDocsumIsn),vDocsumIsn) DocsumIsn,
     Max(decode(deptisn,707480016,0,23735116,0,1)) IsDks--EGAO 03.12.2009  Max(decode(deptisn,11414319,1,0)) IsDks
     from Report_BuhBody_List
     where loadtype is null
      group by Nvl(addisn,agrisn) ,Nvl(dsdatebeg,'01-jan-1900'),Nvl(dsdateend,'01-jan-3000'),
     decode(dsdatebeg,null,decode(dsdateend,null,null,vDocsumIsn),vDocsumIsn)
     ) b, repcond c
    where b.addisn = c.addisn
    and Nvl(c.premiumsum,0)>=0
    and (   dsdatebeg between c.datebeg and c.dateEnd
    or c.datebeg  between dsdatebeg and dsdateend)

    group by c.addisn,c.newaddisn,
    agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
     agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,c.AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,Nvl(c.OBJPRNCLASSISN,C.ObjClassIsn),DocsumIsn,
     Decode(IsDks,1,objisn,null),Decode(IsDks,1,Nvl(c.parentobjisn,c.objisn),null)
     ) ) ) Where Nvl(CondPc,0)<>0;*/

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
    /*insert into REP_COND_LIST
    (loadtype, bodyisn, condisn, repcondisn, condpc, datebeg, dateend,
     datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
     objisn,PARENTOBJISN,bizflg, premusd, premagr,CARRPTCLASS
    )
    Select * from
    (
     select 2 loadtype, agrisn AS Bodyisn,  condisn, repcondisn,
            decode(nvl(Trunc(addprem,2),0),0,1/addcnt,(premagr\*EGAO 08.09.2011 premusd*\*lengthpc)/addprem) CondPc,
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
               SUM (premagr\*EGAO 08.09.2011 premusd*\*lengthpc) OVER (PARTITION BY agrisn) as AddPrem,
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
                   from (SELECT DISTINCT a.agrisn,
                                         a.bizflg,
                                         CASE
                                           WHEN vDateStage BETWEEN trunc(a.agrdatebeg) AND trunc(a.agrdateend) THEN
                                             least(trunc(a.agrdateend), add_months(trunc(a.agrdatebeg),trunc(months_between(vDateStage,trunc(a.agrdatebeg))/12)*12+12)-1)
                                           ELSE trunc(a.agrdateend)
                                         END AS agrinsurancedateend
                         FROM report_buhbody_list a,
                              (select isn from dicti start with isn IN (686160416, 683205716, 47160616) connect by prior isn = parentisn) x
                         WHERE a.loadtype IS NULL AND a.ruleisnagr=x.isn(+) AND x.isn IS NULL
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
    ) Where Nvl(CondPc,0)<>0;*/
    --}EGAO 11.12.2012




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

 --   commit;



    --Медики, туристы
    /*insert into REP_COND_LIST
    (loadtype, bodyisn, condisn, repcondisn, condpc, datebeg, dateend,
     datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
     objisn,PARENTOBJISN,bizflg, premusd, premagr
    )
    Select * from
    (
     select 3 loadtype, addisn AS Bodyisn,  condisn, repcondisn,
            decode(nvl(Trunc(addprem,2),0),0,1/addcnt,st_premagr\*EGAO 08.09.2011 st_premusd*\/addprem) CondPc,
            agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn, agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
            OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
            CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
            objisn, PARENTOBJISN,bizflg, premusd, premagr
    from
        (

        select addisn,condisn, isn repcondisn, st_premagr, \*EGAO 08.09.2011 st_premusd,*\
               agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
               agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
               OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
               SUM (st_premagr\*EGAO 08.09.2011 st_premusd*\) OVER (PARTITION BY addisn) as AddPrem,
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
                        \* KGS 13.01.2012 временно на пробу
                        *((Least(trunc(c.dateend),  CASE
                                                                   WHEN trunc(c.dateend)>vDateStage AND months_between(trunc(c.dateend), trunc(c.datebeg))>ShortCondMonthPeriod THEN
                                                                     add_months (trunc(c.datebeg),trunc(greatest(0, months_between(vDateStage,trunc(c.datebeg)))/12)*12+12)-1
                                                                   ELSE trunc(c.dateend)
                                                                 END) - trunc(c.datebeg)+1)/(trunc(c.dateend)-trunc(c.datebeg)+1))
                          *\                                       )  st_premagr,
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
                                 , c.addisn SourceAdd \* KGS 10.01.2012 поле определяет, из какого аддендума пришел конд. чтобы "пачки также жались"*\
                          FROM a, repcond c
                          WHERE nvl(c.premiumsum,0)>=0
                            AND c.addisn=a.addisn
                          UNION ALL
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
                            AND c.newaddisn=a.addisn
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


    --{EGAO 27.07.2012
    --=======борьба с минусоми на переходах на аддендумы с нулевой плановой премией по медицинским договорам
    insert into REP_COND_LIST
    (loadtype, bodyisn, condisn, repcondisn, condpc, datebeg, dateend,
     datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
     objisn,PARENTOBJISN,bizflg, premusd, premagr
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
                                 c.premagr, -- EGAO 07.09.2011
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
                                 SUM(c.premagr) over (PARTITION BY ssource, addisn, transfernewadd) AS addprem,
                                 COUNT(1) over (PARTITION BY ssource, addisn, transfernewadd) AS addcnt,
                                 COUNT(DISTINCT ssource) over (PARTITION BY addisn, transfernewadd) AS ssourcecnt
                         FROM (
                               WITH c AS (
                                          SELECT c.addisn, c.newaddisn, c.pc, c.agrinsurancedateend, c.bizflg
                                          FROM (
                                                SELECT c.addisn, c.newaddisn, c.bizflg, c.agrinsurancedateend,
                                                       decode(c.premagrall,0,1/c.cntall,c.premagr/c.premagrall) AS pc
                                                FROM (
                                                      SELECT --+ index ( c X_REPCOND_ADDISN ) ordered use_nl ( a c )
                                                             c.addisn,
                                                             c.newaddisn,
                                                             SUM(c.premagr) AS premagr,
                                                             SUM(sum(c.premagr)) over (PARTITION BY c.addisn) AS premagrall,
                                                             COUNT(1)  over (PARTITION BY c.addisn) AS cntall,
                                                             max(a.bizflg) AS bizflg,
                                                             max(a.agrinsurancedateend) AS agrinsurancedateend
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
                                                      GROUP BY c.addisn, c.newaddisn
                                                     ) c

                                               ) c,
                                               storage_source.zeropremiumsumaddendum x
                                          WHERE x.addisn=c.newaddisn
                                            AND nvl(pc, 0)<>0
                                         )
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
                                      ,1 AS ssign,
                                      c.newaddisn AS transfernewadd
                               FROM c, repcond cd
                               WHERE cd.addisn=c.newaddisn
                               UNION ALL
                               SELECT 2,
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
                               FROM c, repcond cd
                               WHERE cd.addisn=c.addisn AND cd.newaddisn=c.newaddisn
                              ) c
                         WHERE trunc(c.datebeg)<=c.agrinsurancedateend
                        ) c
                  WHERE c.ssourcecnt=2
                 )c
            group by c.addisn, c.newaddisn,
                     c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                     c.agrclassisn, c.agrcomission, c.agrdiscr, c.rptclassisn,c.agrisn,
                     c.riskclassisn,c.riskprnclassisn,SourceAdd, transfernewadd, ssource
           )
    ) Where Nvl(CondPc,0)<>0;
    --}*/
    --{EGAO 31.08.2012
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

   -- commit;




/* добавляем кусок для слоя 3 - для него проводки уже нагенерированны*/


/*--\* sagroup 4
    insert into Rep_buh2cond_list
    (loadtype, bodyisn, condisn, repcondisn, condpc, datebeg, dateend,
     datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,objisn,
      PARENTOBJISN)

    select --+ Ordered Use_Hash(b)
     3 loadtype, bodyisn, condisn, repcondisn,
     CONDPC/sum(CONDPC) over (partition by bodyisn) CondPc,
     datebeg, dateend, datebegcond, dateendcond, ruleisn, RULEISNAGR, agrclassisn, COMISSION, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
     CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,objisn,parentobjisn
    from  Rep_buh2cond_list b
    Where Loadtype=2 and newaddisn is not null and b.newaddisn in
    (select n.newaddisn from tt_add_info n where n.newadddateval is not null);


--*\ sagroup 4*/

  /*
  --...потом привязанные к договорам...
    insert into Report_Buh2Cond_List
    (loadtype, bodyisn, condisn, repcondisn, condpc,
     datebeg, dateend, datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN)
    select 3, agrisn, condisn, repcondisn, decode (nvl (Trunc(addprem,2),0),0,1/addcnt,premusd/addprem),
     agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn, agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN
    from (select agrisn, condisn, isn repcondisn, premusd,
     agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
     agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,
          OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,
          PARENTOBJCLASSISN,
     SUM (premusd) OVER (PARTITION BY agrisn) as AddPrem,
     COUNT (isn) OVER (PARTITION BY agrisn) as AddCnt
    from (select --+ use_nl (b c) index (c X_REPCOND2_AGR) ordered

     c.agrisn, agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
     agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,    OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
--   Clientisn,  AgrOldDateEnd,
--     isn, condisn, premusd,Nvl(c.OBJPRNCLASSISN,C.ObjClassIsn) PARENTOBJCLASSISN

     Max(c.CLIENTISN) Clientisn ,Max(AgrOldDateEnd) AgrOldDateEnd,
     max (isn) isn, max (condisn) condisn, sum (premusd) premusd,Nvl(c.OBJPRNCLASSISN,C.ObjClassIsn) PARENTOBJCLASSISN

    from (select distinct agrisn,dateval  from Report_BuhBody_List
     where loadtype is null and agrisn is not null) b, repcond c
    where b.agrisn = c.agrisn And C.NewAddIsn Is Null
    group by c.agrisn, agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
     agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,
        OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,Nvl(c.OBJPRNCLASSISN,C.ObjClassIsn)
    /*datebeg, dateend, parentisn, b.agrisn, decode (b.deptisn, MedicDept, 0, objisn),
     parentobjisn, riskisn, parentriskisn, limitisn, rptclassisn, limclassisn,
     currisn, premcurrisn, objclassisn, limitclassisn,
     c.agrdatebeg, c.agrdateend, c.riskruleisn, c.agrruleisn,
     c.agrclassisn, c.agrcomission, c.agrdiscr/));
--    commit;

    update --+ use_hash (b)
    Report_BuhBody_List b set loadtype = 3
    where loadtype is null
      and agrisn in (select bodyisn from Report_Buh2Cond_List where loadtype = 3);
--    commit;
*/


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

  -- commit;



    update --+ use_hash (b)
    Report_BuhBody_List b set loadtype = 4
    where loadtype is null
      and agrisn in (select bodyisn from REP_COND_LIST where loadtype = 4);

  --  commit;



  --...очищаем мусор...
    delete from REP_COND_LIST where nvl (condpc,0) = 0;
--    commit;



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

/*vCnt:=0;


select --+ordered use_hash(bc)   --MSerp 15.10.2010. Грубая прикидка размера.
sum(nagr) into vCnt from
Report_BuhBody_List bb, (select agrisn, count(*) nagr from Rep_buh2cond_list group by agrisn)  bc
where bb.agrisn = bc.agrisn ;


if vCnt>1000000 then  --MSerp 15.10.2010. Короче, если буфер кондов маленький, то инсертим кучей. Иначе по-старому, по одной записи.
    vCnt:=0;
else
 vBufCnt:=1;
end if;*/

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

---------------------------------------------
Procedure LoadBuh2Cond
(
pLoadIsn IN Number
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
--{EGAO 27.02.2012 В комментариях ниже неустойчивый код (зависит от плана выполнения запроса)
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
  FROM v_rptgroup2rule a;
 --}
--{EGAO 27.02.2012 В комментариях ниже неустойчивый код (зависит от плана выполнения запроса)
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
  FROM v_rptgroup2agrrule a;

--}
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

---------------------------------------------
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
----------------------------------------------
Procedure         LoadRefund_MakeHist
(
      pLoadIsn   in   Number,
      pDateBeg in Date,
      pDateEnd in Date:=null,
      pLastIsn  in Number:=Null
  )
   is

     vMaxHistIsn integer;
     vMinHistIsn integer;
     vCurDate Date;
     vDateEnd Date;
     vIsn    Number;
     vSql varchar2(1000);
     vRc Number;
     vBlockIsn number;
     vBlockIsn1 number;
     vBlockIsn2 number;
     vBlockIsn3 number;
Begin
     select seq_rep_block.NEXTVAL into vBlockIsn from dual;
     RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist', pAction => 'Begin', pBlockIsn=>vBlockIsn);

        /*Проверка ликвидности отрезка*/
         If (pDateEnd is null) or (pDateEnd<=pDateBeg)Then
            vDateEnd:=Least(Trunc(Sysdate)-1,pDateBeg);
         else
            vDateEnd:=Least(Trunc(Sysdate)-1,pDateEnd);
         end if;

        vCurDate:=Least(Trunc(Sysdate)-1,pDateBeg);



         /*Выгружаем по дням*/
 While vCurDate<=vDateEnd Loop
     select seq_rep_block.NEXTVAL into vBlockIsn1 from dual;
     RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist','DAY '||Trunc(vCurDate), pAction => 'BEGIN', pBlockIsn=>vBlockIsn1);

            Select gethistlogisn(vCurDate)-1e-15 into vMinHistIsn from  dual;
            Select gethistlogisn(vCurDate+1) into vMaxHistIsn from  dual;

            IF (pLastIsn is not null) and (pLastIsn>vMinHistIsn) Then
             vMinHistIsn:=pLastIsn;
            end if;


    IF vMinHistIsn<vMaxHistIsn Then


       Delete From  tt_rephistlog;
        Commit;
       select seq_rep_block.NEXTVAL into vBlockIsn2 from dual;
       RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist', 'GetLogPerDay',  pAction => 'Begin', pBlockIsn=>vBlockIsn2);

        vrc:=0;

              /*Перекачиваем логи за день*/
                      insert into tt_RepHistLog (TableName,RecIsn)
                      select /*+ use_nl(h lt) INDEX_ASC (H PK_HISTLOG) */
                         Distinct  H.TableName,H.RecIsn
                         from
                             HistLog H,rephistlogtables Lt
                          where
                                 Lt.proc_isn=3
                             And H.tablename=Lt.TableName
                             And H.isn>vMinHistIsn
                             And H.Isn<vMaxHistIsn
                          ;
                    vrc:=SQL%RowCount;
                   commit;
       RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist', 'GetLogPerDay',  pAction => 'End',pObjCount=>vrc, pBlockIsn=>vBlockIsn2);

         /*Идем по списку таблиц*/

          For Cur In (Select * From rephistlogtables Where proc_isn=3) Loop

            select seq_rep_block.NEXTVAL into vBlockIsn2 from dual;
            RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist', 'GET TABLE '||CUR.TABLENAME,  pAction => 'Begin', pBlockIsn=>vBlockIsn2);

             Delete From rep_buf_recisn;
             Delete From tt_rowid;
            Commit;

        select seq_rep_block.NEXTVAL into vBlockIsn3 from dual;
        RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist', 'Insert into rep_buf_recisn',  pAction => 'Begin', pBlockIsn=>vBlockIsn3);
             /*загоняем записи по одной таблице в буфер*/
                   Insert Into rep_buf_recisn
                    (Select Distinct RecIsn From tt_rephistlog Where TableName=Cur.TableName);
                vrc:=sql%rowcount;
               commit;
        RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist', 'Insert into rep_buf_recisn',  pAction => 'End',pObjCount=>vrc, pBlockIsn=>vBlockIsn3);

        select seq_rep_block.NEXTVAL into vBlockIsn3 from dual;
        RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist', 'Insert into tt_rowid',  pAction => 'Begin', pBlockIsn=>vBlockIsn3);
               /* загоняем в tt_buf_buhbody  список buhbody, полученный из view*/
                 vSql:='Insert Into tt_rowid (isn) Select Distinct ISN FROM '||Cur.VIEWNAME||'))';
                  Execute Immediate vSql;
                  vrc:=sql%rowcount;
        RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist', 'Insert into tt_rowid',  pAction => 'End',pObjCount=>vrc, pBlockIsn=>vBlockIsn3);

        LoadRefund_By_TT_RowId(pLoadIsn,0);
        LoadRepRefund_Hist_By_TT_RowId(pLoadIsn,0);

        RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist','GET TABLE '||CUR.TABLENAME,  pAction => 'End', pBlockIsn=>vBlockIsn2);
    end loop; /* по таблицам*/


    RepLoad_U(pLoadIsn,pLastisnloaded=>vMaxHistIsn);
 end if; /* vMinHistIsn<vMaxHistIsn */

    RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist','DAY '||Trunc(vCurDate), pAction => 'End', pBlockIsn=>vBlockIsn1);
    vCurDate:=vCurDate+1;

 end loop; /*vCurDate<=vDateEnd*/

       RepLog_i (pLoadIsn, 'LoadRepRefund_MakeHist', pAction => 'End', pBlockIsn=>vBlockIsn);
end;
----------------------------------------------
Procedure LoadRefund_MakeFull
   (  pLoadIsn   in   Number,
      pMinIsn  in number:=null
    )

Is
 vMinIsn Number:=Nvl(pMinIsn,-1e30);
 vMaxIsn       Number := 0;
 vCnt Number:=0;
  SesId        Number;
  vSql Varchar2(4000);
  vBlockIsn number;

Begin

select seq_rep_block.NEXTVAL into vBlockIsn from dual;
RepLog_i (pLoadIsn, 'LoadRepRefund_MakeFull', pAction => 'Begin', pBlockIsn=>vBlockIsn);

If pMinIsn is Null Then
Execute Immediate 'truncate table reprefund';
Execute Immediate 'Truncate Table reprefund_hist';
Execute Immediate 'Truncate Table reprefund_re';
end if;


SesId:=PARALLEL_TASKS.createnewsession('LoadRefund');


vSql:='report_buh_storage_new.LoadDocSumm_WO_Buhbody('||pLoadIsn||');';
PARALLEL_TASKS.processtask(sesid,vSql);

Loop


     Select Max(Isn)
     into vMaxIsn
     from
     (
        Select --+ Index (a X_AgrRefund)
          Isn
         From AgrRefund A
          Where Isn>vMinIsn And RowNum<=5000);

  exit when vMaxIsn is null;

            vSql:='Begin REPORT_BUH_STORAGE_NEW.LoadRefund_By_Isns('||ploadisn||','||vMinIsn||','||vMaxIsn||'); END;';
            PARALLEL_TASKS.processtask(sesid,vSql);

           vMinIsn:=vMaxISn;
           RepLoad_U(pLoadIsn,pLastisnloaded=>vMaxIsn);
           vCnt:=vCnt+1;
    DBMS_APPLICATION_INFO.set_module('LoadRefund','Applied : '||vCnt*5000);
end loop;

PARALLEL_TASKS.endsession(sesid);

RepLog_i (pLoadIsn, 'LoadRepRefund_MakeFull', pAction => 'End', pBlockIsn=>vBlockIsn);
end;


----------------------------------------------
Procedure LoadRefund
(  pLoadIsn   in   Number,
   pRunType in Number:=Report_Storage.cpReRun
 )

 Is
    vRunParam number;
    vMode number;
    vDtBeg date;
    vDtEnd date;
    vBlockIsn number;
   Begin

   LoadIsn:=pLoadIsn;

 --  EXECUTE IMMEDIATE 'alter session set sort_area_size = 33554432';
    select seq_rep_block.NEXTVAL into vBlockIsn from dual;
    RepLog_i (pLoadIsn, 'LoadRepRefund', pAction => 'Begin', pBlockIsn=>vBlockIsn);


      Select LASTISNLOADED,f.loadtype,f.datebeg,f.dateend
      Into vRunParam,vMode,vDtBeg,vDtEnd
      from RepLoad f
      Where Isn=pLoadIsn;

     STORE_AND_DROP_TABLE_INDEX('reprefund');

     If pRunType=Report_Storage.cpReRun then
      vRunParam:=null;
     end if;

     RepLoad_U(pLoadIsn,pLastrundate=>Sysdate);


     CreateClaimHistBuffer(pLoadIsn);

      if vMode = Report_Storage.cLoadFull then
        LoadRefund_MakeFull(pLoadIsn,vRunParam);
       else
        LoadRefund_MakeHist(pLoadIsn,vDtBeg,vDtEnd,vRunParam);
      end if;
     RepLoad_U(pLoadIsn,pLastenddate=>Sysdate);
     RepLog_i (pLoadIsn, 'LoadRepRefund', pAction => 'End', pBlockIsn=>vBlockIsn);



RESTORE_TABLE_INDEX('reprefund');

End;
---------------------------------------------
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

/*
        Update
              RepRefund Rf
Set
    RptGroupIsn=null,CondDeptIsn=null,ISREVALUATION=null,BUDGETGROUPISN=null

    where Isn > '||vminisn||' and Isn<='||vlmaxisn||' ;
    Commit;
*/

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


---------------------------------------------
/*select isn, loadisn,
 to_char (datebeg,'dd-mm-yyyy hh24:mi ss'),
 to_char (dateend,'dd-mm-yyyy hh24:mi ss'), module, objcount
from replog --where loadisn = 25
where dateend is not null
order by dateend desc
begin
REPORT_BUH_STORAGE.LoadBuh_Primary;
--REPORT_BUH_STORAGE.LoadBuh2Cond;
end;*/
---------------------------------------------



Procedure LoadRepBuh_MakeFull
   (  pLoadIsn   in   Number,
      pMinIsn  in number:=null
    )
   is
     vIsn          Number := 0;
     vMaxIsn       Number := 0;
     vrc           Number := 0;
     vAll Number;

      SesId        Number;
      vSql Varchar2(4000);
      vBlockIsn number;


Begin
  IF pMinIsn is Null Then
    Execute Immediate 'truncate table RepBuhBody drop storage';
    Execute Immediate 'truncate table RepBuhQuit drop storage';
    vIsn:=-9e30;
   else
    vIsn:=pMinIsn;
  end if;
    select seq_rep_block.NEXTVAL into vBlockIsn from dual;
    RepLog_i (pLoadIsn,'LoadRepBuh_MakeFull', 'Main Loops',  pAction => 'Begin', pBlockIsn=>vBlockIsn);

   Store_and_drop_table_index('RepBuhBody');
   Store_and_drop_table_index('RepBuhQuit');



Select /*+ Index_FFS(b)*/Count(Isn) into vAll from ais.buhbody_t b Where Isn>vIsn;

      SesId:=PARALLEL_TASKS.createnewsession('');

      LOOP

vMaxIsn:=cut_table('ais.buhbody_t','Isn',vIsn,'',LoadObjCnt*20);

   EXIT WHEN vMaxIsn is null;


            vSql:='Begin REPORT_BUH_STORAGE_NEW.LoadRepBuh_By_Isns('||ploadisn||','||vISn||','||vMaxIsn||'); END;';
            PARALLEL_TASKS.processtask(sesid,vSql);

           vIsn:=vMaxISn;
           RepLoad_U(pLoadIsn,pLastisnloaded=>vMaxIsn);
            vrc:=vrc+1;

          DBMS_APPLICATION_INFO.set_module('LoadBuh',vrc*LoadObjCnt*20||' from '||vAll );


         End LOOP;
       RepLog_i (pLoadIsn, 'LoadRepBuh_MakeFull', 'Main Loops',  pAction => 'End', pBlockIsn=>vBlockIsn);

       PARALLEL_TASKS.endsession(sesid);

/*
-- Догрузка начислений премии с субсчетов не своевременных начислений
Delete From TT_BODY_LIST;
Insert Into TT_BODY_LIST
Select RowNum,S.*,3,null
from
(
     select --+ use_nl (b s b1)   ordered
      S.isn,
      s.deptisn,
      Decode(dsclass,414,
      (
       Select MIn(Decode(bs.parentisn,687356516,38,687358016,38,687359516,34,48))
       from ais.buhbody_t qb, ais.buhsubacc_t bs
       Where qb.headisn=s.headisn
       and qb.subaccisn=bs.isn
      ),445,(Select Min(Decode(bs.parentisn,687356516,220,687358016,220,687359516,24,48))
       from ais.buhbody_t qb, ais.buhsubacc_t bs
       Where qb.headisn=s.headisn
       and qb.subaccisn=bs.isn) ,48) Statcode,
      s.classisn
     from(

      Select b.Isn,s.deptisn,b1.classisn,headisn,
      (Select --+ Use_Concat(ds)
         ds.classisn
        From  docsum ds
       Where (ds.debetisn=b.isn or ds.creditisn=b.isn) and discr in ('P','F')
         and rowNum<=1) dsclass

      from  ais.subacc4dept s,ais.buhsubacc_t b1,ais.buhbody_t b
     where s.statcode=48
       and s.subaccisn = b1.isn
       and b1.dateend >= to_date ('01-01-2002','dd-mm-yyyy')
       and b.Dateval >= to_date ('01-01-2002','dd-mm-yyyy')
       AND nvl (damountrub,camountrub) <> 0
        and status = 'А'
        and oprisn not in (9534516, 24422716)
        and b.subaccisn = s.subaccisn
         )S

) S Where Statcode is not null;

           LoadBuh_By_List(1);
commit;

*/



   Restore_table_index('RepBuhBody');
   Restore_table_index('RepBuhQuit');


     SesId:=PARALLEL_TASKS.createnewsession('');

vIsn:=0;
 LOOP

vMaxIsn:=cut_table('storage_source.repbuhbody','BodyIsn',vIsn);

   EXIT WHEN vMaxIsn is null;


            vSql:='
            Begin
            REPORT_BUH_STORAGE_NEW.Set_Body_Dept_Isn_By_List('||vISn||','||vMaxIsn||');
            END;';
            PARALLEL_TASKS.processtask(sesid,vSql);

           vIsn:=vMaxISn;
         End LOOP;

       PARALLEL_TASKS.endsession(sesid);


   End;


Procedure LoadRepBuh_MakeHist
  (
      pLoadIsn   in   Number
  )
   is

     vMaxIsn integer:=0;
     vMinIsn integer:=0;
     SesId        Number;
     vSql Varchar2(4000);
     vBlockIsn number;
     vBlockIsn1 number;
     vCnt number;

   Begin
     select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
     DBMS_APPLICATION_INFO.set_module('LoadRepBuh_MakeHist','');
     RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', pAction => 'Begin', pBlockIsn=>vBlockIsn);
     STORE_AND_DROP_TABLE_INDEX('RepBuhBody',1);
     STORE_AND_DROP_TABLE_INDEX('RepBuhQuit',1);



      SesId:=PARALLEL_TASKS.createnewsession;

     select Seq_rep_block.NEXTVAL into vBlockIsn1 from dual;

     RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist','Get Log List', pAction => 'Begin', pBlockIsn=>vBlockIsn1);


     Update --+ Index( l)
       rep_MV_Log l
      Set
       GetStatus=1
      where Nvl(loadisn,0)=0 And Upper(SHEMANAME)='BUHBODY';
      Commit;
        -- загоняем логи в буфер и обрабатываем
      execute immediate 'truncate table tt_Isns';
       Insert Into  tt_Isns
       Select RowNum, RecIsn
       from
       (Select --+ Index( l X_REP_MV_LOG_SHEMA_LOAD)
        Distinct Recisn from rep_MV_Log l where Nvl(loadisn,0)=0 And Upper(SHEMANAME)='BUHBODY' and GetStatus=1);
        vCnt:=Sql%RowCount;
      Commit;

     RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist','Get Log List', pAction => 'End', pBlockIsn=>vBlockIsn1);
      Loop
        vMaxIsn:=Cut_Table('storages.tt_Isns','ISN',vMinIsn,pRowCount=>5000);
      Exit When vMaxIsn is Null;
    vSql:='begin
      report_buh_storage_new.LoadRepBuh_By_Hist_Isns('||pLoadIsn||','||vMinIsn||','||vMAxIsn||');
           end; ';

          PARALLEL_TASKS.processtask(sesid,vSql);
        DBMS_APPLICATION_INFO.set_module('LoadRepBuh_MakeHist',' proc: '||vMinIsn||' from: '||vCnt);
           vMinIsn:=vMaxIsn;
       End LOOP;


 PARALLEL_TASKS.endsession(sesid);






  RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', pAction => 'End', pBlockIsn=>vBlockIsn);

  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

  RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', 'Index Rebuild', pAction => 'Begin', pBlockIsn=>vBlockIsn);
      RESTORE_TABLE_INDEX('RepBuhBody');
      RESTORE_TABLE_INDEX('RepBuhQuit');
  RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', 'Index Rebuild', pAction => 'End', pBlockIsn=>vBlockIsn);





End;

/*
  (
      pLoadIsn   in   Number,
      pDateBeg in Date,
      pDateEnd in Date:=null,
      pLastIsn  in Number:=Null
  )
   is

     vMaxHistIsn integer;
     vMinHistIsn integer;
     vCurDate Date;
     vDateEnd Date;
     vIsn    Number;
     vSql varchar2(4000);

     vMaxIsn       Number := 0;
     vrc           Number := 0;
     SesId        Number;
     vBlockIsn number;
     vBlockIsn1 number;
     vBlockIsn2 number;
     vBlockIsn3 number;

Begin
     select seq_rep_block.NEXTVAL into vBlockIsn from dual;
     RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', pAction => 'Begin', pBlockIsn=>vBlockIsn);

      --  Проверка ликвидности отрезка
         If (pDateEnd is null) or (pDateEnd<=pDateBeg)Then
            vDateEnd:=Least(Trunc(Sysdate)-1,pDateBeg);
         else
            vDateEnd:=Least(Trunc(Sysdate)-1,pDateEnd);
         end if;

        vCurDate:=Least(Trunc(Sysdate)-1,pDateBeg);

  SesId:=PARALLEL_TASKS.createnewsession;

         --Выгружаем по дням
 While vCurDate<=vDateEnd Loop
     select seq_rep_block.NEXTVAL into vBlockIsn1 from dual;
     RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist','DAY '||Trunc(vCurDate), pAction => 'BEGIN', pBlockIsn=>vBlockIsn1);

            Select gethistlogisn(vCurDate)-1e-15 into vMinHistIsn from  dual;
            Select gethistlogisn(vCurDate+1) into vMaxHistIsn from  dual;

            IF (pLastIsn is not null) and (pLastIsn>vMinHistIsn) Then
             vMinHistIsn:=pLastIsn;
            end if;

            If vCurDate+1>Trunc(sysdate) Then vMaxHistIsn:=vMaxHistIsn+1e-15; end if;


    IF vMinHistIsn<vMaxHistIsn Then


       Delete From  tt_rephistlog;
        Commit;
       select seq_rep_block.NEXTVAL into vBlockIsn2 from dual;
       RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', 'GetLogPerDay',  pAction => 'Begin', pBlockIsn=>vBlockIsn2);

        vrc:=0;

             --Перекачиваем логи за день
                      insert into tt_RepHistLog (TableName,RecIsn)
                      select --+ use_nl(h lt) INDEX_ASC (H PK_HISTLOG)
                         Distinct  H.TableName,H.RecIsn
                         from
                             HistLog H,rephistlogtables Lt
                          where
                                 Lt.proc_isn=2
                             And H.tablename=Lt.TableName
                             And H.isn>vMinHistIsn
                             And H.Isn<vMaxHistIsn
                          ;
                    vrc:=SQL%RowCount;
                   commit;
       RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', 'GetLogPerDay',  pAction => 'End',pObjCount=>vrc, pBlockIsn=>vBlockIsn2);

         --Идем по списку таблиц

          For Cur In (Select * From rephistlogtables Where proc_isn=2) Loop
            select seq_rep_block.NEXTVAL into vBlockIsn2 from dual;
            RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', 'GET TABLE '||CUR.TABLENAME,  pAction => 'Begin', pBlockIsn=>vBlockIsn);

             Execute Immediate 'Truncate table rep_buf_recisn';
             Execute Immediate 'Truncate table TT_buf_buhbody';

        select seq_rep_block.NEXTVAL into vBlockIsn3 from dual;
        RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', 'Insert into rep_buf_recisn',  pAction => 'Begin', pBlockIsn=>vBlockIsn3);
             --загоняем записи по одной таблице в буфер
                   Insert Into rep_buf_recisn
                    (Select Distinct RecIsn From tt_rephistlog Where TableName=Cur.TableName);
                vrc:=sql%rowcount;
               commit;
        RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', 'Insert into rep_buf_recisn',  pAction => 'End',pObjCount=>vrc, pBlockIsn=>vBlockIsn3);

        select seq_rep_block.NEXTVAL into vBlockIsn3 from dual;
        RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', 'Insert into tt_buf_buhbody',  pAction => 'Begin', pBlockIsn=>vBlockIsn3);
               -- загоняем в tt_buf_buhbody  список buhbody, полученный из view
                 vSql:='Insert Into tt_buf_buhbody (Select RowNum,Isn From (Select Distinct ISN FROM '||Cur.VIEWNAME||'))';
                  Execute Immediate vSql;
                  vrc:=sql%rowcount;
        RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', 'Insert into rep_buf_buhbody',  pAction => 'End',pObjCount=>vrc, pBlockIsn=>vBlockIsn3);

              --Загоняем  полученный список bodyisn-нов в буфер и обрабатываем
               vIsn:=0;
                     LOOP

                      Select Max(Isn)
                      into vMaxIsn
                      From
                        (Select Isn
                         From tt_buf_buhbody f
                         Where F.Isn>vIsn*LoadObjCnt And F.Isn<=(vIsn+1)*LoadObjCnt
                        );

                          EXIT WHEN vMaxIsn is null;
                          vSql:='Begin REPORT_BUH_STORAGE_NEW.LoadRepBuh_By_Hist_Isns('||ploadisn||','||vISn||','||vMaxIsn||'); END;';
                          PARALLEL_TASKS.processtask(sesid,vSql);
                         vIsn:=vIsn+1;
                       End LOOP;

             RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist','GET TABLE '||CUR.TABLENAME,  pAction => 'End', pBlockIsn=>vBlockIsn2);
           end loop; -- по таблицам


   RepLoad_U(pLoadIsn,pLastisnloaded=>vMaxHistIsn);
   end if; --vMinHistIsn<vMaxHistIsn

    RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist','DAY '||Trunc(vCurDate), pAction => 'End', pBlockIsn=>vBlockIsn1);
    vCurDate:=vCurDate+1;

 end loop; --vCurDate<=vDateEnd

 PARALLEL_TASKS.endsession(sesid);

       RepLog_i (pLoadIsn, 'LoadRepBuh_MakeHist', pAction => 'End', pBlockIsn=>vBlockIsn);


end;

*/

Procedure LoadRepBuh
(  pLoadIsn   in   Number,
   pRunType in Number:=Report_Storage.cpReRun
 )
   is
    vRunParam number;
    vMode number;
    vDtBeg date;
    vDtEnd date;
    vBlockIsn number;
   Begin

   LoadIsn:=pLoadIsn;

   EXECUTE IMMEDIATE 'alter session set sort_area_size = 33554432';
    select seq_rep_block.NEXTVAL into vBlockIsn from dual;
    RepLog_i (pLoadIsn, 'LoadRepBuh', pAction => 'Begin', pBlockIsn=>vBlockIsn);


      Select LASTISNLOADED,f.loadtype,f.datebeg,f.dateend
      Into vRunParam,vMode,vDtBeg,vDtEnd
      from RepLoad f
      Where Isn=pLoadIsn;



delete from tt_subacc4dept;


   -- замена стандартного subacc4dept на наш с добавлением группы счетов
   Insert Into
    tt_subacc4dept
    select s.code,
           s.deptisn,
           s.subaccisn,
           s.sheettype,
           s.statcode,
           s.updated,
           s.updatedby,
           s.isn,
           s.rptgroupisn,
           10*(Statcode-Round(Statcode))+1,
           Case When Code Like '913%' then '31-dec-2005' else null end
           -- 913 счета содержали комиссию до 31.12.2005 далее она стала начислятся на 263 счета
            -- согласно письму самохвалова от 12.04.2006
    from ais.subacc4dept s;
    Commit;


     If pRunType=Report_Storage.cpReRun then
      vRunParam:=null;
     end if;

     RepLoad_U(pLoadIsn,pLastrundate=>Sysdate);
      if vMode = Report_Storage.cLoadFull then
        LoadRepBuh_MakeFull(pLoadIsn,vRunParam);
       else
        LoadRepBuh_MakeHist(pLoadIsn);--,vDtBeg,vDtEnd,vRunParam);
      end if;
     RepLoad_U(pLoadIsn,pLastenddate=>Sysdate);
     RepLog_i (pLoadIsn, 'LoadRepBuh', pAction => 'End', pBlockIsn=>vBlockIsn);

    select seq_rep_block.NEXTVAL into vBlockIsn from dual;
    RepLog_i (pLoadIsn, 'LoadRepBuh',pOperation=>'SET_REVAL', pAction => 'Begin', pBlockIsn=>vBlockIsn);

     --SET_ISREVAL;

    RepLog_i (pLoadIsn, 'LoadRepBuh',pOperation=>'SET_REVAL', pAction => 'End', pBlockIsn=>vBlockIsn);

   End;


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


procedure LoadDocSumm_WO_Buhbody
(  pLoadIsn   in   Number)
-- Загрузка сумм по убыткам не привязанным к проводкам
Is
Begin

--Execute Immediate 'Truncate Table RepDocSum'; --Неправильно!!!!!!
--Надо вот так!!!


  delete /*+ Full(r) Parallel(r,12) */
  from RepDocSum r
  where classisn = 445;
 commit;


Insert Into RepDocSum
(
Select Seq_Reports.NEXTVAL,ploadisn,A.*
From
(
/*
With BB as
(
Select --+ ordered Use_Nl (sa bb)
  Bb.Isn, Bb.DateVal
From ais.subacc4dept sa, ais.buhbody_T bb
Where
And Sa.subaccisn=bb.subaccisn
)

Select --+ Ordered Index (ds X_DOCSUM_CLASS) Use_Nl(ds b1 b2 sa sa1)

       ds.Isn, Ds.agrisn, Ds.classisn, ds.datepay, ds.classisn2,
       ds.parentisn, ds.currisn, ds.amount, ds.amountrub, ds.amountusd, ds.docisn,
       ds.docisn2, ds.subjisn, ds.remainder, ds.status, ds.discr, ds.reaccisn, ds.accisn,
       ds.subaccisn, ds.debetisn, ds.creditisn, ds.refundisn ,
       0,--Decode(DS.currisn,29448516,ds.amount,gcc.gcc(Ds.amount,Ds.currisn,29448516,Ds.datepay)),
       MAX (Nvl(B1.Isn,Nvl(B2.Isn,0))) OVER
       (PARTITION BY decode (ds.discr,'P',ds.isn,'F',nvl (ds.parentisn,ds.isn))) as StatCode,
       0 dept, 0 rpt, 0 reval,
       MAX (Nvl(B1.DateVal,B2.DateVal)) OVER
       (PARTITION BY decode (ds.discr,'P',ds.isn,'F',nvl (ds.parentisn,ds.isn))) as DateVal
from ais.docsum ds, ais.buhbody_T b1,ais.buhbody_T b2,(select * from ais.subacc4dept where Statcode in (220,24) )sa,
(select * from ais.subacc4dept where Statcode in (220,24) )sa1
Where  Ds.ClassIsn=445 -- выплата
   And DISCR in ('P','F') -- факты и планы
   and nvl (ds.classisn2,0) not in (8729,8731)
   and nvl (ds.directionflg,'I') <> 'O'
   and B1.Isn(+)=Ds.CreditIsn
   And B2.Isn(+)= Ds.Debetisn
   and b1.subaccisn=Sa.subaccisn(+)
   and b2.subaccisn=Sa1.subaccisn(+)
*/
Select --+ Full(ds) Parallel(ds,12)

       ds.Isn, Ds.agrisn, Ds.classisn, ds.datepay, ds.classisn2,
       ds.parentisn, ds.currisn, ds.amount, ds.amountrub, ds.amountusd, ds.docisn,
       ds.docisn2, ds.subjisn, ds.remainder, ds.status, ds.discr, ds.reaccisn, ds.accisn,
       ds.subaccisn, ds.debetisn, ds.creditisn, ds.refundisn ,
       0,--Decode(DS.currisn,29448516,ds.amount,gcc.gcc(Ds.amount,Ds.currisn,29448516,Ds.datepay)),
       0 StatCode,
       0 dept, 0 rpt, 0 reval,
       to_date(null) DateVal
from ais.docsum ds
Where  Ds.ClassIsn=445 -- выплата
   And DISCR in ('F') -- факты и планы
   and nvl (ds.classisn2,0) not in (8729,8731)
   and nvl (ds.directionflg,'I') <> 'O'
  and not exists(select /*+ index ( bb X_REPBUHBODY_DOCSUM ) */ 'a' from repbuhbody bb where bb.docsumisn In (ds.isn,ds.parentisn) and bb.Statcode in (220,24))
)A);
Commit;
--MZ
update --+ index (s X_REPDOCSUM_DOCISN2)
repdocsum s
set dateval = (select signed from docs where isn = s.docisn2)
where docisn2 in (select --+ index (d X_DOCS_PAYFORM)
isn from docs d where payformisn in (930018925,1074571125))
and dateval is null;
Commit;

end;



procedure LoadRZUMemo (  pLoadIsn   in   Number)

-- запускать после простановки учетных групп в repbuh2cond
 Is
  Begin

Execute immediate 'Truncate table reprzumemo';

 --{EGAO 15.03.2013 Для гарантированной уникальности reprzumemo.docisn
 /*Insert Into reprzumemo
(
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
  from Ais.docs d, agreement a, dicx x
  where d.doc_type='11'
   and d.buhaccisn in (9822516, 690060416)
   and d.status not in ('-1','АН','ОШ','ОЛ')
   and a.isn (+)=d.agrisn
   and x.classisn1 (+)=a.ruleisn
   and x.classisn (+) = c.get('xRuleInsurType')
) A
);*/
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
--}EGAO 15.03.2013
 Commit;

end;



procedure LoadAgrRefundExt
(  pLoadIsn   in   Number)
Is
 vMinIsn Number:=0;
 vMaxIsn  Number;
 vLMaxIsn  Number;
 vCnt Number;
 vLoadObjCnt Number:=20000;

 SesId Number;
 vSql Varchar(4000);

Begin
/*
MSerp 15.02.05
заменил AgrISN на RefundISN b добавил ClaimShare
*/


vCnt:=0;

execute Immediate 'Truncate table RepAgrRefundExt';

SesId:=Gcc2.gcc2(1,35,53,Trunc(Sysdate));

SesId:=PArallel_Tasks.createnewsession;

 Loop



     Select Max(refundisn)
     Into vLMaxIsn
     From
       (
        Select --+ Index_Asc (a X_AGRREFUNDEXT_refund)
         refundisn
        From AIS.AgrRefundExt a
        Where refundisn>vMinIsn and RowNum<=vLoadObjCnt
        );

 Exit When vLMaxIsn is null;

VSql:=' Begin
Insert into RepAgrRefundExt
(
Select Seq_Reports.NEXTVAL,'||pLoadIsn||',A.*
From
(
Select --+ ordered Use_Nl (a ag)
AGRISN,Ag.classisn,CLAIMISN,REFUNDISN,dateevent,
A.CURRISN,CLAIMSUM,CLAIMSUMUSD,A.REFUNDSUM,REFUNDSUMUSD,
decode(Trunc((sum(nvl(claimsum,0))  over(partition by refundisn)),2),0,0,Nvl(claimsum,0)/Trunc((sum(nvl(claimsum,0))  over(partition by refundisn)),2)) ClaimShare,
dmc.code, a.classisn  medclassisn
from (
Select --+ Index (a X_AGRREFUNDEXT_AGR)
AGRISN,CLAIMISN,REFUNDISN,Trunc(A.dateevent) dateevent, CURRISN,
Sum(CLAIMSUM) CLAIMSUM,
Sum(Decode(CURRISN,53,CLAIMSUM,gcc2.gcc2(ClaimSum,CURRISN,53,DateEvent))) CLAIMSUMUSD,
Sum(REFUNDSUM) REFUNDSUM,
Sum(Decode(CURRISN,53,REFUNDSUM,gcc2.gcc2(REFUNDSUM,CURRISN,53,DateEvent))) REFUNDSUMUSD,
a.classisn
from AIS.AgrRefundExt A
Where refundisn>'||vMinIsn||' and refundisn<='||vLMaxIsn||'
Group by
AGRISN,CLAIMISN,REFUNDISN,Trunc(A.dateevent), CURRISN,a.classisn
) A, Ais.Agreement Ag, dicti dmc
Where A.agrisn=Ag.isn
and dmc.isn(+)=a.classisn
) A Where Nvl(ClaimShare,0)>0 )
;

 Commit;
end;';

 Parallel_Tasks.processtask(sesid,vsql);
  vMinIsn:=vLMaxIsn;
  vCnt:=vcnt+1;
 DBMS_APPLICATION_INFO.set_module('Load_RepAgrRefundExt','Applied : '||vCnt*vLoadObjCnt);
end loop;

Parallel_Tasks.endsession(sesid);

end;




Procedure LoadRepBuh_By_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number

)
 Is
   vBlockIsn number;
begin
/*
LoadIsn:=pLoadIsn;

Delete from TT_BODY_LIST;
    select seq_rep_block.NEXTVAL into vBlockIsn from dual;
         RepLog_i (pLoadIsn, 'LoadRepBuh_By_Isns', 'Insert report_body_list',  pAction => 'Begin', pBlockIsn=>vBlockIsn);
                 Insert Into TT_BODY_LIST
                 (Select RowNum, S.* ,null
                   From VZ_REPBUHBODY_LIST S
                   Where S.Isn>pMinIsn And S.ISn<=pMaxIsn);
         RepLog_i (pLoadIsn, 'LoadRepBuh_By_Isns', 'Insert report_body_list',  pAction => 'End', pobjCount => SQL%ROWCount, pBlockIsn=>vBlockIsn);
         */
           LoadBuh_By_List(1);
end;


Procedure LoadRepBuh_By_Hist_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number

)
 Is
   vBlockIsn number;
begin


  /*       REPORT_BUH_STORAGE_NEW.Loadisn:=pLoadIsn;

 select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

     RepLog_i (pLoadIsn, 'LoadRepBuh_By_Hist_Isns', 'GET LIST', pAction => 'Begin', pBlockIsn=>vBlockIsn);


   Delete from TT_BODY_LIST;

          Insert Into TT_BODY_LIST
               (Select --+ Ordered
                    RowNum, S.*,t.objisn
                   From tt_isns t,Ais.buhbody_t bb, VZ_REPBUHBODY_LIST S
                   Where t.Isn>pMinIsn And t.ISn<=pMaxIsn
                   And t.objisn=bb.headisn(+)
                   and bb.isn=s.isn(+)
                   );*/
     RepLog_i (pLoadIsn, 'LoadRepBuh_By_Hist_Isns', 'GET LIST', pAction => 'End', pBlockIsn=>vBlockIsn);
                  LoadBuh_By_List(0);




-- Догрузка начислений премии с субсчетов не своевременных начислений

 select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

     RepLog_i (pLoadIsn, 'LoadRepBuh_By_Hist_Isns', 'LOAD91LIST', pAction => 'Begin', pBlockIsn=>vBlockIsn);

             Delete From TT_BODY_LIST;
             Insert Into TT_BODY_LIST
             Select RowNum,S.*,3,null
              from
               (
                select --+ use_nl (t b s b1)   ordered
                    b.isn,
                    s.deptisn,
                 (
       Select Max(Decode(bs.parentisn,687356516,38,687358016,38,687359516,34))
       from ais.buhbody_t qb, ais.buhsubacc_t bs
       Where qb.headisn=b.headisn
       and qb.subaccisn=bs.isn
      ) Statcode,
      b1.classisn
      from  tt_isns t,ais.buhbody_t b,ais.subacc4dept s,ais.buhsubacc_t b1
     where t.Isn>pMinIsn And t.ISn<=pMaxIsn
         And t.objisn=b.headisn
        and b.subaccisn = s.subaccisn
       And s.statcode=48
       and s.subaccisn = b1.isn
       and b1.dateend >= to_date ('01-01-2002','dd-mm-yyyy')
       and b.Dateval >= to_date ('01-01-2002','dd-mm-yyyy')
       AND nvl (damountrub,camountrub) <> 0
        and status = 'А'
        and oprisn not in (9534516, 24422716)

         And Exists(Select --+ Use_Concat(ds)
          'X'
          From  docsum ds
        Where (ds.debetisn=b.isn or ds.creditisn=b.isn) and discr='P'
        and ds.classisn=414    and rowNum<=1)

) S Where Statcode is not null;
     RepLog_i (pLoadIsn, 'LoadRepBuh_By_Hist_Isns', 'LOAD91LIST', pAction => 'End', pBlockIsn=>vBlockIsn);

                      LoadBuh_By_List;

 select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

     RepLog_i (pLoadIsn, 'LoadRepBuh_By_Hist_Isns', 'CLEAR LOG', pAction => 'Begin', pBlockIsn=>vBlockIsn);

    Update --+ Index( h X_REP_MV_LOG_SHEMA_LOAD) Use_Hash(h) ordered
     rep_MV_Log H
    Set
         Loadisn=pLoadIsn
    where   Nvl(loadisn,0)=0 And Upper(SHEMANAME)='BUHBODY' and recisn in (
    Select ObjIsn from tt_isns t
                   Where t.Isn>pMinIsn And t.ISn<=pMaxIsn) and GetStatus=1;

commit;
     RepLog_i (pLoadIsn, 'LoadRepBuh_By_Hist_Isns', 'CLEAR LOG', pAction => 'End', pBlockIsn=>vBlockIsn);


-- простановка департамента для проводок с 91-го субсчета
Set_Body_Dept_Isn_By_List(pMinIsn,pMaxIsn);

  end;



Procedure LoadBuh2cond_By_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number

)
 Is
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




Procedure LoadRefund_By_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number

)

Is
  vBlockIsn number;
Begin

select seq_rep_block.NEXTVAL into vBlockIsn from dual;
RepLog_i (pLoadIsn, 'LoadRefund_By_Isns', pAction => 'Begin', pBlockIsn=>vBlockIsn);

  Delete From tt_RowId;
   Commit;

        Insert Into tt_RowId (Isn)
           Select --+ Index (a X_AgrRefund)
          Isn
         From AgrRefund A
          Where Isn>pMinIsn And Isn<=pMaxIsn;
       Commit;
  LoadRefund_By_TT_RowId(pLoadISn);
  LoadRepRefund_Hist_By_TT_RowId(pLoadIsn);
      Commit;
RepLog_i (pLoadIsn, 'LoadRefund_By_Isns', pAction => 'End', pBlockIsn=>vBlockIsn);
end;


procedure CreateClaimHistBuffer
(
pLoadIsn Number
)
Is
 vCnt Number;
 vMinIsn Number:=0;
 vLMaxIsn Number;
 vLoadObjCnt Number:=50000;

 SesId Number;
 vSql varchar2(32000);

Begin

 Select Nvl(MAX(LoadIsn),0)
 Into vCnt
 from TT_AgrClaim_Hist
 Where RowNum<=1;



If (vCnt<>pLoadIsn) Then
-- строим буфер по клаймам во времени

vCnt:=0;

Execute Immediate 'Truncate Table TT_AgrClaim_Hist';

SesId:=Parallel_Tasks.createnewsession;

 Loop

     Select Max(Isn)
     Into vLMaxIsn
     From
       (
        Select --+ Index_Asc (a x_AgrClaim)
         Isn
        From Ais.AgrClaim a
        Where Isn>vMinIsn and  RowNum<=vLoadObjCnt
        );
        Exit When vLMaxIsn is Null;
vSql:='
Begin
Insert Into TT_AgrClaim_Hist
(
Select  Isn,dateloss,dateclaim,datereg,datesolution, subjisn, status,
 deptisn,agrdatebeg,Lag(RecDateEnd,1,To_Date(''01-jan-1900'')) Over (partition By Isn Order by UPDATED) RecDateBeg,RecDateEnd,emplIsn,'||pLoadIsn||'
From
(
Select Isn,dateloss,dateclaim,datereg,datesolution, subjisn, status,
       deptisn,agrdatebeg,Max(emplIsn) emplIsn,Min(RecDateBeg) RecDateBeg,
       Max(RecDateEnd) RecDateEnd,Max(UPDATED)UPDATED
From
(
Select --+ Index (a x_AgrClaim)
       Isn,dateloss,dateclaim,datereg,datesolution, subjisn, status,
       deptisn,agrdatebeg,emplIsn,To_Date(Null) RecDateBeg,To_Date(''01-jan-3000'') RecDateEnd,To_Date(''01-jan-3000'') UPDATED,
       0 HasNext
from Ais.AgrClaim a
Where Isn>'||vMinIsn||' and Isn<='||vLMaxIsn||'
Union All
Select --+ Index (a PK_AGRCLAIM)
       Isn,dateloss,dateclaim,datereg,datesolution, subjisn, status,
       deptisn,agrdatebeg,emplIsn,To_Date(Null),Trunc(Updated),UPDATED,
 Lag(HistIsn,1,0) Over (partition By Isn,trunc(Updated) order by Updated desc) HasNext
From Hist.agrclaim a
Where Isn>'||vMinIsn||' and Isn<='||vLMaxIsn||'
)
WHERE HasNext=0
Group by Isn,dateloss,dateclaim,datereg,datesolution, subjisn, status,
         deptisn,agrdatebeg
)
);
 Commit;
End;';

 Parallel_Tasks.processtask(sesid,vsql);

vMinIsn:=vLMaxIsn;
vCnt:=vCnt+1;
 DBMS_APPLICATION_INFO.set_module('Load_CLAIMHIST','Applied : '||vCnt*vLoadObjCnt);
end loop;
PARALLEL_TASKS.endsession(sesid);

end if; -- необходимость перезагружать буфер

end;


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

/*KGS 18.01.2012*/








  -- типа поперли далее с всякими змарочками
  /*

  -- смысла нет заходить по конду - ниже зайдем по договору и типу риска

Update --+ Index_Asc(rf x_RepRefund)
    RepRefund Rf
Set
    (RptGroupIsn,CondDeptIsn,ISREVALUATION,BUDGETGROUPISN)=
         (
         Select rptgroupisn,deptisn,isrevaluation,BUDGETGROUPISN
         from
         (
          Select condisn, Nvl(rptgroupisn,0) rptgroupisn,deptisn,isrevaluation,BUDGETGROUPISN,
          Sum(AmountUsd) Cnt
           From RepBuh2Cond
           Where statcode in (38,34,221,224) and sagroup in (1,3)
           Group by condisn,Nvl(rptgroupisn,0) ,deptisn,isrevaluation,BUDGETGROUPISN
            Order By Cnt desc
           ) Where RowNum<=1 and  CondIsn=Rf.CondIsn
            )
where Isn > vMinIsn and Isn<=vLMaxIsn ANd Nvl(rptgroupisn,0)=0 and Nvl(condisn,0)>0;
--returning RptGroupIsn into rptgrp;

--dbms_output.put_line('1 '||rptgrp);


commit;

*/


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
--returning RptGroupIsn into rptgrp;

--dbms_output.put_line('2 '||rptgrp);

commit;

/*--EGAO 16.02.2012 Код обновления поля ISREVALUATION в конце процедуры
\* KGS 06.06.2011 простановка признака переоценки, если по договору с убытком нет начислений*\
Update  --+ Index(rf x_RepRefund) Ordered Use_Nl(rf)
    RepRefund Rf
Set
    ISREVALUATION=
    ( Case When nvl(Agrisn,0)=0 and CurrIsn!=35 Then 1
           When nvl(Agrisn,0)=0 and CurrIsn=35 Then 0
           When nvl(Agrisn,0)>0  and
          Exists (Select \*+ Ordered Use_Nl(sb)*\ ag.isn from repagr ag,ais.subject_t sb Where ag.clientisn=sb.isn and (ag.currisn=35 or sb.resident='Y'))
           Then 0
          else 1


      end
      )
where Isn > vMinIsn and Isn<=vLMaxIsn ANd Nvl(ISREVALUATION,0)=0 ;

commit;*/

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
--returning RptGroupIsn into rptgrp;

--dbms_output.put_line('3 '||rptgrp);



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
--returning RptGroupIsn into rptgrp;

--dbms_output.put_line('4 '||rptgrp);
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





/*
без смысленно, выше уже отработало
      IF  (Cur.AgrIsn is not null and Cur.RPTCLASSISN Is Not null) Then


             Select --+ Index(b)
                Max(rptgroupisn),nvl(Cur.ConddeptIsn,Max(deptisn)),Nvl(Cur.isrevaluation,Max(isrevaluation)),
                Max(BUDGETGROUPISN)
              Into Rpt(Cur.RowNum),Dept(Cur.RowNum),IsReval(Cur.RowNum),Budget(Cur.RowNum)
               From RepBuh2Cond B
               where B.AgrIsn=Cur.AgrIsn And
               Exists (Select 0 from Dicti Where Isn=Cur.rptclassisn Start With Isn=B.rptclassisn connect By Prior ParentIsn=isn)
               And RptGroupIsn>0
               And RowNum<=1;
   end if;


    IF (Rpt(Cur.RowNum) is null) and(Cur.AgrIsn is not null) Then

            Select --+ Index(b)
             Max(rptgroupisn),nvl(Cur.ConddeptIsn,Max(deptisn)),Nvl(Cur.isrevaluation,Max(isrevaluation)),Max(BUDGETGROUPISN)
             Into Rpt(Cur.RowNum),Dept(Cur.RowNum),IsReval(Cur.RowNum),Budget(Cur.RowNum)
               From RepBuh2Cond B
               where AgrIsn=Cur.AgrIsn
                 And RptGroupIsn>0
                And RowNum<=1;


    end if;

    IF (Rpt(Cur.RowNum) is null) Then
          Select Nvl(Cur.CondDeptIsn,GET_DEPT0ISN(Cur.DeptIsn)) Into Dept(Cur.RowNum) From Dual;
             If (Dept(Cur.RowNum) is not null) Then
                select
                  min (rptgroupisn)
                 Into Rpt(Cur.RowNum)
                from Ais.subacc4dept sd
                where deptisn = Dept(Cur.RowNum)
                 and statcode = 220;
              end if;


  end if;

 */





   If (Rpt(Cur.RowNum) is null)  then -- медецинские убытки, привяз к хоздоговору -

     --{ EGAO 15.03.2012
     /*Select Max(747777500),Nvl(Cur.Conddeptisn,Max(23735116))
     Into Rpt(Cur.RowNum),Dept(Cur.RowNum)
     from dicti
     Where ParentIsn=1621309703
     and isn=Cur.RULEISNCLAIM;*/

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

--{EGAO 16.02.2012 Не стал править код выше в плане isrevaLUATION, а просто обновляю поле здесь
UPDATE /*+ index ( a X_REPREFUND )*/
       RepRefund a
SET a.isrevaluation=--{ EGAO 05.06.2012
                    /*CASE
                      WHEN NVL(a.currisn,-1)=35 THEN 0
                    ELSE
                      CASE
                        WHEN nvl(a.agrisn,0)=0 THEN 1
                        ELSE (SELECT COUNT(1)
                              FROM dual
                              WHERE EXISTS (SELECT 'x' FROM rep_isreval b WHERE b.agrisn=a.agrisn)
                             )
                      END
                    END */
                    CASE WHEN NVL(a.currisn,-1)=35 THEN 0 ELSE 1 END
                    --}
where a.Isn > vMinIsn and a.Isn<=vLMaxIsn;
--}

COMMIT;
end;


procedure Set_Body_Dept_Isn_By_List (pMinIsn Number, pMaxIsn Number)
 is
  begin
Update --+ ordered use_nl(b) index ( b X_REPBUHBODY_BODYISN )
 repbuhbody b
 set
  deptisn=Nvl(( Select DeptIsn
            from (
            Select Agrisn,DeptIsn,Max(BuhAmountusd) Cnt
            from repbuhbody b1
             Where  Nvl(Deptisn,0)<>0
            Group by DeptIsn,Agrisn
            Order by cnt desc)
            WHere agrisn=b.agrisn and rownum<=1
            ),
            (
           Select DeptIsn
           from
           (
           Select distinct substr(code,3,5) code,deptisn
           from tt_subacc4dept s
           where s.statcode in (38,34,27) and nvl(deptisn,0)<>0
           and sagroup=1
           )
           Where Code=(select substr(Id,3,5) from buhsubacc sa where sa.isn=corsubaccisn)
            ))
  where bodyisn >pMinIsn and bodyisn<=pMaxIsn And Nvl(DeptIsn,0)=0;
Commit;
end;

procedure LoadReprefund_re(pLoadisn Number)
is
Begin

Null;


/*

Execute Immediate 'Truncate Table reprefund_re';



For Cur In(
Select  /*+ Ordered USe_Nl(ar)/
Distinct Claimisn from agrrefund ar where agrisn in (
Select Distinct Agrisn from ais.agrrefundx
Union
Select /*+ Index_Asc(ar)/Distinct Agrisn from Ais.agrx ar
) ) Loop  -- список перестрахованнх ClaimIsn

begin



/*
  insert  into reprefund_re
(  isn, loadisn, refundisn, currisn, agrxisn, sectisn,
       reisn,  reagrdeptisn, reagrclassisn, xref,sharePc,
       refundsum,subjisn, resum,secttype )
  select --+ use_nl (r s c) ordered
  SEQ_REPORTS.NextVal isn, r.*,
  greatest (0,least (getcrosscover(c.limitsum+prioritysum,s.Currisn,53,sysdate),r.RefundSum)-
  getcrosscover(prioritysum,s.Currisn,53,sysdate)) ReSum,
  s.secttype
  from (
  select --+ use_nl (t r a ra s)  ordered
  ploadisn, r.Isn refundisn, r.currisn,x.xisn agrxisn,
  s.Isn sectisn, s.agrisn reisn,
  ra.deptisn reagrdeptisn,
  ra.classisn reagrclassisn,
   x.XPC xref,
   x.XPC sharePc,
  r.claimsum*x.XPC/100 refundsum,
  ra.clientisn
  from  table(Ais.reinsn.refundretbl_Olap/*EGAO 07.10.2011 refundretbl2/(cast(Cur.Claimisn as number))) x, agrrefund r, repagr ra, ais.resection s
  where x.refundisn = r.isn
   and x.sectisn = s.isn (+)
  and s.AGRISN = ra.agrisn (+)

  ) r, resection s, recond c
  where r.sectisn = s.isn (+)
  and s.secttype (+) = 'XL'
  and s.isn = c.sectisn (+);

exception When others then
  If  sqlcode=-20102 then rollback;
  else
  raise;
 end if;
 end;


/*для старого типа оформления - из Agrrole/
  insert  into reprefund_re
(  isn, loadisn, refundisn, currisn,
        xref,sharePc,
       resum,subjisn)
  select --+ use_nl (r s c) ordered
  SEQ_REPORTS.NextVal isn, r.*
  from (
  select --+ use_nl ( r ar )  ordered
  ploadisn, r.Isn refundisn, r.currisn,
   ar.sharepc xref,
   ag.sharepc sharePc,
  r.claimsum*ar.sharepc*Nvl(ag.sharepc,1)/100 refundsum,
  ar.subjisn
  from   agrrefund r, agrrole ar,agreement ag
  where  r.claimisn=cur.claimisn
  and ar.agrisn=r.agrisn
  and ar.classisn=435
  and r.agrisn=ag.isn) r;




commit;
end loop;
*/
end;



PROCEDURE REP_LONGAGR( pLoadIsn number:=0)
IS
  vCntL      number:=0;
  vCntD      number:=0;
  vMinIsn    Number;
  vMaxIsn    Number := 0;
  LoadObjCnt Number := 100000;
  i          Number := 0;
  SesId      Number;
  vSql       Varchar2(4000);
BEGIN

--если больше в загрузке, то просто перпеписываем все
     execute immediate 'Truncate TABLE REP_AGR_PAYSUM drop storage';
     Execute Immediate 'Truncate Table REP_AGR_PREMIUM_RUB drop storage';
     Execute Immediate 'Truncate Table tt_ds_db_de drop storage';
     EXECUTE IMMEDIATE 'TRUNCATE TABLE rep_agr_pay_return_sum'; -- EGAO 17.12.2009



--инсертим премию по старым фактам (до 2002 года)
     insert into REP_AGR_PAYSUM
     select agrisn, datepay, PaySum, Seq_Reports.NEXTVAL, pLoadIsn, currisn, sharepc
     from (select --+ use_nl (s a) index (a X_REPAGR_AGR) ordered
       s.agrisn, s.datepay, a.currisn, nvl (a.sharepc,100)/100 sharepc,
       sum (getcrosscover (s.amount,s.currisn,a.currisn,s.datepay)) PaySum
      from repdocsum s, repagr a
      where s.classisn = 414
        and s.discr = 'F'
        and s.agrisn = a.agrisn
        and months_between(dateend+1,datebeg)>13
      group by s.agrisn, s.datepay, a.currisn, a.sharepc);

     --EGAO 17.12.2009
     insert into rep_agr_pay_return_sum(agrisn,vdate,ssum,isn,loadisn,currisn,sharepc,statcode)
     select a.agrisn, a.vdate, a.ssum, a.isn, a.LoadIsn, a.currisn, a.sharepc, 38
     from rep_agr_paysum a;
     commit;


  SesId:=Parallel_Tasks.createnewsession('Long Agr');
  vMinIsn:=0;
  loop
    vMaxIsn:=cut_Table('storage_source.repagr','Agrisn',vMinIsn);
    if (vMaxIsn is null) then Exit; end if; ----------------->
    vSql:='
          declare
            vMinIsn number:='||vMinIsn||';
            vMaxIsn number:='||vMaxIsn||';
            vLoadIsn number:='||pLoadIsn||';
          Begin
            storages.REPORT_BUH_STORAGE_NEW.rep_longagr_by_Isns(vLoadIsn,vMinIsn,vMaxIsn);
          end;';
    Parallel_Tasks.processtask(sesid,vsql);
    i := i+1;
    SYS.DBMS_APPLICATION_INFO.Set_Module ('Rep_LongAgr',i*LoadObjCnt);
    vMinIsn:=vMaxIsn;
  end loop;
  Parallel_Tasks.endsession(sesid);
end;

procedure rep_longagr_by_Isns(pLoadIsn number,vMinIsn number,vMaxIsn number)

IS
BEGIN

  /*Insert Into tt_rep_agr_paysum (agrisn,
                                 vdate,
                                 ssum,
                                 loadisn,
                                 currisn,
                                 sharepc, isn
                                 )

         Select agrisn, dateval,
           PaySum,

         pLoadIsn, currisn, sharepc, Seq_Reports.NEXTVAL
         From
        (Select  --+ Ordered Use_Nl(A Agr) Index (agr X_REPAGR_AGR)
           A.agrisn, A.dateval, Max(A.currisn) currisn, Max(sharepc) sharepc,
           sum(PaySum*100)/Max(sharepc) PaySum
        From (Select  --+  Ordered Use_Nl (a)
              a.agrisn,a.bodyisn, Max(dateval) dateval,
              Max(ag.currisn) currisn, Max(Nvl(ag.sharepc,100)) sharepc,
            gcc2.gcc2( Max(BuhAMOUNT),Max(a.CURRISN),Max(ag.currisn),Max(Dateval)) PaySum
           from repagr ag,repbuhbody a
           where ag.Agrisn >vMinIsn and  ag.Agrisn<=vMaxIsn
             and statcode in (34,38)
             and months_between(ag.dateend+1,ag.datebeg)>13
             and sagroup in (1,3)
             and ag.agrisn=a.agrisn
            group by a.agrisn, a.bodyisn
         ) a
         group by  A.agrisn, A.dateval);


               Insert Into  REP_AGR_PREMIUM_RUB
               (
                Select Seq_Reports.NEXTVAL,agrisn,dt,
                PREMRUB, de
               From
                (
                Select --+ Ordered Use_Nl(a b ag)
                a.agrisn,
                nvl(ADDSIGN,AGRDATEBEG) dt, NEWADDSIGN de,
                sum (getcrosscover (  a.premiumsum,a.premcurrisn,b.CurrIsn,a.datebeg)) PREMRUB
                From (Select Distinct AgrIsn,Currisn from tt_REP_AGR_PAYSUM ) b,RepCond a
                Where A.agrisn=b.agrisn
                and a.newaddisn is null
                group by A.agrisn, nvl(ADDSIGN,AGRDATEBEG), NEWADDSIGN
                ));
             Insert into REP_AGR_PAYSUM (agrisn,
                                         vdate,
                                         ssum,
                                         isn,
                                         loadisn,
                                         currisn,
                                         sharepc
                                         )
             (select agrisn,vdate,ssum,isn,loadisn,currisn,sharepc from tt_REP_AGR_PAYSUM);




--структура для фиктивных дат начала-окончания периода начисления для длинных договоров
       insert into tt_ds_db_de
       Select S.* from(
        Select --+  Ordered Use_Nl(bb ag) push_subq
                 distinct Nvl(addisn,agrisn) addisn, bodyisn,docsumisn,Decode(docsumisn,null,dateval,datepay ),dsdatebeg,DSDATEEND ,agrisn-- список всех доксумм по длинным договорам по премии С привязкой к адендуму (договору)
        from repbuhbody bb
           Where bb.agrisn in (Select distinct agrisn From tt_REP_AGR_PAYSUM a) -- это список длинных договоров
           and statcode in (38) and sagroup=1 and (dsdatebeg is null or DSDATEEND is null))s;




--берем datepay и смотрим, в какой страховой год адендума она попала - тут ему и период
    Update
          tt_ds_db_de b
       set

        (dsdatebeg,DSDATEEND)=

              (Select --+  Ordered Use_Nl(bb ag) push_subq
             Trunc( Add_Months(ag.datebeg,12*nb)) yb, trunc(Least(ag.dateend,Add_Months(ag.datebeg,12*ne)-1)) ye
            from
           agreement ag,( select \*+ Full(d)*\ rownum-1 nb,rownum ne from dicti d Where rownum<=40) tt
           Where ag.isn=b.addisn
             and  (ag.dateend>=Add_Months(trunc(ag.datebeg)-1,12*ne) or trunc(ag.dateend) between  Add_Months(trunc(ag.datebeg),12*nb) and Least(trunc(ag.dateend),Add_Months(trunc(ag.datebeg)-1,12*ne)))
             and trunc(least(greatest(b.datepay,ag.datebeg),ag.dateend)) between Add_Months(Trunc(ag.datebeg),12*nb)  and Least(Trunc(ag.dateend),Add_Months(Trunc(ag.datebeg),12*ne)-1)
              )
          Where agrisn in (Select distinct agrisn From tt_REP_AGR_PAYSUM a);
--если под этим периодом в адендуме нет кондов (бывает, надо разобраться), то сбрасываем период - пусть мажется на все
 Update--+  Ordered Use_Nl(b)
          tt_ds_db_de b
       set

        (dsdatebeg,DSDATEEND)=(select null,null from dual)



 Where agrisn in (Select distinct agrisn From tt_REP_AGR_PAYSUM a)
 and not exists (select 'a' from repcond rc where rc.addisn=b.addisn and  Nvl(rc.premiumsum,0)>=0
           and (   dsdatebeg between rc.datebeg and rc.dateEnd
             or  rc.datebeg  between dsdatebeg and dsdateend));*/


  --EGAO 16.12.2009 предыдущий вариант в комментариях выше

  Insert Into tt_rep_agr_paysum
  Select agrisn, dateval, PaySum, pLoadIsn, currisn, sharepc, statcode
  FROM (Select --+ Ordered Use_Nl(A Agr) Index (agr X_REPAGR_AGR)
               A.agrisn, A.dateval,
               a.statcode,
               Max(A.currisn) currisn, Max(sharepc) sharepc,
               sum(PaySum*100)/Max(sharepc) PaySum
        From (SELECT --+  Ordered Use_Nl (ag a) index ( a X_REPBUHBODY_AGR ) index ( ag X_REPAGR_AGR )
                     a.agrisn,a.bodyisn,
                     a.statcode,
                     Max(dateval) dateval,
                     Max(ag.currisn) currisn,
                     Max(Nvl(ag.sharepc,100)) sharepc,
                     gcc2.gcc2( Max(BuhAMOUNT),Max(a.CURRISN),Max(ag.currisn),Max(Dateval)) PaySum
              from repagr ag,repbuhbody a
              where ag.Agrisn >vMinIsn and  ag.Agrisn<=vMaxIsn
                and a.statcode in (34,38, 221, 241)
                and months_between(ag.dateend+1,ag.datebeg)>13
                and a.sagroup in (1,3)
                and ag.agrisn=a.agrisn
              group by a.agrisn, a.bodyisn, a.statcode
             ) a
        group by  A.agrisn, A.dateval, a.statcode
       );

  -- Плановая премия по договору, разваленная на дату подписания аддендума/создания договора (прикол, но NEWADDSIGN всегда равен NULL, т.к. это есть дата подписания аддендума, указанного в newaddisn, а он равен NULL по условию запроса)
  Insert Into  REP_AGR_PREMIUM_RUB
  (Select Seq_Reports.NEXTVAL,agrisn,dt,
          PREMRUB, de
   FROM (Select --+ Ordered Use_Nl(a b ag) index ( a X_REPCOND_AGR )
              a.agrisn,
              nvl(ADDSIGN,AGRDATEBEG) dt, NEWADDSIGN de,
              sum (getcrosscover (  a.premiumsum,premcurrisn,b.CurrIsn,a.datebeg)) PREMRUB
         From (Select Distinct a.AgrIsn, a.Currisn from tt_REP_AGR_PAYSUM a WHERE a.statcode in (34,38)) b,
               RepCond a
         Where A.agrisn=b.agrisn
           and a.newaddisn is null
         group by A.agrisn, nvl(ADDSIGN,AGRDATEBEG), NEWADDSIGN
        ));

  -- Начисленная премия по договору, разваленная на дату проводки
  Insert into REP_AGR_PAYSUM (agrisn, vdate, ssum, isn, loadisn, currisn,sharepc)
  SELECT a.agrisn,
         a.vdate,
         a.ssum,
         Seq_Reports.NEXTVAL,
         a.loadisn,
         a.currisn,
         a.sharepc
  FROM (select a.agrisn,
               a.vdate,
               sum(a.ssum) AS ssum,
               max(a.loadisn) AS loadisn,
               max(a.currisn) AS currisn,
               max(a.sharepc) AS sharepc
        FROM tt_REP_AGR_PAYSUM a
        WHERE statcode in (34,38)
        GROUP BY  A.agrisn, A.vdate) a;




  --структура для фиктивных дат начала-окончания периода начисления для длинных договоров
  insert into tt_ds_db_de
  Select S.*
  from (Select --+  Ordered Use_Nl(bb ag) push_subq index ( bb X_REPBUHBODY_AGR )
               distinct Nvl(addisn,agrisn) addisn, bodyisn,docsumisn,Decode(docsumisn,null,dateval,datepay ),dsdatebeg,DSDATEEND ,agrisn-- список всех доксумм по длинным договорам по премии С привязкой к адендуму (договору)
        from repbuhbody bb
        Where bb.agrisn in (Select distinct agrisn From tt_REP_AGR_PAYSUM a WHERE a.statcode in (34,38)) -- это список длинных договоров
          and statcode in (38) and sagroup=1 and (dsdatebeg is null or DSDATEEND is null)
       )s;

  --берем datepay и смотрим, в какой страховой год адендума она попала - тут ему и период
  UPDATE tt_ds_db_de b
  SET(dsdatebeg,DSDATEEND)=
     (Select --+  Ordered Use_Nl(bb ag) push_subq index ( ag X_AGREEMENT )
             Trunc( Add_Months(ag.datebeg,12*nb)) yb, trunc(Least(ag.dateend,Add_Months(ag.datebeg,12*ne)-1)) ye
      FROM agreement ag,
           ( select --+ Full(d)
                    rownum-1 nb,rownum ne from dicti d Where rownum<=40) tt
      Where ag.isn=b.addisn
       AND (ag.dateend>=Add_Months(trunc(ag.datebeg)-1,12*ne) or trunc(ag.dateend) between  Add_Months(trunc(ag.datebeg),12*nb) and Least(trunc(ag.dateend),Add_Months(trunc(ag.datebeg)-1,12*ne)))
       and trunc(least(greatest(b.datepay,ag.datebeg),ag.dateend)) between Add_Months(Trunc(ag.datebeg),12*nb)  and Least(Trunc(ag.dateend),Add_Months(Trunc(ag.datebeg),12*ne)-1)
     )
  Where agrisn in (Select distinct agrisn From tt_REP_AGR_PAYSUM a WHERE a.statcode in (34,38));

  --если под этим периодом в адендуме нет кондов (бывает, надо разобраться), то сбрасываем период - пусть мажется на все
  Update--+  Ordered Use_Nl(b)
          tt_ds_db_de b
  SET (dsdatebeg,DSDATEEND)=(select null,null from dual)
  Where agrisn in (Select distinct agrisn From tt_REP_AGR_PAYSUM a WHERE a.statcode in (34,38))
    and not exists (select --+ index ( rc X_REPCOND_ADDISN )
                           'a'
                    from repcond rc
                    where rc.addisn=b.addisn
                    and  Nvl(rc.premiumsum,0)>=0
                    and (dsdatebeg between rc.datebeg and rc.dateEnd or  rc.datebeg  between dsdatebeg and dsdateend)
                   );
  -- начисленная премия и возвраты по договорам, разваленные на дату проводки
  INSERT INTO rep_agr_pay_return_sum(agrisn, vdate, ssum, isn, loadisn, currisn, sharepc, statcode)
  SELECT a.agrisn,
         a.vdate,
         a.ssum,
         Seq_Reports.NEXTVAL,
         a.loadisn,
         a.currisn,
         a.sharepc,
         statcode
  FROM tt_REP_AGR_PAYSUM a;

  commit;
end;


procedure get_log_to_reload_repbuh2cond
is
now date:=sysdate;
begin

execute immediate 'truncate table tt_isns drop storage';

/*
Update --+ Parallel (a,12)
storage_source.MLOG$_REPBUHBODY a
set
SNAPTIME$$=Now;

Update  --+ Parallel (a,12)
storage_source.MLOG$_REPAGR a
set
SNAPTIME$$=Now;

Update --+ Parallel (a,12)
 storage_source.MLOG$_REPCOND a
set
SNAPTIME$$=Now;

-- чистим логи апдайта - они нам не нужны
loop
delete --+ Parallel (a,12) from storage_source.MLOG$_REPREFUND a Where DMLTYPE$$ not in ('I','D') and rownum<=300000;
exit when sql%rowcount<=0;
commit;
end loop;


Update  --+ Parallel (a,12)
storage_source.MLOG$_REPREFUND a
set
SNAPTIME$$=Now;

commit;

Insert into tt_isns

Select seq_reports.NEXTVAL,isn
from
(

Select --+ Parallel (a,12)
distinct bodyisn isn from storage_source.MLOG$_REPBUHBODY a
Where SNAPTIME$$=Now
);
commit;

delete from tt_rowid;
Insert into tt_rowid(Isn)
(select --+ Parallel (a,12)
distinct agrisn from storage_source.MLOG$_REPAGR a Where SNAPTIME$$=Now
         union
       select --+ Parallel (a,12)
      distinct agrisn from storage_source.MLOG$_REPCOND a Where SNAPTIME$$=Now
);
commit;

Insert into tt_isns
Select seq_reports.NEXTVAL,bodyisn
from
(
Select --+ Ordered Use_Nl(b)  Parallel (t,12)
distinct bodyisn
from
tt_rowid t,repbuh2cond b
Where t.isn=b.agrisn
and Nvl(b.refundisn,0)=0
and b.bodyisn>0
minus
Select objIsn from tt_isns);


delete from tt_rowid;

Insert into tt_rowid (Isn)
(select --+ Parallel (a,12)
distinct refundisn from storage_source.MLOG$_REPREFUND a Where SNAPTIME$$=Now );
commit;

Insert into tt_isns
Select seq_reports.NEXTVAL,bodyisn
from
(
Select --+  Ordered Use_Nl(b) Parallel (t,12)
distinct bodyisn
from  tt_rowid t,repbuh2cond b
Where t.isn=b.refundisn
and b.bodyisn>0
minus
Select objIsn from tt_isns);



commit;


delete --+ Parallel (a,12)
 from storage_source.MLOG$_REPBUHBODY a Where SNAPTIME$$=Now;
commit;
delete --+ Parallel (a,12)
from storage_source.MLOG$_REPAGR a Where SNAPTIME$$=Now;
commit;
delete --+ Parallel (a,12)
from storage_source.MLOG$_REPCOND a Where SNAPTIME$$=Now;
commit;
delete --+ Parallel (a,12)
from storage_source.MLOG$_REPREFUND a Where SNAPTIME$$=Now;
commit;

*/

end;




Procedure LoadBuh2Cond_BY_LOGS
(
pLoadIsn IN Number
)
Is
vMinIsn Number:=-1e30;

     vMaxIsn integer:=0;
     SesId        Number;
     vSql Varchar2(4000);
     vCnt Number:=0;
     vBlockIsn number;
     vBlockIsn1 number;
     vLoadObjcnt Number:=1000;
Begin


vCnt:=0;
DBMS_APPLICATION_INFO.set_module('LoadBuh2Cond','');

/*построили список измененных проводок*/
 select seq_rep_block.NEXTVAL into vBlockIsn1 from dual;
 RepLog_i (pLoadIsn, 'getlog', pAction => 'Begin', pBlockIsn=>vBlockIsn1);
 --get_log_to_reload_repbuh2cond;
 RepLog_i (pLoadIsn, 'getlog', pAction => 'End', pBlockIsn=>vBlockIsn1);


   select seq_rep_block.NEXTVAL into vBlockIsn from dual;
     RepLog_i (pLoadIsn, 'LoadRepBuh2Cond', pAction => 'Begin', pBlockIsn=>vBlockIsn);

-- заполняем буффер для простановки учетных групп

execute immediate 'truncate table TT_RULE_RPNGRP';
for cur in (Select Isn from ais.rule) loop

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
               Insert into TT_RULE_RPNGRP Values
               (Cur.ISN,SesId,'COND');
       end if;

 end loop;

for cur in (select ISN
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

 end loop;
commit;



STORE_AND_DROP_TABLE_INDEX('storages.repbuh2cond',1);

 SesId:=PARALLEL_TASKS.createnewsession('LoadBuh2cond');
/*
нужно для РЗУ, пока место загрузки не понятно.
vSql:='report_buh_storage_new.LoadDocSumm_WO_Buhbody('||pLoadIsn||');';
PARALLEL_TASKS.processtask(sesid,vSql);
*/
loop

vMaxIsn:=cut_table('storages.tt_isns','isn',vMinIsn,null,vLoadObjcnt);

    Exit when vMaxIsn is null;
    vCnt:=vCnt+1;
          vSql:='Begin
                 DBMS_APPLICATION_INFO.set_module(''LoadBuh2Cond'',''Process: '||vCnt||''');
                 REPORT_BUH_STORAGE_NEW.LoadBuh2cond_By_Log_Isns('||ploadisn||','||vMinIsn||','||vMaxIsn||');
                 END;';
          PARALLEL_TASKS.processtask(sesid,vSql,psetsortarea=>1);
          vMinIsn:=vMaxIsn;
          RepLoad_U(pLoadIsn,pLastisnloaded=>vMaxIsn);

    DBMS_APPLICATION_INFO.set_module('LoadBuh2Cond','Applied : '||vCnt*vLoadObjcnt);
end loop;


PARALLEL_TASKS.endsession(sesid);

RepLog_i (pLoadIsn, 'LoadRepBuh2Cond', pAction => 'End', pBlockIsn=>vBlockIsn);

--перестроили битмап индексы
 select seq_rep_block.NEXTVAL into vBlockIsn1 from dual;
 RepLog_i (pLoadIsn, 'Restore_INDEX', pAction => 'Begin', pBlockIsn=>vBlockIsn1);
   Restore_TABLE_INDEX('storages.repbuh2cond');
 RepLog_i (pLoadIsn, 'Restore_INDEX', pAction => 'End', pBlockIsn=>vBlockIsn1);






 select seq_rep_block.NEXTVAL into vBlockIsn from dual;
 RepLog_i (pLoadIsn, 'setrefundrptgroup', pAction => 'Begin', pBlockIsn=>vBlockIsn);
 setrefundrptgroup;
 RepLog_i (pLoadIsn, 'setrefundrptgroup', pAction => 'End', pBlockIsn=>vBlockIsn);

end;



Procedure LoadBuh2cond_By_Log_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number

)
 Is
begin

Delete from tt_rowid;

     Insert Into tt_rowid
     (Isn)
      Select --+ Index_Asc (b X_REPBUHBODY_BODYISN)
       Distinct ObjIsn
      from tt_isns
      where Isn > pMinIsn And  Isn <= pMaxIsn;
 --     And DeptIsn<> 23735116;--c.get('MedicDept')
    Commit;

     LoadBuh2Cond_By_List(pLoadIsn,pIsFull=>0,pCommitEveryPut=>1);
end;



Procedure LoadBuh2Cond_By_List_PT
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
      AGRSTATUS, Dsstatus
      )
    select --+ Full(a) PArallel(a,32) ordered Use_Nl(b ag lds adpt ad) index ( b X_REPBUHBODY_BODYISN )
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
      b.dsstatus
    from tt_rowid a, RepBuhBody b, /*EGAO 20.02.2012 rep_isreval r,*/repagr ag, agreement ad, tt_ds_db_de Lds, repagradept adpt
    where B.bodyisn=A.isn /*EGAO 16.02.2012 and a.isn=r.bodyisn(+)*/
    and b.agrisn=ag.agrisn(+)
    and b.Addisn=ad.isn(+)
    and b.bodyisn=Lds.bodyisn(+)
    and Nvl(b.docsumisn,0)=Nvl(Lds.docsumisn(+),0)
    AND adpt.agrisn(+)=b.agrisn;




  SET_RPTGROUPISN_BY_BUH_P; -- проставили учетную группу "по бухгалтерии"
  set_agr_buhdate_by_buh_P; -- проставили AgrBuhDate "по бухгалтерии" KGS 15.03.2011



  INSERT INTO tt_add_info(agrisn, addisn)
  SELECT/*+ Full(x) Parallel(x,32)*/
    x.agrisn, a.isn
  FROM (SELECT DISTINCT x.agrisn FROM Report_BuhBody_List x WHERE x.statcode NOT IN (220,24)) x,
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

  --Инсертим сначала привязанные к рефандам...

  /* НЕ МЕНЯЕМ!*/
    insert into REP_COND_LIST
    (loadtype, bodyisn, condisn, repcondisn, condpc,
     datebeg, dateend, datebegcond, dateendcond, ruleisn, ruleisnagr,
     agrclassisn, comission, agrdiscr, objclassisn,RPTCLASSISN,AgrIsn,OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
     CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,REFUNDEXTISN,addsign,  addbeg,objisn,parentobjisn, refundclassisn,
     BizFlg, premusd, premagr)
    select --+ ordered use_nl (b r c ag) index (c X_REPCOND_COND) no_merge( b ) index ( r X_REPREFUND_REFUNDISN ) index ( ag X_REPAGR_AGR )
    1, b.refundisn, c.condisn, c.isn, r.condpc,
    ag.datebeg agrdatebeg,ag.dateend agrdateend, c.datebeg, c.dateend, c.riskruleisn,ag.ruleisn agrruleisn,
    ag.classisn agrclassisn, ag.comission agrcomission,ag.discr agrdiscr, c.objclassisn,c.rptclassisn,nvl(r.agrisn,C.AgrIsn),
    C.OBJREGION,C.OBJCOUNTRY,c.Riskclassisn,RISKPRNCLASSISN,c.CLIENTISN,c.AgrOldDateEnd,Nvl(c.OBJPRNCLASSISN,C.ObjClassIsn) PARENTOBJCLASSISN,
    AgrEXTISN,addsign,  addbeg,c.objisn,Nvl(c.parentobjisn,c.objisn) PARENTOBJISN,
    r.classisn,-- EGAO 02.03.2011
    ag.bizflg,
    decode(nvl(c.premusd,0),0,gcc2.gcc2(c.premiumsum,c.premcurrisn,53,sysdate-1),c.premusd) -- EGAO 15.03.2011
    ,c.premagr -- EGAO 07.09.2011
    from (select /*+ Full(b) Parallel(b,32)*/ distinct refundisn from Report_BuhBody_List b
    where  refundisn is not null) b, reprefund r, repcond c,repagr ag
    where b.refundisn = r.refundisn
--      and r.condisn is not null
      and r.condisn = c.condisn(+)
      and r.agrisn=ag.agrisn(+);

 --   commit;


   update --+ Full(b) Parallel(b,32) use_hash (b)
    Report_BuhBody_List b set
      loadtype = 1
    where refundisn In (select  bodyisn from REP_COND_LIST b);
 --  Commit;





    --EGAO Все кроме медиков, туристов и парковых
    insert into REP_COND_LIST
    (loadtype, bodyisn, condisn, repcondisn, condpc, datebeg, dateend,
     datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
     objisn,PARENTOBJISN,bizflg, premusd, premagr
    )
    Select * from
    (
     select 2 loadtype, agrisn AS Bodyisn,  condisn, repcondisn,
            decode(nvl(Trunc(addprem,2),0),0,1/addcnt,(premagr/*EGAO 08.09.2011 premusd*/*lengthpc)/addprem) CondPc,
            agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn, agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
            OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
            CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
            objisn, PARENTOBJISN,bizflg, premusd, premagr
    from
        (
        select addisn,condisn, isn repcondisn, premusd, premagr,
               agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
               agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
               OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
               SUM (premagr/*EGAO 08.09.2011 premusd*/*lengthpc) OVER (PARTITION BY agrisn) as AddPrem,
               Count(*) OVER (PARTITION BY agrisn) as AddCnt,
               CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
               objisn,PARENTOBJISN,bizflg, lengthpc
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
                         ((Least(c.dateend, CASE
                                                 WHEN c.datebeg<=vDateStage AND c.dateend>vDateStage THEN
                                                   add_months (c.datebeg,trunc (months_between (vDateStage,c.datebeg)/12)*12+12)-1
                                                 ELSE c.dateend
                                               END) - c.datebeg+1)/(c.dateend-c.datebeg+1)) AS lengthpc,
                         c.Clientisn, c.AgrOldDateEnd, c.addsign, c.addbeg, c.objisn, Nvl(c.parentobjisn,c.objisn) AS parentobjisn,
                         bb.bizflg,
                         bb.agrinsurancedateend
                   from (SELECT  /*+ Full(a) Parallel(a,32)*/
                                 DISTINCT a.agrisn,
                                         a.bizflg,
                                         CASE
                                           WHEN a.agrdatebeg<=vDateStage AND a.agrdateend>vDateStage THEN
                                             add_months (a.agrdatebeg,trunc (months_between (vDateStage,a.agrdatebeg)/12)*12+12)-1
                                           ELSE a.agrdateend
                                         END AS agrinsurancedateend
                         FROM report_buhbody_list a,
                              (select isn from dicti start with isn IN (686160416, 683205716, 47160616) connect by prior isn = parentisn) x
                         WHERE a.loadtype IS NULL AND a.ruleisnagr=x.isn(+) AND x.isn IS NULL
                        ) bb,
                        tt_add_info b,
                        repcond c
                   where b.agrisn=bb.agrisn
                     AND b.addisn = c.addisn
                     and Nvl(c.premiumsum,0)>=0
                     AND (c.newaddisn is null or c.newaddisn not in (select addisn FROM tt_add_info x where x.agrisn=b.agrisn))
                  ) a
              WHERE a.datebeg<=a.agrinsurancedateend
             )
        )
    ) Where Nvl(CondPc,0)<>0;





    update --+ Full(b) Parallel(b,32)use_hash (b)
    Report_BuhBody_List b set loadtype = 2
    where loadtype is null
      and b.agrisn in (select bodyisn from REP_COND_LIST where loadtype = 2);-- EGAO 02.03.2011 and decode(dsdatebeg,null,decode(dsdateend,null,Nvl(b.addisn,b.agrisn),vDocsumIsn),vDocsumIsn) in (select bodyisn from Rep_buh2cond_list where loadtype = 2);

  --  commit;

    --Парковые
    insert into REP_COND_LIST
    (loadtype, bodyisn, condisn, repcondisn, condpc, datebeg, dateend,
     datebegcond, dateendcond, ruleisn, ruleisnagr, agrclassisn, comission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
     OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
     objisn,PARENTOBJISN,bizflg, premusd, premagr
    )
    Select * from
    (
     select 5 loadtype, addisn AS Bodyisn,  condisn, repcondisn,
            decode(nvl(Trunc(addprem,2),0),0,1/addcnt,(premagr/*EGAO 08.09.2011 premusd*/*lengthpc*premsign)/addprem) CondPc, -- EGAO 30.08.2011 decode(nvl(Trunc(addprem,2),0),0,1/addcnt,(premusd*lengthpc)/addprem) CondPc,
            agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn, agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
            OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
            CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
            objisn, PARENTOBJISN,bizflg, premusd, premagr
    from
        (

        select addisn,condisn, isn repcondisn,
               agrdatebeg, agrdateend, datebeg, dateend, riskruleisn, agrruleisn,
               agrclassisn, agrcomission, agrdiscr, objclassisn,rptclassisn,AgrIsn,
               OBJREGION,OBJCOUNTRY,Riskclassisn,RISKPRNCLASSISN,
               SUM (premagr/*EGAO 08.09.2011 premusd*/*lengthpc*premsign) OVER (PARTITION BY addisn) as AddPrem, -- EGAO 30.08.2011 SUM (premusd*lengthpc) OVER (PARTITION BY addisn) as AddPrem,
               Count(*) OVER (PARTITION BY addisn) as AddCnt,
               CLIENTISN,AgrOldDateEnd,PARENTOBJCLASSISN,newaddisn,addsign,  addbeg,
               objisn,PARENTOBJISN,bizflg, premusd, premagr, lengthpc, premsign
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
                    ((Least(c.dateend, CASE
                                           WHEN c.datebeg<=vDateStage AND c.dateend>vDateStage THEN
                                             add_months (c.datebeg,trunc (months_between (vDateStage,c.datebeg)/12)*12+12)-1
                                           ELSE c.dateend
                                         END) - c.datebeg+1)/(c.dateend-c.datebeg+1)) AS lengthpc
                    ,c.premsign
             FROM ( WITH a AS (SELECT /*+ Full(b) Parallel(b,32)*/
                                    DISTINCT
                                      nvl(b.addisn, agrisn) AS addisn,
                                      b.bizflg,
                                      CASE
                                        WHEN b.agrdatebeg<=vDateStage AND b.agrdateend>vDateStage THEN
                                          add_months (b.agrdatebeg,trunc (months_between (vDateStage,b.agrdatebeg)/12)*12+12)-1
                                        ELSE b.agrdateend
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
                    FROM a, repcond c
                    WHERE nvl(c.premiumsum,0)>=0
                      AND c.newaddisn=a.addisn

                  ) c
             WHERE c.datebeg<=c.agrinsurancedateend
            )
        )
    ) Where Nvl(CondPc,0)<>0;

    update --+  Full(b) Parallel(b,32) use_hash (b)
    Report_BuhBody_List b set loadtype = 5
    where loadtype is null
      and nvl(b.addisn, b.agrisn) in (select bodyisn from REP_COND_LIST where loadtype = 5);

 --   commit;



    --Медики, туристы
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
                   --{EGAO 08.09.2011
                   /*sum (
                        c.premusd*((Least(c.dateend, CASE
                                           WHEN c.datebeg<=vDateStage AND c.dateend>vDateStage THEN
                                             add_months (c.datebeg,trunc (months_between (vDateStage,c.datebeg)/12)*12+12)-1
                                           ELSE c.dateend
                                         END) - c.datebeg+1)/(c.dateend-c.datebeg+1)))  st_premusd,*/
                   sum (
                        c.premagr*((Least(c.dateend, CASE
                                           WHEN c.datebeg<=vDateStage AND c.dateend>vDateStage THEN
                                             add_months (c.datebeg,trunc (months_between (vDateStage,c.datebeg)/12)*12+12)-1
                                           ELSE c.dateend
                                         END) - c.datebeg+1)/(c.dateend-c.datebeg+1)))  st_premagr,
                   --}
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
                           c.newaddisn
                   FROM ( WITH a AS (SELECT  /*+ Full(b) Parallel(b,32)*/
                                          DISTINCT
                                            nvl(b.addisn, b.agrisn) AS addisn,
                                            b.bizflg,
                                            CASE
                                              WHEN b.agrdatebeg<=vDateStage AND b.agrdateend>vDateStage THEN
                                                add_months (b.agrdatebeg,trunc (months_between (vDateStage,b.agrdatebeg)/12)*12+12)-1
                                              ELSE b.agrdateend
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
                          FROM a, repcond c
                          WHERE nvl(c.premiumsum,0)>=0
                            AND c.addisn=a.addisn
                          UNION ALL
                          SELECT --+ index ( c X_REPCOND_NEWADDISN ) ordered use_nl ( a c )
                                 c.newaddisn AS addisn,
                                 c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                                 c.agrclassisn, c.agrcomission, c.agrdiscr,c.rptclassisn,c.AgrIsn,c.riskclassisn, c.riskprnclassisn,
                                 -c.premusd AS premusd, -c.premagr AS premagr, -c.premiumsum AS premiumsum,
                                 c.premcurrisn, c.clientisn, c.AgrOldDateEnd, c.addsign, c.addbeg,
                                 c.condisn, c.isn,
                                 c.objisn, nvl(c.parentobjisn,c.objisn) AS parentobjisn,
                                 nvl(c.objprnclassisn,c.objclassisn) AS parentobjclassisn,
                                 c.objregion,c.objcountry, c.objclassisn
                                 ,c.newaddisn
                                 ,a.bizflg
                                 ,agrinsurancedateend
                          FROM a, repcond c
                          WHERE nvl(c.premiumsum,0)>=0
                            AND c.newaddisn=a.addisn
                        ) c
                   WHERE c.datebeg<=c.agrinsurancedateend
                 )c
            group by c.addisn, c.newaddisn,
                     c.agrdatebeg, c.agrdateend, c.datebeg, c.dateend, c.riskruleisn, c.agrruleisn,
                     c.agrclassisn, c.agrcomission, c.agrdiscr, c.rptclassisn,c.agrisn,
                     c.riskclassisn,c.riskprnclassisn


            )
        )
    ) Where Nvl(CondPc,0)<>0;




    update --+  Full(b) Parallel(b,32) use_hash (b)
    Report_BuhBody_List b set loadtype = 3
    where loadtype is null
      and nvl(b.addisn, b.agrisn) in (select bodyisn from REP_COND_LIST where loadtype = 3);

   -- commit;







  --...потом привязанные к договорам без кондов...
    insert into REP_COND_LIST (loadtype, bodyisn, repcondisn, condpc,
     datebeg, dateend, ruleisnagr, agrclassisn, comission, agrdiscr,AgrIsn,CLIENTISN,bizflg)
    select 4, agrisn, repcondisn, 1,
     datebeg, dateend, ruleisn, classisn, comission, discr,AgrIsn,CLIENTISN,bizflg
    from (select --+ ordered use_nl (b a) index (a X_REPAGR_AGR) no_merge ( b )
    a.agrisn, isn repcondisn,
    datebeg, dateend, ruleisn, classisn, comission, discr,CLIENTISN,bizflg
    from (select  /*+ Full(b) Parallel(b,32)*/ distinct agrisn from Report_BuhBody_List b
     where loadtype is null  and agrisn is not null) b, repagr a
    where b.agrisn = a.agrisn);

  -- commit;



    update --+  Full(b) Parallel(b,32) use_hash (b)
    Report_BuhBody_List b set loadtype = 4
    where loadtype is null
      and agrisn in (select bodyisn from REP_COND_LIST where loadtype = 4);

  --  commit;



  --...очищаем мусор...
    delete /*+ Full(b) Parallel(b,32)*/ from REP_COND_LIST b where nvl (condpc,0) = 0;
--    commit;



SET_RPTGROUPISN_BY_COND_P; -- проставили учетную группу "по кондам"

set_budgetroupisn_by_cond_P; --  проставили мотивационную группу "по кондам" KGS. 14.03.2011










select /*+ Full(b) Parallel(b,32)*/ count(*) into vBufCnt from Report_BuhBody_List B;

/*vCnt:=0;


select --+ordered use_hash(bc)   --MSerp 15.10.2010. Грубая прикидка размера.
sum(nagr) into vCnt from
Report_BuhBody_List bb, (select agrisn, count(*) nagr from Rep_buh2cond_list group by agrisn)  bc
where bb.agrisn = bc.agrisn ;


if vCnt>1000000 then  --MSerp 15.10.2010. Короче, если буфер кондов маленький, то инсертим кучей. Иначе по-старому, по одной записи.
    vCnt:=0;
else
 vBufCnt:=1;
end if;*/

    -- контроль размера буфера, собираем результат по 1-й проводке В ЖОПУ!



  --...собираем RepBuh2Cond...
    insert into TT_RepBuh2Cond (Isn, LoadIsn, BuhIsn, CondIsn, RepCondIsn, CondPC, DocSumPC, BuhPC, FactPC, BodyIsn, AgrIsn, AddIsn, RefundIsn,
     StatCode, DeptIsn, ClassIsn, SubAccIsn, BuhCurrIsn, BuhAmount, BuhAmountRub,BUHAMOUNTUSD, QuitAmount, FactCurrIsn, FactSum,
     DateVal, DateQuit, DatePay, DatePayLast,  DateBeg, DateEnd, RuleIsn, RuleIsnAgr, AgrClassIsn, Comission,
     AccCurrIsn, AgrDiscr, ObjClassIsn,rptclassisn,datebegcond,dateendcond,OBJREGION,OBJCOUNTRY,Riskclassisn,
     DocSumIsn, DatePayPlan, AgrBuhDate, DocIsn, AddSign, ReprDeptIsn,BODYSUBJISN,SUBJISN,HEADISN,CLIENTISN,CLIENTJURIDICAL,ADDDATEBEG,
     AgrOldDateEnd,AgrCurrIsn,BizFlg,Amount,AmountRub,AmountUsd,PARENTOBJCLASSISN,
     REFUNDEXTISN,RISKPRNCLASSISN,DocIsn2,Sagroup,CORSUBACCISN,Parentisn,RptGroupIsn,ISREVALUATION,objisn,
      PARENTOBJISN,adeptisn, buhheadfid, dsclassisn, dsclassisn2, refundclassisn,
     motivgroupisn,budgetgroupisn /*KGS. 14.03.2011*/, premusd, premagr, Dsstatus
      )
    select --+  Full(b) Parallel(b,32) use_hash (b r) use_Nl(r ag sb) ordered
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
     Nvl(b.ISREVALUATION,0) ISREVALUATION,
     r.objisn, r.PARENTOBJISN,
     b.adeptisn,b.buhheadfid,b.dsclassisn,b.dsclassisn2, r.refundclassisn,
     r.motivgroupisn, --KGS. 14.03.2011
     r.budgetgroupisn --KGS. 14.03.2011
     ,r.premusd
     ,r.premagr -- EGAO 07.09.2011
     ,b.dsstatus
    from Report_BuhBody_List b, REP_COND_LIST r,repagr ag,repsubject sb
    where b.loadtype = r.loadtype (+)
      and  decode (b.loadtype,
      1,b.refundisn,
      2,b.agrisn,--EGAO 02.03.2011 2,decode(dsdatebeg,null,decode(dsdateend,null,Nvl(b.addisn,b.agrisn),vDocsumIsn),vDocsumIsn),
      3,nvl(b.addisn, b.agrisn),--EGAO 02.03.2011 3,decode(dsdatebeg,null,decode(dsdateend,null,Nvl(b.addisn,b.agrisn),vDocsumIsn),vDocsumIsn),
      5,nvl(b.addisn, b.agrisn),
      4,b.agrisn) = r.bodyisn (+)
      And R.AgrIsn=Ag.AgrIsn(+)
      And ag.ClientIsn=Sb.Isn(+);
vCnt:=sql%rowcount;


/* проставляем синтетические классификаторы */
     SET_RPTCLASS_P;
-- KGS. 14.03.2011 теперь по кондам, выше    SET_BUDGETROUPISN_NEW;
     /*закомментировал EG_AO 25.07.2008 т.к. поле motivgroupisn
       таблицы tt_RepBuh2Cond изменяется в процедуре SET_BUDGETROUPISN_NEW
     set_motivgroupisn;
     */
-- KGS 15.03.2011 теперь по проводкам, выше     SET_AGR_BUHDATE;
--     CheckRevaluation;



--  Insert Into Repbuh2cond (select * from tt_repbuh2cond);

  Insert /*+ APPEND*/ Into repbuh2cond_small (select * from v_repbuh2cond_small);


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


End;
