 CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPBUHBODY" ("DEPTISN", "STATCODE", "CLASSISN", "BODYISN", "DATEVAL", "CURRISN", "DEPTISNBUH", "SUBJISN", "SUBACCISN", "BUHAMOUNT", "BUHAMOUNTRUB", "DOCSUMISN", "DATEPAYLAST", "AGRISN", "DEPTISNAN", "REPRDEPTISN", "BIZFLG", "ADDISN", "REFUNDISN", "DOCSUMPC", "BUHQUITBODYISN", "BUHQUITBODYCNT", "ACCCURRISN", "QUITDEBETISN", "QUITCREDITISN", "QUITDATEVAL", "DATEQUIT", "QUITCURRISN", "QUITDEBETSUBACCISN", "QUITCREDITSUBACCISN", "QUITDEBETBUHAMOUNT", "QUITCREDITBUHAMOUNT", "AMOUNTCLOSEDQUIT", "BUHQUITAMOUNT", "BUHQUITPARTAMOUNT", "BUHQUITDATE", "PARENTISN", "AMOUNTCLOSINGQUIT", "FULLAMOUNTCLOSINGQUIT", "AGRBUHDATE", "FACTISN", "BUHQUITISN", "BUHHEADFID", "BUHAMOUNTUSD", "OPRISN", "OPRDEPTISN", "DOCISN", "DATEPAY", "HEADISN", "DOCSUMSUBJ", "DOCISN2", "SAGROUP", "CORSUBACCISN", "DSDATEBEG", "DSDATEEND", "ADEPTISN", "DSCLASSISN", "DSCLASSISN2", "FACTPC", "BUHPC", "AMOUNT", "AMOUNTRUB", "AMOUNTUSD", "REMARK", "DSSTATUS") AS 
  With REPBUHBODY_LIST as
(Select z.*,
     --Поля корреспонденции
     (select max (isn)
      from ais.buhbody_t b
      where headisn = Z.headisn
        and status = 'А'
        and decode (z.damountrub,null,b.damountrub,b.camountrub) is not null) BuhQuitBodyIsn,
     (select count (*)
      from ais.buhbody_t b
      where headisn = Z.headisn
        and status = 'А'
        and decode (z.damountrub,null,b.damountrub,b.camountrub) is not null) BuhQuitBodyCnt

from VZ_REPBUHBODY_LIST z),

DocsumList AS

(Select --+ Use_Concat
   l.bodyIsn,Ds.*
 from REPBUHBODY_LIST l,docsum ds
 Where l.bodyIsn in (ds.debetisn,ds.creditisn) and l.DSDISCR=ds.discr),



QuitBodyList as

(Select
  b.bodyisn,
      nvl (bb.damount,-bb.camount) BuhQuitAmount,
     nvl (bb1.damount,-bb1.camount) BuhQuitPartAmount,
     bb1.datequit BuhQuitDate,
     nvl (bb1.isn,bb.isn) BuhQuitIsn,
        bb.subaccisn CorSubAccIsn

from REPBUHBODY_LIST b,ais.buhbody_t bb, ais.buhbody_t bb1
Where         b.BuhQuitBodyIsn = bb.isn
          and bb.isn In (bb1.isn,bb1.Parentisn)
          and  Decode(bb1.isn,bb.isn,686696616,bb1.oprisn)= 686696616
          and bb1.status = 'А'
)



SELECT Deptisn, StatCode, ClassIsn, bodyisn, dateval, currisn, DeptIsnBuh,  SubjIsn, subaccisn,
       buhamount, buhamountrub, docsumisn, datepaylast, Agrisn, DeptIsnAn, ReprDeptIsn, BizFlg, AddIsn, Refundisn,
       docsumpc, BuhQuitBodyIsn, BuhQuitBodyCnt, AccCurrIsn, QuitDebetIsn, QuitCreditIsn, QuitDateVal, DateQuit, QuitCurrIsn,
       QuitDebetSubAccIsn, QuitCreditSubAccIsn, QuitDebetBuhAmount, QuitCreditBuhAmount, AmountClosedQuit, BuhQuitAmount,
       BuhQuitPartAmount, BuhQuitDate, ParentIsn, AmountClosingQuit, FullAmountClosingQuit, AgrBuhDate, FactIsn, BuhQuitIsn,
       BuhHeadFid, BuhAmountUsd, OprIsn,OprDeptIsn, docisn, DatePay, HeadIsn, DocSumSubj,
       DocIsn2, Sagroup, CorSubAccIsn, DsDatebeg, DsDateend, adeptisn, DsClassIsn, DsClassIsn2,
       decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit) AS factpc,
       decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount) AS buhpc,
       buhamount*decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit)*
       decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount)*docsumpc AS amount, -- EGAO 02.02.2010
       buhamountrub*decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit)*
       decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount)*docsumpc AS amountrub, -- EGAO 02.02.2010
       buhamountusd*decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit)*
       decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount)*docsumpc AS amountusd, -- EGAO 02.02.2010
       remark,
       Dsstatus /*KGS 01.08.2011 статут доксуммы для дебиторки*/
FROM (
select --+ use_nl (b d dp s d2 a aa r f b1 b2 bb bb1 opr) ordered Index (bb ) Use_Concat Index (bb1)
     b.Deptisn, b.StatCode, b.ClassIsn,
     b.bodyisn, b.dateval, b.currisn, b.DeptIsnBuh,  b.SubjIsn, b.subaccisn,
     b.buhamount, b.buhamountrub, b.docsumisn, b.datepaylast,Nvl(aa.isn,0) Agrisn,
     b.DeptIsnAn, b.ReprDeptIsn, b.BizFlg, a.isn AddIsn, r.isn Refundisn,
     to_number(decode (nvl (fullamountdoc,0),0,decode (docsumcnt,0,0+null,1/docsumcnt),b.amountdoc/fullamountdoc)) docsumpc,
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
      BuhQuitAmount,
      BuhQuitPartAmount,
      BuhQuitDate,
     b.ParentIsn,
     f.amountdoc AmountClosingQuit,
     SUM (abs (f.amountdoc)) OVER (PARTITION BY b.docsumisn, BuhQuitIsn) AS FullAmountClosingQuit,
     to_date(decode (b.statcode,38,decode (b.deptisn,707480016,dp.signed))) AgrBuhDate,
     f.isn FactIsn,  BuhQuitIsn,
     b.BuhHeadFid, b.BuhAmountUsd,B.OprIsn,Opr.CLASSISN1 OprDeptIsn,
     b.docisn,
     b.DatePay,
     B.HeadIsn,
     Nvl(Dssubjisn,F.SubjIsn) DocSumSubj,
     b.DocIsn2,
     Sagroup,
      CorSubAccIsn,
     DsDatebeg,
     DsDateend,
     b.adeptisn,   -- EGAO 29.04.2009  в рамках ДИТ-09-1-083535
     b.DsClassIsn, -- EGAO 02.02.2010
     b.DsClassIsn2 ,  -- EGAO 02.02.2010
     b.remark,
     b.Dsstatus




--     decode (decode (b.statcode,38,1,34,1),1,decode (nvl (a.isn,aa.isn),null,null,Ais.Get_Agr_BuhDate (nvl (a.isn,aa.isn), b.DocSumIsn))) AgrBuhDate
    from (select --+ use_nl (r b pc pd h adept ) ordered index ( adept X_KINDACCSET_ACC_KIND ) Use_Hash(ds) Index (b)
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
     /*(select max (decode (classisn, 980350425 /*c.get ('cBizCenter'), 'Ц', 980350525 /*c.get ('cBizFil'), 'Ф'))
     from kindaccset where kindaccisn = b.SubKindIsn and kindisn = 980357325 /*c.get ('cKindBiz')) */ '' BizFlg,
     --Поля плановой доксуммы
     Ds.isn  DocSumIsn,
     Ds.DatePay DatePay,
     Ds.DatePayLast DatePayLast,
     Ds.DateBeg DsDateBeg,
     Ds.DateEnd DsDateEnd,
     Ds.classisn DsClassisn,
     Ds.classisn2 DsClassisn2,
     Ds.status Dsstatus,
     ds.subjisn Dssubjisn,
     nvl (Ds.agrisn, b.AgrIsn) AgrIsn,
     Ds.RefundIsn RefundIsn,
     gcc2.gcc2(Ds.Amount,Ds.CURRISN,b.currisn,b.dateval) AmountDoc,
     Ds.DocIsn DocIsn,
     Ds.DocIsn2 DocIsn2,
     B.oprisn,
     SUM (gcc2.gcc2(Ds.Amount,ds.CURRISN,b.currisn,b.dateval)) OVER (PARTITION BY b.isn) AS FullAmountDoc,
     COUNT (*) OVER (PARTITION BY b.isn) AS DocSumCnt,
     BuhQuitBodyIsn,
     BuhQuitBodyCnt,
--        Decode(pc.Isn,null,Pd.CreditIsn,Pc.DebetIsn) PdsBuhQuitIsn,
        r.sagroup,
        adept.classisn AS adeptisn, -- EGAO 29.04.2009  в рамках ДИТ-09-1-083535
        b.remark
    from REPBUHBODY_LIST r, ais.buhbody_t b,DocsumList Ds, ais.buhhead_t h, ais.kindaccset adept
    where r.bodyisn = b.isn
      and r.bodyisn = Ds.BodyIsn (+)
      and b.headisn = h.isn
      AND adept.kindaccisn(+) = b.SubKindIsn -- EGAO 29.04.2009 в рамках ДИТ-09-1-083535
      AND adept.kindisn(+)=56645916 -- EGAO 29.04.2009  в рамках ДИТ-09-1-083535 (c.get('ckinddeptfull')-подразделения сао "ингосстрах"0
    ) b, docs d, docs dp, ais.subacc s, docs d2, agreement a, agreement aa, agrrefund r, docsum f, ais.buhbody_t b1, ais.buhbody_t b2,
    QuitBodyList QbL,
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

      and b.bodyIsn=QbL.bodyIsn(+)
        And Opr.CLASSISN2(+)= B.OprIsn);