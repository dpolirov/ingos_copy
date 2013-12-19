 CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUHBODY_REINS" ("PARENTISN", "BODYISN", "CODE", "DAMOUNTRUB", "CAMOUNTRUB", "DATEVAL", "OPRISN", "SUBJISN", "AGRISN", "DSSUBJISN", "AMOUNTSUM", "FULLAMOUNTSUM", "DSKOEF", "DOCSUMCNT", "DSISN", "DISCR", "STATUS", "CLASSISN", "CLASSISN2", "DATEPAY", "DOCISN", "DOCISN2", "SPLITISN", "AMOUNT", "DSCURRISN", "GROUPISN", "STATCODE", "DSAGRISN", "REACCISN", "SUBACCISN", "FID") AS 
  (

Select --+ Ordered Use_Nl(opr)

 s.PARENTISN,
 S.BODYISN,
 S.CODE,
 S.DAMOUNTRUB,
 S.CAMOUNTRUB,
 S.DATEVAL,
 s.OPRISN,


  s.subjisn ,
  s.agrisn,



  s.dssubjisn ,
  s.AmountSum,
  s.FullAmountSum,
  Nvl(s.AmountSum/s.FullAmountSum,1) as DSKoef,
  s.DocSumCnt,
  s.dsisn,
  s.DISCR,
  s.STATUS,
  s.CLASSISN,
  s.CLASSISN2,
  s.DATEPAY,
  s.DOCISN,
  s.DOCISN2,

decode(s.splitisn,null,null,
(
Select DDs.isn
from docsum dds
Where dds.Splitisn is null
start with dds.isn=s.splitisn
connect by prior splitisn=isn
))  SPLITISN,
  s.AMOUNT,
  s.DSCURRISN,
  s.GROUPISN,
  S.Statcode,
  dsagrisn,
  REaccisn,
  SUBACCISN,
  fid
from(
Select --+ Ordered use_nl(b sd ba pc pd bh)
  b.ParentIsn,b.Isn BodyIsn,b.Code, Nvl(b.damountrub,0)damountrub ,
  Nvl(b.camountrub,0) camountrub,b.Dateval, b.OprIsn,
  b.subaccisn,
  b.subjisn,
  b.agrisn,
  SUM (gcc2.gcc2(nvl (pc.Amount, pd.Amount),nvl(pc.CURRISN,pd.CURRISN),35,b.dateval)) OVER (PARTITION BY b.isn) AS FullAmountSum,
  COUNT (*) OVER (PARTITION BY b.isn) AS DocSumCnt,

  gcc2.gcc2(nvl (pc.Amount, pd.Amount),nvl(pc.CURRISN,pd.CURRISN),35,b.dateval) as AmountSum,




  nvl (pc.isn, pd.isn) as dsisn,
  nvl (pc.subjisn, pd.subjisn) as dssubjisn,
  nvl (pc.agrisn, pd.agrisn) as dsagrisn,
  nvl (pc.discr,pd.discr) as discr,
  nvl (pc.status,pd.status) as status,
  nvl (pc.CLASSISN,pd.CLASSISN) as classisn,
  nvl (pc.CLASSISN2,pd.CLASSISN2) as classisn2,
  nvl (pc.DATEPAY,pd.DATEPAY) as datepay,
  nvl (pc.DOCISN,pd.DOCISN) as docisn,
  nvl (pc.DOCISN2,pd.DOCISN2) as docisn2,
  nvl (pc.SPLITISN,pd.SPLITISN) as splitisn,
  nvl (pc.AMOUNT,pd.AMOUNT) as amount,
  nvl (pc.CURRISN,pd.CURRISN) as DScurrisn,
  nvl (pc.GROUPISN,pd.GROUPISN) as groupisn,
 nvl (pc.REaccisn,pd.REaccisn) as REaccisn,

sd.statcode,
bh.fid

  from tt_rowid t, buhbody b, buhhead bh,
  (select Subaccisn,Statcode from storages.V_REP_SUBACC4DEPT where statcode in
   (select statcode from rep_statcode where grp in ('Входящее перестрахование','Исходящее перестрахование'))
   Union 
select Isn,to_Number(Substr(Id,1,3)) from buhsubacc Where (Id Like '913%' Or id Like '914%')
and dateend>='31-dec-2010'
                   ) Sd,
 buhsubacc ba, docsum pc, docsum pd

  where t.isn = b.isn
  and b.subaccisn=ba.isn
  and b.subaccisn =Sd.SubaccIsn
  and b.headisn=bh.isn
  and b.isn = pc.creditisn(+)
  and b.isn = pd.debetisn(+)
  and pc.Discr(+) between 'F' and 'P'
  and pd.Discr(+) between 'F' and 'P'
  and  sd.statcode is not null
 
  and nvl (b.damountrub,b.camountrub) <> 0              -- условие из   VZ_REPBUHBODY_LIST
  and b.status = 'А'                                  -- условие из   VZ_REPBUHBODY_LIST
  and b.oprisn not in (9534516, 24422716)           -- условие из   VZ_REPBUHBODY_LIST
--  and b.dateval<=Ba.dateend

)s
);     