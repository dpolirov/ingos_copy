CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_DOCSUMBODY" ("BODYISN", "BAMOUNT", "AGRISN", "SUBJISN", "AMOUNTRUB", "DATEPAYLAST", "DSCLASSISN", "DSISN", "DISCR", "SUBACCISN", "SPLITISN", "C_AGR_1", "AGRKOEF", "DSKOEF", "AGRDSKOEF", "DB", "DE", "REMAINDER_1", "REACCISN", "AGENTISN", "AGRDATEBEG") AS 
  Select --+ ORdered Use_Nl(ar)
S."BODYISN","BAMOUNT",S."AGRISN",S."SUBJISN","AMOUNTRUB","DATEPAYLAST","DSCLASSISN","DSISN",S."DISCR","SUBACCISN","SPLITISN","C_AGR","AGRKOEF","DSKOEF","AGRDSKOEF","DB","DE","REMAINDER","REACCISN",
AR.AGENTISN,
AR.DATEBEG
from (
Select d2."BODYISN",d2."BAMOUNT",d2."AGRISN",d2."SUBJISN",d2."AMOUNTRUB",d2."DATEPAYLAST",d2."DSCLASSISN",d2."DSISN",d2."DISCR",d2."SUBACCISN",d2."SPLITISN",d2."C_AGR",d2."AGRKOEF",
  d2.KfBase/sum(d2.KfBase) over(partition by d2.bodyisn,d2.agrisn) as DSKoef,
  d2.AgrKoef*d2.KfBase/sum(d2.KfBase) over(partition by d2.bodyisn,d2.agrisn) as AGRDSKoef,
  Greatest(storage_adm.Load_Storage.gethistdb,BodyStatBeg) db, BodyStatEnd as de,
   d2."REMAINDER",REACCISN

from
(
Select D.*, c_agr/sum(d.KfBase*Sgn) over(partition by bodyisn) as AgrKoef
from
(
Select d.*,
Decode(trunc(sum(remainder) over(partition by bodyisn)),0,
sum( Sgn*AmountRub) over(partition by bodyisn,agrisn),
sum( Sgn*remainder) over(partition by bodyisn,agrisn)) c_agr,


Decode(trunc(sum(remainder) over(partition by bodyisn)),0,AmountRub,Remainder) KfBase


From
(
Select --+ Ordered Use_Nl(b pc pd)
 b.bodyisn,
 b.BASEAMOUNTRUB bamount,
ds.agrisn,
Ds.subjisn,
Case /* если знак remainder и amountrub совпадают или remainder=0*/
When Sign(Ds.amountrub)=Sign(Ds.remainder) Then Gcc2.gcc2(Ds.remainder, Ds.currisn, 35, b.Dateval)
When Sign(Nvl(Ds.remainder,-1))=0 Then 0
Else
Gcc2.Gcc2(Ds.amount,Ds.Currisn,35, b.Dateval)
End   Remainder,
Gcc2.Gcc2(Ds.amount,Ds.currisn,35,b.Dateval) AmountRub,
Nvl(Nvl(Ds.DatePayLast,Ds.DatePay), Ds.DocDate) DatePayLast,
Ds.classisn dsclassisn,
Ds.isn DsIsn,
Ds.discr discr,
Ds.REACCISN REACCISN,
B.subaccisn,
ds.splitisn splitisn,
Decode(Sign(BASEAMOUNTRUB),Sign(Sum(Ds.amount) over (PARTITION by bodyisn,Discr)),-1,1) sgn,
BodyStatEnd,
BodyStatBeg,
Max(Ds.Discr) over (PARTITION by bodyisn) MaxDiscr,
Min(Ds.Discr) over (PARTITION by bodyisn) MinDiscr,
CODE,DAmountrub,CAmountrub

from
(
/*только интересующие нас доксуммы*/
Select --+ Ordered Use_Nl(b b1)
  b.BaseIsn BodyIsn,
  Max(b.BASEAMOUNTRUB) BASEAMOUNTRUB,
  Max(b.subaccisn) subaccisn,
  Max(b.BaseDateval) Dateval,
  Max(b.DE) BodyStatEnd,
  Min(b.DB) BodyStatBeg,
  Max(b.Code) Code,
  Max(BaseDAmountrub) DAmountrub,
 Max(BaseCAmountrub) CAmountrub
from tt_rowid t,STORAGES.ST_BODYDEBCRE B
where
  t.isn=b.baseisn
group by   b.BaseIsn
Having  Max(b.BASEAMOUNTRUB)<>0
)b,ais.docsum  Ds
Where   b.BodyIsn In (ds.DebetIsn,ds.creditisn)
And Ds.Discr In ('F','P')
And Ds.amountrub<>0
) d
where
/* дебильная врезка - для счета 76197 надо отдавать предпочтение фактическим доксуммам, чтобу получить страховщика причинителя вреда по ПВУ*/
/*(Code='76197' and CAmountrub is not null and discr=minDiscr) or
((Code<>'76197' or DAmountrub is not null) and discr=maxDiscr) */
discr=maxDiscr
)D
where
 Sign(d.c_agr) <> Sign(bamount)  And c_agr<>0 -- Заменить на > 0
) d2
where Sign(d2.amountrub*Sgn) <> Sign(d2.bamount)
) S, Storage_source.repagr ar
Where S.agrisn=ar.agrisn(+);
--Where Db<=De    
;