 CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUH_TURN_CONTR" ("SUBACCISN", "CODE", "OPRISN", "DB", "DE", "DAMOUNTRUB", "CAMOUNTRUB", "RESIDENT", "BRANCHISN", "PRM_KEY", "CURRISN", "JURIDICAL") AS 
  (
Select
subaccisn,
Max(Code) Code,
oprIsn,
DB ,
DE,

Nvl(Sum(damountrub),0) damountrub,
Nvl(Sum(camountrub),0) camountrub,
Resident,
branchisn,
to_number(to_char(DB,'YYYYMMDD')||To_Char(Subaccisn)) PRM_KEY,
CURRISN,
JURIDICAL

From (



Select --+ Ordered Use_Nl(b sb ) Index ( b X_BUHBODY_SUBACC_DATE)
b.subaccisn,
B.Code,
b.oprIsn,
Trunc(B.Dateval,'month') DB ,
add_months(Trunc(B.Dateval,'month'),1)-1 DE,
b.damountrub damountrub,
b.camountrub camountrub,
b.CURRISN,
Resident,
branchisn,
sb.juridical
from tt_rowid t, buhsubacc bacc, Ais.buhbody_t b,ais.subject_t sb
where bacc.isn=Substr(t.Isn,9)
and bacc.isn=b.subaccisn
and b.subjisn is not null
and b.dateval between trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month') and add_months( trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month'),1)-1
and b.Status='À'
and Nvl(b.damountrub,b.camountrub)<>0
and b.subjisn=sb.isn(+)

union all

Select --+ Ordered Use_Nl(sb )
subaccisn,
S.Code,
oprIsn,
DB ,
DE,
s.damountrub*Decode(Nvl(FullAmountSum,0),0,1/DsCnt,AmountSum/FullAmountSum) damountrub,
s.camountrub*Decode(Nvl(FullAmountSum,0),0,1/DsCnt,AmountSum/FullAmountSum) camountrub,
s.CURRISN,
Resident,
branchisn,
sb.juridical
From (
Select --+ Ordered Use_Nl(b pc pd ) Index ( b X_BUHBODY_SUBACC_DATE)
b.subaccisn,
B.Code,
b.oprIsn,
Trunc(B.Dateval,'month') DB ,
add_months(Trunc(B.Dateval,'month'),1)-1 DE,
SUM (Nvl(gcc2.gcc2(nvl (pc.Amount, pd.Amount),nvl(pc.CURRISN,pd.CURRISN),35,b.dateval),nvl (pc.AmountRub, pd.AmountRub))) OVER (PARTITION BY b.isn) AS FullAmountSum,
Nvl(gcc2.gcc2(nvl (pc.Amount, pd.Amount),nvl(pc.CURRISN,pd.CURRISN),35,b.dateval),Nvl(pc.AmountRub, pd.AmountRub)) AmountSum,
Count(*) OVER (PARTITION BY b.isn) AS DsCnt,
b.damountrub Damountrub,
b.camountrub camountrub,
Nvl(Pc.subjisn,Pd.SubjIsn) SubjIsn,
b.CURRISN
from tt_rowid t, buhsubacc bacc, Ais.buhbody_t b, docsum pc, docsum pd
where bacc.isn=Substr(t.Isn,9)
and bacc.isn=b.subaccisn
and b.subjisn is  null
and b.dateval between trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month') and add_months( trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month'),1)-1
and b.Status='À'
and Nvl(b.damountrub,b.camountrub)<>0
and b.isn = pc.creditisn(+)
and b.isn = pd.debetisn(+)
and pc.Discr(+) between 'F' and 'P'
and pd.Discr(+) between 'F' and 'P') S, ais.subject_t sb
Where S.SubjIsn=Sb.Isn(+)




)

group by
subaccisn,
oprIsn,
DB ,
DE,
Resident,
branchisn,
CURRISN,
JURIDICAL
);