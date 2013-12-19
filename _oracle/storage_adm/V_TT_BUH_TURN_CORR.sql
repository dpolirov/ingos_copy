 CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUH_TURN_CORR" ("SUBACCISN", "CODE", "CORCODE", "OPRISN", "DB", "DE", "DAMOUNTRUB", "CAMOUNTRUB", "PRM_KEY", "SUBKINDISN") AS 
  (
Select 
subaccisn,
Max(Code) Code,
CorCode,
oprIsn,
DB ,
 DE,


Nvl(Sum(damountrub),0) damountrub,
Nvl(Sum(camountrub),0) camountrub,


/*
Nvl(Sum(damountrub*Nvl(CorPc,1)),0) damountrub,
Nvl(Sum(camountrub*Nvl(CorPc,1)),0) camountrub,
*/
to_number(to_char(DB,'YYYYMMDD')||To_Char(Subaccisn)) PRM_KEY,
SubKindISn

From (



Select --+ Ordered Use_Nl(b bc ) Index ( b X_BUHBODY_SUBACC_DATE) Index (bc X_BUHBODY_head)

b.subaccisn,
B.Code,
b.oprIsn,
b.SubKindISn,
Trunc(B.Dateval,'month') DB ,
add_months(Trunc(B.Dateval,'month'),1)-1 DE,



Case
--When Nvl(b.DAmountrub,B.Camountrub)=0 Then 0
When Count(*) over (PARTITION by B.Isn)=1 Then B.DAmountRub
When Nvl(b.DAmountrub,B.Camountrub)<>Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn) then BC.CAmountRub/Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn)
Else
 BC.CAmountRub
End damountrub,


Case
--When Nvl(b.DAmountrub,B.Camountrub)=0 Then 0
When Count(*) over (PARTITION by B.Isn)=1 Then B.CAmountRub
When Nvl(b.DAmountrub,B.Camountrub)<>Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn) then BC.DAmountRub/Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn)
Else
 BC.DAmountRub
End Camountrub,


/*
b.damountrub damountrub,
b.camountrub camountrub,

/*
decode(Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn) ,0,
1/Count(*) over (PARTITION by B.Isn),
Nvl(bc.damountrub,bc.camountrub)/Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn))
CorPc,
*/




bc.Code CorCode
 
from tt_rowid t, buhsubacc bacc, Ais.buhbody_t b,buhbody bc
where bacc.isn=Substr(t.Isn,9)
and bacc.isn=b.subaccisn
and b.dateval between trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month') and add_months( trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month'),1)-1
and b.Status='À'
and Nvl(b.damountrub,b.camountrub)<>0
and b.headisn=bc.headisn
and bc.status='À'
--and Nvl(bc.damountrub,bc.camountrub)<>0
and decode(b.damountrub,null,'D','C')<> decode(bc.damountrub,null,'D','C')


)

group by
subaccisn,
CorCode,
oprIsn,
SubKindISn,
DB ,
DE
);    