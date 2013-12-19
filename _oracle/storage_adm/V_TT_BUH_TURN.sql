CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUH_TURN" ("SUBACCISN", "CODE", "SUBKINDISN", "OPRISN", "CURRISN", "DB", "DE", "DAMOUNT", "DAMOUNTRUB", "DAMOUNTUSD", "CAMOUNT", "CAMOUNTRUB", "CAMOUNTUSD", "PRM_KEY", "DEB", "DEE") AS 
  (

Select --+ Ordered Use_Nl(bacc b) Index( b X_BUHBODY_SUBACC_DATE)
b.subaccisn,
B.Code ,
b.SUBKINDISN,
b.oprIsn,
B.CurrIsn,
Trunc(Dateval,'month') DB ,
add_months(Trunc(Dateval,'month'),1)-1 DE,
--Sb.resident,
Sum(b.damount) damount,
Sum(b.damountrub) damountrub,
Sum(b.damountusd) damountusd,
Sum(b.camount) camount,
Sum(b.camountrub) camountrub,
Sum(b.camountusd) camountusd,
to_number(to_char(Trunc(Dateval,'month'),'YYYYMMDD')||To_Char(Subaccisn)) PRM_KEY,
Trunc(Nvl(DATEEVENT,Dateval),'month') DEB ,
add_months(Trunc(Nvl(DATEEVENT,Dateval),'month'),1)-1 DEE
from tt_rowid t,  Ais.buhbody_t b--,subject  sb
where b.subaccisn=Substr(t.Isn,9)
and b.dateval between trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month') and add_months( trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month'),1)-1
and b.Status='À'
--and b.subjisn=sb.isn(+)

group by
b.subaccisn,
B.cODE,
b.SUBKINDISN,
b.oprIsn,
B.CurrIsn,
Trunc(Dateval,'month') ,
add_months(Trunc(Dateval,'month'),1)-1,
Trunc(Nvl(DATEEVENT,Dateval),'month'),
add_months(Trunc(Nvl(DATEEVENT,Dateval),'month'),1)-1
);   