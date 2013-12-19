 CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUHBODY" ("BASEISN", "PARENTISN", "BODYISN", "CODE", "DAMOUNTRUB", "CAMOUNTRUB", "DATEVAL", "DATEQUIT", "QUITSTATUS", "OPRISN", "SUBACCISN", "BALANCE", "BASEDATEVAL", "FID", "BASEDAMOUNTRUB", "BASECAMOUNTRUB", "SUBJISN", "CURRISN", "AGRISN", "BASEDAMOUNT", "BASECAMOUNT", "DAMOUNT", "CAMOUNT") AS 
  Select --+ Ordered Use_Nl(b bh)
 unique  S."BASEISN",S."PARENTISN",S."BODYISN",S."CODE",S."DAMOUNTRUB",S."CAMOUNTRUB",
 S."DATEVAL",S."DATEQUIT",S."QUITSTATUS",S."OPRISN",S."SUBACCISN",
 Nvl(B.Damountrub,0)-Nvl(B.camountrub,0) as balance,
  b.dateval as basedateval,
  bh.Fid ,
  b.damountrub as basedamountrub,b.camountrub as basecamountrub,b.subjisn as subjisn,b.currisn,b.agrisn,
  b.damount as basedamount,b.camount as basecamount,S."DAMOUNT", S."CAMOUNT"
from(
Select
Decode(D.Isn,null,b.Isn,/*идем вверх только по проводкам операций "автоматические" - 200 02 03 */
Decode(b.Parentisn,null,b.Isn,b.isn,b.isn, /*бывают проводки, ссылающиес€ сами на себ€*/
( select Max(b1.Isn) KEEP ( dense_rank FIRST ORDER BY decode(b1.subaccisn,b.subaccisn,0,1),Level desc )
   from buhbody b1
   Start with b1.isn=b.isn
   connect  by nocycle  prior  b1.parentisn= b1.isn
   ))) BaseIsn,
     b.ParentIsn,b.Isn BodyIsn,ba.ID Code, Nvl(b.damountrub,0)damountrub ,
  Nvl(b.camountrub,0) camountrub, Nvl(b.damount,0) damount , Nvl(b.camount,0) camount,
  b.Dateval, Nvl(Decode(b.quitstatus,null,b.Datequit,null),'01-jan-3000') Datequit,b.QuitStatus,b.OprIsn,
  b.subaccisn
  from /*(select rowid as RId from buhbody where isn in (8329848903,8492132803,8668517903, 8249586303)) */tt_rowid t, buhbody b,buhsubacc ba,
  (Select * from   dicti d
     Where d.parentisn=759033300 and code in('200','02','03') -- необходимо ограничение по операци€м 200 02 03
     ) D
  where t.isn = b.isn
  and status='ј'
  And (b.quitstatus is not null or b.dateval<Nvl(b.Datequit,'01-jan-3000'))
  and b.oprisn=d.isn(+)        -- здесь ограничение по целой подветке автоматических операций (200 02 03)
  and b.subaccisn=ba.isn
  and (b.Code Like'77%' or b.Code Like'78%'  Or ba.Id like '7619%') -- ограничение по счетам

) s, buhbody b,buhhead bh
Where s.baseisn=b.isn
and b.headisn=bh.isn

Union all


Select --+ Ordered Use_Nl(b bh)
 unique  S."BASEISN",S."PARENTISN",S."BODYISN",S."CODE",
 Nvl(S."DAMOUNTRUB",0)"DAMOUNTRUB", Nvl(S."CAMOUNTRUB",0) "CAMOUNTRUB",
 S."DATEVAL",S."DATEQUIT",S."QUITSTATUS",S."OPRISN",S."SUBACCISN",
 Nvl(B.Damountrub,0)-Nvl(B.camountrub,0) as balance,
  b.dateval as basedateval,
  bh.Fid ,
  b.damountrub as basedamountrub,b.camountrub as basecamountrub,b.subjisn as subjisn,b.currisn,b.agrisn,
  b.damount as basedamount,b.camount as basecamount,Nvl(S."DAMOUNT",0)"DAMOUNT", Nvl(S."CAMOUNT",0) "CAMOUNT"
from(
Select
Decode(D.Isn,null,b.Isn,/*идем вверх только по проводкам операций "автоматические" - 200 02 03 */
Decode(b.Parentisn,null,b.Isn,b.isn,b.isn, /*бывают проводки, ссылающиес€ сами на себ€*/
( select Max(b1.Isn) KEEP ( dense_rank FIRST ORDER BY decode(b1.subaccisn,b.subaccisn,0,1),Level desc )
   from buhbody b1
   Start with b1.isn=b.isn
   connect  by nocycle  prior  b1.parentisn= b1.isn
   ))) BaseIsn,
     b.ParentIsn,b.Isn BodyIsn,ba.ID Code,

      Case When dg.QUEISN Is null and quitstatus='„' Then decode(dAmountrub,null,0,RemainRub)
      else DAmountrub end
      DAmountrub,
      Case When dg.QUEISN Is null and quitstatus='„' Then decode(CAmountrub,null,0,RemainRub)
      else CAmountrub end
      CAmountrub,
      Case When dg.QUEISN Is null and quitstatus='„' Then decode(dAmount,null,0,Remain)
      else DAmount end
      DAmount,
      Case When dg.QUEISN Is null and quitstatus='„' Then decode(CAmount,null,0,Remain)
      else CAmount end
      CAmount,


     b.Dateval, Nvl(Decode(b.quitstatus,null,b.Datequit,null),'01-jan-3000') Datequit,b.QuitStatus,b.OprIsn,
     b.subaccisn
  from /*(select rowid as RId from buhbody where isn in (8329848903,8492132803,8668517903, 8249586303)) */
  tt_rowid t, buhbody b,buhsubacc ba,
  (Select * from   dicti d
     Where d.parentisn=759033300 and code in('200','02','03') -- необходимо ограничение по операци€м 200 02 03
     ) D, docgrp dg
  where t.isn = b.isn
  and b.status='ј'
  And (b.quitstatus is not null or Trunc(b.dateval,'mm')<Nvl(trunc(b.Datequit,'mm'),'01-jan-3000'))
  and b.oprisn=d.isn(+)        -- здесь ограничение по целой подветке автоматических операций (200 02 03)
  and b.subaccisn=ba.isn
  and  (b.Code Like'60%' or b.Code Like'71%'  Or  ( ba.Id like '76%' and  not ba.Id like '7619%')) -- ограничение по счетам !!!! ќбратное!!!
  and b.groupisn=dg.isn(+)
  

) s, buhbody b,buhhead bh
Where s.baseisn=b.isn
and b.headisn=bh.isn;
