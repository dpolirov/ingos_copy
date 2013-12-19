 CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BODYDEBCRE" ("BASEISN", "DB", "DE", "BASESALDO", "SUBACCISN", "CODE", "BASEAMOUNTRUB", "BASEDAMOUNTRUB", "BASECAMOUNTRUB", "BASEDATEVAL", "FID", "SUBJISN", "CURRISN", "AGRISN", "BASEAMOUNT", "BASEDAMOUNT", "BASECAMOUNT") AS 
  Select /*+leading(dt) use_nl(b) Index(dt) index(b x_st_buhbody_base)*/
      Dt.BaseIsn, db,de,
      Sum(dAmountrub)-sum(camountrub) as BaseSaldo,
      Max(subaccisn) subaccisn,Max(code) Code,
      Max(nvl(baseDAMOUNTRUB,0)-nvl(baseCAMOUNTRUB,0)) as BaseAmountRub,
      Max(basedamountrub) as BaseDamountrub,
      Max(basecamountrub) as BaseCamountrub,
      Max(basedateval) as BaseDateval,
      Max(fid) as fid, Max(subjisn) as subjisn, Max(currisn) as currisn, Max(agrisn) as agrisn,
      Max(nvl(baseDAMOUNT,0)-nvl(baseCAMOUNT,0)) as BaseAmount,
      Max(basedamount) as BaseDamount,
      Max(basecamount) as BaseCamount
From
(
/*все варианты Dateval и Datequit в один не прирывный столбик - набор интервалов*/
Select /*+Index(dbe ) */
  dbe.BaseIsn,dbe.Dateval Db,Nvl(Lead(dbe.Dateval) over (PARTITION by dbe.BaseIsn Order by dbe.Dateval )-1,'01-jan-3000') De
from(

  Select /*+ index(b x_st_buhbody_base)  */Distinct b.BaseIsn,b.Dateval from STORAGES.st_buhbody b
  Where b.BaseIsn  in (select isn from tt_rowid)   -- baseisn = 397668316)

  Union

  Select /*+ index(b x_st_buhbody_base)  */ Distinct b.BaseIsn,b.Datequit from STORAGES.st_buhbody b
  Where b.BaseIsn in (select isn from tt_rowid)
) dbe
 ) Dt, STORAGES.st_buhbody b
 Where Dt.BaseIsn=b.baseIsn
   and (b.Dateval<=dt.De AND nvl(b.Datequit,'01-jan-3000')>dt.De)
   --and dt.baseisn=397668316   -----
   --and dt.loadisn = 10.2002
 group by Dt.BaseIsn, dt.db, dt.de;