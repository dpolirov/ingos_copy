  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUHBODY_NMS" ("BODYISN", "HEADISN", "CURRISN", "SUBACCISN", "DEPTISN", "CODE", "DATEVAL", "DAMOUNT", "DAMOUNTRUB", "DAMOUNTUSD", "CAMOUNT", "CAMOUNTRUB", "CAMOUNTUSD", "OPRISN", "SUBKINDISN", "AGRISN", "DOCITEMISN", "FOBJISN") AS 
  (
SELECT  B.Isn bodyisn, B.headisn, B.currisn, B.subaccisn,
       B.deptisn, B.code, B.dateval, B.damount, B.damountrub,
       B.damountusd, B.camount, B.camountrub, B.camountusd, B.oprisn,
       B.subkindisn, B.agrisn, B.docitemisn, B.fobjisn
  FROM  tt_rowId t, Ais.buhbody B, buhsubacc Bs
  Where T.Isn=B.ISn
  and b.status='А'
  and b.subaccisn=BS.isn
  And Bs.dateend>'01-jan-2012'
  and (( (   Bs.id like '01%' or Bs.id like '02%' or Bs.id like '03%' Or Bs.id like '04%' Or Bs.id like '05%' Or Bs.id like '08%' or Bs.id like '10%' or Bs.id like '19%' OR Bs.Id Like 'Н0%'

 Or bs.ID like '008%'
 
 Or bs.ID like '009%'
 
 Or bs.ID like '003%'

/*GGM 08.07.13*/ OR Bs.Id Like '00К%' --Корректировка аналитики МЦ
   )

/*GGM 08.07.13*/ -- and Bs.id not like '00%'
   --and Nvl(Bs.active,'Y')<>'Z'
   ) )
   and b.dateval>='31-dec-2011'
  );