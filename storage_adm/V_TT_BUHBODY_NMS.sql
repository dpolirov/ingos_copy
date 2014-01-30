create or replace view v_tt_buhbody_nms (
   bodyisn,
   headisn,
   currisn,
   subaccisn,
   deptisn,
   code,
   dateval,
   damount,
   damountrub,
   damountusd,
   camount,
   camountrub,
   camountusd,
   oprisn,
   subkindisn,
   agrisn,
   docitemisn,
   fobjisn )
as
(
    select  b.isn bodyisn, b.headisn, b.currisn, b.subaccisn,
           b.deptisn, b.code, b.dateval, b.damount, b.damountrub,
           b.damountusd, b.camount, b.camountrub, b.camountusd, b.oprisn,
           b.subkindisn, b.agrisn, b.docitemisn, b.fobjisn
      from  tt_rowid t, ais.buhbody b, buhsubacc bs
      where t.isn = b.isn
          and b.status = 'а'
          and b.subaccisn = bs.isn
          and bs.dateend > '01-jan-2012'
          and (( (   bs.id like '01%' or bs.id like '02%' or bs.id like '03%' or bs.id like '04%' or bs.id like '05%' or bs.id like '08%' or bs.id like '10%' or bs.id like '19%' or bs.id like 'н0%'

                    or bs.id like '008%'
                 
                    or bs.id like '009%'
                 
                    or bs.id like '003%'

    /*ggm 08.07.13*/ or bs.id like '00к%' --корректировка аналитики мц
                )

    /*ggm 08.07.13*/ -- and bs.id not like '00%'
       --and nvl(bs.active,'y')<>'z'
             ))
          and b.dateval >= '31-dec-2011'
       
      
  
  );
