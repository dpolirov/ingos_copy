create or replace view storage_adm.v_tt_bodydebcre (
   baseisn,
   db,
   de,
   basesaldo,
   subaccisn,
   code,
   baseamountrub,
   basedamountrub,
   basecamountrub,
   basedateval,
   fid,
   subjisn,
   currisn,
   agrisn,
   baseamount,
   basedamount,
   basecamount,
   basesaldoval)
as
(
    select /*+leading(dt) use_nl(b) index(dt) index(b x_st_buhbody_base)*/
              dt.baseisn, 
              db,
              de,
              sum(damountrub)-sum(camountrub) as basesaldo,
              max(subaccisn) subaccisn,
              max(code) code,
              max(oracompat.nvl(basedamountrub,0::numeric)-oracompat.nvl(basecamountrub,0::numeric)) as baseamountrub,
              max(basedamountrub) as basedamountrub,
              max(basecamountrub) as basecamountrub,
              max(basedateval) as basedateval,
              max(fid) as fid, 
              max(subjisn) as subjisn, 
              max(currisn) as currisn, 
              max(agrisn) as agrisn,
              max(oracompat.nvl(basedamount,0::numeric)-oracompat.nvl(basecamount,0::numeric)) as baseamount,
              max(basedamount) as basedamount,
              max(basecamount) as basecamount,
              sum(damount)-sum(camount) as basesaldoval
        from
            (
            /*все варианты dateval и datequit в один не прирывный столбик - набор интервалов*/
            select /*+index(dbe ) */
                    dbe.baseisn,dbe.dateval db,oracompat.nvl(lead(dbe.dateval) over (partition by dbe.baseisn order by dbe.dateval ) - interval '1 day','01-jan-3000') de
                from(

                      select /*+ index(b x_st_buhbody_base)  */distinct b.baseisn,b.dateval 
                        from storages.st_buhbody b
                        where b.baseisn  in (select isn from storage_adm.tt_rowid)   -- baseisn = 397668316)
                      union
                      select /*+ index(b x_st_buhbody_base)  */ distinct b.baseisn,b.datequit 
                        from storages.st_buhbody b
                        where b.baseisn in (select isn from storage_adm.tt_rowid)
                    ) dbe
            ) dt, storages.st_buhbody b
        where dt.baseisn = b.baseisn
            and (b.dateval <= dt.de and oracompat.nvl(b.datequit,'01-jan-3000') > dt.de)
            --and dt.baseisn=397668316   -----
            --and dt.loadisn = 10.2002
    group by dt.baseisn, dt.db, dt.de
);