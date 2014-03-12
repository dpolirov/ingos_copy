
create or replace view storage_adm.vz_repbuhbody_list (
   BODYISN, 
   DEPTISN, 
   STATCODE, 
   CLASSISN, 
   SAGROUP, 
   DSDISCR, 
   DAMOUNTRUB, 
   CAMOUNTRUB, 
   HEADISN)
as (        select
          b.isn,
          s.deptisn,
	      s.Statcode mStatcode,
          b1.classisn,
          s.sagroup,
          oracompat.nvl(Max(oracompat.nvl(ds1.discr, ds2.discr)), 'P'),
          b.Damountrub,
          b.Camountrub,
          b.HeadIsn
                FROM storage_adm.tt_rowid t
                     inner join ais.buhbody_t b on t.isn = b.Headisn
                     inner join storage_adm.v_rep_subacc4dept s on b.subaccisn = s.subaccisn AND b.dateval <= oracompat.nvl(to_date(s.dateend,'dd-mon-yyyy'), oracompat.trunc(CURRENT_DATE + interval '1 day')::date)
                     inner join ais.buhsubacc_t b1 on s.subaccisn = b1.isn
                     left join ais.docsum ds1 on ds1.debetisn = b.isn and ds1.discr between 'F' and 'P'
                     left join ais.docsum ds2 on ds2.CreditIsn = b.isn and ds2.discr between 'F' and 'P'
         where s.statcode is not null
           AND b1.dateend >= to_date('01-01-2002', 'dd-mm-yyyy')
           AND oracompat.nvl(damountrub, camountrub) <> 0
           and b.status = 'А'
           and oprisn not in (9534516, 24422716)
           group by
                               b.isn,
                               s.deptisn,
                               b1.classisn,
                               s.sagroup,
                               s.Statcode,
                               b.Damountrub,
                               b.Camountrub,
                               b.HeadIsn
);
