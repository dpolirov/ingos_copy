create or replace view storage_adm.v_longagraddendum (
   agrisn,
   addisn,
   discr,
   datebeg,
   datesign,
   premiumsum,
   currisn )
as
(select a.unisn as agrisn,
       a.isn as addisn,
       a.discr,
       a.datebeg,
       a.datesign,
       case a.discr
         when 'А' then a.premiumsum
         when 'Д' then (select a.premiumsum - oracompat.nvl(sum(z.premiumsum), 0::numeric) 
                            from ais.agreement_nh z 
                            where  z.parentisn = a.isn and z.discr = 'А')
       end as premiumsum,
       a.currisn
    from (select   isn,  
                    unnest(__hier) as unisn, 
                    previsn,
                    discr, 
                    datebeg, 
                    datesign, 
                    premiumsum, 
                    currisn,
                    shared_system.get_level(__hier) level
                from ais.agreement_nh_prev) as a
            inner join (select --+ ordered use_nl ( t ag )
                                 distinct   t.isn
                            from storage_adm.tt_rowid t, ais.agreement_nh_prev ag
                            where ag.isn=t.isn
                                and sign(oracompat.months_between (ag.dateend::date,ag.datebeg::date)-13)=1
                                and ag.discr in ('Д', 'Г')
                                and ag.classisn in (select isn
                                                       from ais.dicti_nh d
                                                       where shared_system.is_subtree(__hier,34711216))
                        ) as t2 
            on a.unisn = t2.isn
    where (level > 1 and oracompat.nvl(a.discr,'Y') = 'А') or level = 1 
);

/*
    1. Unnest the __hier field
    2. Inner join selected isns with unnested values
    3. Keep isn as node ID and joined __hier value as a connect_by_root(isn)
*/