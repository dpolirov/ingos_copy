create or replace view v_longagraddendum (
   agrisn,
   addisn,
   discr,
   datebeg,
   datesign,
   premiumsum,
   currisn )
as
(select /*+ all_rows ordered(a) use_nl(a) index(a) */
       a.unisn as agrisn,
       a.isn as addisn,
       a.discr,
       a.datebeg,
       a.datesign,
       case a.discr
         when 'à' then a.premiumsum
         when 'ä' then (select a.premiumsum - oracompat.nvl(sum(z.premiumsum), 0) from agreement_nh z where  z.parentisn = a.isn and z.discr = 'à')
       end as premiumsum,
       a.currisn
    from (select   isn,  
                    unnest(__hier) as unisn, 
                    discr, 
                    datebeg, 
                    datesign, 
                    premiumsum, 
                    currisn 
                from agreement_nh) as a
            inner join (select --+ ordered use_nl ( t ag )
                                 distinct   t.isn
                            from tt_rowid t, agreement_nh ag
                            where ag.isn=t.isn
                                and sign(oracompat.months_between (ag.dateend,ag.datebeg)-13)=1
                                and ag.discr in ('ä', 'ã')
                                and ag.classisn in (select isn
                                                       from dicti_nh d
                                                       where hierarchies.is_subtree(__hier,34711216))
                        ) as t2 
            on a.unisn = t2.isn
    where oracompat.nvl(a.discr,'y') = 'à'
);

/*
    1. Unnest the __hier field
    2. Inner join selected isns with unnested values
    3. Keep isn as node ID and joined __hier value as a connect_by_root(isn)
*/