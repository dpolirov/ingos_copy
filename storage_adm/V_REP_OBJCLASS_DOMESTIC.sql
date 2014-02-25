create or replace view storage_adm.v_rep_objclass_domestic (
   agrisn,
   objclassisn,
   domestic,
   parentobjclassisn )
as
(
    select a.agrisn,
            a.objclassisn,
            max(decode(dt.parentisn,36776016,'N', 36775916,'Y')) domestic,
            a.parentobjclassisn
        from (select    --+ ordered use_nl (a o op oc t )
                        a.isn agrisn,
                        o.classisn objclassisn,
                        oracompat.nvl((select max(rt.x2)
                                            from ais.rultariff rt
                                            where rt.tariffisn = 703301816 -- c.get('trf_tariffgroup') - тарифная группа для модификации тс. (...го регион-кредит)
                                                    and x1 = t.modelisn
                                                    and (rt.datebeg <= a.datebeg or rt.datebeg between a.datebeg and a.dateend)),oc.tariffgroupisn) tariffgroupisn, -- egao 27.10.2009
                        op.classisn parentobjclassisn
                  from  ais.agrobject op 
                            left join ais.objcar oc
                            on op.descisn = oc.isn
                            left join ais.cartarif t--egao 27.10.2009 глюки начались, dicti dt
                            on oc.tarifisn = t.isn,
                        storage_adm.tt_rowid tt,
                        ais.agreement a,
                        (select isn
                             from ais.dicti_nh
                             where shared_system.is_subtree(__hier, 683209116) -- комплексное страхование
                             --start with isn=683209116 -- КОМПЛЕКСНОЕ СТРАХОВАНИЕ
                             --connect by prior isn=parentisn
                        ) rl,
                        (select isn
                             from ais.dicti_nh  -- тип договора страхования
                             where shared_system.is_subtree(__hier, 34711216)
                             --start with isn=34711216
                             --connect by prior isn=parentisn
                        ) ac,
                        ais.agrobject o 
                  where a.isn = tt.isn
                        and ruleisn = rl.isn
                        and a.classisn = ac.isn
                        and not exists (select /*+ index(j x_subject_class) */ isn 
                                            from ais.subject j 
                                            where isn = a.emplisn and classisn = 491)
                        and o.agrisn = a.isn
                        and op.isn = (-- для группировки по родительским объектам
                            select isn 
                                from ais.agrobject_nh agr
                                where parentisn is null and shared_system.is_subtree(agr.__hier, o.isn)
                            )
                            --start with isn = o.isn
                            --connect by prior parentisn = isn and prior parentisn is not null)
             ) a left join ais.dicti dt
                 on dt.isn = a.tariffgroupisn   
    group by a.agrisn, 
              a.objclassisn,
              a.parentobjclassisn
);