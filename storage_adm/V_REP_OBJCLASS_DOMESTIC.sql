create or replace view v_rep_objclass_domestic (
   agrisn,
   objclassisn,
   domestic,
   parentobjclassisn )
as
(
    select a.agrisn,
            a.objclassisn,
            max(decode(dt.parentisn,36776016,'n', 36775916,'y')) domestic,
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
                  from  agrobject op 
                            left join objcar oc
                            on op.descisn = oc.isn
                            left join cartarif t--egao 27.10.2009 глюки начались, dicti dt
                            on oc.tarifisn = t.isn
                        tt_rowid tt,
                        agreement a,
                        (select isn
                             from dicti_nh
                             where hierarchies.is_butree(__hier, 683209116) -- комплексное страхование
                             --start with isn=683209116 -- КОМПЛЕКСНОЕ СТРАХОВАНИЕ
                             --connect by prior isn=parentisn
                        ) rl,
                        (select isn
                             from dicti_nh  -- тип договора страхования
                             where hierarchies.is_subtree(__hier, 34711216)
                             --start with isn=34711216
                             --connect by prior isn=parentisn
                        ) ac,
                        ais.agrobject o 
                  where a.isn = tt.isn
                        and ruleisn = rl.isn
                        and a.classisn = ac.isn
                        and not exists (select /*+ index(j x_subject_class) */ isn 
                                            from subject j 
                                            where isn = a.emplisn and classisn = 491)
                        and o.agrisn = a.isn
                        and op.rowid = (-- для группировки по родительским объектам
                            select rowid from agrobject where parentisn is null
                            start with isn = o.isn
                            connect by prior parentisn = isn and prior parentisn is not null)
             ) a left join dicti dt
                 on dt.isn = a.tariffgroupisn   
    group by a.agrisn, 
              a.objclassisn,
              a.parentobjclassisn
);