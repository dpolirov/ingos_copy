create or replace view storage_adm.v_repagr_economic (
   agrisn,
   id,
   datebeg,
   dateend,
   datesign,
   classisn,
   ruleisn,
   deptisn,
   dept0isn,
   filisn,
   ruledept,
   emplisn,
   clientisn,
   currisn,
   premiumsum,
   premusd,
   premrub,
   premeur,
   incomerate,
   status,
   discr,
   applisn,
   sharepc,
   reinspc,
   groupisn,
   bizflg,
   parentisn,
   insurantisn,
   insurantcount,
   agentisn,
   agentcount,
   emitisn,
   emitcount,
   comission,
   buhdate,
   limitsum,
   limitsumusd,
   insuredsum,
   insuredsumusd,
   agrcreated,
   agentjuridical,
   firmisn )
as
(select --+ ordered use_nl( r a ar dr drp sd sdf sdf1) no_merge ( zdept ) no_merge( zdept2 ) use_hash (zdept zdept2 )
          a.isn agrisn,
          a.id,
          a.datebeg,
          a.dateend,
          a.datesign,
          a.classisn,
          a.ruleisn,
          a.deptisn,
          sd.isn dept0isn,
          min(oracompat.nvl(sdf.isn,sdf1.isn)) filisn,
          dr.filterisn ruledept,
          a.emplisn,
          a.clientisn,
          a.currisn,
          a.premiumsum,
          decode(a.currisn,53,a.premiumsum,shared_system.gcc2(a.premiumsum,a.currisn,53,a.datebeg)) premusd,
          decode(a.currisn,35,a.premiumsum,shared_system.gcc2(a.premiumsum,a.currisn,35,a.datebeg)) premrub,
          decode(a.currisn,29448516,a.premiumsum,shared_system.gcc2(a.premiumsum,a.currisn,29448516,a.datebeg)) premeur,
          decode(a.premiumsum,0,0,a.incomesum/a.premiumsum) incomerate,
          a.status,
          a.discr,
          a.applisn,
          a.sharepc,
          a.reinspc,
          a.groupisn,
          a.bizflg,
          a.parentisn,
          oracompat.nvl(min(decode(ar.classisn,430 /*c.get('insured')*/, subjisn)), clientisn) insurantisn,
          count(decode(ar.classisn,430 /*c.get('insured')*/, 1)) insurantcount,
          min(decode(ar.classisn,437 /*c.get('agent')*/, subjisn, 438, subjisn)) agentisn,
          sum(decode(ar.classisn,437 /*c.get('agent')*/, 1, 438,1)) agentcount,
          min(decode(ar.classisn,13157916 /*c.get('emittent')*/, subjisn)) emitisn,
          count(decode(ar.classisn,13157916 /*c.get('emittent')*/ ,1)) emitcount,
          max(a.comission) comission,
          null::timestamp buhdate,
          a.limitsum,
          decode(a.currisn,53,a.limitsum,shared_system.gcc2(a.limitsum,a.currisn,53,a.datebeg)) limitsumusd,
          a.insuredsum,
          decode(a.currisn,53,a.insuredsum,shared_system.gcc2(a.insuredsum,a.currisn,53,a.datebeg)) insuredsumusd,
          a.created agrcreated,
          min(case when ar.classisn in(437,438) then (select juridical from ais.subject where isn = ar.subjisn)end) agentjuridical,
          a.firmisn
    from storage_adm.tt_rowid t 
            inner join ais.agreement a
            on t.isn = a.isn
            left join ais.agrrole ar
            on ar.agrisn = a.isn
            left join ais.dicti dr
            on dr.isn = a.ruleisn
            left join (select distinct t1.isn, first_value(case when t2.classisn = 956867125 or t2.parentisn = 28763316/*c.get('subsidedept')*/ then t2.isn end)
                                                           over ( partition by t1.isn
									order by    case when (case when t2.classisn = 956867125 
                                                                                               then shared_system.get_level(t2.__hier) 
                                                                                       end) is null
                                                                                   then 1 else 0
                                                                           end ,
                                                                           case when t2.classisn = 956867125 
                                                                                then shared_system.get_level(t2.__hier) 
                                                                            end ,
									     case when (case when t2.parentisn = 28763316/*c.get('subsidedept')*/ 
                                                                                               then shared_system.get_level(t2.__hier)
                                                                                      end) is null
                                                                               then 1 else 0
                                                                           end ,
                                                                            case when t2.parentisn = 28763316/*c.get('subsidedept')*/ 
                                                                                then shared_system.get_level(t2.__hier) 
                                                                            end 
                                                                           rows between unbounded preceding and unbounded following) as filisn
                                                                            from (
			select unnest(__hier) as unh, isn from 
			ais.subdept_nh) as t1
			inner join ais.subdept_nh t2
			on t1.unh = t2.isn) as zdept
            on zdept.isn = a.deptisn
            left join ais.subdept sdf
            on sdf.isn = zdept.filisn
            left join (select distinct t1.isn, first_value(case when t2.classisn = 956867125 or t2.parentisn = 28763316/*c.get('subsidedept')*/ then t2.isn end)
                                                           over ( partition by t1.isn
									order by    case when (case when t2.classisn = 956867125 
                                                                                               then shared_system.get_level(t2.__hier) 
                                                                                       end) is null
                                                                                   then 1 else 0
                                                                           end ,
                                                                           case when t2.classisn = 956867125 
                                                                                then shared_system.get_level(t2.__hier) 
                                                                            end ,
									     case when (case when t2.parentisn = 28763316/*c.get('subsidedept')*/ 
                                                                                               then shared_system.get_level(t2.__hier)
                                                                                      end) is null
                                                                               then 1 else 0
                                                                           end ,
                                                                            case when t2.parentisn = 28763316/*c.get('subsidedept')*/ 
                                                                                then shared_system.get_level(t2.__hier) 
                                                                            end 
                                                                           rows between unbounded preceding and unbounded following) as filisn
                                                                            from (
			select unnest(__hier) as unh, isn from 
			ais.subdept_nh) as t1
			inner join ais.subdept_nh t2
			on t1.unh = t2.isn) as zdept2
			on zdept2.isn = case ar.classisn when 13157916 then ar.subjisn end
            left join ais.subdept sdf1
            on sdf1.isn = zdept2.filisn
            inner join
            (select t.isn, t1.isn t1_isn 
                             from (select z.isn,
                                         z.parentisn,
                                         unnest(z.__hier) as unh
                                    from ais.subdept_nh z) t1
                                inner join ais.subdept_nh t
                                    on t.isn = t1.unh
                                    and t.parentisn = 0) sd
                                    on sd.t1_isn = a.deptisn
    where a.discr in('Д','Г')
          and a.classisn  in (select isn 
                                from ais.dicti_nh 
                                where shared_system.is_subtree(__hier, 12415216))
                                 --start with isn = 12415216 
                                --connect  by prior isn = parentisn)
                            --start with isn = a.deptisn 
                            --connect by prior parentisn = isn)
          /*and sdf.isn  --(+) mserp 26.10.2009. убрал открытый join, т.к. в 10g этот фокус больше не проходит. если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
                    = oracompat.nvl(
                    (select distinct first_value(case when classisn=956867125 or parentisn = 28763316\*c.get('subsidedept')*\ then isn end)
                            over (order by case when classisn=956867125 then level end desc nulls last,
                                     case when parentisn = 28763316\*c.get('subsidedept')*\ then level end desc nulls last
                            rows between unbounded preceding and unbounded following )
                    from subdept z
                    start with isn=a.deptisn
                    connect by prior parentisn = isn
                    ),0*a.isn)*/
          -- egao 27.10.2009 глюки появились
          /*and sdf1.isn  --(+) mserp 26.10.2009. убрал открытый join, т.к. в 10g этот фокус больше не проходит. если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
                      =oracompat.nvl(
                      (select
                      distinct first_value(case when classisn=956867125 or parentisn = 28763316\*c.get('subsidedept')*\ then isn end)
                      over (order by case when classisn=956867125 then level end desc nulls last,
                               case when parentisn = 28763316\*c.get('subsidedept')*\ then level end desc nulls last
                      rows between unbounded preceding and unbounded following )
                      from subdept z start with isn=decode(ar.classisn,13157916 \*c.get('emittent')*\, subjisn) connect by prior parentisn = isn
                      ),0*a.isn)*/
          -- egao 27.10.2009 глюки появились
group by
        a.isn,
        a.id,
        a.datebeg,
        a.dateend,
        a.datesign,
        a.classisn,
        a.ruleisn,
        a.deptisn,
        dr.filterisn,
        a.emplisn,
        a.clientisn,
        a.currisn,
        a.incomesum,
        a.premiumsum,
        a.status,
        a.discr,
        a.applisn,
        a.sharepc,
        a.reinspc,
        a.groupisn,
        a.bizflg,
        a.parentisn,
        sd.isn,
        limitsum,
        insuredsum,
        a.created,
        a.firmisn
);
