create or replace view v_repagr_economic (
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
          min(oracompat.oracompat.nvl(sdf.isn,sdf1.isn)) filisn,
          dr.filterisn ruledept,
          a.emplisn,
          a.clientisn,
          a.currisn,
          a.premiumsum,
          decode(a.currisn,53,a.premiumsum,gcc2.gcc2(a.premiumsum,a.currisn,53,a.datebeg)) premusd,
          decode(a.currisn,35,a.premiumsum,gcc2.gcc2(a.premiumsum,a.currisn,35,a.datebeg)) premrub,
          decode(a.currisn,29448516,a.premiumsum,gcc2.gcc2(a.premiumsum,a.currisn,29448516,a.datebeg)) premeur,
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
          decode(a.currisn,53,a.limitsum,gcc2.gcc2(a.limitsum,a.currisn,53,a.datebeg)) limitsumusd,
          a.insuredsum,
          decode(a.currisn,53,a.insuredsum,gcc2.gcc2(a.insuredsum,a.currisn,53,a.datebeg)) insuredsumusd,
          a.created agrcreated,
          min(case when ar.classisn in(437,438) then (select juridical from ais.subject where isn = ar.subjisn)end) agentjuridical,
          a.firmisn
    from tt_rowid t 
            inner join ais.agreement a
            on t.isn = a.isn
            left join ais.agrrole ar
            on ar.agrisn = a.isn
            left join dicti dr
            on dr.isn = a.ruleisn
            left join (select isn,
                         (select distinct first_value(case when x.classisn = 956867125 or x.parentisn = 28763316/*c.get('subsidedept')*/ then isn end)
                                  over (order by case when x.classisn = 956867125 then level end desc nulls last,
                                        case when x.parentisn = 28763316/*c.get('subsidedept')*/ then level end desc nulls last
                                        rows between unbounded preceding and unbounded following)
                              from ais.subdept x
                              connect by prior x.parentisn = x.isn
                              start with x.isn=z.isn
                         ) as filisn
                            from ais.subdept z
                    ) zdept
            on zdept.isn = a.deptisn
            left join ais.subdept sdf
            on sdf.isn = zdept.filisn
            left join (select isn,
                         (select distinct first_value(case when x.classisn = 956867125 or x.parentisn = 28763316/*c.get('subsidedept')*/ then isn end)
                                  over (order by case when x.classisn = 956867125 then level end desc nulls last,
                                        case when x.parentisn = 28763316/*c.get('subsidedept')*/ then level end desc nulls last
                                        rows between unbounded preceding and unbounded following)
                              from ais.subdept x
                              connect by prior x.parentisn = x.isn
                              start with x.isn = z.isn
                        ) as filisn
                            from ais.subdept z
                      ) zdept2
            on zdept2.isn = case ar.classisn when 13157916 then ar.subjisn end
            left join ais.subdept sdf1
            on sdf1.isn = zdept2.filisn    
         ais.subdept sd
    where a.discr in('д','г')
          and a.classisn  in (select isn from dicti start with isn = 12415216 connect  by prior isn = parentisn)
          and sd.rowid = (select rowid from ais.subdept z where parentisn = 0 start with isn = a.deptisn connect by prior parentisn = isn)
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
