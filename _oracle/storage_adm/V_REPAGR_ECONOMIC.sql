CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPAGR_ECONOMIC" ("AGRISN", "ID", "DATEBEG", "DATEEND", "DATESIGN", "CLASSISN", "RULEISN", "DEPTISN", "DEPT0ISN", "FILISN", "RULEDEPT", "EMPLISN", "CLIENTISN", "CURRISN", "PREMIUMSUM", "PREMUSD", "PREMRUB", "PREMEUR", "INCOMERATE", "STATUS", "DISCR", "APPLISN", "SHAREPC", "REINSPC", "GROUPISN", "BIZFLG", "PARENTISN", "INSURANTISN", "INSURANTCOUNT", "AGENTISN", "AGENTCOUNT", "EMITISN", "EMITCOUNT", "COMISSION", "BUHDATE", "LIMITSUM", "LIMITSUMUSD", "INSUREDSUM", "INSUREDSUMUSD", "AGRCREATED", "AGENTJURIDICAL", "FIRMISN") AS 
  (select --+ ordered use_nl( r a ar dr drp sd sdf sdf1) no_merge ( zdept ) no_merge( zdept2 ) use_hash (zdept zdept2 )
      a.isn agrisn,
      a.id,
      a.datebeg,
      a.dateend,
      a.datesign,
      a.classisn,
      a.ruleisn,
      a.deptisn,
      sd.isn dept0Isn,
      Min(Nvl(sdf.isn,sdf1.isn)) filisn,
      dr.filterisn ruledept,
      a.emplisn,
      a.clientisn,
      a.currisn,
      a.premiumsum,
      decode(a.currisn,53,a.premiumsum,gcc2.gcc2(a.premiumsum,a.currisn,53,a.datebeg)) PremUSD,
      decode(a.currisn,35,a.premiumsum,gcc2.gcc2(a.premiumsum,a.currisn,35,a.datebeg)) PremRUB,
      decode(a.currisn,29448516,a.premiumsum,gcc2.gcc2(a.premiumsum,a.currisn,29448516,a.datebeg)) PremEUR,
      decode(a.premiumsum,0,0,a.incomesum/a.premiumsum) IncomeRate,
      a.status,
      a.discr,
      a.applisn,
      a.sharepc,
      a.reinspc,
      a.groupisn,
      a.bizflg,
      a.parentisn,
      nvl(min(decode(ar.classisn,430 /*c.get('Insured')*/, subjisn)), clientisn) InsurantIsn,
      count(decode(ar.classisn,430 /*c.get('Insured')*/, 1)) InsurantCount,
      min(decode(ar.classisn,437 /*c.get('Agent')*/, subjisn, 438, subjisn)) AgentIsn,
      sum(decode(ar.classisn,437 /*c.get('Agent')*/, 1, 438,1)) AgentCount,
      min(decode(ar.classisn,13157916 /*c.get('Emittent')*/, subjisn)) EmitIsn,
      count(decode(ar.classisn,13157916 /*c.get('Emittent')*/ ,1)) EmitCount,
      Max(a.comission) comission,
      to_date(Null) BuhDate,
      a.limitsum,
      decode(a.currisn,53,a.limitsum,gcc2.gcc2(a.limitsum,a.currisn,53,a.datebeg)) limitsumUsd,
      a.insuredsum,
      decode(a.currisn,53,a.insuredsum,gcc2.gcc2(a.insuredsum,a.currisn,53,a.datebeg)) insuredsumUsd,
      a.created agrcreated,
      min(case when ar.classisn in(437,438) then (Select JURIDICAL From Ais.Subject Where Isn=Ar.subjisn)end) AGENTJURIDICAL,
      a.firmisn
FROM tt_rowId t,
     ais.agreement a,
     ais.agrrole ar,
     dicti dr,
     ais.subdept sd,
     ais.subdept sdf,
     ais.subdept sdf1,
     (SELECT isn,
             (SELECT DISTINCT first_value(case when x.classisn=956867125 or x.parentisn = 28763316/*c.get('SubsideDept')*/ then isn end)
                      over (order by case when x.classisn=956867125 then Level end desc nulls last,
                            CASE when x.parentisn = 28763316/*c.get('SubsideDept')*/ then Level end desc nulls last
                            ROWS BETWEEN unbounded preceding and unbounded following)
              FROM ais.subdept x
              CONNECT BY PRIOR x.parentisn = x.isn
              START WITH x.isn=z.isn
            ) AS filisn
      FROM ais.subdept z
     ) zdept,
     (SELECT isn,
             (SELECT DISTINCT first_value(case when x.classisn=956867125 or x.parentisn = 28763316/*c.get('SubsideDept')*/ then isn end)
                      over (order by case when x.classisn=956867125 then Level end desc nulls last,
                            CASE when x.parentisn = 28763316/*c.get('SubsideDept')*/ then Level end desc nulls last
                            ROWS BETWEEN unbounded preceding and unbounded following)
              FROM ais.subdept x
              CONNECT BY PRIOR x.parentisn = x.isn
              START WITH x.isn=z.isn
            ) AS filisn
      FROM ais.subdept z
     ) zdept2
WHERE t.isn=a.isn
  and a.discr in('Д','Г')
  and a.classisn  in (select isn from dicti start with isn = 12415216 connect  by prior isn = parentisn)
  and ar.agrisn(+)=a.isn
  and dr.isn(+)=a.ruleisn
  and sd.rowid=(select rowid from ais.subdept z where parentisn=0 start with isn=a.deptisn connect by prior parentisn = isn)

  /*and sdf.isn  --(+) MSerp 26.10.2009. Убрал открытый join, т.к. в 10g этот фокус больше не проходит. Если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. Насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
            = Nvl(
            (SELECT distinct first_value(case when classisn=956867125 or parentisn = 28763316\*c.get('SubsideDept')*\ then isn end)
                    over (order by case when classisn=956867125 then Level end desc nulls last,
                             case when parentisn = 28763316\*c.get('SubsideDept')*\ then Level end desc nulls last
                    rows between unbounded preceding and unbounded following )
            from subdept z
            start with isn=a.deptisn
            connect by prior parentisn = isn
            ),0*a.isn)*/
  -- EGAO 27.10.2009 Глюки появились
  AND zdept.isn(+)=a.deptisn
  AND sdf.isn(+)=zdept.filisn

  /*and sdf1.isn  --(+) MSerp 26.10.2009. Убрал открытый join, т.к. в 10g этот фокус больше не проходит. Если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. Насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
              =Nvl(
              (select
              distinct first_value(case when classisn=956867125 or parentisn = 28763316\*c.get('SubsideDept')*\ then isn end)
              over (order by case when classisn=956867125 then Level end desc nulls last,
                       case when parentisn = 28763316\*c.get('SubsideDept')*\ then Level end desc nulls last
              rows between unbounded preceding and unbounded following )
              from subdept z start with isn=decode(ar.classisn,13157916 \*c.get('Emittent')*\, subjisn) connect by prior parentisn = isn
              ),0*a.isn)*/
  -- EGAO 27.10.2009 Глюки появились
  AND zdept2.isn(+)=CASE ar.classisn WHEN 13157916 THEN ar.subjisn END
  AND sdf1.isn(+)=zdept2.filisn
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
        LimitSum,
        insuredsum,
        a.created,
        a.firmisn
);
