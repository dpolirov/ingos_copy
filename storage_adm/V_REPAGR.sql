create or replace view v_repagr (
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
   insurantisn#,
   insurantcount#,
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
   firmisn,
   agentclassisn,
   salergoisn,
   salerfisn,
   ownerdeptisn,
   clientjuridical,
   filcommision,
   bemitisn,
   bfilisn,
   calcbizflg,
   previsn,
   crossalerisn,
   transfercomission,
   beneficiaryisn,
   partnerisn,
   limitsumrub,
   insuredsumrub,
   olddateend,
   calcemitisn,
   calcfilisn,
   gmisn,
   addrisn,
   agentdeptisn,
   datecalc,
   brokerisn,
   leaseficiaryisn,
   agentcollectflg,
   agrdetailisn,
   pawnbrokerisn,
   saleschannelisn,
   datebase,
   recommenderisn,
   formisn,
   createdby,
   uprisn,
   incomesum,
   incomesumusd,
   incomesumrub,
   discount,
   dateissue,
   createdate )
as
(select --+ ordered use_hash(sd  fil bfil) use_nl(ar)
       s.agrisn, s.id, s.datebeg, s.dateend, s.datesign, s.classisn, s.ruleisn, s.deptisn, oracompat.nvl(sd.dept0isn, 0) dept0isn,
       fil.filisn, s.ruledept, s.emplisn, s.clientisn, s.currisn, s.premiumsum, s.premusd, s.premrub, s.premeur, s.incomerate,
       s.status, s.discr, s.applisn, s.sharepc, s.reinspc, s.groupisn, s.bizflg, s.parentisn, s.insurantisn#, s.insurantcount#,
       oracompat.nvl(ar.agentisn,ar.brokerisn) agentisn/* kgr 10.05.2011*/, ar.agentcount, ar.emitisn, ar.emitcount, s.comission, s.buhdate, s.limitsum, s.limitsumusd, s.insuredsum,
       s.insuredsumusd, s.agrcreated, oracompat.nvl(ar.agentjuridical,ar.brokerjuridical) agentjuridical /* kgr 10.05.2011*/,
       s.firmisn, 437 agentclassisn, ar.salergoisn, ar.salerfisn, s.ownerdeptisn,
       s.clientjuridical, ar.filcommision, ar.bemitisn, bfil.filisn bfilisn,
       decode(decode(bemitisn, null, fil.rcisn, bfil.rcisn), null, 'ц', 'ф') calcbizflg,
       s.previsn, ar.crossalerisn, ar.transfercomission, ar.beneficiaryisn, ar.partnerisn, s.limitsumrub, s.insuredsumrub,
       s.olddateend, oracompat.nvl(bemitisn, emitisn) calcemitisn,
       decode(bemitisn, null, fil.filisn, bfil.filisn) calcfilisn,
       s.gmisn, s.addrisn, ar.agentdeptisn, s.datecalc, ar.brokerisn, ar.leaseficiaryisn, ar.agentcollectflg, s.agrdetailisn,
       ar.pawnbrokerisn, -- od 6.05.2010 дит-10-2-098743 залогодержатель
       s.saleschannelisn,
       s.datebase,
       ar.recommenderisn, -- od 07.09.2010 дит-10-3-117604
       s.formisn, -- egao 04.03.2011
       s.createdby, -- od 22.03.2011
       ar.uprisn, -- egao 18.04.2012
       -- sts 19.10.2012 - task(38397275003)
       s.incomesum,
       s.incomesumusd,
       s.incomesumrub,
       -- sts 06.11.2012 - скидка для туристов
       s.discount,
       s.dateissue, -- egao 08.05.2013
       s.createdate
      from ( select --+ ordered use_nl(r a ar dr drp ad sd sdf sdf1 arc clnt) use_hash(agnt gm)
                    a.isn agrisn,a.id, a.datebeg, a.dateend, a.datesign, a.classisn, a.ruleisn, a.deptisn,
                    dr.filterisn ruledept, a.emplisn, a.clientisn, a.currisn, a.premiumsum,
                    decode(a.currisn, 53, a.premiumsum, gcc2.gcc2(a.premiumsum, a.currisn, 53, least(a.datesign, oracompat.trunc(current_timestamp), a.datebeg))) premusd,
                    decode(a.currisn, 35, a.premiumsum, gcc2.gcc2(a.premiumsum, a.currisn, 35, least(a.datesign, oracompat.trunc(current_timestamp), a.datebeg))) premrub,
                    decode(a.currisn, 29448516, a.premiumsum, gcc2.gcc2(a.premiumsum, a.currisn, 29448516, least(a.datesign, oracompat.trunc(current_timestamp), a.datebeg))) premeur,
                    decode(a.premiumsum, 0, 0, a.incomesum / a.premiumsum) incomerate,
                    a.status, a.discr, a.applisn, a.sharepc, a.reinspc, a.groupisn, a.bizflg, a.parentisn,
                    /*insurantisn переименован в insurantisn# для того, чтобы выяснить где использовались эти поля.
                    в дальнейшем, эти два поля надо из repagr убрать (вемсте с agrrole) */
                    -- oracompat.nvl(min(decode(ar.classisn, 430, ar.subjisn)), clientisn) insurantisn#,
                    -- count(decode(ar.classisn, 430, 1)) insurantcount#,
                    null::numeric insurantisn#,
                    null::numeric insurantcount#,
                    a.comission comission,
                    null buhdate,
                    a.limitsum,
                    decode(a.currisn, 53, a.limitsum, gcc2.gcc2(a.limitsum, a.currisn, 53, least(a.datesign, oracompat.trunc(current_timestamp), a.datebeg))) limitsumusd,
                    decode(a.currisn, 35, a.limitsum, gcc2.gcc2(a.limitsum, a.currisn, 35, least(a.datesign, oracompat.trunc(current_timestamp), a.datebeg))) limitsumrub,
                    a.insuredsum,
                    decode(a.currisn, 53, a.insuredsum, gcc2.gcc2(a.insuredsum, a.currisn, 53, least(a.datesign, oracompat.trunc(current_timestamp), a.datebeg))) insuredsumusd,
                    decode(a.currisn, 35, a.insuredsum, gcc2.gcc2(a.insuredsum, a.currisn, 35, least(a.datesign, oracompat.trunc(current_timestamp), a.datebeg))) insuredsumrub,
                    a.created agrcreated,
                    a.firmisn,
                    a.ownerdeptisn ownerdeptisn,
                    clnt.juridical clientjuridical,
                    a.previsn previsn,
                    a.olddateend olddateend,
                    gm.gmisn gmisn,
                    a.addrisn,
                    a.datecalc,
                    ad.agrdetailisn agrdetailisn, -- od 11.11.09 детализация дог-ра
                    a.saleschannelisn saleschannelisn,
                    a.datebase as datebase, -- egao 05.07.2010
                    a.formisn as formisn, -- egao 0.03.2011
                    a.createdby createdby, -- od 22.03.2011
                    -- sts 19.10.2012 - task(38397275003)
                    a.incomesum,
                    system.gcc2.gcc2(a.incomesum, a.currisn, 53, least(a.datesign, oracompat.trunc(current_timestamp), a.datebeg)) as incomesumusd,
                    system.gcc2.gcc2(a.incomesum, a.currisn, 35, least(a.datesign, oracompat.trunc(current_timestamp), a.datebeg)) as incomesumrub,
                    -- sts 06.11.2012 - скидка для туристов
                    a.discount,
                    a.dateissue, -- egao 08.05.2013
                    null createdate
               from tt_rowid r
                        inner join ais.agreement a
                        on a.isn = r.isn
                        left join dicti dr
                        on a.ruleisn = dr.isn
                        left join agr_detail_agrhash ad
                        on a.isn = ad.agrisn
                        left join ( select --+ ordered use_nl(x) index(x x_agrext_agr) use_hash(d)
                                             agrisn,
                                             max(x1) gmisn
                                        from tt_rowid r,
                                             agrext x,
                                             ( select isn
                                                 from dicti_nh z
                                                 hierarchies.is_subtree(__hier, 2255842303) -- продукты gm
                                                 --start with ISN = 2255842303 -- продукты gm
                                                 --connect by prior ISN = PARENTISN ) D
                                             ) d
                                       where x.agrisn   = r.isn
                                         and x.classisn = 1071774425
                                         and x.x1       = d.isn
                                       group by agrisn ) gm
                        on r.isn = gm.agrisn
                        left join ais.subject_t clnt
                        on a.clientisn = clnt.isn
              where a.discr in ('д', 'г')
                and a.classisn in ( select isn
                                      from dicti_nh
                                     where hierarchies.is_subtree(__hier, 34711216) -- тип договора страхования
                                     --start with ISN = 34711216 -- тип договора страхования
                                     --connect by prior ISN = PARENTISN )
                                   )
    /* kgs  24.10.11 нафиг не нужен тут этот гроупбай
              group by a.isn, a.id, a.datebeg, a.dateend, a.datesign, a.classisn, a.ruleisn,
                       a.deptisn, dr.filterisn, a.emplisn, a.clientisn, a.currisn, a.incomesum,
                       a.premiumsum, a.status, a.discr, a.applisn, a.sharepc, a.reinspc, a.groupisn,
                       a.bizflg, a.parentisn, limitsum, insuredsum, a.created, a.firmisn, a.addrisn,
                       a.datecalc */) s
                        left join repagrroleagr ar
                        on s.agrisn = ar.agrisn
                        left join rep_dept sd
                        on s.deptisn = sd.deptisn
                        left join rep_dept fil
                        on ar.emitisn = fil.deptisn
                        left join rep_dept bfil
                        on ar.bemitisn = bfil.deptisn
);
