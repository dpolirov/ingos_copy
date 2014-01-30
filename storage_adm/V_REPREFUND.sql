create or replace view v_reprefund (
   refundisn,
   agrisn,
   condisn,
   currisn,
   claimsum,
   dateloss,
   dateclaim,
   subjisn,
   status,
   datesolution,
   claimstatus,
   dateevent,
   deptisn,
   franchtype,
   franchtariff,
   franchsum,
   agrdatebeg,
   rptclassisn,
   lossshare,
   claimisn,
   datereg,
   emplisn,
   objisn,
   parentobjisn,
   rptgroupisn,
   conddeptisn,
   isrevaluation,
   franchcurrisn,
   franchdeducted,
   classisn,
   refundsum,
   refundsumusd,
   claimsumusd,
   claimid,
   firmisn,
   daterefund,
   limitsum,
   limitcurrisn,
   ruleisnagr,
   ruleisnclaim,
   nrzu,
   budgetgroupisn,
   objclassisn,
   agrextisn,
   condpc,
   parentobjclassisn,
   ragrisn,
   extdateevent,
   totalloss,
   rfranchcurrisn,
   rfranchsum,
   saleremplisn,
   salerdeptisn,
   motivgroupisn,
   riskruleisn,
   riskclassisn,
   rdateval,
   repdateloss,
   claimdatetotalloss,
   claimcurrisn,
   refundsumrub,
   claimsumrub,
   regress,
   claimclassisn,
   refcreated,
   parentisn,
   aggrievednumber,
   refundid,
   agrclassisn )
as
(/* kgs 10.01.2013    repcond нельз€ использовать - в нем теперь нет медиц кондов с 0 плановой премией*/
    select --+ use_nl(s ac ar ao ) ordered push_subq index ( rc x_repcond_cond )
           s.isn refundisn,
           s.agrisn,
           s.condisn,
           s.currisn,
           s.claimsum,
           s.dateloss,
           s.dateclaim,
           s.subjisn,
           s.refstatus status,
           s.datesolution,
           s.clstatus claimstatus,
           s.dateevent,
           s.deptisn,
           ac.franchtype,
           ac.franchtariff,
           ac.franchsum,
           s.agrdatebeg,
           s.rptclassisn,
           lossshare,
           s.claimisn,
           s.datereg,
           s.emplisn,
           s.objisn,
           case 
                when ao.parentisn is null then ao.isn
                else
             (  select --+ rule
                        max(zz.isn)
                    from agrobject zz
                    where zz.parentisn is null
                    start with zz.isn = ao.parentisn
                    connect by nocycle prior zz.parentisn = zz.isn
              )  
           end parentobjisn,
    --!!       oracompat.nvl(rc.parentobjisn,rc.objisn) parentobjisn,
           null::numeric rptgroupisn, --поле заполн€етс€ после загрузки reprefund с помощью апдейта
           null::numeric conddeptisn,
           null::numeric isrevaluation, --пол€ заполн€ютс€ после загрузки reprefund с помощью апдейт
           ac.franchcurrisn,
           oracompat.nvl(decode(oracompat.nvl(ac.franchtype, 'б'), 'б', decode (ac.franchtariff, null, gcc2.gcc2(ac.franchsum, ac.franchcurrisn, s.currisn, oracompat.nvl(oracompat.nvl(dateloss, dateevent), datereg)))), 0) +
           oracompat.nvl(s.claimsum * decode(oracompat.nvl(ac.franchtype, 'б'), 'б', ac.franchtariff), 0) / 100 franchdeducted,
           s.classisn,
           s.refundsum,
           s.refundsumusd,
           s.claimsumusd,
           s.id claimid,
           s.firmisn,
           s.daterefund,
           ac.limitsum,
           ac.currisn limitcurrisn,
           s.ruleisnagr,
           s.ruleisn ruleisnclaim,
           s.nrzu,
           null::numeric budgetgroupisn, --поле заполн€етс€ после загрузки reprefund с помощью апдейта
           ao.classisn objclassisn,
           agrextisn,
           --mserp 04.02.2011 дит-11-1-128014 {
           --decode(allrefundsum, 0, 1 / allrefund, decode(oracompat.nvl(refundsum, 0), 0, oracompat.nvl(claimsum, 0), oracompat.nvl(refundsum, 0)) / allrefundsum) condpc,
           decode(allrefundsum, 0, 1 / allrefund, decode(oracompat.nvl(s.refundsum, 0), 0, decode(agrextisn, null,oracompat.nvl(s.claimsum, 0),0)  , oracompat.nvl(s.refundsum, 0)) / allrefundsum) condpc,
           --}дит-11-1-128014
           case when ao.parentisn is null then null
                else
             (  select --+ rule
                     max(zz.classisn)
                     from agrobject zz
                     where zz.parentisn is null
                     start with zz.isn = ao.parentisn
                     connect by nocycle prior zz.parentisn = zz.isn
              )  
           end parentobjclassisn,
           ragrisn,
           extdateevent,
           totalloss,
           s.franchcurrisn rfranchcurrisn,
           s.franchsum rfranchsum,
           ( select max(subjisn)
                   from agrrole ar
                   where ar.agrisn = s.agrisn
                        and ar.refundisn = s.isn
                        and ar.classisn = 1521585603 ) saleremplisn,
           ( select max(deptisn)
                   from agrrole ar
                   where ar.agrisn = s.agrisn
                        and ar.refundisn = s.isn
                        and ar.classisn = 1521585603 ) salerdeptisn,
           null::numeric motivgroupisn, --поле заполн€етс€ после заргузки reprefund процедурой set_refund_motivgroupisn
           ar.ruleisn riskruleisn,
           ar.classisn riskclassisn,
           dateval rdateval,
           oracompat.trunc(decode(agrextisn, null, oracompat.nvl(oracompat.nvl(dateloss, dateclaim), datereg), oracompat.nvl(oracompat.nvl(extdateevent, oracompat.nvl(dateloss, dateclaim)), datereg))) repdateloss,
           s.claimdatetotalloss,
           s.claimcurrisn, -- egao 20.03.2009 дит-09-1-086869
           s.refundsumrub,
           s.claimsumrub,
           s.regress,
           s.claimclassisn, -- egao 27.10.2010 дит-10-4-121049
           s.created refcreated,-- od 01.07.2011
           s.parentisn,
           (select count(distinct subjisn) 
                from agrrole rl  
                where rl.agrisn = s.agrisn  
                    and rl.refundisn = s.isn 
                    and rl.classisn = 971382125) as aggrievednumber, -- egao 20.03.2013 дит-12-4-176083
           s.refundid,  -- egao 20.03.2013 дит-12-4-176083
           s.agrclassisn
      from ( select s.isn,
                    s.claimisn,
                    s.agrisn,
                    s.rptclassisn,
                    s.condisn,
                    s.currisn,
                    s.claimsum,
                    s.dateloss,
                    s.dateclaim,
                    s.datereg,
                    s.datesolution,
                    oracompat.nvl(s.extdateevent, s.dateevent) dateevent,
                    s.subjisn,
                    s.refstatus,
                    s.clstatus,
                    s.deptisn,
                    s.daterefund,
                    s.franchsum,
                    s.franchcurrisn,
                    s.agrdatebeg,
                    s.lossshare,
                    s.emplisn,
                    s.classisn,
                    s.refundsum,
                    s.objisn,
                    gcc2.gcc2(s.refundsum, s.currisn, 53, oracompat.nvl(s.daterefund, s.dateevent)) refundsumusd,
                    gcc2.gcc2(s.claimsum, s.currisn, 53, oracompat.nvl(s.dateloss, s.dateclaim)) claimsumusd,
                    gcc2.gcc2(s.refundsum, s.currisn, 35, oracompat.nvl(s.daterefund, s.dateevent)) refundsumrub,
                    gcc2.gcc2(s.claimsum, s.currisn, 35, oracompat.nvl(s.dateloss, s.dateclaim)) claimsumrub,
                    s.id,
                    s.ruleisn,
                    s.nrzu,
                    s.agrextisn,
                    --mserp 04.02.2011 дит-11-1-128014 {
                    --sum(decode(oracompat.nvl(decode(ext.isn, null, r.refundsum, ext.refundsum), 0), 0, oracompat.nvl(decode(ext.isn, null, r.claimsum, ext.claimsum), 0), decode(ext.isn, null, r.refundsum, ext.refundsum)))over(partition by r.isn) allrefundsum,
                    sum(decode(oracompat.nvl(s.refundsum, 0), 0, oracompat.nvl(decode(s.agrextisn, null, s.claimsum, s.refundsum), 0), s.refundsum)) over (partition by s.isn) allrefundsum,
                    --} дит-11-1-128014
                    count(*) over (partition by s.isn) allrefund,
                    s.ragrisn,
                    s.extdateevent,
                    s.totalloss,
                    s.dateval,
                    /*egao 21.11.2012
                      ( select min(q.datesend)
                        from ais.queue q
                       where q.classisn = 1647725903 -- c.get('qeclaimtotal')
                         and q.objisn   = s.claimisn )*/ 
                    null::timestamp as claimdatetotalloss,
                    s.claimcurrisn,
                    s.regress,
                    s.claimclassisn, -- egao 27.10.2010 дит-10-4-121049
                    s.created, -- od 01.07.2011
                    s.firmisn, s.ruleisnagr,
                    s.parentisn,
                    s.refundid,
                    s.agrclassisn
               from (
                     select --+ ordered use_nl ( s ag ) no_merge ( s ) index ( ag x_repagr_agr )
                            ag.firmisn, 
                            ag.ruleisn as ruleisnagr, 
                            ag.classisn as agrclassisn,
                            s.*
                         from (
                               select --+ ordered use_nl ( t cl r ext cr )
                                      r.isn,
                                      r.claimisn,
                                      r.agrisn as ragrisn,
                                      oracompat.nvl(ext.agrisn, r.agrisn) as agrisn,
                                      r.rptclassisn,
                                      oracompat.nvl(ext.condisn, r.condisn) condisn,
                                      decode(ext.isn, null, r.currisn, ext.currisn) currisn,
                                      decode(ext.isn, null, r.claimsum, ext.claimsum) claimsum,
                                      cl.dateloss,
                                      cl.dateclaim,
                                      cl.datereg,
                                      cl.datesolution,
                                      ext.dateevent as extdateevent, 
                                      r.dateevent,
                                      cl.subjisn,
                                      r.status refstatus,
                                      cl.status clstatus,
                                      oracompat.nvl(r.deptisn, cl.deptisn) deptisn,
                                      r.daterefund,
                                      r.franchsum,
                                      r.franchcurrisn,
                                      cl.agrdatebeg,
                                      r.lossshare,
                                      oracompat.nvl(r.emplisn, cl.emplisn) emplisn,
                                      oracompat.nvl(ext.classisn, r.classisn) classisn,
                                      decode(ext.isn, null, r.refundsum, ext.refundsum) refundsum,
                                      oracompat.nvl(ext.objisn, r.objisn) objisn,
                                      cl.id,
                                      cl.ruleisn,
                                      r.nrzu,
                                      ext.isn agrextisn,
                                      cl.currisn as claimcurrisn,
                                      r.regress,
                                      cl.classisn claimclassisn, -- egao 27.10.2010 дит-10-4-121049
                                      r.created, -- od 01.07.2011
                                      r.dateval,
                                      cr.totalloss,
                                      r.parentisn,
                                      r.refundid -- egao 20.03.2013 дит-12-4-176083
                                   from tt_rowid t
                                            inner join ais.agrclaim cl
                                            on t.isn = cl.isn
                                            inner join ais.agrrefund r
                                            on r.claimisn = cl.isn
                                            left join ais.agrrefundext ext
                                            on r.isn = ext.refundisn
                                            left join ais.claimrefundcar cr
                                            on r.isn = cr.isn
                                   where r.emplisn not in ( select --+ index (sb x_subject_class)
                                                                    isn
                                                                from subject sb
                                                                where classisn = 491 ) -- тестовый пользователь
                                        and oracompat.nvl(cl.classisn, 0) <> 2835056703 -- акци€ "помощь друга" od 27.11.2009 12475086503
                              ) s 
                                left join repagr ag
                                on s.agrisn = ag.agrisn
                    ) s
               where oracompat.nvl(s.agrclassisn,0) <> 28470016
               ) s
                left join agrcond ac
                on s.condisn = ac.isn
                left join agrrisk ar
                on ac.riskisn = ar.isn
                left join agrobject ao
                on ac.objisn = ao.isn
);
