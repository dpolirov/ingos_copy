create or replace view v_tt_buhbody_reins (
   parentisn,
   bodyisn,
   code,
   damountrub,
   camountrub,
   dateval,
   oprisn,
   subjisn,
   agrisn,
   dssubjisn,
   amountsum,
   fullamountsum,
   dskoef,
   docsumcnt,
   dsisn,
   discr,
   status,
   classisn,
   classisn2,
   datepay,
   docisn,
   docisn2,
   splitisn,
   amount,
   dscurrisn,
   groupisn,
   statcode,
   dsagrisn,
   reaccisn,
   subaccisn,
   fid )
as
(

    select --+ ordered use_nl(opr)
         s.parentisn,
         s.bodyisn,
         s.code,
         s.damountrub,
         s.camountrub,
         s.dateval,
         s.oprisn,
         s.subjisn ,
         s.agrisn,
         s.dssubjisn ,
         s.amountsum,
         s.fullamountsum,
         oracompat.nvl(s.amountsum/s.fullamountsum,1) as dskoef,
         s.docsumcnt,
         s.dsisn,
         s.discr,
         s.status,
         s.classisn,
         s.classisn2,
         s.datepay,
         s.docisn,
         s.docisn2,
         decode(s.splitisn,null,null,q.isn) splitisn,
         s.amount,
         s.dscurrisn,
         s.groupisn,
         s.statcode,
         dsagrisn,
         reaccisn,
         subaccisn,
         fid
    from
        ...
        left join 
        (select isn, unnest(__hier) as __hier_isn
            from docsum
            where splitisn is null
        ) as q on q.__hier_isn = s.splitisn
        (
            select --+ ordered use_nl(b sd ba pc pd bh)
                  b.parentisn,
                  b.isn bodyisn,
                  b.code, 
                  oracompat.nvl(b.damountrub,0) damountrub,
                  oracompat.nvl(b.camountrub,0) camountrub,
                  b.dateval, 
                  b.oprisn,
                  b.subaccisn,
                  b.subjisn,
                  b.agrisn,
                  sum (gcc2.gcc2(oracompat.nvl (pc.amount, pd.amount),
                  oracompat.nvl(pc.currisn,pd.currisn),35,b.dateval)) over (partition by b.isn) as fullamountsum,
                  count (*) over (partition by b.isn) as docsumcnt,
                  gcc2.gcc2(oracompat.nvl (pc.amount, pd.amount),oracompat.nvl(pc.currisn,pd.currisn),35,b.dateval) as amountsum,
                  oracompat.nvl (pc.isn, pd.isn) as dsisn,
                  oracompat.nvl (pc.subjisn, pd.subjisn) as dssubjisn,
                  oracompat.nvl (pc.agrisn, pd.agrisn) as dsagrisn,
                  oracompat.nvl (pc.discr,pd.discr) as discr,
                  oracompat.nvl (pc.status,pd.status) as status,
                  oracompat.nvl (pc.classisn,pd.classisn) as classisn,
                  oracompat.nvl (pc.classisn2,pd.classisn2) as classisn2,
                  oracompat.nvl (pc.datepay,pd.datepay) as datepay,
                  oracompat.nvl (pc.docisn,pd.docisn) as docisn,
                  oracompat.nvl (pc.docisn2,pd.docisn2) as docisn2,
                  oracompat.nvl (pc.splitisn,pd.splitisn) as splitisn,
                  oracompat.nvl (pc.amount,pd.amount) as amount,
                  oracompat.nvl (pc.currisn,pd.currisn) as dscurrisn,
                  oracompat.nvl (pc.groupisn,pd.groupisn) as groupisn,
                  oracompat.nvl (pc.reaccisn,pd.reaccisn) as reaccisn,
                  sd.statcode,
                  bh.fid
              from tt_rowid t
                        inner join buhbody b
                        on t.isn = b.isn
                        inner join buhsubacc ba
                        on b.subaccisn = ba.isn
                        inner join (select subaccisn,statcode 
                                        from storages.v_rep_subacc4dept 
                                        where statcode in
                                            (select statcode 
                                                from rep_statcode 
                                                where grp in ('входящее перестрахование','исходящее перестрахование'))
                                     union 
                                     select isn,cast(oracompat.substr(id,1,3) as numeric) 
                                        from buhsubacc 
                                        where (id like '913%' or id like '914%')
                                            and dateend >= '31-dec-2010'
                                    ) sd
                        on b.subaccisn = sd.subaccisn
                        inner join buhhead bh
                        on b.headisn = bh.isn
                        left join docsum pc
                        on b.isn = pc.creditisn
                        left join docsum pd
                        on b.isn = pd.debetisn
              where pc.discr between 'f' and 'p'
                  and pd.discr between 'f' and 'p'
                  and  sd.statcode is not null
                  and oracompat.nvl (b.damountrub,b.camountrub) <> 0              -- условие из   vz_repbuhbody_list
                  and b.status = 'а'                                  -- условие из   vz_repbuhbody_list
                  and b.oprisn not in (9534516, 24422716)           -- условие из   vz_repbuhbody_list
            --  and b.dateval<=ba.dateend
        )s
);
