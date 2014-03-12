create or replace view storage_adm.v_tt_buhbody_reins (
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
         oracompat.nvl(s.amountsum/s.fullamountsum,1::numeric) as dskoef,
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
            (
            select --+ ordered use_nl(b sd ba pc pd bh)
                  b.parentisn,
                  b.isn bodyisn,
                  b.code, 
                  oracompat.nvl(b.damountrub,0::numeric) damountrub,
                  oracompat.nvl(b.camountrub,0::numeric) camountrub,
                  b.dateval, 
                  b.oprisn,
                  b.subaccisn,
                  b.subjisn,
                  b.agrisn,
                  sum (shared_system.gcc2(oracompat.nvl (pc.amount, pd.amount),
                  oracompat.nvl(pc.currisn,pd.currisn),35,b.dateval)) over (partition by b.isn) as fullamountsum,
                  count (*) over (partition by b.isn) as docsumcnt,
                  shared_system.gcc2(oracompat.nvl (pc.amount, pd.amount),oracompat.nvl(pc.currisn,pd.currisn),35,b.dateval) as amountsum,
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
              from storage_adm.tt_rowid t
                        inner join ais.buhbody b
                        on t.isn = b.isn
                        inner join ais.buhsubacc ba
                        on b.subaccisn = ba.isn
                        inner join (select subaccisn,statcode 
                                        from storages.v_rep_subacc4dept 
                                        where statcode in
                                            (select statcode 
                                                from storages.rep_statcode 
                                                where grp in ('Входящее перестрахование','Исходящее перестрахование'))
                                     union 
                                     select isn,cast(oracompat.substr(id,1,3) as numeric) 
                                        from ais.buhsubacc 
                                        where (id like '913%' or id like '914%')
                                            and dateend >= to_date('31-dec-2010','dd-mon-yyyy')
                                    ) sd
                        on b.subaccisn = sd.subaccisn
                        inner join ais.buhhead bh
                        on b.headisn = bh.isn
                        left join ais.docsum pc
                        on b.isn = pc.creditisn
                        and pc.discr between 'F' and 'P'
                        left join ais.docsum pd
                        on b.isn = pd.debetisn
                        and pd.discr between 'F' and 'P'
              where 
                    sd.statcode is not null
                  and oracompat.nvl (b.damountrub,b.camountrub) <> 0              -- условие из   vz_repbuhbody_list
                  and b.status = 'А'                                  -- условие из   vz_repbuhbody_list
                  and b.oprisn not in (9534516, 24422716)           -- условие из   vz_repbuhbody_list
            --  and b.dateval<=ba.dateend
        )s
        left join 
        (select isn, unnest(__hier) as __hier_isn
            from ais.docsum_nh
            where splitisn is null
        ) as q 
        on q.__hier_isn = s.splitisn
);
