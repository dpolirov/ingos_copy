create or replace view storage_adm.v_tt_buh_turn_contr (
   subaccisn,
   code,
   oprisn,
   db,
   de,
   damountrub,
   camountrub,
   resident,
   branchisn,
   prm_key,
   currisn,
   juridical )
as
(
    select
            subaccisn,
            max(code) code,
            oprisn,
            db ,
            de,
            oracompat.nvl(sum(damountrub),0::numeric) damountrub,
            oracompat.nvl(sum(camountrub),0::numeric) camountrub,
            resident,
            branchisn,
            cast((to_char(db,'yyyymmdd')||cast(subaccisn as varchar)) as numeric) prm_key,
            currisn,
            juridical
    from (
            select --+ ordered use_nl(b sb ) index ( b x_buhbody_subacc_date)
                    b.subaccisn,
                    b.code,
                    b.oprisn,
                    oracompat.trunc(b.dateval,'month') db ,
                    oracompat.add_months(oracompat.trunc(b.dateval,'month')::date,1)-1 de,
                    b.damountrub damountrub,
                    b.camountrub camountrub,
                    b.currisn,
                    resident,
                    branchisn,
                    sb.juridical
                from ais.buhbody_t b inner join ais.subject_t sb on b.subjisn = sb.isn, storage_adm.tt_rowid t, ais.buhsubacc bacc 
                where bacc.isn = oracompat.substr(t.isn,9)
                    and bacc.isn = b.subaccisn
                    and b.subjisn is not null
                    and b.dateval between oracompat.trunc(to_date(oracompat.substr(t.isn,1,8),'yyyymmdd'),'month') and oracompat.add_months(oracompat.trunc(to_date(oracompat.substr(t.isn,1,8),'yyyymmdd'),'month')::date,1)-1
                    and b.status = 'А'
                    and oracompat.nvl(b.damountrub,b.camountrub) <> 0
            union all
            select --+ ordered use_nl(sb )
                    subaccisn,
                    s.code,
                    oprisn,
                    db ,
                    de,
                    s.damountrub*decode(oracompat.nvl(fullamountsum,0::numeric),0,1/dscnt,amountsum/fullamountsum) damountrub,
                    s.camountrub*decode(oracompat.nvl(fullamountsum,0::numeric),0,1/dscnt,amountsum/fullamountsum) camountrub,
                    s.currisn,
                    resident,
                    branchisn,
                    sb.juridical
                from (
                        select --+ ordered use_nl(b pc pd ) index ( b x_buhbody_subacc_date)
                                b.subaccisn,
                                b.code,
                                b.oprisn,
                                oracompat.trunc(b.dateval,'month') db ,
                                oracompat.add_months(oracompat.trunc(b.dateval,'month')::date,1)-1 de,
                                sum (oracompat.nvl(shared_system.gcc2(oracompat.nvl (pc.amount, pd.amount),oracompat.nvl(pc.currisn,pd.currisn),35,b.dateval),oracompat.nvl (pc.amountrub, pd.amountrub))) over (partition by b.isn) as fullamountsum,
                                oracompat.nvl(shared_system.gcc2(oracompat.nvl (pc.amount, pd.amount),oracompat.nvl(pc.currisn,pd.currisn),35,b.dateval),oracompat.nvl(pc.amountrub, pd.amountrub)) amountsum,
                                count(*) over (partition by b.isn) as dscnt,
                                b.damountrub damountrub,
                                b.camountrub camountrub,
                                oracompat.nvl(pc.subjisn,pd.subjisn) subjisn,
                                b.currisn
                            from ais.buhbody_t b
                                    inner join ais.buhsubacc bacc
                                    on bacc.isn = b.subaccisn    
                                    left join ais.docsum pc 
                                    on b.isn = pc.creditisn
                                    left join ais.docsum pd
                                    on b.isn = pd.debetisn,
                                    storage_adm.tt_rowid t
                            where bacc.isn = oracompat.substr(t.isn,9)
                                and pc.discr between 'F' and 'P'
                                and pd.discr between 'F' and 'P'
                                and b.subjisn is  null
                                and b.dateval between oracompat.trunc(to_date(oracompat.substr(t.isn,1,8),'yyyymmdd'),'month') and oracompat.add_months(oracompat.trunc(to_date(oracompat.substr(t.isn,1,8),'yyyymmdd'),'month')::date,1)-1
                                and b.status = 'А'
                                and oracompat.nvl(b.damountrub,b.camountrub) <> 0
                    ) s left join ais.subject_t sb on s.subjisn = sb.isn
        ) as q
    group by
    subaccisn,
    oprisn,
    db ,
    de,
    resident,
    branchisn,
    currisn,
    juridical
);
