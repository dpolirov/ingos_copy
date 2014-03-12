create or replace view storage_adm.v_tt_buh_turn (
   subaccisn,
   code,
   subkindisn,
   oprisn,
   currisn,
   db,
   de,
   damount,
   damountrub,
   damountusd,
   camount,
   camountrub,
   camountusd,
   prm_key,
   deb,
   dee )
as
(

    select --+ ordered use_nl(bacc b) index( b x_buhbody_subacc_date)
            b.subaccisn,
            b.code ,
            b.subkindisn,
            b.oprisn,
            b.currisn,
            oracompat.trunc(dateval,'month') db ,
            oracompat.add_months(oracompat.trunc(dateval,'month')::date,1)-1 de,
            --sb.resident,
            sum(b.damount) damount,
            sum(b.damountrub) damountrub,
            sum(b.damountusd) damountusd,
            sum(b.camount) camount,
            sum(b.camountrub) camountrub,
            sum(b.camountusd) camountusd,
            cast((to_char(oracompat.trunc(dateval,'month'),'yyyymmdd')||cast(subaccisn as varchar)) as numeric) prm_key,
            oracompat.trunc(oracompat.nvl(dateevent,dateval),'month') deb ,
            oracompat.add_months(oracompat.trunc(oracompat.nvl(dateevent,dateval),'month')::date,1)-1 dee
    from storage_adm.tt_rowid t,  ais.buhbody_t b--,subject  sb
    where b.subaccisn = oracompat.substr(t.isn,9)
            and b.dateval between oracompat.trunc(to_date(oracompat.substr(t.isn,1,8),'yyyymmdd'),'month') and oracompat.add_months(oracompat.trunc(to_date(oracompat.substr(t.isn,1,8),'yyyymmdd'),'month'),1)-1
            and b.status='А'
            --and b.subjisn=sb.isn(+)
    group by
                b.subaccisn,
                b.code,
                b.subkindisn,
                b.oprisn,
                b.currisn,
                oracompat.trunc(dateval,'month') ,
                oracompat.add_months(oracompat.trunc(dateval,'month')::date,1)-1,
                oracompat.trunc(oracompat.nvl(dateevent,dateval),'month'),
                oracompat.add_months(oracompat.trunc(oracompat.nvl(dateevent,dateval),'month')::date,1)-1
);
