create or replace view storage_adm.v_tt_buh_turn_corr (
   subaccisn,
   code,
   corcode,
   oprisn,
   db,
   de,
   damountrub,
   camountrub,
   prm_key,
   subkindisn)
as
(
    select 
        subaccisn,
        max(code) code,
        corcode,
        oprisn,
        db,
        de,
        oracompat.nvl(sum(damountrub),0::numeric) damountrub,
        oracompat.nvl(sum(camountrub),0::numeric) camountrub,
        /*
        nvl(sum(damountrub*nvl(corpc,1)),0) damountrub,
        nvl(sum(camountrub*nvl(corpc,1)),0) camountrub,
        */
        cast((to_char(db,'yyyymmdd')||cast(subaccisn as varchar)) as numeric) prm_key,
        subkindisn
    from (
            select --+ ordered use_nl(b bc ) index ( b x_buhbody_subacc_date) index (bc x_buhbody_head)
                    b.subaccisn,
                    b.code,
                    b.oprisn,
                    b.subkindisn,
                    oracompat.trunc(b.dateval,'month') db ,
                    oracompat.add_months(oracompat.trunc(b.dateval,'month')::date,1)-1 de,
                    case
                    --when nvl(b.damountrub,b.camountrub)=0 then 0
                        when count(*) over (partition by b.isn) = 1 
                            then b.damountrub
                        when oracompat.nvl(b.damountrub,b.camountrub) <> sum(oracompat.nvl(bc.damountrub,bc.camountrub)) over (partition by b.isn) 
                            then bc.camountrub/sum(oracompat.nvl(bc.damountrub,bc.camountrub)) over (partition by b.isn)
                    else
                        bc.camountrub
                    end damountrub,
                    case
                    --when nvl(b.damountrub,b.camountrub)=0 then 0
                        when count(*) over (partition by b.isn) = 1 
                            then b.camountrub
                        when oracompat.nvl(b.damountrub,b.camountrub) <> sum(oracompat.nvl(bc.damountrub,bc.camountrub)) over (partition by b.isn) 
                            then bc.damountrub/sum(oracompat.nvl(bc.damountrub,bc.camountrub)) over (partition by b.isn)
                    else
                        bc.damountrub
                    end camountrub,
                    bc.code corcode     
            from storage_adm.tt_rowid t, ais.buhsubacc bacc, ais.buhbody_t b,ais.buhbody bc
            where bacc.isn = oracompat.substr(t.isn,9)
                and bacc.isn = b.subaccisn
                and b.dateval between oracompat.trunc(to_date(oracompat.substr(t.isn,1,8),'yyyymmdd'),'month') and oracompat.add_months(oracompat.trunc(to_date(oracompat.substr(t.isn,1,8),'yyyymmdd'),'month')::date,1)-1
                and b.status = 'А'
                and oracompat.nvl(b.damountrub,b.camountrub) <> 0
                and b.headisn = bc.headisn
                and bc.status = 'A'
                --and nvl(bc.damountrub,bc.camountrub)<>0
                and decode(b.damountrub,null,'D','C') <> decode(bc.damountrub,null,'D','C')
    ) as q

    group by
            subaccisn,
            corcode,
            oprisn,
            subkindisn,
            db ,
            de
);
