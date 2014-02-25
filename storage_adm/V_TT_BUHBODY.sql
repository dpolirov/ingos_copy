create or replace view storage_adm.v_tt_buhbody (
   baseisn,
   parentisn,
   bodyisn,
   code,
   damountrub,
   camountrub,
   dateval,
   datequit,
   quitstatus,
   oprisn,
   subaccisn,
   balance,
   basedateval,
   fid,
   basedamountrub,
   basecamountrub,
   subjisn,
   currisn,
   agrisn,
   basedamount,
   basecamount,
   damount,
   camount )
as
(
    select --+ ordered use_nl(b bh)
             distinct s.baseisn,
             s.parentisn,
             s.bodyisn,
             s.code,
             s.damountrub,
             s.camountrub,
             s.dateval,
             s.datequit,
             s.quitstatus,
             s.oprisn,
             s.subaccisn,
             oracompat.nvl(b.damountrub,0::numeric) - oracompat.nvl(b.camountrub,0::numeric) as balance,
             b.dateval as basedateval,
             bh.fid ,
             b.damountrub as basedamountrub,
             b.camountrub as basecamountrub,
             b.subjisn as subjisn,
             b.currisn,
             b.agrisn,
             b.damount as basedamount,
             b.camount as basecamount,
             s.damount, 
             s.camount
        from(select max(BaseIsn) as BaseIsn,
                    max(q.parentisn) as parentisn, 
                    q.bodyisn,
                    max(q.code) as code, 
                    max(q.damountrub) as damountrub,
                    max(q.camountrub) as camountrub, 
                    max(q.damount) as damount, 
                    max(q.camount) as camount,
                    max(q.dateval) as dateval, 
                    max(q.datequit) as datequit,
                    max(q.quitstatus) as quitstatus,
                    max(q.oprisn) as oprisn,
                    max(q.subaccisn) as subaccisn
                from(
                        select decode(q.d_isn,null,q.bodyisn,/*идем вверх только по проводкам операций автоматические - 200 02 03 */
                                            decode(q.parentisn,null,q.bodyisn,q.bodyisn,q.bodyisn, /*бывают проводки, ссылающиеся сами на себя*/
                                                    ( 
                                                        max(q2.isn) over (partition by q.bodyisn order by decode(q.subaccisn,q2.subaccisn,0,1), q2.b1_level desc)
                                                    ))) as BaseIsn, 
                                q.parentisn,
                                q.bodyisn,
                                q.code,
                                q.damountrub,
                                q.camountrub,
                                q.damount,
                                q.camount,
                                q.dateval,
                                q.datequit,
                                q.quitstatus,
                                q.oprisn,
                                q.subaccisn
                            from(                        
                                select
                                        d.isn as d_isn,  
                                        b.parentisn as parentisn,
                                        b.isn as bodyisn,
                                        ba.id as code, 
                                        oracompat.nvl(b.damountrub,0::numeric) as damountrub ,
                                        oracompat.nvl(b.camountrub,0::numeric) as camountrub, 
                                        oracompat.nvl(b.damount,0::numeric) as damount, 
                                        oracompat.nvl(b.camount,0::numeric) as camount,
                                        b.dateval as dateval, 
                                        oracompat.nvl(decode(b.quitstatus,null,b.datequit,null),to_date('01-01-3000','dd-mm-yyyy')::timestamp) as datequit,
                                        b.quitstatus as quitstatus,
                                        b.oprisn as oprisn,
                                        b.subaccisn as subaccisn
                                  from /*(select rowid as rid from buhbody where isn in (8329848903,8492132803,8668517903, 8249586303)) */
                                        storage_adm.tt_rowid t
                                            inner join ais.buhbody b
                                            on t.isn = b.isn
                                            inner join ais.buhsubacc ba
                                            on b.subaccisn = ba.isn
                                            left join (select * from ais.dicti d
                                                         where d.parentisn=759033300 and code in ('200','02','03') -- необходимо ограничение по операциям 200 02 03
                                                         ) d
                                            on b.oprisn = d.isn
                                  where status = 'А'
                                      and (b.quitstatus is not null or b.dateval < oracompat.nvl(b.datequit,to_date('01-01-3000','dd-mm-yyyy')::timestamp))
                                      and (b.code like'77%' or b.code like'78%'  or ba.id like '7619%') -- ограничение по счетам
                                ) as q
                                left join 
                                (
                                    select b1.isn, unnest(b1.__hier) as parent,
                                            b1.subaccisn, shared_system.get_level(b1.__hier) as b1_level
                                        from ais.buhbody_nh b1
                            ) as q2 
                            on q.bodyisn = q2.parent
                    ) as q
                group by q.bodyisn
                ) s, 
                ais.buhbody b,
                ais.buhhead bh
        where s.baseisn = b.isn
            and b.headisn = bh.isn
    union all
    select --+ ordered use_nl(b bh)
             distinct s.baseisn,
             s.parentisn,
             s.bodyisn,
             s.code,
             oracompat.nvl(s.damountrub,0::numeric)damountrub, 
             oracompat.nvl(s.camountrub,0::numeric) camountrub,
             s.dateval,
             s.datequit,
             s.quitstatus,
             s.oprisn,
             s.subaccisn,
             oracompat.nvl(b.damountrub,0::numeric) - oracompat.nvl(b.camountrub,0::numeric) as balance,
             b.dateval as basedateval,
             bh.fid ,
             b.damountrub as basedamountrub,
             b.camountrub as basecamountrub,
             b.subjisn as subjisn,
             b.currisn,
             b.agrisn,
             b.damount as basedamount,
             b.camount as basecamount,
             oracompat.nvl(s.damount,0::numeric)damount, 
             oracompat.nvl(s.camount,0::numeric) camount
        from
            (select max(BaseIsn) as BaseIsn,
                    max(q.parentisn) as parentisn,
                    q.bodyisn,
                    max(q.code) as code,
                    max(q.damountrub) as damountrub,
                    max(q.camountrub) as camountrub,
                    max(q.damount) as damount,
                    max(q.camount) as camount,
                    max(q.dateval) as dateval, 
                    max(q.datequit) as datequit,
                    max(q.quitstatus) as quitstatus,
                    max(q.oprisn) as oprisn,
                    max(q.subaccisn) as subaccisn
                from (
                        select decode(q.d_isn,null,q.bodyisn,/*идем вверх только по проводкам операций автоматические - 200 02 03 */
                                            decode(q.parentisn,null,q.bodyisn,q.bodyisn,q.bodyisn, /*бывают проводки, ссылающиеся сами на себя*/
                                                    ( 
                                                        max(q2.isn) over (partition by q.bodyisn order by decode(q.subaccisn,q2.subaccisn,0,1), q2.b1_level desc)
                                                    ))) as BaseIsn,
                                q.parentisn,
                                q.bodyisn,
                                q.code,
                                q.damountrub,
                                q.camountrub,
                                q.damount,
                                q.camount,
                                q.dateval,
                                q.datequit,
                                q.quitstatus,
                                q.oprisn,
                                q.subaccisn
                        from
                        (
                            select
                                    d.isn as d_isn,
                                    b.parentisn,
                                    b.isn bodyisn,
                                    ba.id code,
                                    case
                                        when dg.queisn is null and quitstatus = 'Ч' then decode(damountrub,null,0,remainrub)
                                        else damountrub
                                    end as damountrub,
                                    case
                                        when dg.queisn is null and quitstatus = 'Ч' then decode(camountrub,null,0,remainrub)
                                        else camountrub
                                    end as camountrub,
                                    case
                                        when dg.queisn is null and quitstatus = 'Ч' then decode(damount,null,0,remain)
                                        else damount
                                    end as damount,
                                    case
                                        when dg.queisn is null and quitstatus = 'Ч' then decode(camount,null,0,remain)
                                        else camount
                                    end as camount,
                                    b.dateval, 
                                    oracompat.nvl(decode(b.quitstatus,null,b.datequit,null),to_date('01-01-3000','dd-mm-yyyy')::timestamp) as datequit,
                                    b.quitstatus,
                                    b.oprisn,
                                    b.subaccisn
                              from /*(select rowid as rid from ais.ais.buhbody where isn in (8329848903,8492132803,8668517903, 8249586303)) */
                                  storage_adm.tt_rowid t
                                    inner join ais.buhbody b
                                    on t.isn = b.isn
                                    inner join ais.buhsubacc ba
                                    on b.subaccisn = ba.isn
                                    left join (select * from ais.dicti d
                                                 where d.parentisn = 759033300 and code in('200','02','03') -- необходимо ограничение по операциям 200 02 03
                                                 ) d
                                    on b.oprisn = d.isn
                                    left join ais.docgrp dg
                                    on b.groupisn = dg.isn
                                  where b.status = 'А'
                                      and (b.quitstatus is not null or oracompat.trunc(b.dateval,'mm') < oracompat.nvl(oracompat.trunc(b.datequit,'mm')::timestamp,to_date('01-01-3000','dd-mm-yyyy')::timestamp))
                                      and  (b.code like'60%' or b.code like'71%' or ( ba.id like '76%' and  not ba.id like '7619%')) -- ограничение по счетам !!!! обратное!!!
                    
                        ) as q
                        left join
                        (
                            select b1.isn, unnest(b1.__hier) as parent,
                                    b1.subaccisn, shared_system.get_level(b1.__hier) as b1_level
                                from ais.buhbody_nh b1
                        ) as q2
                        on q.bodyisn = q2.parent
                ) as q        
                group by q.bodyisn
            ) s, 
        ais.buhbody b,
        ais.buhhead bh
        where s.baseisn = b.isn
        and b.headisn = bh.isn
);