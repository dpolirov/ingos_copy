create or replace view v_subject_attrib (
   subjisn,
   home_addrisn,
   home_cityisn,
   home_regionisn,
   home_zip,
   post_addrisn,
   post_cityisn,
   post_regionisn,
   temporary_addrisn,
   temporary_cityisn,
   temporary_regionisn,
   passport_addrisn,
   passport_cityisn,
   passport_regionisn,
   passport_zip,
   passport_address,
   fact_addrisn,
   fact_cityisn,
   fact_regionisn,
   jur_addrisn,
   jur_cityisn,
   jur_regionisn,
   vipclassisn,
   subjsecuritystr,
   addresssecuritystr,
   phonesecuritystr,
   drivingdatebeg,
   agegroup,
   citizenship,
   n_kids,
   marriagestateisn,
   familystateisn,
   stoadayspay,
   driverst,
   motivation,
   no_mail,
   email,
   serv_phone,
   mobilephone,
   phone,
   home_phone,
   birthday,
   addrisn,
   cityisn,
   regionisn,
   sto_priority,
   juridical,
   subj_classisn,
   sms_phone,
   deny_info_sms,
   deny_promo_sms,
   deny_info_email,
   deny_promo_email,
   agentcategoryisn,
   bestaddrisn,
   bestaddr,
   mainokvedisn,
   clientisarrested,
   deny_info_post,
   deny_info_call,
   monitoringisn,
   monitoringbeg,
   monitoringend,
   monitoringupd )
as
(
    with tt as (select isn from tt_rowid)
    select --+ ordered use_nl(s sj optaddr)
          s.subjisn,
          s.home_addrisn,
          s.home_cityisn,
          s.home_regionisn,
          s.home_zip,
          s.post_addrisn,
          s.post_cityisn,
          s.post_regionisn,
          s.temporary_addrisn,
          s.temporary_cityisn,
          s.temporary_regionisn,
          s.passport_addrisn,
          s.passport_cityisn,
          s.passport_regionisn,
          s.passport_zip,
          s.passport_address,
          s.fact_addrisn,
          s.fact_cityisn,
          s.fact_regionisn,
          s.jur_addrisn,
          s.jur_cityisn,
          s.jur_regionisn,
          s.vipclassisn,
          s.subjsecuritystr,
          s.addresssecuritystr,
          s.phonesecuritystr,
          s.drivingdatebeg,
          s.agegroup,
          s.citizenship,
          s.n_kids,
          s.marriagestateisn,
          s.familystateisn,
          s.stoadayspay,
          s.driverst,
          s.motivation,
          s.no_mail,  -- запрет на информационную рассылку: y - запрещено, n - разрешено
          s.email,
          s.serv_phone,
          s.mobilephone,
          s.phone,
          s.home_phone,
          s.birthday,
          s.addrisn,
          s.cityisn,
          s.regionisn,
          s.sto_priority,
          sj.juridical,
          sj.classisn as subj_classisn,
          s.sms_phone,
          decode(sj.classisn, 497, 'n', deny_info_sms) as deny_info_sms,         -- запрет на информационное оповещение по смс
          decode(sj.classisn, 497, 'n', deny_promo_sms) as deny_promo_sms,       -- запрет на рекламное оповещение по смс
          decode(sj.classisn, 497, 'n', deny_info_email) as deny_info_email,     -- запрет на информационное оповещение по email
          decode(sj.classisn, 497, 'n', deny_promo_email) as deny_promo_email,   -- запрет на рекламное оповещение по email
          s.agentcategoryisn,    -- категория агента
          optaddr.addrisn as bestaddrisn,  -- fk(subaddr) - isn
          optaddr.subaddr as bestaddr,     -- строка адреса
          s.mainokvedisn,  -- основной оквэд fk(dicti)
          q.clientisarrested,  -- признак клиента под арестом
          decode(sj.classisn, 497, 'n', deny_info_post) as deny_info_post,     -- запрет на информационное оповещение в виде бумажной почтовой рассылки
          decode(sj.classisn, 497, 'n', deny_info_call) as deny_info_call,     -- запрет на информационное оповещение в виде звонка
          s.monitoringisn,
          s.monitoringbeg,
          s.monitoringend,
          s.monitoringupd
    from (
          select
                coalesce(o.o_subjisn, adr.adr_subjisn, ph.ph_subjisn, vip.vip_subjisn, sh.sh_subjisn) as subjisn,
                o.drivingdatebeg,
                o.agegroup,
                o.citizenship,
                o.n_kids,
                o.marriagestateisn,
                o.familystateisn,
                o.stoadayspay,
                o.driverst,
                o.motivation,
                o.sto_priority,
                o.agentcategoryisn,
                o.mainokvedisn,  -- основной оквэд
                oracompat.nvl(o.no_mail, 'n') as no_mail,  -- запрет на информационную рассылку: y - запрещено, n - разрешено (по умолчанию)
                oracompat.nvl(o.deny_info_sms, 'n') as deny_info_sms,
                oracompat.nvl(o.deny_promo_sms, 'y') as deny_promo_sms,
                oracompat.nvl(o.deny_info_email, 'n') as deny_info_email,
                oracompat.nvl(o.deny_promo_email, 'y') as deny_promo_email,
                oracompat.nvl(o.deny_info_post, 'n') as deny_info_post,
                oracompat.nvl(o.deny_info_call, 'n') as deny_info_call,
                adr.*,
                ph.*,
                vip.*,
                sh.birthday,
                o.monitoringisn,
                o.monitoringbeg,
                o.monitoringend,
                o.monitoringupd
              from
                    tt
                        left join (select --+ ordered use_nl(tt oa) use_hash(d)
                                            oa.objisn as o_subjisn,
                                            max(decode(d.isn, 2647785103, oa.vald)) as drivingdatebeg,
                                            max(decode(d.isn, 1686027703, oa.val)) as agegroup,
                                            max(decode(d.isn, 2200008503, oa.val)) as citizenship,
                                            max(decode(d.isn, 2626755703, oa.valn)) as n_kids,
                                            max(decode(d.isn, 2626755803, oa.valn)) as marriagestateisn,
                                            max(decode(d.isn, 3028738303, oa.valn, 2638580803, oa.valn)) as familystateisn,
                                            max(decode(d.isn, 1343855503, oa.valn)) as stoadayspay,
                                            max(decode(d.isn, 1686031603, oa.val)) as driverst,
                                            max(decode(d.isn, 1428587803, oa.valn)) as motivation,
                                            max(decode(d.isn, 1683459803, oa.valn)) as sto_priority,
                                            max(decode(d.isn, 2291578303, oa.valn)) as agentcategoryisn,  -- категория агента
                                            max(decode(d.isn, 3994769103, oa.valn)) as mainokvedisn,  -- основной оквэд
                                            max(decode(d.isn, 3096320703, oracompat.nvl2(oa.valn, 'y', 'n'))) as no_mail,  -- запрет на информационную рассылку (по умолчанию разрешена)
                                            max(decode(d.isn, 3096320703, decode(oa.valn, 3546162703, 'y', 'n'))) as deny_info_sms,
                                            max(decode(d.isn, 2896523903, decode(oa.valn, 3546162703, 'n', 'y'))) as deny_promo_sms,
                                            max(decode(d.isn, 3096320703, decode(oa.valn, 3546162503, 'y', 'n'))) as deny_info_email,
                                            max(decode(d.isn, 2896523903, decode(oa.valn, 3546162503, 'n', 'y'))) as deny_promo_email,
                                            max(decode(d.isn, 3096320703, decode(oa.valn, 3546162303, 'y', 'n'))) as deny_info_post,
                                            max(decode(d.isn, 3096320703, decode(oa.valn, 3546162103, 'y', 'n'))) as deny_info_call,
                                            max(decode(d.isn, 3002827403, oa.valn)) as monitoringisn,
                                            max(decode(d.isn, 3002827403, oa.datebeg)) as monitoringbeg,
                                            max(decode(d.isn, 3002827403, oa.dateend)) as monitoringend,
                                            max(decode(d.isn, 3002827403, oa.updatedby)) as monitoringupd
                                      from
                                            tt
                                                inner join ais.obj_attrib oa
                                                on tt.isn = oa.objisn
                                                inner join (select
                                                                  connect_by_root d.isn as root,
                                                                  d.isn
                                                                from dicti d
                                                                start with d.isn in (
                                                                  2647785103,              -- водительский стаж
                                                                  1686027703,              -- возрастная группа
                                                                  2200008503,              -- гражданство
                                                                  2626755703,              -- количество детей
                                                                  2626755803,              -- нахождение в браке
                                                                  3028738303, 2638580803,  -- семейное положение
                                                                  1343855503,              -- срок оплаты стоа
                                                                  1686031603,              -- водительский стаж (категории),
                                                                  1428587803,              -- мотивационная группа
                                                                  3096320703,              -- запрет на информационное оповещение - c.get('attrnoinfoflag')
                                                                  2896523903,              -- согласие на рекламное оповещение - c.get('attrinfoflag')
                                                                  1683459803,              -- приоритет при направлении
                                                                  2291578303,              -- категория агента
                                                                  3994769103,              -- основной оквэд
                                                                  3002827403               -- мониторинг
                                                                )
                                                                connect by prior d.isn = d.parentisn
                                                            ) d
                                                on oa.classisn = d.isn
                                      where oa.discr = 'c'
                                            and current_timestamp between oracompat.nvl(oa.datebeg, current_timestamp) and oracompat.nvl(oa.dateend, current_timestamp)
                                     group by oa.objisn
                                      ) o
                        on tt.isn = o.o_subjisn
                        left join ( select
                                          adr.subjisn as adr_subjisn,
                                          max(decode(adr.classisn, 471, adr.addrisn)) as home_addrisn,
                                          max(decode(adr.classisn, 471, adr.cityisn)) as home_cityisn,
                                          max(decode(adr.classisn, 471, adr.regionisn)) as home_regionisn,
                                          max(decode(adr.classisn, 471, adr.postcode)) as home_zip,
                                          max(decode(adr.classisn, 472, adr.addrisn)) as post_addrisn,
                                          max(decode(adr.classisn, 472, adr.cityisn)) as post_cityisn,
                                          max(decode(adr.classisn, 472, adr.regionisn)) as post_regionisn,
                                          max(decode(adr.classisn, 1166402903, adr.addrisn)) as temporary_addrisn,
                                          max(decode(adr.classisn, 1166402903, adr.cityisn)) as temporary_cityisn,
                                          max(decode(adr.classisn, 1166402903, adr.regionisn)) as temporary_regionisn,
                                          max(decode(adr.classisn, 11441319, adr.addrisn)) as passport_addrisn,
                                          max(decode(adr.classisn, 11441319, adr.cityisn)) as passport_cityisn,
                                          max(decode(adr.classisn, 11441319, adr.regionisn)) as passport_regionisn,
                                          max(decode(adr.classisn, 11441319, adr.postcode)) as passport_zip,
                                          max(decode(adr.classisn, 11441319, adr.address)) as passport_address,
                                          max(decode(adr.classisn, 15522816, adr.addrisn)) as fact_addrisn,
                                          max(decode(adr.classisn, 15522816, adr.cityisn)) as fact_cityisn,
                                          max(decode(adr.classisn, 15522816, adr.regionisn)) as fact_regionisn,
                                          max(decode(adr.classisn, 470, adr.addrisn)) as jur_addrisn,
                                          max(decode(adr.classisn, 470, adr.cityisn)) as jur_cityisn,
                                          max(decode(adr.classisn, 470, adr.regionisn)) as jur_regionisn,
                                          max(adr.addrisn) as addrisn,
                                          max(adr.cityisn) as cityisn,
                                          max(adr.regionisn) as regionisn
                                        from (
                                              select --+ ordered use_nl(adr cty reg)
                                                    adr.addrisn,
                                                    adr.subjisn,
                                                    adr.classisn,
                                                    adr.cityisn,
                                                    oracompat.nvl(decode(reg.parentisn, 0, null::numeric, reg.parentisn), reg.isn) as regionisn,
                                                    adr.postcode,
                                                    adr.address
                                                  from
                                                       (select --+ ordered use_nl(t adr)
                                                              adr.subjisn,
                                                              adr.classisn,
                                                              max(adr.isn) keep(dense_rank first order by adr.updated desc, adr.isn desc) as addrisn,
                                                              max(adr.cityisn) keep(dense_rank first order by adr.updated desc, adr.isn desc) as cityisn,
                                                              max(adr.postcode) keep(dense_rank first order by adr.updated desc, adr.isn desc) as postcode,
                                                              max(adr.address) keep(dense_rank first order by adr.updated desc, adr.isn desc) as address
                                                            from
                                                                  tt t,
                                                                  ais.subaddr_t adr
                                                            where
                                                                  t.isn = adr.subjisn
                                                                  and oracompat.nvl(adr.active, 's') <> 's'  -- учитываем только активные записи
                                                        group by adr.subjisn, 
                                                                  adr.classisn
                                                       ) adr
                                                            left join ais.city cty
                                                            on adr.cityisn = cty.isn
                                                            left join ais.region reg
                                                            on cty.regionisn = reg.isn
                                        ) adr
                                    group by adr.subjisn
                                  ) adr
                        on tt.isn = adr.adr_subjisn
                        left join ( select --+ ordered use_nl(t p)
                                          ph.*,
                                          decode(ph.smsphone,null, null,ais.sms.pformatphone(ph.smsphone)) as sms_phone
                                        from
                                          ( select --+ ordered use_nl(t p dx)
                                                  p.subjisn as ph_subjisn,
                                                  max(decode(p.classisn, 424, p.phone)) keep(dense_rank first order by decode(p.classisn, 424, 0, 1), p.updated desc) as email,
                                                  max(decode(p.classisn, 1482515703, p.phone)) keep(dense_rank first order by decode(p.classisn, 1482515703, 0, 1), p.updated desc) as serv_phone,
                                                  max(decode(p.classisn, 25152816, p.phone)) keep(dense_rank first order by decode(p.classisn, 25152816, 0, 1), p.updated desc) as mobilephone,
                                                  max(decode(p.classisn, 420, p.phone)) keep(dense_rank first order by decode(p.classisn, 420, 0, 1), p.updated desc) as phone,
                                                  max(decode(p.classisn, 29155416, p.phone)) keep(dense_rank first order by decode(p.classisn, 29155416, 0, 1), p.updated desc) as home_phone,
                                                  max(oracompat.nvl2(dx.classisn1, p.remark || p.phone, null)) keep(dense_rank first order by oracompat.nvl2(dx.classisn1, 0, 1), dx.classisn2, p.updated desc) as smsphone
                                                from
                                                      tt
                                                        inner join ais.subphone_t p
                                                        on tt.isn = p.subjisn
                                                        left join ais.dicx dx
                                                        on p.classisn = dx.classisn1
                                                where dx.classisn = 3378432503
                                            group by p.subjisn
                                          ) ph
                                  ) ph
                        on tt.isn = ph.ph_subjisn
                        left join ( select v.*
                                        from (
                                              select --+ ordered use_nl(t s sh sa sp) use_hash(vip)
                                                    s.isn as vip_subjisn,
                                                    max(vip.isn) as vipclassisn,
                                                    max(sh.securitystr) as subjsecuritystr,
                                                    conc(distinct sa.securitystr) as addresssecuritystr,
                                                    conc(distinct sp.securitystr) as phonesecuritystr
                                                  from
                                                        tt_rowid t
                                                            inner join ais.subject_t s
                                                            on t.isn = s.isn
                                                            left join (select isn 
                                                                            from dicti d 
                                                                            start with isn in (11634718, 2431326703) 
                                                                            connect by prior isn = parentisn) vip
                                                            on s.classisn = vip.isn
                                                            left join ais.subhuman_t sh
                                                            on s.isn = sh.isn
                                                            left join ais.subaddr_t sa
                                                            on s.isn = sa.subjisn
                                                            left join ais.subphone_t sp
                                                            on s.isn = sp.subjisn
                                              group by
                                                s.isn
                                         ) v
                                       where
                                             v.vipclassisn is not null
                                             or
                                             coalesce(v.subjsecuritystr, v.addresssecuritystr, v.phonesecuritystr) is not null
                                  ) vip
                        on tt.isn = vip.vip_subjisn
                        left join (select --+ ordered use_nl(t sh)
                                         sh.isn as sh_subjisn,
                                         sh.birthday, rownum as rn
                                       from
                                             tt_rowid t
                                                inner join ais.subhuman_t sh
                                                on t.isn = sh.isn
                                       where sh.birthday is not null   -- отбираем только записи со значащими показателями
                                  ) sh
                        on tt.isn = sh.sh_subjisn          
          ) s
            inner join ais.subject_t sj
            on s.subjisn = sj.isn
            left join storage_source.subj_best_addr optaddr
            on s.subjisn = optaddr.subjisn
            left join (select --+ ordered use_nl(t q)
                               q.objisn as subjisn,
                               max(decode(q.request, '1', 'y', 'n')) as clientisarrested  -- признак "арест"
                             from tt_rowid t
                                    inner join  ais.queue q
                                    on t.isn = q.objisn
                             where q.classisn = 1175052903   -- страхователи/полисы под арестом (new) / c.get('qeinarrestednew')
                                   and q.objisn2 is null
                                   and q.formisn = 33024916  -- юр. лицо / c.get('fmlegal')
                                   and q.status = 'w'        -- как в аис
                                   and q.request = '1'       -- пока только одно поле про арест, поэтому сразу отбираю только арестованых
                         group by q.objisn
                        ) q
            on s.subjisn = q.subjisn
);
