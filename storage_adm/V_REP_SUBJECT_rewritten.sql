create or replace view storage_adm.v_rep_subject (
   subjisn,
   classisn,
   roleclassisn,
   countryisn,
   branchisn,
   juridical,
   resident,
   vip,
   inn,
   id,
   fid,
   code,
   shortname,
   fullname,
   active,
   updated,
   updatedby,
   licenseno,
   licensedate,
   okpo,
   okohx,
   synisn,
   createdby,
   created,
   profittaxflag,
   parentisn,
   namelat,
   orgformisn,
   remark,
   kpp,
   searchname,
   securitylevel,
   ogrn,
   okved,
   securitystr,
   regnm,
   sbclass,
   relicend,
   licend,
   nrezname,
   likvidstatus,
   r_best,
   r_fitch,
   r_moodys,
   r_sp,
   r_weiss,
   addrcode,
   addrtype,
   cityisn,
   postcode,
   address,
   parentsubj,
   parentfullname,
   parentclass,
   parentinn,
   updatedby_name,
   bnk_vkey,
   bnk_vkeydel,
   bnk_active,
   valaam_name,
   regnmdtend,
   likvidstatusdtend,
   curator,
   curatordeptisn,
   dealer,
   repsynkisn )
as
(
    select --+ ordered use_nl(psb sbb psbcl updby cr)  optimizer_features_enable('11.1.0.6') 
            s.isn subjisn ,s.classisn,s.roleclassisn,s.countryisn,s.branchisn,
            s.juridical,s.resident,s.vip,s.inn,s.id,s.fid,s.code,
            s.shortname,s.fullname,s.active,s.updated,s.updatedby,
            decode(s.licenseno::varchar,null,
             (select min(extid ) from ais.subdoc sd where sd.subjisn = s.isn and classisn in (1735713203,974582025))::varchar,
             s.licenseno::varchar) licenseno,
            decode(s.licensedate,null,
             (select oracompat.trunc( min(signed )::date) from ais.subdoc sd where sd.subjisn=s.isn and classisn in (1735713203,974582025)),
            s.licensedate)  licensedate,
            s.okpo,s.okohx,s.synisn,
            s.createdby,s.created,
            s.profittaxflag,s.parentisn,
            s.namelat,s.orgformisn,s.remark,s.kpp,
            s.searchname,s.securitylevel,s.ogrn,
            s.okved,s.securitystr,s.regnm,s.sbclass,
            s.relicend,s.licend,s.nrezname,s.likvidstatus,
            s.r_best,s.r_fitch,s.r_moodys,s.r_sp,s.r_weiss,
            s.addrcode,s.addrtype,s.cityisn,s.postcode,
            s.address,s.parentsubj,
            psb.fullname parentfullname,
            psbcl.shortname parentclass,
            psb.inn parentinn,
            updby.fullname updatedby_name,
            sbb.vkey bnk_vkey,
            sbb.vkeydel bnk_vkeydel,
            sbb.active  bnk_active,
            valaam_name,
            regnmdtend,likvidstatusdtend,
            cr.shortname curator,
            s.crdeptisn curatordeptisn,
            s.dealer,
            (
             case
                when s.juridical = 'Y' and s.resident = 'Y' then
                    (case
         /*если филиал*/when s.parentsubj is not null and s.inn = psb.inn then
                            (case /*если филиал и головная ликвидированна*/
                                when
                                  (select max(isn) 
                                    from ais.obj_attrib oa where oa.objisn = cast(s.parentsubj as numeric) and oa.classisn = shared_system.get('attrliquidation') and dateend <= current_timestamp) is not null
    /*ais.u.getname(ais.utl.getnumberattrib( to_number(s.parentsubj),'liquidation','c',date # sq(prompt ('отчетная дата'; 'date'))# )) is not null*/
                                 then oracompat.nvl(shared_system.reins_utils_getsuccessor(s.parentsubj::numeric,current_timestamp::timestamp ), cast(s.parentsubj as numeric))
                               else
                                cast(s.parentsubj as numeric) end
                            )
     /*если ликвидация*/when
    /* ais.u.getname(ais.utl.getnumberattrib(s.subjisn,'liquidation','c',date #sq( prompt ('отчетная дата'; 'date'))# )) is not null  */
                            (select max(isn) 
                                from ais.obj_attrib oa where oa.objisn = s.isn and oa.classisn = shared_system.get('attrliquidation') and dateend <= current_timestamp) is not null
                        then oracompat.nvl(shared_system.reins_utils_getsuccessor(s.isn,current_timestamp::timestamp),s.isn)
                        else
                          s.isn 
                     end)
    /* не резиденты : совпадают регномера и страна регистрации */
                when  s.resident = 'n' and s.parentsubj is not null and s.regnm = (select distinct first_value(o.val) over (order by oracompat.nvl(dateend::date,to_date('01-01-3000','dd-mm-yyyy')) asc) 
                                                                                        from ais.obj_attrib o where o.objisn = psb.isn and o.classisn = 1813887503)
                     and s.countryisn = psb.countryisn
                then cast(s.parentsubj as numeric)
                else s.isn
             end
            ) repsynkisn
        from (
                select  --+ ordered use_nl(sc) use_merge(sa)
                         sb.*,
                        o.regnm,
                        o.regnmdtend,
                        sc.shortname sbclass,
                        (select oracompat.nvl(min(oracompat.nvl(dateend::date,decode(isn,null,null,to_date('01-01-3000','dd-mm-yyyy')))) ,to_date('01-01-1900','dd-mm-yyyy'))
                            from ais.subdoc where subjisn = sb.isn and classisn = 1735713203) relicend,
                        (select oracompat.nvl(min(oracompat.nvl(dateend::date,decode(isn,null,null,to_date('01-01-3000','dd-mm-yyyy')))) ,to_date('01-01-1900','dd-mm-yyyy'))
                            from ais.subdoc where subjisn = sb.isn and classisn = 974582025) licend,
                         case  when resident = 'n' 
                            then oracompat.nvl(namelat,sb.fullname) 
                            else sb.fullname 
                         end nrezname,
                        o.likvidstatus,
                        o.likvidstatusdtend,
                        o.r_best,
                        o.r_fitch,
                        o.r_moodys,
                        o.r_sp,
                        o.r_weiss,    
                        o.valaam_name,
                        (select distinct first_value(so.humanisn) over (order by so.updated asc, so.humanisn asc)
                            from ais.subowner so, storage_source.rep_dept rd 
                            where so.subjisn = sb.isn and so.deptisn = rd.deptisn
                                  and (rd.dept1isn = 3381054603 or rd.oisn = 1746865203 or rd.dept2isn in(3381054003,1393203203))
                        ) crisn,
                        (select distinct first_value(so.deptisn) over (order by so.updated asc, so.deptisn asc)
                            from ais.subowner so, storage_source.rep_dept rd
                            where so.subjisn = sb.isn and so.deptisn = rd.deptisn
                                and (rd.dept1isn = 3381054603 or rd.oisn = 1746865203 or rd.dept2isn in(3381054003,1393203203))
                        ) crdeptisn,
                         o.dealer,
                         sa.addrcode,
                         sa.addrtype,
                         sa.cityisn,
                         sa.postcode,
                         sa.address,
                         decode(sb.parentisn,null,null, t1.isn) parentsubj
                    from storage_adm.tt_rowid t
                            inner join ais.subject_t sb
                            on t.isn = sb.isn
                            left join (select t1.isn
                                            from (
                                                    select ssb.isn,
                                                           unnest(ssb.__hier) as unh,
                                                           ssb.resident,
                                                           ssb.parentisn
                                                     from ais.subject_t_nh ssb
                                                ) t1
                                                inner join (
                                                             select sb.parentisn,
                                                                    sb.resident
                                                                from ais.subject_t sb) t2
                                                on t1.unh = t2.parentisn
                                            where oracompat.nvl(t1.resident,'m') = oracompat.nvl(t2.resident,'m')
                                                and t1.parentisn is null
                                        ) as t1
                            on t1.isn = sb.isn            
                            left join ais.dicti sc
                            on sc.isn = sb.classisn
                            left join ( select sa.subjisn, 
                                                max(sa.addrcode) as addrcode,
                                                max(sa.addrtype) as addrtype,
                                                max(sa.countryisn) as countryisn,
                                                max(sa.cityisn) as cityisn,
                                                max(sa.postcode) as postcode,
                                                max(sa.address) as address
                                            from    
                                                (select distinct subjisn,
                                                                    first_value(dc.code) over (order by decode(cityisn,null,1,0) asc, dc.code asc,sa.isn asc) addrcode,
                                                                    first_value(dc.shortname) over (order by decode(cityisn,null,1,0) asc, dc.code asc,sa.isn asc) addrtype,
                                                                    first_value(countryisn) over (order by decode(cityisn,null,1,0) asc, dc.code asc,sa.isn asc) countryisn,
                                                                    first_value(cityisn)over (order by decode(cityisn,null,1,0) asc, dc.code asc,sa.isn asc) cityisn,
                                                                    first_value(postcode) over (order by decode(cityisn,null,1,0) asc, dc.code asc,sa.isn asc) postcode,
                                                                    first_value(address) over (order by decode(cityisn,null,1,0) asc, dc.code asc,sa.isn asc) address
                                                                from storage_adm.tt_rowid t,
                                                                    ais.subaddr_t sa,
                                                                    ais.dicti dc
                                                                where sa.classisn = dc.isn
                                                                and t.isn = sa.subjisn
                                                ) as sa
                                          group by subjisn
                                    ) sa 
                            on sb.isn = sa.subjisn
                            left join ( select a2.objisn,
                                            max(decode(a2.classisn,1813887503,a2.val)) as regnm,
                                            max(decode(a2.classisn,1813887503,a.maxdateend)) as regnmdtend,
                                            max(decode(a2.classisn,2453825203,a.maxval)) as likvidstatus,
                                            max(decode(a2.classisn,2453825203,a.maxdateend)) as likvidstatusdtend,
                                            max(decode(a2.classisn,1979965303,a2.val)) as r_best,
                                            max(decode(a2.classisn,1979962403,a2.val)) as r_fitch,
                                            max(decode(a2.classisn,1979960103,a2.val)) as r_moodys,
                                            max(decode(a2.classisn,1979958603,a2.val)) as r_sp,
                                            max(decode(a2.classisn,1979966903,a2.val)) as r_weiss,
                                            max(decode(a2.classisn,3019835703,a2.val)) as valaam_name,
                                            max(decode(a2.classisn,1693932103,a2.valn)) as dealer
                                        from  (select   max(coalesce(datebeg, '01-01-1900'))lastdate, 
                                                        max(val) maxval, 
                                                        max(dateend) maxdateend, 
                                                        objisn,
                                                        classisn
                                                    from ais.obj_attrib 
                                                    where classisn in (
                                                    1813887503, 2453825203, 1979965303, 
                                                    1979962403, 1979960103, 1979958603,
                                                    1979966903, 3019835703, 1693932103)
                                                    group by objisn,classisn
                                                ) a 
                                                inner join ais.obj_attrib a2
                                                    on a.objisn=a2.objisn and a.classisn=a2.classisn and coalesce(a2.datebeg, '01-01-1900')=a.lastdate
                                                group by a2.objisn
                                       ) o
                            on o.objisn = sb.isn

         ) s
            left join ais.subject_t psb
            on s.parentsubj = psb.isn
            left join ais.subbank sbb
            on s.isn = sbb.isn
            left join ais.dicti psbcl
            on psb.classisn = psbcl.isn
            left join ais.subject_t updby
            on s.updatedby = updby.isn
            left join ais.subject_t cr
            on s.crisn = cr.isn
 );
