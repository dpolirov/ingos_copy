create or replace view v_rep_subject (
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
            decode(s.licenseno,null,
             (select min(extid )from ais.subdoc sd where sd.subjisn = s.isn and classisn in (1735713203,974582025)),
             s.licenseno) licenseno,
            decode(s.licensedate,null,
             (select trunc( min(signed ) ) from ais.subdoc sd where sd.subjisn=s.isn and classisn in (1735713203,974582025)),
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
                when s.juridical = 'y' and s.resident = 'y' then
                    (case
         /*если филиал*/when s.parentsubj is not null and s.inn = psb.inn then
                            (case /*если филиал и головная ликвидированна*/
                                when
                                  (select max(isn) 
                                    from obj_attrib oa where oa.objisn = cast(s.parentsubj as numeric) and oa.classisn = c.get('attrliquidation') and dateend <= current_timestamp) is not null
    /*ais.u.getname(ais.utl.getnumberattrib( to_number(s.parentsubj),'liquidation','c',date # sq(prompt ('отчетная дата'; 'date'))# )) is not null*/
                                 then oracompat.nvl(ais.reins_utils.getsuccessor(to_number(s.parentsubj),current_timestamp ), cast(s.parentsubj as numeric))
                               else
                                cast(s.parentsubj as numeric) end
                            )
     /*если ликвидация*/when
    /* ais.u.getname(ais.utl.getnumberattrib(s.subjisn,'liquidation','c',date #sq( prompt ('отчетная дата'; 'date'))# )) is not null  */
                            (select max(isn) 
                                from obj_attrib oa where oa.objisn = s.isn and oa.classisn = c.get('attrliquidation') and dateend <= current_timestamp) is not null
                        then oracompat.nvl(ais.reins_utils.getsuccessor(s.isn,current_timestamp ),s.isn)
                        else
                          s.isn 
                     end)
    /* не резиденты : совпадают регномера и страна регистрации */
                when  s.resident = 'n' and s.parentsubj is not null and s.regnm = (select max(o.val) keep (dense_rank last order by oracompat.nvl(dateend,'01-jan-3000')) 
                                                                                        from ais.obj_attrib o where o.objisn = psb.isn and o.classisn = 1813887503)
                     and s.countryisn = psb.countryisn
                then cast(s.parentsubj as numeric)
                else s.isn
             end
            ) repsynkisn
        from (
                select  --+ ordered use_nl(sc) use_merge(sa)
                         sb.*,
                        (select max(o.val) keep (dense_rank last order by oracompat.nvl(dateend,'01-jan-3000')) 
                            from ais.obj_attrib o where o.objisn = sb.isn and o.classisn = 1813887503) regnm,
                        (select max(o.dateend) 
                            from ais.obj_attrib o where o.objisn = sb.isn and o.classisn = 1813887503) regnmdtend,
                         sc.shortname sbclass,
                        (select oracompat.nvl(min(oracompat.nvl(dateend,decode(isn,null,null,'01-jan-3000'))) ,'01-jan-1900')
                            from ais.subdoc where subjisn = sb.isn and classisn = 1735713203) relicend,
                        (select oracompat.nvl(min(oracompat.nvl(dateend,decode(isn,null,null,'01-jan-3000'))) ,'01-jan-1900')
                            from ais.subdoc where subjisn = sb.isn and classisn = 974582025) licend,
                         case  when resident = 'n' then oracompat.nvl(namelat,sb.fullname) else sb.fullname end nrezname,
                        (select max(o.val) 
                            from ais.obj_attrib o where o.objisn = sb.isn and o.classisn = 2453825203) likvidstatus,
                          (select max(o.dateend) 
                            from ais.obj_attrib o where o.objisn = sb.isn and o.classisn = 2453825203) likvidstatusdtend,
                         (select max(o.val) keep (dense_rank first order by oracompat.nvl(datebeg,'01-jan-1900') desc) 
                            from ais.obj_attrib o where o.objisn = sb.isn and o.classisn = 1979965303) r_best,
                         (select max(o.val) keep (dense_rank first order by oracompat.nvl(datebeg,'01-jan-1900') desc)
                            from ais.obj_attrib o where o.objisn = sb.isn and o.classisn = 1979962403) r_fitch,
                         (select max(o.val) keep (dense_rank first order by oracompat.nvl(datebeg,'01-jan-1900') desc)
                            from ais.obj_attrib o where o.objisn = sb.isn and o.classisn = 1979960103) r_moodys,
                         (select max(o.val) keep (dense_rank first order by oracompat.nvl(datebeg,'01-jan-1900') desc)
                            from ais.obj_attrib o where o.objisn = sb.isn and o.classisn = 1979958603) r_sp,
                         (select max(o.val)keep (dense_rank first order by oracompat.nvl(datebeg,'01-jan-1900') desc) 
                            from ais.obj_attrib o where o.objisn = sb.isn and o.classisn = 1979966903) r_weiss,    
                        (select max(o.val)keep (dense_rank first order by oracompat.nvl(datebeg,'01-jan-1900') desc) 
                            from ais.obj_attrib o where o.objisn=sb.isn and o.classisn = 3019835703) valaam_name,
                        (select max(so.humanisn)keep (dense_rank first order by so.updated)
                            from subowner so, storage_source.rep_dept rd 
                            where so.subjisn = sb.isn and so.deptisn = rd.deptisn
                                  and (rd.dept1isn = 3381054603 or rd.oisn = 1746865203 or rd.dept2isn in(3381054003,1393203203))
                        ) crisn,
                        (select max(so.deptisn)keep(dense_rank first order by so.updated)
                            from subowner so, storage_source.rep_dept rd
                            where so.subjisn = sb.isn and so.deptisn = rd.deptisn
                                and (rd.dept1isn = 3381054603 or rd.oisn = 1746865203 or rd.dept2isn in(3381054003,1393203203))
                        ) crdeptisn,
                        (select max(o.valn)keep (dense_rank first order by oracompat.nvl(datebeg,'01-jan-1900') desc) 
                            from ais.obj_attrib o where o.objisn = sb.isn and o.classisn = 1693932103) dealer,
                         sa.addrcode,
                         sa.addrtype,
                         sa.cityisn,
                         sa.postcode,
                         sa.address,
                         decode(sb.parentisn,null,null,
                                    (select /*+ optimizer_features_enable('11.1.0.6') */
                                            isn
                                         from ais.subject_t ssb
                                          --where parentisn is null
                                          where connect_by_isleaf = 1
                                         start with ssb.isn = sb.parentisn
                                         connect by prior ssb.parentisn = ssb.isn and oracompat.nvl(ssb.resident,'m') = oracompat.nvl(sb.resident,'m')
                                    )) parentsubj
                    from tt_rowid t
                            inner join ais.subject_t sb
                            on t.isn = sb.isn
                            left join ais.dicti sc
                            on sc.isn = sb.classisn
                            left join (select subjisn,
                                                max(dc.code) keep (dense_rank first  order by decode(cityisn,null,1,0), dc.code,sa.isn) addrcode,
                                                max(dc.shortname) keep (dense_rank first  order by decode(cityisn,null,1,0), dc.code,sa.isn) addrtype,
                                                max(countryisn) keep (dense_rank first  order by decode(cityisn,null,1,0), dc.code,sa.isn) countryisn,
                                                max(cityisn)keep (dense_rank first  order by decode(cityisn,null,1,0), dc.code,sa.isn) cityisn,
                                                max(postcode) keep (dense_rank first  order by decode(cityisn,null,1,0), dc.code,sa.isn) postcode,
                                                max(address) keep (dense_rank first  order by decode(cityisn,null,1,0), dc.code,sa.isn) address
                                            from tt_rowid t,
                                                ais.subaddr_t sa,
                                                dicti dc
                                            where sa.classisn = dc.isn
                                            and t.isn = sa.subjisn
                                        group by subjisn
                                        ) sa 
                            on sb.isn = sa.subjisn
         ) s
            left join ais.subject_t psb
            on s.parentsubj = psb.isn
            left join subbank sbb
            on s.isn = sbb.isn
            left join dicti psbcl
            on psb.classisn = psbcl.isn
            left join ais.subject_t updby
            on s.updatedby = updby.isn
            left join ais.subject_t cr
            on s.crisn = cr.isn
 );
