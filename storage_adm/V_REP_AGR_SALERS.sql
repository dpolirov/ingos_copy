create or replace view v_rep_agr_salers (
   agrisn,
   salerisn,
   agrsalerclassisn,
   deptisn,
   dept0isn,
   dept1isn,
   doisn,
   oisn,
   is_salergo,
   is_salerf,
   salerclassisn,
   datebeg,
   dateend )
as
(
    select --+ ordered use_nl(s oa) use_hash(rd)
    -- sts 24.07.2012 - вьюха по продавцам (аналог tt_agr_salers)
    -- используется нарезалка по agrisn в tt_rowid и параметры. использование неявное ч/з вьюху v_rep_agr_salers_max_dates
       --s.ploadisn,
       s.agrisn,
       s.salerisn,
       s.agrsclass as agrsalerclassisn,
       s.deptisn,
       rd.dept0isn,
       rd.dept1isn,
       rd.doisn,
       rd.oisn,
       s.is_salergo,
       s.is_salerf,
       -- мотивационная группа. приоритет тем записям, у которых наиболее заполнены даты начала и окончания
       max(oa.valn) keep (dense_rank first
                            order by case
                                   when oa.datebeg is not null and oa.dateend is not null then 2
                                   else case
                                          when oa.datebeg is not null then 1
                                          else case
                                                 when oa.dateend is not null then 1
                                                 else 0
                                               end
                                        end
                                 end desc,
                                 oracompat.nvl(oa.datebeg, s.pmindate) desc,
                                 oracompat.nvl(oa.dateend, s.pmaxdate) desc
                       ) as salerclassisn,
       s.dt as datebeg,
       s.dte as dateend
        from
             (select a.*
                  from
                   (select
                      a.*,
                      -- определяем конец интервала (как дату начала следующего интервала минус 1 день)
                      first_value(a.dt) over(partition by a.agrisn, a.salerisn, a.agrsclass
                        order by a.dt range between 1 following and unbounded following) - 1 as dte
                    from
                      ( select
                            distinct
                              --rol.ploadisn,
                              rol.pmindate,
                              rol.pmaxdate,
                              rol.salerisn,
                              rol.agrisn,
                              rol.agrsclass,
                              rol.role_datebeg,
                              rol.role_dateend,
                              rol.deptisn,
                              rol.is_salergo,
                              rol.is_salerf,
                              oa.valn,
                              oracompat.nvl(oa.datebeg, rol.pmindate) as oa_datebeg,
                              oracompat.nvl(oa.dateend, rol.pmaxdate) as oa_dateend,
                              decode(
                                n.n,
                                1, rol.role_datebeg,
                                2, rol.role_dateend + 1,
                                3, oracompat.nvl(oa.datebeg, rol.pmindate),
                                4, oracompat.nvl(oa.dateend, rol.pmaxdate) + 1
                              ) as dt
                        from
                           ( select --+ ordered use_nl(s ar)
                                     distinct
                                       --s.ploadisn,
                                       s.pmindate,
                                       s.pmaxdate,
                                       ar.agrisn,
                                       ar.subjisn as salerisn,
                                       ar.classisn as agrsclass,
                                       ar.deptisn,
                                       max(s.is_salergo) over (partition by ar.agrisn) as is_salergo,
                                       max(s.is_salerf) over (partition by ar.agrisn) as is_salerf,
                                       oracompat.nvl(oracompat.trunc(ar.datebeg), s.pmindate) as role_datebeg,
                                       oracompat.nvl(oracompat.trunc(ar.dateend), s.pmaxdate) as role_dateend
                                 from (-- самое важное место - выделяем 1-го продавца по отделу на период действия.
                                       -- в принципе, этого всего можно не делать, если данные в базе абсолютно правильные. ;-)
                                       select --+ ordered use_nl(ra ar) use_hash(slr)
                                               distinct
                                                 --ra.ploadisn,
                                                 ra.pmindate,
                                                 ra.pmaxdate,
                                                 first_value(ar.isn)
                                                   over (partition by ar.agrisn, oracompat.trunc(ar.datebeg), oracompat.trunc(ar.dateend), ar.deptisn -- od 29.10.2010 /*case when sd.shortname like  'сектор%' then sd.parentisn else sd.isn end*/
                                                     order by
                                                       (case when ar.classisn in (1738885603, 1738885903, 1738886903) then 1 else 0 end ) desc,
                                                        decode(ar.classisn, 1738886903, 1, 0), -- кросспродавец имеет преимущество перед го, когда они вместе
                                                        ar.updated
                                                   ) arisn,
                                                 decode(slr.code, 'sales_g', 'y', 'n') as is_salergo,
                                                 decode(slr.code, 'sales_f', 'y', 'n') as is_salerf
                                           from
                                                 v_rep_agr_salers_max_dates ra,
                                                 agrrole ar,
                                                (select
                                                    d.isn, d.code
                                                  from
                                                    ais.dicti d
                                                  where
                                                    d.parentisn = 402   -- субъекты договора
                                                    and d.code in ('sales_g', 'sales_f')
                                                ) slr
                                           where
                                             ra.agrisn = ar.agrisn
                                             and ar.classisn = slr.isn
                                             and ar.deptisn is not null
                                    ) s,
                                      agrrole ar
                                 where s.arisn = ar.isn
                            ) rol
                                left join obj_attrib oa
                                on rol.salerisn = oa.objisn,
                              -- формируем четыре строки, чтобы раскидать четыре даты (beg-end из ролей и атрибутов) в одну колонку
                            generate_series(1,4) n
                        where oa.classisn = 1428587803
                              and oa.discr = 'c'
                         ) a
                       ) a
                   where
                     /*
                       из полученных отрезков оставляем те роли, которые действуют на середину текущего отрезка
                       sts - на мой взгляд - не принципиально, на сколько делить, т.к. отрезки разбиты таким образом,
                       что один отрезок не может действовать на несколько ролей. видимо делят на два,
                       чтобы исключить "выпадение" отрезков на "граничных" условиях - т.к. дата берется ч/з trunc()

                       upd 24.07.2012 - а может и принципиально. вариант со строгим равенством по a.dt дает чуть меньше число записей:
                         200 944 639  -- строгое равенство
                         200 952 028  -- нестрогое равенство
                       видимо дело все таки в trunc()-аньи дат - разбираться не стал
                       a.dt between a.role_datebeg and a.role_dateend
                     */
                       a.dt + (a.dte - a.dt) / 2 between a.role_datebeg and a.role_dateend
               ) s
                left join ais.obj_attrib oa
                on s.salerisn = oa.objisn
                left join rep_dept rd
                on s.deptisn = rd.deptisn
          where oa.classisn = 1428587803
                and oa.discr = 'c'
                and s.dt between oracompat.nvl(oracompat.trunc(oa.datebeg), s.pmindate) and oracompat.nvl(oracompat.trunc(oa.dateend), s.pmaxdate)
    group by
       --s.ploadisn,
       s.agrisn,
       s.salerisn,
       s.agrsclass,
       s.deptisn,
       rd.dept0isn,
       rd.dept1isn,
       rd.doisn,
       rd.oisn,
       s.is_salergo,
       s.is_salerf,
       s.dt,
       s.dte
);
