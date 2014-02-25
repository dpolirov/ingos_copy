create or replace view storage_adm.v_rep_agr_salers_line (
   agrisn,
   datebeg,
   dateend,
   salergoisn,
   salergoclassisn,
   salergodept,
   salergodept0isn,
   salercrgoisn,
   salercrclassisn,
   salercrgodept,
   salercrgodept0isn,
   salerfisn,
   salerfclassisn,
   salerfdept,
   salerfdept0isn)
   
as
(
    with p as (
    -- sts 24.08.2012 - вьюха по продавцам (аналог tt_agr_salers_line)
    -- используется таблица storage_adm.tt_rep_agr_salers, которая заполняется при загрузке таблицы tt_agr_salers
    -- при загрузке хранилища по логам

                  select --+ ordered use_nl(t ar) index(ar x_rep_agr_salers_agr)
                          distinct
                            agrisn,
                            -- sts - убрал nvl(), т.к. в новой версии datebeg и dateend всегда заполнены
                            datebeg,
                            dateend
                      from
                        storage_adm.tt_rowid t,
                        storages.rep_agr_salers ar
                      where t.isn = ar.agrisn
            )
    select
            agrisn,
            datebeg,
            dateend,
            salergoisn,
            salergoclassisn ,
            salergodept,
            salergodept0isn ,
            salercrgoisn,
            salercrclassisn,
            salercrgodept,
            salercrgodept0isn,
            salerfisn,
            salerfclassisn,
            salerfdept,
            salerfdept0isn
        from (
                select agrisn,
                        datebeg,
                        dateend,
                        max(salergoisn) as salergoisn,
                        max(salergoclassisn) as salergoclassisn,
                        max(salergodept) as salergodept,
                        max(salergodept0isn) as salergodept0isn,
                        max(salercrgoisn) as salercrgoisn,
                        max(salercrclassisn) as salercrclassisn,
                        max(salercrgodept) as salercrgodept,
                        max(salercrgodept0isn) as salercrgodept0isn,
                        max(salerfisn) as salerfisn,
                        max(salerfclassisn) as salerfclassisn,
                        max(salerfdept) as salerfdept,
                        max(salerfdept0isn) as salerfdept0isn
                    from (
                          select --+ ordered use_nl(per ar d1) index(ar x_rep_agr_salers_agr)
                                per.agrisn,
                                per.db as datebeg,
                                per.de as dateend,
                                --max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',salerisn))) keep (dense_rank first order by decode(agrsalerclassisn ,1738886903,1,0)) salergoisn ,
                                first_value(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',salerisn))) over (order by decode(agrsalerclassisn ,1738886903,1,0) asc) salergoisn,
                               -- max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',salerclassisn)))  keep (dense_rank first order by decode(agrsalerclassisn ,1738886903,1,0)) salergoclassisn,
                                first_value(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',salerclassisn))) over (order by decode(agrsalerclassisn ,1738886903,1,0) asc) salergoclassisn,
                                --max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',deptisn)))  keep (dense_rank first order by decode(agrsalerclassisn ,1738886903,1,0)) salergodept,
                                first_value(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',deptisn))) over (order by decode(agrsalerclassisn ,1738886903,1,0) asc) salergodept,
                                --max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',dept0isn)))  keep (dense_rank first order by decode(agrsalerclassisn ,1738886903,1,0))  salergodept0isn,
                                first_value(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',dept0isn))) over (order by decode(agrsalerclassisn ,1738886903,1,0) asc) salergodept0isn,
                                --max(decode(agrsalerclassisn ,1738886903,salerisn)) salercrgoisn,
                                decode(agrsalerclassisn ,1738886903,salerisn) salercrgoisn,
                                --max(decode(agrsalerclassisn ,1738886903,salerclassisn))salercrclassisn,
                                decode(agrsalerclassisn ,1738886903,salerclassisn) salercrclassisn,
                                --max(decode(agrsalerclassisn ,1738886903,deptisn)) salercrgodept,
                                decode(agrsalerclassisn ,1738886903,deptisn) salercrgodept,
                                --max(decode(agrsalerclassisn ,1738886903,dept0isn)) salercrgodept0isn,
                                decode(agrsalerclassisn ,1738886903,dept0isn) salercrgodept0isn,
                                --max(decode(d1.code ,'sales_f',salerisn))salerfisn,
                                decode(d1.code ,'sales_f',salerisn) salerfisn,
                                --max(decode(d1.code ,'sales_f',salerclassisn)) salerfclassisn,
                                decode(d1.code ,'sales_f',salerclassisn) salerfclassisn,
                                --max(decode(d1.code ,'sales_f',deptisn)) salerfdept,
                                decode(d1.code ,'sales_f',deptisn) salerfdept,
                                --max(decode(d1.code ,'sales_f',dept0isn)) salerfdept0isn
                                decode(d1.code ,'sales_f',dept0isn) salerfdept0isn
                            from storages.rep_agr_salers ar 
                                    left join ais.dicti d1
                                    on ar.agrsalerclassisn = d1.isn, 
                                   (
                                      select *
                                          from (
                                                select
                                                      d as db,
                                                      lag(d-interval '1 day') over(partition by agrisn order by d desc) as de,
                                                      agrisn
                                                    from (
                                                          select distinct agrisn, datebeg as d from p
                                                          union
                                                          select distinct agrisn, dateend + interval '1 day' as d from p
                                                        ) x
                                            ) x
                                      where de is not null
                                ) per
                            where per.agrisn = ar.agrisn
                              /*
                              and (
                                -- sts - убрал nvl(), т.к. в новой версии datebeg и dateend всегда заполнены
                                per.db between ar.datebeg and ar.dateend
                                or
                                ar.datebeg between per.db and per.de
                              )
                              */
                                  and per.de >= ar.datebeg and per.db <= ar.dateend
                        ) as q      
              group by
                          q.agrisn,
                          q.datebeg,
                          q.dateend
        ) as qq
);
