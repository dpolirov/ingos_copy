create or replace view v_rep_agr_salers_line (
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
              select --+ ordered use_nl(per ar d1) index(ar x_rep_agr_salers_agr)
                    per.agrisn,
                    per.db as datebeg,
                    per.de as dateend,
                    --max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',salerisn))) keep (dense_rank first order by decode(agrsalerclassisn ,1738886903,1,0)) salergoisn ,
                    first_value(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',salerisn))) over (partition by per.agrisn, per.db, per.de order by decode(agrsalerclassisn ,1738886903,1,0)) salergoisn,
                   -- max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',salerclassisn)))  keep (dense_rank first order by decode(agrsalerclassisn ,1738886903,1,0)) salergoclassisn,
                    first_value(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',salerclassisn))) over (partition by per.agrisn, per.db, per.de order by decode(agrsalerclassisn ,1738886903,1,0)) salergoclassisn,
                    --max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',deptisn)))  keep (dense_rank first order by decode(agrsalerclassisn ,1738886903,1,0)) salergodept,
                    first_value(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',deptisn))) over (partition by per.agrisn, per.db, per.de order by decode(agrsalerclassisn ,1738886903,1,0)) salergodept,
                    --max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',dept0isn)))  keep (dense_rank first order by decode(agrsalerclassisn ,1738886903,1,0))  salergodept0isn,
                    first_value(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'sales_g',dept0isn))) over (partition by per.agrisn, per.db, per.de order by decode(agrsalerclassisn ,1738886903,1,0)) salergodept0isn
                    --max(decode(agrsalerclassisn ,1738886903,salerisn)) salercrgoisn,
                    max(decode(agrsalerclassisn ,1738886903,salerisn)) over (partition by per.agrisn, per.db, per.de) salercrgoisn,
                    --max(decode(agrsalerclassisn ,1738886903,salerclassisn))salercrclassisn,
                    max(decode(agrsalerclassisn ,1738886903,salerclassisn)) over (partition by per.agrisn, per.db, per.de) salercrclassisn,
                    --max(decode(agrsalerclassisn ,1738886903,deptisn)) salercrgodept,
                    max(decode(agrsalerclassisn ,1738886903,deptisn)) over (partition by per.agrisn, per.db, per.de) salercrgodept,
                    --max(decode(agrsalerclassisn ,1738886903,dept0isn)) salercrgodept0isn,
                    max(decode(agrsalerclassisn ,1738886903,dept0isn)) over (partition by per.agrisn, per.db, per.de) salercrgodept0isn,
                    --max(decode(d1.code ,'sales_f',salerisn))salerfisn,
                    max(decode(d1.code ,'sales_f',salerisn)) over (partition by per.agrisn, per.db, per.de) salerfisn,
                    --max(decode(d1.code ,'sales_f',salerclassisn)) salerfclassisn,
                    max(decode(d1.code ,'sales_f',salerclassisn)) over (partition by per.agrisn, per.db, per.de) salerfclassisn,
                    --max(decode(d1.code ,'sales_f',deptisn)) salerfdept,
                    max(decode(d1.code ,'sales_f',deptisn)) over (partition by per.agrisn, per.db, per.de) salerfdept,
                    --max(decode(d1.code ,'sales_f',dept0isn)) salerfdept0isn
                    max(decode(d1.code ,'sales_f',dept0isn)) over (partition by per.agrisn, per.db, per.de) salerfdept0isn
                from storages.rep_agr_salers ar 
                        left join ais.dicti d1
                        on ar.agrsalerclassisn = d1.isn, 
                       (
                          select *
                              from (
                                    select
                                          d as db,
                                          lag(d-1) over(partition by agrisn order by d desc) as de,
                                          agrisn
                                        from (
                                              select distinct agrisn, datebeg as d from p
                                              union
                                              select distinct agrisn, dateend+1 as d from p
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
              group by
                          per.agrisn,
                          per.db,
                          per.de
        )
)
