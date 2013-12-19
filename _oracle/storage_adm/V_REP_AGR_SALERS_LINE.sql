 CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_AGR_SALERS_LINE" ("AGRISN", "DATEBEG", "DATEEND", "SALERGOISN", "SALERGOCLASSISN", "SALERGODEPT", "SALERGODEPT0ISN", "SALERCRGOISN", "SALERCRCLASSISN", "SALERCRGODEPT", "SALERCRGODEPT0ISN", "SALERFISN", "SALERFCLASSISN", "SALERFDEPT", "SALERFDEPT0ISN") AS 
  with P as (
-- sts 24.08.2012 - вьюха по продавцам (аналог TT_AGR_SALERS_LINE)
-- Используется таблица STORAGE_ADM.tt_rep_agr_salers, которая заполняется при загрузке таблицы TT_AGR_SALERS
-- при загрузке хранилища по логам

  Select --+ ordered use_nl(t ar) index(ar X_REP_AGR_SALERS_AGR)
  Distinct
    Agrisn,
    -- sts - убрал nvl(), т.к. в новой версии datebeg и dateend всегда заполнены
    datebeg,
    dateend
  from
    STORAGE_ADM.tt_rowid t,
    STORAGES.REP_AGR_SALERS ar
  where
    t.ISN = ar.AgrISN
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
  select --+ ordered use_nl(Per ar d1) index(ar X_REP_AGR_SALERS_AGR)
    Per.Agrisn,
    Per.db as datebeg,
    Per.De as dateend,
    Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',salerisn))) keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0)) salergoIsn ,
    Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',salerclassisn)))  keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0)) salergoclassisn,
    Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',deptisn)))  keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0)) salergodept,
    Max(decode(agrsalerclassisn ,1738886903,null,decode(d1.code ,'SALES_G',dept0isn)))  keep (dense_rank First order by decode(agrsalerclassisn ,1738886903,1,0))  salergodept0isn,

    Max(decode(agrsalerclassisn ,1738886903,salerisn))salercrgoisn  ,
    Max(decode(agrsalerclassisn ,1738886903,salerclassisn))salercrclassisn,
    Max(decode(agrsalerclassisn ,1738886903,deptisn)) salercrgodept,
    Max(decode(agrsalerclassisn ,1738886903,dept0isn)) salercrgodept0isn,

    Max(decode(d1.code ,'SALES_F',salerisn))salerfisn ,
    Max(decode(d1.code ,'SALES_F',salerclassisn)) salerfclassisn,
    Max(decode(d1.code ,'SALES_F',deptisn)) salerfdept,
    Max(decode(d1.code ,'SALES_F',dept0isn)) salerfdept0isn
from (
  select
    *
  from (
    select
      d as db,
      lag(d-1) over(partition by Agrisn order by d desc) as de,
      Agrisn
    from (
      select distinct agrisn, datebeg as d from P
      union
      select distinct agrisn, dateend+1 as d from P
    ) X
  ) X
  where
    de is not null
) Per,
  STORAGES.REP_AGR_SALERS ar,
  AIS.dicti d1
where
  Per.agrisn = ar.agrisn
  /*
  and (
    -- sts - убрал nvl(), т.к. в новой версии datebeg и dateend всегда заполнены
    Per.db between ar.datebeg and ar.dateend
    or
    ar.datebeg between Per.db and Per.de
  )
  */
  and Per.DE >= ar.DateBeg and Per.DB <= ar.DateEnd
  and ar.AGRSALERCLASSISN = d1.Isn(+)
group by
  Per.Agrisn,
  Per.db,
  Per.De
);