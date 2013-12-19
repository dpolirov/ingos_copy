CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_AGREXT" ("AGRISN", "CLASSISN", "X1", "X2", "X3", "X4", "X5") AS 
  (Select agrisn,classisn,x1,x2,x3,x4,x5
  from AgrExt
  Where AgrIsn In (Select Isn from tt_rowid)
    and x1 in (select 1283165703 isn -- лефдсмюпндмюъ опнцпюллю
               from dual
               union all
               Select Isn From Dicti
               Start With isn=1071775625 -- ярпюунбюмхе гюкнцнбнцн хлсыеярбю
               connect by prior Isn=Parentisn)
);