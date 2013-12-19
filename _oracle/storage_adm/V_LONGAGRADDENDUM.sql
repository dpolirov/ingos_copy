
  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_LONGAGRADDENDUM" ("AGRISN", "ADDISN", "DISCR", "DATEBEG", "DATESIGN", "PREMIUMSUM", "CURRISN") AS 
  (select /*+ ALL_ROWS Ordered(a) Use_Nl(a) Index(a) */
       connect_by_root (isn) AS agrisn,
       a.isn AS addisn,
       a.discr,
       a.datebeg,
       a.datesign,
       CASE a.discr
         WHEN 'À' THEN a.premiumsum
         WHEN 'Ä' THEN (SELECT A.PremiumSum - NVL(sum(Z.PremiumSum), 0) FROM Agreement Z WHERE  Z.ParentISN = A.ISN and Z.Discr = 'À')
       END AS premiumsum,
       a.currisn
from Agreement A
start  with ISN IN (SELECT --+ ordered use_nl ( t ag )
                         Distinct   t.isn
                    FROM tt_rowid t, agreement ag
                    WHERE ag.isn=t.isn
                      AND sign(months_between (ag.DateEnd,ag.DateBeg)-13)=1
                      AND ag.discr IN ('Ä', 'Ã')
                      AND ag.classisn IN (SELECT ISN
                                          FROM DICTI D
                                          START WITH D.ISN = 34711216
                                          CONNECT BY PRIOR D.ISN = D.PARENTISN
                                         )
                   )
connect by prior A.ISN = A.PrevISN
       and Nvl(A.Discr,'Y') = 'À'
);