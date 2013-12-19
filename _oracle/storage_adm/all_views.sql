  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_AGREXT" ("AGRISN", "CLASSISN", "X1", "X2", "X3", "X4", "X5") AS 
  (Select agrisn,classisn,x1,x2,x3,x4,x5
  from AgrExt
  Where AgrIsn In (Select Isn from tt_rowid)
    and x1 in (select 1283165703 isn -- МЕЖДУНАРОДНАЯ ПРОГРАММА
               from dual
               union all
               Select Isn From Dicti
               Start With isn=1071775625 -- СТРАХОВАНИЕ ЗАЛОГОВОГО ИМУЩЕСТВА
               connect by prior Isn=Parentisn)
)

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPAGRROLEAGR" ("AGRISN", "AGENTISN", "SALERGOISN", "SALERFISN", "CROSSALERISN", "CARDEALERISN", "BROKERISN", "PARTNERISN", "LEASEFICIARYISN", "PAWNBROKERISN", "RECOMMENDERISN", "IRECOMMENDERISN", "EMITISN", "BEMITISN", "AGENTDEPTISN", "AGENTJURIDICAL", "AGENTCOLLECTFLG", "AGENTCOUNT", "EMITCOUNT", "FILCOMMISION", "TRANSFERCOMISSION", "BENEFICIARYISN", "BROKERDEPTISN", "BROKERJURIDICAL", "BROKERCOLLECTFLG", "BROKERCOUNT", "HEADCLIENT", "AGENTSHAREPC", "BROKERSHAREPC", "REINCLIENTISN", "SALERGODEPTISN", "SALERFDEPTISN", "MANAGERKKISN", "EMPOPGOISN", "EMPOPGODEPTISN", "UPRISN", "EMPOPERU", "SALERGOCLASSISN", "SALERFCLASSISN", "CROSSALERDEPTISN", "AVTODILLERISN", "ADMCURATORISN", "DOCTORCURATORISN", "UNDERWRITERISN", "UNDERWRITEROLDISN", "REPRESENTATIVEISN", "CROSSALERFISN", "CROSSALERFDEPTISN", "AGENT_MAXCOMISSION_ISN", "CONTRACTORISN", "CONTRCOMISSION", "CONTRCOUNT") AS 
  select R.AGRISN,
       -- sts 20.09.2011 - добавил nvl() на даты и привел сортировку связанных полей к одному виду
       -- нпр, поля агента имели разную сортировку для AGENTISN и AGENTJURIDICAL
       -- kgs агентам и брокерам и их аттрибутам поставил FIST, R.DATEBEG asc

       max(decode(R.CLASSISN, 437, R.SUBJISN))keep(dense_rank Last order by decode(R.CLASSISN, 437, 1, 0), R.CALCFLG, R.ORDERNO,R.DATEBEG Desc, R.ISN desc) AGENTISN,  -- агент
       max(decode(R.CODE, 'SALES_G', R.SUBJISN))keep(dense_rank last order by decode(R.CODE, 'SALES_G', 1, 0), /*sts 10.10.2012 - флаг не учитывать (by Гоша) R.CALCFLG, */ R.DATEEND, R.ISN) SALERGOISN,                -- продавец головного офиса
       max(decode(R.CODE, 'SALES_F', R.SUBJISN))keep(dense_rank last order by decode(R.CODE, 'SALES_F', 1, 0), /* R.CALCFLG, */ R.DATEEND, R.ISN) SALERFISN,                 -- продавец филиала
       max(decode(R.CLASSISN, 1738886903, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 1738886903, 1, 0), /* R.CALCFLG, */ R.DATEEND, R.ISN) CROSSALERISN,    -- кросс-продавец головного офиса
       max(decode(R.CLASSISN, 731194000, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 731194000, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) CARDEALERISN,      -- автосалон
       max(decode(R.CLASSISN, 438, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 438, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) BROKERISN,                     -- брокер
       max(decode(R.CLASSISN, 13381416, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 13381416, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) PARTNERISN,          -- партнер
       max(decode(R.CLASSISN, 682566316, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 682566316, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) LEASEFICIARYISN,   -- лизингополучатель
       max(decode(R.CLASSISN, 1064403825, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 1064403825, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) PAWNBROKERISN,   -- залогодержатель
       max(decode(R.CLASSISN, 47228116, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 47228116, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) RECOMMENDERISN,      -- рекомендатель
       max(decode(R.CLASSISN, 1574889603, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 1574889603, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) IRECOMMENDERISN, -- внутренний рекомендатель
       max(decode(R.CLASSISN, 13157916, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 13157916, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) EMITISN,             -- эмитент полиса
       max(decode(R.CLASSISN, 1617366603, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 1617366603, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) BEMITISN,        -- эмитент бизнеса
       max(decode(R.CLASSISN, 437, R.ROLEDEPTISN))keep (dense_rank Last order by decode(R.CLASSISN, 437, 1, 0), R.CALCFLG, R.ORDERNO,R.DATEBEG Desc, R.ISN desc) AGENTDEPTISN,
       max(decode(R.CLASSISN, 437, R.JURIDICAL))keep (dense_rank Last order by decode(R.CLASSISN, 437, 1, 0), R.CALCFLG, R.ORDERNO,R.DATEBEG Desc, R.ISN desc) AGENTJURIDICAL,
       max(decode(R.CLASSISN, 437, R.COLLECTFLG))keep (dense_rank Last order by decode(R.CLASSISN, 437, 1, 0), R.CALCFLG, R.ORDERNO,R.DATEBEG Desc, R.ISN desc) AGENTCOLLECTFLG,
       count(decode(R.CLASSISN, 437, R.SUBJISN)) AGENTCOUNT,
       count(decode(R.CLASSISN, 13157916, R.SUBJISN)) EMITCOUNT,
       min(greatest(nvl(decode(R.CLASSISN, 13157916/*c.get('emittent')*/, R.SHAREPC), 0),
                    nvl(decode(R.CLASSISN, 1617366603/*bemittent*/, R.SHAREPC), 0))) FILCOMMISION,
       min(decode(R.CLASSISN, 1738886903, R.SHAREPC)) TRANSFERCOMISSION,
       /* sts 12.07.2012 task(34327332503)
       count(decode(R.CLASSISN, 430, 1)) INSURANTCOUNT,
       */
       max(decode(R.CLASSISN, 433, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 433, 1, 0), R.CALCFLG,R.DATEBEG asc, R.ISN desc) BENEFICIARYISN,            -- выгодоприобретатель
       max(decode(R.CLASSISN, 438, R.ROLEDEPTISN))keep(dense_rank last order by decode(R.CLASSISN, 438, 1, 0), R.CALCFLG, R.DATEBEG asc, R.ISN) BROKERDEPTISN,
       max(decode(R.CLASSISN, 438, R.JURIDICAL))keep(dense_rank last order by decode(R.CLASSISN, 438, 1, 0), R.CALCFLG, R.DATEBEG asc, R.ISN) BROKERJURIDICAL,
       max(decode(R.CLASSISN, 438, R.COLLECTFLG))keep(dense_rank last order by decode(R.CLASSISN, 438, 1, 0), R.CALCFLG, R.DATEBEG asc , R.ISN) BROKERCOLLECTFLG,
       count(decode(R.CLASSISN, 438, R.SUBJISN)) BROKERCOUNT,
       max(decode(R.CLASSISN, 2616961403, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 2616961403, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) HEADCLIENT,       -- головной клиент
       max(decode(R.CLASSISN, 437, R.SHAREPC))keep (dense_rank Last order by decode(R.CLASSISN, 437, 1, 0), R.CALCFLG, R.ORDERNO, R.DATEBEG Desc, R.ISN desc) AGENTSHAREPC,
       max(decode(R.CLASSISN, 438, R.SHAREPC))keep(dense_rank last order by decode(R.CLASSISN, 438, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) BROKERSHAREPC,
       Max(Decode(classisn,434,SubjIsn,435,SubjIsn,null)) keep(dense_rank last order by Decode(classisn,434,1,435,1,0), Decode(SUBJCLASSISN,12212016,0,658813916,0,1), Decode(CalcFlg,'Y',1,0),Nvl(R.SHAREPC,0),SubjIsn) REINCLIENTISN, -- перестрахователь или перестраховщик из участников , брокеров не в приоритет
       -- sts 20.09.2011 - добавил подразделения продавцов
       max(decode(R.CODE, 'SALES_G', R.ROLEDEPTISN))keep(dense_rank last order by decode(R.CODE, 'SALES_G', 1, 0), /* R.CALCFLG, */ R.DATEEND, R.ISN) SALERGODEPTISN,                -- подразделение продавца головного офиса
       max(decode(R.CODE, 'SALES_F', R.ROLEDEPTISN))keep(dense_rank last order by decode(R.CODE, 'SALES_F', 1, 0), /* R.CALCFLG, */ R.DATEEND, R.ISN) SALERFDEPTISN,                  -- подразделение продавца филиала
       max(decode(R.CLASSISN, 1943199903, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 1943199903, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) MANAGERKKISN,          --распорядитель кк(комерч. кредит)
       -- {EGAO 13.04.2012
       max(decode(R.CLASSISN, 2846444203, R.SUBJISN))keep(dense_rank last order by  decode(R.CLASSISN, 2846444203, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) EMPOPGOISN,  --сотрудник оп го
       max(decode(R.CLASSISN, 2846444203, R.ROLEDEPTISN))keep(dense_rank last order by  decode(R.CLASSISN, 2846444203, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) EMPOPGODEPTISN,
       --}
       --{EGAO 17.04.2012
       MAX(CASE WHEN r.code='SALES_G' OR R.CLASSISN=2846444203 THEN uprisn END) KEEP (dense_rank LAST ORDER BY CASE WHEN r.code='SALES_G' THEN 3 WHEN R.CLASSISN=2846444203 THEN 2 ELSE 1 END, decode(r.code, 'SALES_G', 'Y', R.CALCFLG), R.DATEEND, R.ISN) uprisn, -- EGAO 17.04.2012 Виды аналитики "Подразделения Ингосстрах"
       /*!!! Ахтунг, смотри комментарии сверху*/
       --}
       -- EGAO 21.05.2012 в рамках ДИТ-12-2-167253
       MAX(decode(r.classisn,1101648603,r.subjisn))KEEP(dense_rank LAST ORDER BY decode(r.classisn,1101648603,1,0), r.calcflg, r.dateend, r.isn) AS empoperu,
       -- sts 20.09.2012 - добавил роли продавцов
       max(decode(R.CODE, 'SALES_G', R.CLASSISN))keep(dense_rank last order by decode(R.CODE, 'SALES_G', 1, 0), /* R.CALCFLG, */ R.DATEEND, R.ISN) as SALERGOCLASSISN,        -- класс (роль в договоре) продавца головного офиса
       max(decode(R.CODE, 'SALES_F', R.CLASSISN))keep(dense_rank last order by decode(R.CODE, 'SALES_F', 1, 0), /* R.CALCFLG, */ R.DATEEND, R.ISN) as SALERFCLASSISN ,         -- класс (роль в договоре) продавца филиала
       max(decode(R.CLASSISN, 1738886903, R.ROLEDEPTISN))keep(dense_rank last order by decode(R.CLASSISN, 1738886903, 1, 0), /* R.CALCFLG, */ R.DATEEND, R.ISN) as CROSSALERDEPTISN,
       -- sts 07.12.2012 - добавил роль автодиллер
       max(decode(R.CLASSISN, 3081540003, R.SUBJISN)) keep(dense_rank last order by decode(R.CLASSISN, 3081540003, 1, 0), R.DATEEND, R.ISN) as AvtoDillerISN,   -- АВТОДИЛЕР (CarDealerISN уже есть выше как "Автосалон", поэтому AvtoDillerISN)
       -- sts 07.12.2012 - task(40524747403)
       max(decode(R.CLASSISN, 2626553403, R.SUBJISN)) keep(dense_rank last order by decode(R.CLASSISN, 2626553403, 1, 0), R.DATEEND, R.ISN) as AdmCuratorISN,   -- Административный куратор
       max(decode(R.CLASSISN, 693962316, R.SUBJISN)) keep(dense_rank last order by decode(R.CLASSISN, 693962316, 1, 0), R.DATEEND, R.ISN) as DoctorCuratorISN,   -- Врач-куратор
       max(decode(R.CLASSISN, 693962016, R.SUBJISN)) keep(dense_rank last order by decode(R.CLASSISN, 693962016, 1, 0), R.DATEEND, R.ISN) as UnderwriterISN,   -- Андеррайтер
       max(decode(R.CLASSISN, 444, R.SUBJISN)) keep(dense_rank last order by decode(R.CLASSISN, 444, 1, 0), R.DATEEND, R.ISN) as UnderwriterOldISN,   -- Андеррайтер(старый)
       -- sts 05.01.2013 - task(ДИТ-12-4-173936)
       max(decode(R.CLASSISN, 35435216, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 35435216, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) as REPRESENTATIVEISN,      -- ПРЕДСТАВИТЕЛЬ
       -- VAA 07.05.2013 ДИТ-13-2-199474
       max(decode(R.CLASSISN, 3676722703, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 3676722703, 1, 0), /* R.CALCFLG, */ R.DATEEND, R.ISN) as CROSSALERFISN,    -- кросс-продавец филиала
       max(decode(R.CLASSISN, 3676722703, R.ROLEDEPTISN))keep(dense_rank last order by decode(R.CLASSISN, 3676722703, 1, 0), /* R.CALCFLG, */ R.DATEEND, R.ISN) as CROSSALERFDEPTISN, -- подразделение кросс-продавца филиала
       -- kds (06.08.2013) task(52120579303)
       max(decode(R.CLASSISN, 437, R.SUBJISN))keep(dense_rank Last order by decode(R.CLASSISN, 437, 1, 0), R.SHAREPC, R.DATEEND, r.datebeg, r.isn) as AGENT_MAXCOMISSION_ISN,         -- агент с максимальной комиссией
       -- kds (30.09.2013) task(???)
       max(decode(R.CLASSISN, 4207938903, R.SUBJISN))keep(dense_rank last order by decode(R.CLASSISN, 4207938903, 1, 0), R.CALCFLG, R.DATEEND, R.ISN) CONTRACTORISN, -- Подрядчик
       avg(decode(R.CLASSISN, 4207938903, R.SHAREPC)) CONTRCOMISSION, -- Процент подрядчика
       count(decode(R.CLASSISN, 4207938903, 1, null)) CONTRCOUNT -- Количество подрядчиков


  from ( select --+ ordered use_nl(t r s sh) use_hash(d rdt) index(r x_repagrrole_agr) index ( SH X_SUBHUMAN )
                R.AGRISN,
                R.CLASSISN,
                R.SUBJISN,
                nvl(R.DATEBEG, '01-jan-1900') DATEBEG,
                nvl(R.DATEEND, '01-jan-3000') DATEEND,
                R.SHAREPC,
                R.SHARESUM,
                nvl(R.CALCFLG, 'N') CALCFLG,
                R.DEPTISN ROLEDEPTISN,
                D.CODE,
                R.COLLECTFLG,
                SH.DEPTISN,
                S.JURIDICAL,
                R.ISN,
                S.CLASSISN SUBJCLASSISN,
                rdt.uprisn, -- EGAO 13.04.2012
                abs(R.OrderNO) as OrderNO  -- sts 22.05.2012 - сортировка по модулю

           from tt_rowid t,
                AIS.AGRROLE R,
                SUBJECT S,
                SUBHUMAN SH,
               ( select
                         D.ISN,
                         D.CODE
                    from DICTI D
                   where D.CODE in ('SALES_G', 'SALES_F')
                     and D.ISN <> 1738886903 ) D, -- кросс-продавец головного офиса (sts - т.к. кросс продавец выше идет отдельно)
                rep_dept rdt -- EGAO 13.04.2012
          where t.ISN       = R.AGRISN
            and R.CLASSISN  = D.ISN(+)
            and R.SUBJISN   = S.ISN(+)
            and R.SUBJISN   = SH.ISN(+)
            AND rdt.deptisn(+)=r.deptisn
            ) R
 where R.CLASSISN <> 430 -- страхователь
 group by R.AGRISN


  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_AGR_SALERS" ("AGRISN", "SALERISN", "AGRSALERCLASSISN", "DEPTISN", "DEPT0ISN", "DEPT1ISN", "DOISN", "OISN", "IS_SALERGO", "IS_SALERF", "SALERCLASSISN", "DATEBEG", "DATEEND") AS 
  select --+ ordered use_nl(S oa) use_hash(RD)
-- sts 24.07.2012 - вьюха по продавцам (аналог TT_AGR_SALERS)
-- используется нарезалка по AgrISN в tt_rowid и параметры. Использование неявное ч/з вьюху V_REP_AGR_SALERS_MAX_DATES
   --S.pLoadISN,
   S.agrisn,
   S.SalerISN,
   S.AgrSClass as AGRSALERCLASSISN,
   S.deptisn,
   rd.Dept0ISN,
   rd.Dept1ISN,
   rd.DOISN,
   rd.OISN,
   S.IS_SALERGO,
   S.IS_SALERF,
   -- Мотивационная группа. Приоритет тем записям, у которых наиболее заполнены даты начала и окончания
   max(oa.valn) keep (dense_rank FIRST
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
                             nvl(oa.datebeg, s.pMinDate) desc,
                             nvl(oa.dateend, s.pMaxDate) desc
                   ) AS SALERCLASSISN,
   s.dt as datebeg,
   s.dte as dateend
from
 (select
    a.*
  from
   (select
      a.*,
      -- определяем конец интервала (как дату начала следующего интервала минус 1 день)
      first_value(a.dt) over(partition by a.agrisn, a.SalerISN, a.AgrSClass
        order by a.dt range between 1 following and unbounded following) - 1 as dte
    from
      ( select
        distinct
          --rol.pLoadISN,
          rol.pMinDate,
          rol.pMaxDate,
          rol.SalerISN,
          rol.AgrISN,
          rol.AgrSClass,
          rol.role_datebeg,
          rol.role_dateend,
          rol.DeptISN,
          rol.IS_SALERGO,
          rol.IS_SALERF,
          oa.ValN,
          nvl(oa.datebeg, rol.pMinDate) as oa_datebeg,
          nvl(oa.dateend, rol.pMaxDate) as oa_dateend,
          decode(
            n.n,
            1, rol.role_datebeg,
            2, rol.role_dateend + 1,
            3, nvl(oa.datebeg, rol.pMinDate),
            4, nvl(oa.dateend, rol.pMaxDate) + 1
          ) as dt
        from
           ( select --+ ordered use_nl(s ar)
             distinct
               --s.pLoadIsn,
               s.pMinDate,
               s.pMaxDate,
               Ar.Agrisn,
               ar.SubjISN as SalerISN,
               ar.classisn as AgrSClass,
               Ar.DeptISN,
               Max(s.IS_SALERGO) over (partition by ar.AgrISN) as IS_SALERGO,
               Max(s.IS_SALERF) over (partition by ar.AgrISN) as IS_SALERF,
               nvl(trunc(ar.datebeg), s.pMinDate) as role_datebeg,
               nvl(trunc(ar.dateend), s.pMaxDate) as role_dateend
             from (-- САМОЕ ВАЖНОЕ МЕСТО - ВЫДЕЛЯЕМ 1-го продавца по отделу на период действия.
                   -- В принципе, этого всего можно не делать, если данные в базе АБСОЛЮТНО правильные. ;-)
                   select --+ ordered use_nl(ra ar) use_hash(SLR)
                   distinct
                     --ra.PLOADISN,
                     ra.PMINDATE,
                     ra.PMAXDATE,
                     First_Value(ar.isn)
                       over (partition by Ar.Agrisn, trunc(ar.datebeg), trunc(ar.dateend), ar.DeptIsn -- OD 29.10.2010 /*CASE WHEN sD.SHORTNAME LIKE  'СЕКТОР%' THEN SD.pARENTiSN ELSE SD.ISN END*/
                         order by
                           (case when ar.Classisn in (1738885603, 1738885903, 1738886903) then 1 else 0 end ) desc,
                            decode(ar.classisn, 1738886903, 1, 0), -- кросспродавец имеет преимущество перед ГО, когда они вместе
                            ar.UPDATED
                       ) ARISN,
                     decode(SLR.CODE, 'SALES_G', 'Y', 'N') as IS_SALERGO,
                     decode(SLR.CODE, 'SALES_F', 'Y', 'N') as IS_SALERF
                   FROM
                     V_REP_AGR_SALERS_MAX_DATES ra,
                     agrrole ar,
                    ( select
                        D.ISN, D.CODE
                      from
                        AIS.DICTI D
                      where
                        D.PARENTISN = 402   -- СУБЪЕКТЫ ДОГОВОРА
                        and D.CODE in ('SALES_G', 'SALES_F')
                    ) SLR
                   WHERE
                     ra.AgrISN = ar.AgrISN
                     and ar.classisn = SLR.ISN
                     and ar.deptisn is not null
                ) s,
                  AgrRole ar
                where
                  s.ARISN = ar.isn
            ) rol,
              obj_attrib oa,
              -- формируем четыре строки, чтобы раскидать четыре даты (beg-end из ролей и атрибутов) в одну колонку
              (select rownum n from dicti where rownum <=4) n
            where
              rol.SalerISN = oa.objisn(+)
              and oa.classisn(+) = 1428587803
              and oa.discr(+) = 'C'
         ) a
       ) a
     where
     /*
       из полученных отрезков оставляем те роли, которые действуют на середину текущего отрезка
       sts - на мой взгляд - не принципиально, на сколько делить, т.к. отрезки разбиты таким образом,
       что один отрезок не может действовать на несколько ролей. Видимо делят на два,
       чтобы исключить "выпадение" отрезков на "граничных" условиях - т.к. дата берется ч/з trunc()

       upd 24.07.2012 - а может и принципиально. Вариант со строгим равенством по a.dt дает чуть меньше число записей:
         200 944 639  -- строгое равенство
         200 952 028  -- нестрогое равенство
       Видимо дело все таки в trunc()-аньи дат - разбираться не стал
       a.dt between a.role_datebeg and a.role_dateend
     */
       a.dt + (a.dte - a.dt) / 2 between a.role_datebeg and a.role_dateend
   ) s,
     ais.obj_attrib oa,
     rep_dept rd
  where
    s.SalerISN = oa.objisn(+)
    and oa.classisn(+) = 1428587803
    and oa.discr(+) = 'C'
    and s.dt between nvl(trunc(oa.datebeg(+)), s.pMinDate) and nvl(trunc(oa.dateend(+)), s.pMaxDate)
    and s.DeptISN = rd.DeptISN(+)
group by
   --S.pLoadISN,
   S.agrisn,
   S.SalerISN,
   S.AgrSClass,
   S.deptisn,
   rd.Dept0ISN,
   rd.Dept1ISN,
   rd.DOISN,
   rd.OISN,
   S.IS_SALERGO,
   S.IS_SALERF,
   s.dt,
   s.dte

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
)

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPAGRTUR" ("AGRISN", "ISRUSSIA", "ISSHENGEN") AS 
  (select --+ use_nl (a l) ordered
       a.agrisn,
       Max(Decode(subclassisn,2570/*c.get('Russia')*/,1,2458716 /*c.get('Ter_Russia')*/,1,0))isrussia, 
       Max(Decode(subclassisn,13310216,1,0)) isshengen
     from tt_rowId t, Ais.agrlimit a, Ais.agrlimitem l
     where a.agrisn = T.Isn
       and a.isn = l.limisn
       and l.subclassisn in (2570/*c.get('Russia')*/,2458716 /*c.get('Ter_Russia')*/,13310216/*Shengem*/)
     Group by a.agrisn
)


  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPBUHBODY" ("DEPTISN", "STATCODE", "CLASSISN", "BODYISN", "DATEVAL", "CURRISN", "DEPTISNBUH", "SUBJISN", "SUBACCISN", "BUHAMOUNT", "BUHAMOUNTRUB", "DOCSUMISN", "DATEPAYLAST", "AGRISN", "DEPTISNAN", "REPRDEPTISN", "BIZFLG", "ADDISN", "REFUNDISN", "DOCSUMPC", "BUHQUITBODYISN", "BUHQUITBODYCNT", "ACCCURRISN", "QUITDEBETISN", "QUITCREDITISN", "QUITDATEVAL", "DATEQUIT", "QUITCURRISN", "QUITDEBETSUBACCISN", "QUITCREDITSUBACCISN", "QUITDEBETBUHAMOUNT", "QUITCREDITBUHAMOUNT", "AMOUNTCLOSEDQUIT", "BUHQUITAMOUNT", "BUHQUITPARTAMOUNT", "BUHQUITDATE", "PARENTISN", "AMOUNTCLOSINGQUIT", "FULLAMOUNTCLOSINGQUIT", "AGRBUHDATE", "FACTISN", "BUHQUITISN", "BUHHEADFID", "BUHAMOUNTUSD", "OPRISN", "OPRDEPTISN", "DOCISN", "DATEPAY", "HEADISN", "DOCSUMSUBJ", "DOCISN2", "SAGROUP", "CORSUBACCISN", "DSDATEBEG", "DSDATEEND", "ADEPTISN", "DSCLASSISN", "DSCLASSISN2", "FACTPC", "BUHPC", "AMOUNT", "AMOUNTRUB", "AMOUNTUSD", "REMARK", "DSSTATUS") AS 
  With REPBUHBODY_LIST as
(Select z.*,
     --Поля корреспонденции
     (select max (isn)
      from ais.buhbody_t b
      where headisn = Z.headisn
        and status = 'А'
        and decode (z.damountrub,null,b.damountrub,b.camountrub) is not null) BuhQuitBodyIsn,
     (select count (*)
      from ais.buhbody_t b
      where headisn = Z.headisn
        and status = 'А'
        and decode (z.damountrub,null,b.damountrub,b.camountrub) is not null) BuhQuitBodyCnt

from VZ_REPBUHBODY_LIST z),

DocsumList AS

(Select --+ Use_Concat
   l.bodyIsn,Ds.*
 from REPBUHBODY_LIST l,docsum ds
 Where l.bodyIsn in (ds.debetisn,ds.creditisn) and l.DSDISCR=ds.discr),



QuitBodyList as

(Select
  b.bodyisn,
      nvl (bb.damount,-bb.camount) BuhQuitAmount,
     nvl (bb1.damount,-bb1.camount) BuhQuitPartAmount,
     bb1.datequit BuhQuitDate,
     nvl (bb1.isn,bb.isn) BuhQuitIsn,
        bb.subaccisn CorSubAccIsn

from REPBUHBODY_LIST b,ais.buhbody_t bb, ais.buhbody_t bb1
Where         b.BuhQuitBodyIsn = bb.isn
          and bb.isn In (bb1.isn,bb1.Parentisn)
          and  Decode(bb1.isn,bb.isn,686696616,bb1.oprisn)= 686696616
          and bb1.status = 'А'
)



SELECT Deptisn, StatCode, ClassIsn, bodyisn, dateval, currisn, DeptIsnBuh,  SubjIsn, subaccisn,
       buhamount, buhamountrub, docsumisn, datepaylast, Agrisn, DeptIsnAn, ReprDeptIsn, BizFlg, AddIsn, Refundisn,
       docsumpc, BuhQuitBodyIsn, BuhQuitBodyCnt, AccCurrIsn, QuitDebetIsn, QuitCreditIsn, QuitDateVal, DateQuit, QuitCurrIsn,
       QuitDebetSubAccIsn, QuitCreditSubAccIsn, QuitDebetBuhAmount, QuitCreditBuhAmount, AmountClosedQuit, BuhQuitAmount,
       BuhQuitPartAmount, BuhQuitDate, ParentIsn, AmountClosingQuit, FullAmountClosingQuit, AgrBuhDate, FactIsn, BuhQuitIsn,
       BuhHeadFid, BuhAmountUsd, OprIsn,OprDeptIsn, docisn, DatePay, HeadIsn, DocSumSubj,
       DocIsn2, Sagroup, CorSubAccIsn, DsDatebeg, DsDateend, adeptisn, DsClassIsn, DsClassIsn2,
       decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit) AS factpc,
       decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount) AS buhpc,
       buhamount*decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit)*
       decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount)*docsumpc AS amount, -- EGAO 02.02.2010
       buhamountrub*decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit)*
       decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount)*docsumpc AS amountrub, -- EGAO 02.02.2010
       buhamountusd*decode (nvl (FullAmountClosingQuit,0),0,1,abs (AmountClosingQuit)/FullAmountClosingQuit)*
       decode (nvl (BuhQuitAmount,0),0,1,nvl (BuhQuitPartAmount,BuhQuitAmount)/BuhQuitAmount)*docsumpc AS amountusd, -- EGAO 02.02.2010
       remark,
       Dsstatus /*KGS 01.08.2011 статут доксуммы для дебиторки*/
FROM (
select --+ use_nl (b d dp s d2 a aa r f b1 b2 bb bb1 opr) ordered Index (bb ) Use_Concat Index (bb1)
     b.Deptisn, b.StatCode, b.ClassIsn,
     b.bodyisn, b.dateval, b.currisn, b.DeptIsnBuh,  b.SubjIsn, b.subaccisn,
     b.buhamount, b.buhamountrub, b.docsumisn, b.datepaylast,Nvl(aa.isn,0) Agrisn,
     b.DeptIsnAn, b.ReprDeptIsn, b.BizFlg, a.isn AddIsn, r.isn Refundisn,
     to_number(decode (nvl (fullamountdoc,0),0,decode (docsumcnt,0,0+null,1/docsumcnt),b.amountdoc/fullamountdoc)) docsumpc,
     b.BuhQuitBodyIsn, b.BuhQuitBodyCnt, s.CurrIsn AccCurrIsn,
     b1.isn QuitDebetIsn, b2.isn QuitCreditIsn,
     nvl (nvl (b1.dateval, b2.dateval),f.DatePay) QuitDateVal,
     nvl (b1.datequit, b2.datequit) DateQuit,
     nvl (f.CurrIsn,nvl (b1.CurrIsn, b2.CurrIsn)) QuitCurrIsn,
     b1.SubAccIsn QuitDebetSubAccIsn,
     b2.SubAccIsn QuitCreditSubAccIsn,
     nvl (b1.damount, -b1.camount) QuitDebetBuhAmount,
     nvl (b2.camount, -b2.damount) QuitCreditBuhAmount,
     f.amount AmountClosedQuit,
      BuhQuitAmount,
      BuhQuitPartAmount,
      BuhQuitDate,
     b.ParentIsn,
     f.amountdoc AmountClosingQuit,
     SUM (abs (f.amountdoc)) OVER (PARTITION BY b.docsumisn, BuhQuitIsn) AS FullAmountClosingQuit,
     to_date(decode (b.statcode,38,decode (b.deptisn,707480016,dp.signed))) AgrBuhDate,
     f.isn FactIsn,  BuhQuitIsn,
     b.BuhHeadFid, b.BuhAmountUsd,B.OprIsn,Opr.CLASSISN1 OprDeptIsn,
     b.docisn,
     b.DatePay,
     B.HeadIsn,
     Nvl(Dssubjisn,F.SubjIsn) DocSumSubj,
     b.DocIsn2,
     Sagroup,
      CorSubAccIsn,
     DsDatebeg,
     DsDateend,
     b.adeptisn,   -- EGAO 29.04.2009  в рамках ДИТ-09-1-083535
     b.DsClassIsn, -- EGAO 02.02.2010
     b.DsClassIsn2 ,  -- EGAO 02.02.2010
     b.remark,
     b.Dsstatus




--     decode (decode (b.statcode,38,1,34,1),1,decode (nvl (a.isn,aa.isn),null,null,Ais.Get_Agr_BuhDate (nvl (a.isn,aa.isn), b.DocSumIsn))) AgrBuhDate
    from (select --+ use_nl (r b pc pd h adept ) ordered index ( adept X_KINDACCSET_ACC_KIND ) Use_Hash(ds) Index (b)
    --Поля из report_body_list
     r.Deptisn, r.StatCode, r.ClassIsn,
    --Поля проводки
     b.isn BodyIsn, b.dateval DateVal, b.currisn CurrIsn, h.fid BuhHeadFid,
     b.deptisn DeptIsnBuh, b.SubjIsn, b.SubAccIsn, b.parentisn,H.Isn HeadIsn,
     nvl (b.camount, -b.damount) BuhAmount,
     nvl (b.camountrub, -b.damountrub) BuhAmountRub,
     nvl (b.camountusd, -b.damountusd) BuhAmountUsd,
     --Поля аналитики
     AIS.BuhKind_Utils.GetDeptFromKindAcc (b.SubKindIsn) DeptIsnAn,
     AIS.BuhKind_Utils.GetReprDeptFromKindAcc (b.SubKindIsn) ReprDeptIsn,
     /*(select max (decode (classisn, 980350425 /*c.get ('cBizCenter'), 'Ц', 980350525 /*c.get ('cBizFil'), 'Ф'))
     from kindaccset where kindaccisn = b.SubKindIsn and kindisn = 980357325 /*c.get ('cKindBiz')) */ '' BizFlg,
     --Поля плановой доксуммы
     Ds.isn  DocSumIsn,
     Ds.DatePay DatePay,
     Ds.DatePayLast DatePayLast,
     Ds.DateBeg DsDateBeg,
     Ds.DateEnd DsDateEnd,
     Ds.classisn DsClassisn,
     Ds.classisn2 DsClassisn2,
     Ds.status Dsstatus,
     ds.subjisn Dssubjisn,
     nvl (Ds.agrisn, b.AgrIsn) AgrIsn,
     Ds.RefundIsn RefundIsn,
     gcc2.gcc2(Ds.Amount,Ds.CURRISN,b.currisn,b.dateval) AmountDoc,
     Ds.DocIsn DocIsn,
     Ds.DocIsn2 DocIsn2,
     B.oprisn,
     SUM (gcc2.gcc2(Ds.Amount,ds.CURRISN,b.currisn,b.dateval)) OVER (PARTITION BY b.isn) AS FullAmountDoc,
     COUNT (*) OVER (PARTITION BY b.isn) AS DocSumCnt,
     BuhQuitBodyIsn,
     BuhQuitBodyCnt,
--        Decode(pc.Isn,null,Pd.CreditIsn,Pc.DebetIsn) PdsBuhQuitIsn,
        r.sagroup,
        adept.classisn AS adeptisn, -- EGAO 29.04.2009  в рамках ДИТ-09-1-083535
        b.remark
    from REPBUHBODY_LIST r, ais.buhbody_t b,DocsumList Ds, ais.buhhead_t h, ais.kindaccset adept
    where r.bodyisn = b.isn
      and r.bodyisn = Ds.BodyIsn (+)
      and b.headisn = h.isn
      AND adept.kindaccisn(+) = b.SubKindIsn -- EGAO 29.04.2009 в рамках ДИТ-09-1-083535
      AND adept.kindisn(+)=56645916 -- EGAO 29.04.2009  в рамках ДИТ-09-1-083535 (c.get('ckinddeptfull')-подразделения сао "ингосстрах"0
    ) b, docs d, docs dp, ais.subacc s, docs d2, agreement a, agreement aa, agrrefund r, docsum f, ais.buhbody_t b1, ais.buhbody_t b2,
    QuitBodyList QbL,
      (select x.* from dicx x,dicti d where x.classisn = c.get('xDeptReprOper')
        And d.isn=x.classisn1 and Nvl(d.active,'S')<>'S') Opr
    where b.docisn = d.isn (+)
      and d.parentisn = dp.isn (+)
      and d.accisn =  s.isn (+)
      and b.docisn2 = d2.isn (+)
      and b.refundisn = a.isn (+)
      and b.agrisn = aa.isn (+)
      and b.refundisn = r.isn (+)
      and b.docsumisn = f.parentisn (+)
      and f.discr (+) = 'F'
      and f.status (+) is null
      and f.amount (+) <> 0
      and f.debetisn = b1.isn (+)
      and f.creditisn = b2.isn (+)

      and b.bodyIsn=QbL.bodyIsn(+)
        And Opr.CLASSISN2(+)= B.OprIsn)


  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPBUHQUIT" ("DEPTISN", "STATCODE", "CLASSISN", "BODYISN", "DATEVAL", "CURRISN", "BUHHEADFID", "DEPTISNBUH", "SUBJISN", "SUBACCISN", "PARENTISN", "HEADISN", "BUHAMOUNT", "BUHAMOUNTRUB", "BUHAMOUNTUSD", "OPRISN", "BUHQUITBODYISN", "BUHQUITBODYCNT", "SAGROUP", "BUHPC", "BUHQUITPC", "BUHQUITDATE", "GROUPISN", "BUHQUITISN", "QUEISN", "CORSUBACCISN", "QUITSUM", "QUITPC", "FACT", "QUITBODYISN", "QUITDATEVAL", "REPCURSDIFF") AS 
  Select --+ use_nl (bbq) ordered no_merge ( b )
  b.deptisn, b.statcode, b.classisn, b.bodyisn,
       b.dateval, b.currisn, b.buhheadfid, b.deptisnbuh, b.subjisn,
       b.subaccisn, b.parentisn, b.headisn, b.buhamount, b.buhamountrub,
       b.buhamountusd, b.oprisn, b.buhquitbodyisn, b.buhquitbodycnt,
       b.sagroup,
       b.buhpc,-- коэф. частичной квитовки
       b.buhquitpc, -- коэф. корреспонденции
       b.buhquitdate,-- дата операции квитовки
        b.groupisn,
       b.buhquitisn, b.queisn, b.corsubaccisn, Sum(b.quitsum) quitsum,Sum( b.quitpc) quitpc,
       b.fact,
       Max(b.quitbodyisn) quitbodyisn ,/*KGS 04.10.11 чтобы было много места*/
       bbq.dateval QuitDateval ,-- Дата начисления проводки, с которой квитовали

       Case When b.currisn=35 Then 0
       else
         Sum
             (
        
               NVL((  - gcc2.gcc2(Nvl(b.quitsum,0)*B.BuhQuitSumPc,b.currisn,35,Nvl(B.BUHQUITDATE, B.Dateval))+ -- валютируем зачет на дату начисления
                     gcc2.gcc2(Nvl(b.quitsum,0)*B.BuhQuitSumPc,B.currisn,35, Decode(B.FACT,'Y',bbq.dateval,Nvl(B.BUHQUITDATE,B.Dateval))) -- валютируем поступление на дату оплаты
               ),0)+
               
                Nvl(( -b.buhamountrub*BUHPC*QUITPC*BuhQuitPc +  -- Сумма в рублях
                     gcc2.gcc2(b.buhamount*BUHPC*QUITPC*BuhQuitPc,B.currisn,35, Nvl(B.BUHQUITDATE,B.Dateval))
                  ) ,0)   -- разница валютирования даты начисления и даты квитовки
               
              )
               
             end RepCursDiff /* курсовая разница (в руб) валютирования не рублевого поступления в зависимости от типа потом ее к сквитованной сумме прибавлять просто надо*/
from
(
Select --+ use_nl (qd1  qd2) ordered no_merge ( b ) index (qd1 X_QUEDECODE_OBJISN ) index ( qd2 X_QUEDECODE_REFISN ) Index (bb) Index (bb1 X_BUHBODY_PARENT)
 B.*,
 Nvl(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2),0) quitSum,
Nvl(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2),1)/decode(Nvl((Sum(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2)) over (Partition by bodyisn,BuhQuitIsn)),1),0,1,
Nvl((Sum(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2)) over (Partition by bodyIsn,BuhQuitIsn)),1)) quitPc,
Decode(Nvl(qd1.REFISN,qd2.ObjIsn),null,null,
(Select Nvl(Max('Y'),'N') from docsum where  b.buhquitisn in (debetisn,creditisn)/*(creditisn=Nvl(qd1.REFISN,qd2.ObjIsn) or debetisn=Nvl(qd1.REFISN,qd2.ObjIsn)) */and discr='F'  )) Fact,
Nvl(qd1.REFISN,qd2.ObjIsn) QuitBodyIsn
from(
 Select --+ use_nl (r bb bb1 dg) ordered
     b.*,
    decode( nvl (bb1.damount,-bb1.camount),0,1,nvl (bb1.damount,-bb1.camount)/ nvl (bb.damount,-bb.camount)) BuhPc,
    /*decode(nvl (bb.damount,-bb.camount),0,1,BuhAmount/nvl (bb.damount,-bb.camount))*/

  Case
      When nvl (bb.damount,-bb.camount)=0 Then 1
      When Abs(BuhAmount/nvl (bb.damount,-bb.camount))>1 then 1 -- если сумма корресп проводки меньше нашей - то 1, иначе - коэффициент
      else
         Abs(BuhAmount/nvl (bb.damount,-bb.camount))
       end    BuhQuitSumPc,
       
  Case
      When nvl (bb.damount,-bb.camount)=0 Then 1
      When Abs(BuhAmount/nvl (bb.damount,-bb.camount))<1 then 1 -- если сумма корресп проводки больше нашей - то 1, иначе - коэффициент
      else
         nvl (bb.damount,-bb.camount)/BuhAmount
       end    BuhQuitPc,

       
     bb1.datequit BuhQuitDate,
     bb1.groupisn,
     bb1.isn BuhQuitIsn,
     dg.queisn,
     bb.subaccisn CorSubAccIsn

 from
 (
 select --+ use_nl (r b pc pd h) ordered Index (b) Index (cb)
    --Поля из report_body_list
     r.Deptisn, r.StatCode, r.ClassIsn,
    --Поля проводки
     b.isn BodyIsn, b.dateval DateVal, b.currisn CurrIsn, h.fid BuhHeadFid,
     b.deptisn DeptIsnBuh, b.SubjIsn, b.SubAccIsn, b.parentisn,H.Isn HeadIsn,
     nvl (b.camount, -b.damount) BuhAmount,
     nvl (b.camountrub, -b.damountrub) BuhAmountRub,
     nvl (b.camountusd, -b.damountusd) BuhAmountUsd,
     --Поля аналитики
     B.oprisn,
     --Поля корреспонденции
/*     (select max (isn)
      from ais.buhbody_t
      where headisn = b.headisn
        and status = 'А'
        and decode (b.damount,null,damount,camount) is not null)*/
        Cb.Isn BuhQuitBodyIsn,
     (select --+ Index(bbb)
       count (*)
      from ais.buhbody_t bbb
      where bbb.headisn = b.headisn
        and bbb.status = 'А'
        and decode (b.damount,null,damount,camount) is not null) BuhQuitBodyCnt,
        r.sagroup
    from VZ_REPBUHBODY_LIST r, ais.buhbody_t b,  ais.buhhead_t h, ais.buhbody_t cb
    where r.bodyisn = b.isn
      and b.headisn = h.isn
      
        and b.headisn = cb.headisn
        and cb.status = 'А'
        and decode (b.damount,null,Cb.damount,Cb.camount) is not null

) b,ais.buhbody_t bb, ais.buhbody_t bb1,ais.DOCGRP dg

    where b.BuhQuitBodyIsn = bb.isn (+)
       and (bb.isn = bb1.isn
        or bb.isn = bb1.parentisn
        and bb1.status = 'А'
        and bb1.oprisn = c.get('oPartQuit')
        and Nvl(bb1.camount,bb1.damount)<>0 -- у операций частичной квитовки сумм 0 не рассматриваем
        )
        And  bb1.groupisn=dg.isn(+)


        ) b,ais.quedecode qd1,ais.quedecode qd2
        Where b.queisn=qd1.queisn(+)
        And  b.BuhQuitIsn=qd1.ObjIsn(+)

        and b.queisn=qd2.queisn(+)
        And  b.BuhQuitIsn=qd2.REFISN(+)
) b,ais.buhbody_t bbq
Where b.QuitBodyIsn=bbq.isn(+)
/*KGS 04.10.11 чтобы было много места*/
Group by
  b.deptisn, b.statcode, b.classisn, b.bodyisn,
       b.dateval, b.currisn, b.buhheadfid, b.deptisnbuh, b.subjisn,
       b.subaccisn, b.parentisn, b.headisn, b.buhamount, b.buhamountrub,
       b.buhamountusd, b.oprisn, b.buhquitbodyisn, b.buhquitbodycnt,
       b.sagroup, b.buhpc, b.buhquitpc,
       b.buhquitdate,
        b.groupisn,
       b.buhquitisn, b.queisn, b.corsubaccisn,
       b.fact,
       bbq.dateval

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPCOND" ("CONDISN", "DATEBEG", "DATEEND", "PARENTISN", "AGRISN", "ADDISN", "ADDSTATUS", "ADDNO", "ADDBEG", "ADDSIGN", "PARENTADDISN", "NEWADDISN", "OBJISN", "PARENTOBJISN", "RISKISN", "PARENTRISKISN", "LIMITISN", "RPTCLASSISN", "LIMCLASSISN", "CURRISN", "PREMCURRISN", "FRANCHCURRISN", "FRANCHTYPE", "PREMIUMSUM", "PREMUSD", "PREMRUB", "PREMEUR", "LIMITSUM", "LIMITUSD", "LIMITRUB", "LIMITEUR", "FRANCHSUM", "FRANCHUSD", "FRANCHRUB", "FRANCHEUR", "OBJCLASSISN", "OBJRPTCLASSISN", "DESCISN", "OBJPRNCLASSISN", "OBJPRNRPTCLASSISN", "RISKCLASSISN", "RISKPRNCLASSISN", "RISKRPTCLASSISN", "RISKPRNRPTCLASSISN", "RISKRULEISN", "RISKPRNRULEISN", "LIMITCLASSISN", "AGRDATEBEG", "AGRDATEEND", "AGRRULEISN", "AGRCLASSISN", "AGRCOMISSION", "AGRDISCR", "NEWADDSIGN", "QUANTITY", "FRANCHTARIFF", "OBJREGIONISN", "OBJCOUNTRYISN", "CLIENTISN", "AGROLDDATEEND", "ADDPREMIUMSUM", "AGRCURRISN", "AGRDETAILISN", "PREMAGR", "CARRPTCLASS", "DISCOUNT", "DISCOUNT2", "AGRSHAREPC", "COST", "TARIFF", "YEARTARIFF") AS 
  (/* 10.01.2013 KGS !!!!!! АХТУНГ !!!! ДЛЯ МЕДИКОВ КОНДЫ С 0 ПЛАНОВОЙ ПРЕМИЕЙ НЕ ГРУЗИМ!!!!! */
select /*+ ALL_ROWS */
"CONDISN","DATEBEG","DATEEND","PARENTISN","AGRISN","ADDISN","ADDSTATUS","ADDNO","ADDBEG",
"ADDSIGN","PARENTADDISN","NEWADDISN","OBJISN","PARENTOBJISN","RISKISN","PARENTRISKISN","LIMITISN","RPTCLASSISN",
"LIMCLASSISN","CURRISN","PREMCURRISN","FRANCHCURRISN","FRANCHTYPE","PREMIUMSUM","PREMUSD","PREMRUB","PREMEUR","LIMITSUM",
"LIMITUSD","LIMITRUB","LIMITEUR","FRANCHSUM","FRANCHUSD","FRANCHRUB","FRANCHEUR","OBJCLASSISN","OBJRPTCLASSISN","DESCISN",
"OBJPRNCLASSISN","OBJPRNRPTCLASSISN","RISKCLASSISN","RISKPRNCLASSISN","RISKRPTCLASSISN","RISKPRNRPTCLASSISN","RISKRULEISN",
"RISKPRNRULEISN","LIMITCLASSISN","AGRDATEBEG","AGRDATEEND","AGRRULEISN","AGRCLASSISN","AGRCOMISSION","AGRDISCR","NEWADDSIGN",
"QUANTITY","FRANCHTARIFF","OBJREGIONISN","OBJCOUNTRYISN","CLIENTISN","AGROLDDATEEND","ADDPREMIUMSUM","AGRCURRISN","AGRDETAILISN",
"PREMAGR","CARRPTCLASS",
"DISCOUNT","DISCOUNT2","AGRSHAREPC",
  Cost,  -- sts 14.03.2013 - Страховая стоимость
  --kds(14.11.2013) task(57056418903)
  "TARIFF",
  "YEARTARIFF"
from
(select --+ ordered  no_merge(acpx) no_merge(aopx) use_nl(t ac  acp ar ao arp aop al ad adn a city city1 addr adh) use_hash(acpx aopx  CarRules)
       AC.ISN CONDISN,
       trunc(AC.DATEBEG)DATEBEG,
       trunc(AC.DATEEND)DATEEND,
       AC.PARENTISN,
       AC.AGRISN,
       AC.ADDISN,
       AD.STATUS ADDSTATUS,
       AD.NO ADDNO,
       AD.DATEBEG ADDBEG,
       AD.DATESIGN ADDSIGN,
       ACP.ADDISN PARENTADDISN,
       AC.NEWADDISN,
       AC.OBJISN,
       AOP.ISN PARENTOBJISN,
       AC.RISKISN,

/*       ARP.ISN PARENTRISKISN, */
/*kgs 13.07.12 по письму дмитревской. искуственный групповой риск для правильного рассчета перестрахования*/

           Case When AR.PARENTISN is null and AR.RULEISN in (707051716 ,2381976903,206916 ,207016,207116)
           then
             (Select max(Ar1.Isn) from agrrisk ar1 Where ar1.agrisn=ar.agrisn and ar1.ruleisn=707045516
             and ar1.parentisn is null )
                else
             AR.PARENTISN end PARENTRISKISN,

       AC.LIMITISN,
       AC.RPTCLASSISN,
       AC.LIMCLASSISN,
       AC.CURRISN,
       AC.PREMCURRISN,
       AC.FRANCHCURRISN,
       AC.FRANCHTYPE,
       AC.PREMIUMSUM,
       gcc2.gcc2(AC.PREMIUMSUM,AC.PREMCURRISN, 53, coalesce(a.datebeg,ad.datesign,trunc(SYSDATE))/*EGAO 20.05.2011 least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)*/) PREMUSD,
       gcc2.gcc2(AC.PREMIUMSUM,AC.PREMCURRISN, 35, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) PREMRUB,
       gcc2.gcc2(AC.PREMIUMSUM,AC.PREMCURRISN, 29448516, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) PREMEUR,
       AC.LIMITSUM,
       gcc2.gcc2(AC.LIMITSUM,AC.CURRISN, 53, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) LIMITUSD,
       gcc2.gcc2(AC.LIMITSUM,AC.CURRISN, 35, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) LIMITRUB,
       gcc2.gcc2(AC.LIMITSUM,AC.CURRISN, 29448516, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) LIMITEUR,
       AC.FRANCHSUM,
       gcc2.gcc2(AC.FRANCHSUM,AC.FRANCHCURRISN, 53, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) FRANCHUSD,
       gcc2.gcc2(AC.FRANCHSUM,AC.FRANCHCURRISN, 35, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) FRANCHRUB,
       gcc2.gcc2(AC.FRANCHSUM,AC.FRANCHCURRISN, 29448516, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) FRANCHEUR,
       AO.CLASSISN OBJCLASSISN,
       AO.RPTCLASSISN OBJRPTCLASSISN,
       AO.DESCISN,
       AOP.CLASSISN OBJPRNCLASSISN,
       AOP.RPTCLASSISN OBJPRNRPTCLASSISN,
       AR.CLASSISN RISKCLASSISN,
       ARP.CLASSISN RISKPRNCLASSISN,
       AR.RPTCLASSISN RISKRPTCLASSISN,
       ARP.RPTCLASSISN RISKPRNRPTCLASSISN,
       AR.RULEISN RISKRULEISN,
       ARP.RULEISN RISKPRNRULEISN,
       AL.CLASSISN LIMITCLASSISN,
       A.DATEBEG AGRDATEBEG,
       A.DATEEND AGRDATEEND,
       A.RULEISN AGRRULEISN,
       A.CLASSISN AGRCLASSISN,
       A.COMISSION AGRCOMISSION,
       A.DISCR AGRDISCR,
       ADN.DATESIGN NEWADDSIGN,
       AC.QUANTITY,
       AC.FRANCHTARIFF,
       nvl(nvl(CITY.PARENTREGIONISN,CITY1.PARENTREGIONISN),(select C.PARENTREGIONISN from AIS.AGRADDR ADR,REP_CITY C where ADR.AGRISN=AC.AGRISN and ROWNUM<=1 and ADR.CITYISN=C.CITYISN)) OBJREGIONISN,
       nvl(nvl(CITY.PARENTCOUNTRYISN,CITY1.PARENTCOUNTRYISN),(select C.PARENTCOUNTRYISN from AIS.AGRADDR ADR,REP_CITY C where ADR.AGRISN=AC.AGRISN and ROWNUM<=1 and ADR.CITYISN=C.CITYISN)) OBJCOUNTRYISN,
       A.CLIENTISN,
       A.OLDDATEEND AGROLDDATEEND,
       AD.PREMIUMSUM ADDPREMIUMSUM,
       A.CURRISN AGRCURRISN, --egao 14.07.2010
       ADH.AGRDETAILISN -- OD 25.10.2010
       ,CASE
          WHEN ac.premcurrisn=a.currisn THEN ac.premiumsum
          ELSE gcc2.gcc2(ac.premiumsum,ac.premcurrisn, a.currisn, coalesce(ac.datebeg, a.datebeg,trunc(SYSDATE)))
        END AS premagr ,-- EGAO 31.08.2011 в рамках ДИТ-07-1-027944

        /* sts - old 13.01.2012 -- в качестве второго параметра ф-ии должен быть RULEISN договора, а не риска!
           корректная версия ниже
        Decode(CarRules.Isn,null,null,motor.f_get_rptclass(AR.CLASSISN, AR.RULEISN, AC.RPTCLASSISN))  Carrptclass
        */

        Decode(CarRules.Isn,null,null,753518300,'ГО',motor.f_get_rptclass(AR.CLASSISN, A.RULEISN, AC.RPTCLASSISN)) as Carrptclass,
        AC.Discount,
        AC.Discount2,
        NVL(a.sharepc,100) AS agrsharepc, -- EGAO 19.03.2012
        ac.Cost, -- sts 14.03.2013 - Страховая стоимость
        --kds(14.11.2013) task(57056418903)
        AC.TARIFF,
        AC.YEARTARIFF
  from TT_ROWID T,
       AGRCOND AC,
       ( select --+ ordered use_nl(zt zac)
                distinct
                ZAC.ISN,
                ( select
                         max(ROWID)
                    from AGRCOND ZZ
                   where ZZ.PARENTISN is null
                   start with ZZ.ISN = ZAC.PARENTISN
                 connect by NoCYCLE prior ZZ.PARENTISN = ZZ.ISN ) RID /*NoCYCLE - KGS 08.10.11 не нужен. надо данные в АИС вычистить*/
           from TT_ROWID ZT,
                AGRCOND ZAC
          where ZAC.AGRISN = ZT.ISN ) ACPX,
       AGRCOND ACP,
       AGRRISK AR,
       AGROBJECT AO,
       AGRRISK ARP,
       ( select --+ ordered use_nl(zt zao)
                distinct
                ZAO.ISN,
                ( select
                         max(ROWID) RID
                    from AGROBJECT ZZ
                   where ZZ.PARENTISN is null
                   start with ZZ.ISN = ZAO.PARENTISN
                 connect by NoCYCLE prior ZZ.PARENTISN = ZZ.ISN ) RID
           from TT_ROWID ZT,
                AGROBJECT ZAO
          where ZAO.AGRISN = ZT.ISN ) AOPX,
       AGROBJECT AOP,
       AGRLIMIT AL,
       AGREEMENT AD,
       AGREEMENT ADN,
       AGREEMENT A,
       REP_CITY CITY,
       REP_CITY CITY1,
       AIS.AGRADDR ADDR,
       AGR_DETAIL_AGRHASH ADH,
       /*KGS 19.12.2011 Простановка поля RPTCLASS для автострахования. Далее будем пользовать отсюда */
       ( Select r.* from  motor.v_dicti_rule  r ) CarRules
 where T.ISN        = AC.AGRISN
   and AR.ISN(+)    = AC.RISKISN
   and AO.ISN(+)    = AC.OBJISN
   and ARP.ISN(+)   = AR.PARENTISN
   and AOPX.ISN(+)  = AO.ISN
   and AOP.ROWID(+) = AOPX.RID --mserp 27.10.2009 глюки начались.
   --(+) mserp 26.10.2009. убрал открытый join, т.к. в 10g этот фокус больше не проходит. если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
   --= nvl((select/*+rule*/ decode(count(*), 0, null, max(rowid))  from agrobject where parentisn is null  start with isn = ao.parentisn connect by prior parentisn = isn),ao.rowid)
   and AL.ISN(+)    = AC.LIMITISN
   and AD.ISN(+)    = AC.ADDISN
   and ADN.ISN(+)   = AC.NEWADDISN
   and ACPX.ISN(+)  = AC.ISN
   and ACP.ROWID(+) = ACPX.RID -- mserp 27.10.2009 глюки начались.
   --(+) mserp 26.10.2009. убрал открытый join, т.к. в 10g этот фокус больше не проходит. если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
   --= nvl((select/*+rule*/ decode(count(*), 0, null, max(rowid)) from agrcond where parentisn is null  start with isn = ac.parentisn connect by prior parentisn = isn),ac.rowid)
   and T.ISN        = A.ISN(+)
   and AOP.CITYISN  = CITY.CITYISN(+)
   and AOP.CITYISN  = ADDR.ISN(+)
   and ADDR.CITYISN = CITY1.CITYISN(+)
   and AC.AGRISN    = ADH.AGRISN(+)

   and A.ruleisn =CarRules.Isn(+)
)
Where AGRRULEISN  NOT IN ( select D.ISN
                           from DICTI D
                           start with D.ISN = 686160416
                           connect by prior D.ISN = D.PARENTISN  )
OR NVL(PREMIUMSUM,0)>0
)


  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPCRGDOC" ("AGRISN", "CLASSISN", "OBJISN", "SUBJISN", "JURIDICAL") AS 
  (Select /*+ Ordered USe_Nl(sb) */ S."AGRISN",S."CLASSISN",S."OBJISN",S."SUBJISN",sb.juridical
       from
         (Select --+ Index_Asc(d X_CRGDOC_AGR) ordered use_Nl(d sb)
            d.agrisn, d.classisn, Max(d.objisn) objisn, Max(d.subjisn) subjisn
          from tt_rowid t,ais.CrgDoc d
          Where t.isn=d.agrisn
            and d.classisn=34709216 -- ПАСПОРТ ТРАНСПОРНОГО СРЕДСТВА
          Group by d.agrisn,d.classisn) S,subject sb
       Where sb.isn(+)=S.subjisn
)

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_OBJCLASS_DOMESTIC" ("AGRISN", "OBJCLASSISN", "DOMESTIC", "PARENTOBJCLASSISN") AS 
  (SELECT a.AgrIsn,a.ObjClassIsn,
       Max(decode(dt.parentisn,36776016 /*c.get('CarClassForeign') - КЛАСС МАШИН ИНОСТРАННОГО ПРОИЗВОДСТВА*/,'N',
                               36775916 /*c.get('CarClassLocal') - КЛАСС МАШИН ОТЕЧЕСТВЕННОГО ПРОИЗВОДСТВА*/,'Y')) domestic,
       a.ParentObjClassIsn
FROM (Select --+ Ordered Use_Nl (a o op oc t )
              A.Isn AgrIsn,o.ClassIsn ObjClassIsn,
              nvl((select /*+index(rt x_rultariff_x1)*/ max(rt.x2)
                   from ais.rultariff rt
                   where rt.TariffISN=703301816 -- C.Get('TRF_TariffGroup') - Тарифная группа для модификации ТС. (...ГО регион-кредит)
                     and x1=t.modelisn
                     AND (rt.DateBeg<=a.datebeg Or rt.DateBeg Between a.datebeg and a.dateend)),oc.tariffgroupisn) tariffgroupisn, -- EGAO 27.10.2009
              /*Max(decode(dt.parentisn,36776016 \*c.get('CarClassForeign') - КЛАСС МАШИН ИНОСТРАННОГО ПРОИЗВОДСТВА*\,'N',
                                      36775916 \*c.get('CarClassLocal') - КЛАСС МАШИН ОТЕЧЕСТВЕННОГО ПРОИЗВОДСТВА*\,'Y')) domestic, -- EGAO 27.10.2009*/
              op.ClassIsn ParentObjClassIsn
      From  tt_rowid tt,AGREEMENT a,
            (select Isn
             from dicti
             start with isn=683209116 -- КОМПЛЕКСНОЕ СТРАХОВАНИЕ
             connect by prior isn=parentisn
            ) rl,
            (select Isn
             from dicti  -- ТИП ДОГОВОРА СТРАХОВАНИЯ
             start with isn=34711216
             connect by prior isn=parentisn
            ) ac,
            Ais.AGROBJECT o, AGROBJECT op, OBJCAR oc, CARTARIF t--EGAO 27.10.2009 Глюки начались, DICTI dt
      where a.Isn=tt.isn
        and ruleisn =rl.isn
        and a.classisn =ac.isn
        and not exists (select /*+ index(j x_subject_class) */ isn from subject j where isn=a.emplisn and classisn=491)
        and o.AgrIsn=A.Isn
        ANd op.rowid=(-- для группировки по родительским объектам
            select rowid from agrobject where parentisn is null
            start with isn=o.Isn
            connect by prior parentisn=isn and prior parentisn is not null)
        and op.descisn=oc.isn(+)
        and oc.tarifisn=t.isn(+)
        /*EGAO 27.10.2009 Глюки начались Вынес dt в самый внешний join, а select max(rt.x2) внес во from внутреннего запроса. До моих изменений был тока внутренний запрос, без поля tariffgroupisn
        and dt.isn  --(+) MSerp 26.10.2009. Убрал открытый join, т.к. в 10g этот фокус больше не проходит. Если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. Насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
            = nvl(
        (select \*+index(rt x_rultariff_x1)*\ max(rt.x2)
         from ais.rultariff rt
         where rt.TariffISN=703301816 -- C.Get('TRF_TariffGroup') - Тарифная группа для модификации ТС. (...ГО регион-кредит)
           and x1=t.modelisn
           AND (rt.DateBeg<=a.datebeg Or rt.DateBeg Between a.datebeg and a.dateend))
         ,oc.tariffgroupisn)*/
      --EGAO 27.10.2009 Глюки начались  Group by A.Isn, o.ClassIsn, op.ClassIsn
     ) a, dicti dt
WHERE dt.isn(+)=a.tariffgroupisn
Group by A.agrisn, a.ObjClassIsn, a.ParentObjClassIsn
)


  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPREFUND" ("REFUNDISN", "AGRISN", "CONDISN", "CURRISN", "CLAIMSUM", "DATELOSS", "DATECLAIM", "SUBJISN", "STATUS", "DATESOLUTION", "CLAIMSTATUS", "DATEEVENT", "DEPTISN", "FRANCHTYPE", "FRANCHTARIFF", "FRANCHSUM", "AGRDATEBEG", "RPTCLASSISN", "LOSSSHARE", "CLAIMISN", "DATEREG", "EMPLISN", "OBJISN", "PARENTOBJISN", "RPTGROUPISN", "CONDDEPTISN", "ISREVALUATION", "FRANCHCURRISN", "FRANCHDEDUCTED", "CLASSISN", "REFUNDSUM", "REFUNDSUMUSD", "CLAIMSUMUSD", "CLAIMID", "FIRMISN", "DATEREFUND", "LIMITSUM", "LIMITCURRISN", "RULEISNAGR", "RULEISNCLAIM", "NRZU", "BUDGETGROUPISN", "OBJCLASSISN", "AGREXTISN", "CONDPC", "PARENTOBJCLASSISN", "RAGRISN", "EXTDATEEVENT", "TOTALLOSS", "RFRANCHCURRISN", "RFRANCHSUM", "SALEREMPLISN", "SALERDEPTISN", "MOTIVGROUPISN", "RISKRULEISN", "RISKCLASSISN", "RDATEVAL", "REPDATELOSS", "CLAIMDATETOTALLOSS", "CLAIMCURRISN", "REFUNDSUMRUB", "CLAIMSUMRUB", "REGRESS", "CLAIMCLASSISN", "REFCREATED", "PARENTISN", "AGGRIEVEDNUMBER", "REFUNDID", "AGRCLASSISN") AS 
  (/* KGS 10.01.2013    REPCOND нельзя использовать - в нем теперь нет медиц кондов с 0 плановой премией*/
select --+ use_nl(s ac ar ao ) ordered push_subq index ( rc x_repcond_cond )
       S.ISN REFUNDISN,
       S.AGRISN,
       S.CONDISN,
       S.CURRISN,
       S.CLAIMSUM,
       S.DATELOSS,
       S.DATECLAIM,
       S.SUBJISN,
       S.REFSTATUS STATUS,
       S.DATESOLUTION,
       S.CLSTATUS CLAIMSTATUS,
       S.DATEEVENT,
       S.DEPTISN,
       AC.FRANCHTYPE,
       AC.FRANCHTARIFF,
       AC.FRANCHSUM,
       S.AGRDATEBEG,
       S.RPTCLASSISN,
       LOSSSHARE,
       S.CLAIMISN,
       S.DATEREG,
       S.EMPLISN,
       S.OBJISN,
       CASE WHEN AO.PARENTISN IS NULL THEN AO.ISN
        ELSE
         (  select --+ rule
             Max(ZZ.ISN)
             from AGROBJECT ZZ
            where ZZ.PARENTISN is null
            start with ZZ.ISN = AO.PARENTISN
             connect by NoCYCLE prior ZZ.PARENTISN = ZZ.ISN
          )  END  PARENTOBJISN,
--!!       Nvl(RC.PARENTOBJISN,RC.OBJISN) PARENTOBJISN,
       to_Number(null) RPTGROUPISN, --поле заполняется после загрузки reprefund с помощью апдейта
       to_Number(null) CONDDEPTISN,
       to_Number(null) ISREVALUATION, --поля заполняются после загрузки reprefund с помощью апдейт
       AC.FRANCHCURRISN,
       nvl(decode(nvl(AC.FRANCHTYPE, 'Б'), 'Б', decode (AC.FRANCHTARIFF, null, gcc2.gcc2(AC.FRANCHSUM, AC.FRANCHCURRISN, S.CURRISN, nvl(nvl(DATELOSS, DATEEVENT), DATEREG)))), 0) +
       nvl(S.CLAIMSUM * decode(nvl(AC.FRANCHTYPE, 'Б'), 'Б', AC.FRANCHTARIFF), 0) / 100 FRANCHDEDUCTED,
       S.CLASSISN,
       S.REFUNDSUM,
       S.REFUNDSUMUSD,
       S.CLAIMSUMUSD,
       S.ID CLAIMID,
       s.FIRMISN,
       S.DATEREFUND,
       AC.LIMITSUM,
       AC.CURRISN LIMITCURRISN,
       s.RULEISNAGR,
       S.RULEISN RULEISNCLAIM,
       S.NRZU,
       to_Number(null) BUDGETGROUPISN, --поле заполняется после загрузки reprefund с помощью апдейта
       AO.CLASSISN OBJCLASSISN,
       AGREXTISN,
       --MSerp 04.02.2011 ДИТ-11-1-128014 {
       --decode(ALLREFUNDSUM, 0, 1 / ALLREFUND, decode(nvl(REFUNDSUM, 0), 0, nvl(CLAIMSUM, 0), nvl(REFUNDSUM, 0)) / ALLREFUNDSUM) CONDPC,
       decode(ALLREFUNDSUM, 0, 1 / ALLREFUND, decode(nvl(S.REFUNDSUM, 0), 0, decode(AGREXTISN, null,nvl(S.CLAIMSUM, 0),0)  , nvl(S.REFUNDSUM, 0)) / ALLREFUNDSUM) CONDPC,
       --}ДИТ-11-1-128014

       CASE WHEN AO.PARENTISN IS NULL THEN NULL
        ELSE
         (  select --+ rule
             Max(ZZ.CLASSISN)
             from AGROBJECT ZZ
            where ZZ.PARENTISN is null
            start with ZZ.ISN = AO.PARENTISN
             connect by NoCYCLE prior ZZ.PARENTISN = ZZ.ISN
          )  END   PARENTOBJCLASSISN,
       RAGRISN,
       EXTDATEEVENT,
       TOTALLOSS,
       S.FRANCHCURRISN RFRANCHCURRISN,
       S.FRANCHSUM RFRANCHSUM,
       ( select max(SUBJISN)
           from AGRROLE AR
          where AR.AGRISN    = S.AGRISN
            and AR.REFUNDISN = S.ISN
            and AR.CLASSISN  = 1521585603 ) SALEREMPLISN,
       ( select max(DEPTISN)
           from AGRROLE AR
          where AR.AGRISN    = S.AGRISN
            and AR.REFUNDISN = S.ISN
            and AR.CLASSISN  = 1521585603 ) SALERDEPTISN,
       to_Number(null) MOTIVGROUPISN, --поле заполняется после заргузки reprefund процедурой set_refund_motivgroupisn
       AR.RULEISN RISKRULEISN,
       Ar.CLASSISN RISKCLASSISN,
       DATEVAL RDATEVAL,
       trunc(decode(AGREXTISN, null, nvl(nvl(DATELOSS, DATECLAIM), DATEREG), nvl(nvl(EXTDATEEVENT, nvl(DATELOSS, DATECLAIM)), DATEREG))) REPDATELOSS,
       S.CLAIMDATETOTALLOSS,
       S.CLAIMCURRISN, -- egao 20.03.2009 ДИТ-09-1-086869
       S.REFUNDSUMRUB,
       S.CLAIMSUMRUB,
       S.REGRESS,
       S.CLAIMCLASSISN, -- egao 27.10.2010 ДИТ-10-4-121049
       S.CREATED REFCREATED,-- OD 01.07.2011
       S.PARENTISN,
       (SELECT COUNT(DISTINCT subjisn) FROM agrrole rl  WHERE rl.agrisn=s.agrisn  AND rl.refundisn=s.isn AND rl.classisn=971382125) AS aggrievednumber, -- EGAO 20.03.2013 ДИТ-12-4-176083
       s.refundid,  -- EGAO 20.03.2013 ДИТ-12-4-176083
       s.agrclassisn
  from ( select s.ISN,
                s.CLAIMISN,
                s.AGRISN,
                s.RPTCLASSISN,
                s.CONDISN,
                s.CURRISN,
                s.CLAIMSUM,
                s.DATELOSS,
                s.DATECLAIM,
                s.DATEREG,
                s.DATESOLUTION,
                nvl(s.EXTDATEEVENT, s.DATEEVENT) DATEEVENT,
                s.SUBJISN,
                s.REFSTATUS,
                s.CLSTATUS,
                s.DEPTISN,
                s.DATEREFUND,
                s.FRANCHSUM,
                s.FRANCHCURRISN,
                s.AGRDATEBEG,
                s.LOSSSHARE,
                s.EMPLISN,
                s.CLASSISN,
                s.REFUNDSUM,
                s.OBJISN,
                gcc2.gcc2(s.REFUNDSUM, s.CURRISN, 53, nvl(s.DATEREFUND, s.DATEEVENT)) REFUNDSUMUSD,
                gcc2.gcc2(s.CLAIMSUM, s.CURRISN, 53, nvl(s.DATELOSS, s.DATECLAIM)) CLAIMSUMUSD,
                gcc2.gcc2(s.REFUNDSUM, s.CURRISN, 35, nvl(s.DATEREFUND, s.DATEEVENT)) REFUNDSUMRUB,
                gcc2.gcc2(s.CLAIMSUM, s.CURRISN, 35, nvl(s.DATELOSS, s.DATECLAIM)) CLAIMSUMRUB,
                s.ID,
                s.RULEISN,
                s.NRZU,
                s.AGREXTISN,
                --MSerp 04.02.2011 ДИТ-11-1-128014 {
                --sum(decode(nvl(decode(EXT.ISN, null, R.REFUNDSUM, EXT.REFUNDSUM), 0), 0, nvl(decode(EXT.ISN, null, R.CLAIMSUM, EXT.CLAIMSUM), 0), decode(EXT.ISN, null, R.REFUNDSUM, EXT.REFUNDSUM)))over(partition by R.ISN) ALLREFUNDSUM,

                sum(decode(nvl(s.REFUNDSUM, 0), 0, nvl(decode(s.AGREXTISN, null, s.CLAIMSUM, s.REFUNDSUM), 0), s.REFUNDSUM))over(partition by s.ISN) ALLREFUNDSUM,
                --} ДИТ-11-1-128014
                count(*)over(partition by s.ISN) ALLREFUND,
                s.RAGRISN,
                s.EXTDATEEVENT,
                s.TOTALLOSS,
                s.DATEVAL,
                /*EGAO 21.11.2012
                  ( select min(Q.DATESEND)
                    from AIS.QUEUE Q
                   where Q.CLASSISN = 1647725903 -- c.get('qeclaimtotal')
                     and Q.OBJISN   = s.CLAIMISN )*/ to_date(NULL) AS CLAIMDATETOTALLOSS,
                S.CLAIMCURRISN,
                s.REGRESS,
                s.CLAIMCLASSISN, -- egao 27.10.2010 ДИТ-10-4-121049
                s.CREATED, -- OD 01.07.2011
                s.firmisn, s.ruleisnagr,
                s.PARENTISN,
                s.refundid,
                s.agrclassisn
           from (
                 SELECT --+ ordered use_nl ( s ag ) no_merge ( s ) index ( ag X_REPAGR_AGR )
                        ag.firmisn, ag.ruleisn AS ruleisnagr, ag.classisn AS agrclassisn,
                        s.*
                 FROM (
                       SELECT --+ ordered use_nl ( t cl r ext cr )
                              R.ISN,
                              R.CLAIMISN,
                              r.agrisn AS ragrisn,
                              nvl(EXT.AGRISN, R.AGRISN) AS AGRISN,
                              R.RPTCLASSISN,
                              nvl(EXT.CONDISN, R.CONDISN) CONDISN,
                              decode(EXT.ISN, null, R.CURRISN, EXT.CURRISN) CURRISN,
                              decode(EXT.ISN, null, R.CLAIMSUM, EXT.CLAIMSUM) CLAIMSUM,
                              CL.DATELOSS,
                              CL.DATECLAIM,
                              CL.DATEREG,
                              CL.DATESOLUTION,
                              EXT.DATEEVENT AS EXTDATEEVENT, R.DATEEVENT,
                              CL.SUBJISN,
                              R.STATUS REFSTATUS,
                              CL.STATUS CLSTATUS,
                              nvl(R.DEPTISN, CL.DEPTISN) DEPTISN,
                              R.DATEREFUND,
                              R.FRANCHSUM,
                              R.FRANCHCURRISN,
                              CL.AGRDATEBEG,
                              r.LOSSSHARE,
                              nvl(R.EMPLISN, CL.EMPLISN) EMPLISN,
                              nvl(EXT.CLASSISN, R.CLASSISN) CLASSISN,
                              decode(EXT.ISN, null, R.REFUNDSUM, EXT.REFUNDSUM) REFUNDSUM,
                              nvl(EXT.OBJISN, R.OBJISN) OBJISN,
                              CL.ID,
                              CL.RULEISN,
                              R.NRZU,
                              EXT.ISN AGREXTISN,
                              CL.CURRISN AS CLAIMCURRISN,
                              R.REGRESS,
                              CL.CLASSISN CLAIMCLASSISN, -- egao 27.10.2010 ДИТ-10-4-121049
                              R.CREATED, -- OD 01.07.2011
                              R.DATEVAL,
                              CR.TOTALLOSS,
                              R.PARENTISN,
                              r.refundid -- EGAO 20.03.2013 ДИТ-12-4-176083
                       FROM TT_ROWID T,
                            AIS.AGRCLAIM CL,
                            AIS.AGRREFUND R,
                            AIS.AGRREFUNDEXT EXT,
                            AIS.CLAIMREFUNDCAR CR
                       where T.ISN      = CL.ISN
                         and R.CLAIMISN = CL.ISN
                         and R.ISN      = EXT.REFUNDISN(+)
                         and R.ISN      = CR.ISN(+)
                         and R.EMPLISN not in ( select --+ index (sb x_subject_class)
                                                       ISN
                                                from SUBJECT SB
                                                where CLASSISN = 491 ) -- Тестовый пользователь
                         and nvl(CL.CLASSISN, 0) <> 2835056703 -- акция "помощь друга" od 27.11.2009 12475086503
                      ) s, repagr ag
                 WHERE S.AGRISN=AG.AGRISN(+)
                ) s
            WHERE nvl(s.agrclassisn,0)<>28470016
           ) S,
         AGRCOND AC,
         AGRRISK AR,
         AGROBJECT AO

 where S.CONDISN=Ac.Isn(+)
   and AC.RISKISN=AR.ISN(+)
   and AC.OBJISN=AO.ISN(+)
)


  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_SUBJECT" ("SUBJISN", "CLASSISN", "ROLECLASSISN", "COUNTRYISN", "BRANCHISN", "JURIDICAL", "RESIDENT", "VIP", "INN", "ID", "FID", "CODE", "SHORTNAME", "FULLNAME", "ACTIVE", "UPDATED", "UPDATEDBY", "LICENSENO", "LICENSEDATE", "OKPO", "OKOHX", "SYNISN", "CREATEDBY", "CREATED", "PROFITTAXFLAG", "PARENTISN", "NAMELAT", "ORGFORMISN", "REMARK", "KPP", "SEARCHNAME", "SECURITYLEVEL", "OGRN", "OKVED", "SECURITYSTR", "REGNM", "SBCLASS", "RELICEND", "LICEND", "NREZNAME", "LIKVIDSTATUS", "R_BEST", "R_FITCH", "R_MOODYS", "R_SP", "R_WEISS", "ADDRCODE", "ADDRTYPE", "CITYISN", "POSTCODE", "ADDRESS", "PARENTSUBJ", "PARENTFULLNAME", "PARENTCLASS", "PARENTINN", "UPDATEDBY_NAME", "BNK_VKEY", "BNK_VKEYDEL", "BNK_ACTIVE", "VALAAM_NAME", "REGNMDTEND", "LIKVIDSTATUSDTEND", "CURATOR", "CURATORDEPTISN", "DEALER", "REPSYNKISN") AS 
  (
Select --+ Ordered Use_Nl(PSb Sbb PsbCl UpdBy Cr)  optimizer_features_enable('11.1.0.6') 
 S."ISN" subjisn ,S."CLASSISN",S."ROLECLASSISN",S."COUNTRYISN",S."BRANCHISN",
 S."JURIDICAL",S."RESIDENT",S."VIP",S."INN",S."ID",S."FID",S."CODE",
 S."SHORTNAME",S."FULLNAME",S."ACTIVE",S."UPDATED",S."UPDATEDBY",
Decode(S."LICENSENO",Null,
 (select Min(ExtId )from ais.subdoc sd where sd.subjisn=s.isn and classisn in (1735713203,974582025)),
 S."LICENSENO") "LICENSENO",

Decode(S."LICENSEDATE",Null,
 (select trunc( Min(SIGNED ) ) from ais.subdoc sd where sd.subjisn=s.isn and classisn in (1735713203,974582025)),
 S."LICENSEDATE")  LICENSEDATE,
 S."OKPO",S."OKOHX",S."SYNISN",
 S."CREATEDBY",S."CREATED",
 S."PROFITTAXFLAG",S."PARENTISN",
 S."NAMELAT",S."ORGFORMISN",S."REMARK",S."KPP",S."SEARCHNAME",S."SECURITYLEVEL",S."OGRN",S."OKVED",S."SECURITYSTR",S."REGNM",S."SBCLASS",S."RELICEND",S."LICEND",S."NREZNAME",S."LIKVIDSTATUS",S."R_BEST",S."R_FITCH",S."R_MOODYS",S."R_SP",S."R_WEISS",S."ADDRCODE",S."ADDRTYPE",S."CITYISN",S."POSTCODE",S."ADDRESS",S."PARENTSUBJ",
Psb.FullName ParentFullName,
PsbCl.ShortName ParentClass,
PSb.INN ParentINN,
UpdBy.FullName UPDATEDBY_NAME,

Sbb.VKEY BNK_VKEY,

Sbb.VKEYDEL BNK_VKEYDEL,

Sbb.ACTIVE  BNK_ACTIVE,
VALAAM_NAME,
REGNMDTEND,LikvidStatusDTEND,
Cr.ShortName Curator,
s.CrDeptisn CuratorDeptIsn,
s.Dealer,


              (
              case
                 when S.JURIDICAL  ='Y' and S.RESIDENT='Y' Then
                    (Case
         /*если филиал*/when    S.PARENTSUBJ is not null and S.INN=PSb.INN   Then
                          (Case /*если филиал и головная ликвидированна*/
                             When
                              (Select Max(Isn) from obj_attrib oa where oa.objisn=to_number(S.PARENTSUBJ) and oa.classisn=c.get('AttrLIQUIDATION') and Dateend<=sysdate ) Is not null
/*ais.u.getname(ais.utl.getnumberattrib( to_number(S.PARENTSUBJ),'LIQUIDATION','C',DATE # Sq(prompt ('Отчетная дата'; 'Date'))# )) is not null*/
                             then Nvl(ais.REINS_UTILS.GETSUCCESSOR(to_number(S.PARENTSUBJ),SYSdate ), to_number(S.PARENTSUBJ))
                           else
                            to_number(S.PARENTSUBJ) End
                           )
   /*если ликвидация*/When
/* ais.u.getname(ais.utl.getnumberattrib(S.SUBJISN,'LIQUIDATION','C',DATE #sq( prompt ('Отчетная дата'; 'Date'))# )) is not null  */
                         (Select Max(Isn) from obj_attrib oa where oa.objisn=S.ISN and oa.classisn=c.get('AttrLIQUIDATION') and Dateend<=sysdate) Is not null
                      Then Nvl(ais.REINS_UTILS.GETSUCCESSOR(S.ISN,sysdate ),S.Isn)
                   else
                      S.ISN End)
/* не резиденты : совпадают регномера и страна регистрации */
           when  S.RESIDENT='N' and  S.PARENTSUBJ is not null and S.REGNM = (Select Max(o.Val) keep (dense_rank last order by Nvl(Dateend,'01-jan-3000')) from ais.obj_attrib o where o.objisn=Psb.isn and  o.classisn=1813887503)
                 AND S.COUNTRYISN=PSB.COUNTRYISN
           THEN to_number(S.PARENTSUBJ)



             else S.isn
            end
              ) repsynkisn





from (

Select  --+ Ordered Use_Nl(Sc) Use_Merge(Sa)
 SB.*,
 (Select Max(o.Val) keep (dense_rank last order by Nvl(Dateend,'01-jan-3000')) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1813887503)   REGNM,
  (Select Max(o.DATEEND)  from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1813887503)   REGNMDTEND,
 Sc.Shortname SBCLASS,
(select Nvl(Min(Nvl(Dateend,decode(isn,null,null,'01-jan-3000'))) ,'01-jan-1900')from ais.subdoc where subjisn=sb.isn and classisn=1735713203) ReLicEnd,
(select Nvl(Min(Nvl(Dateend,decode(isn,null,null,'01-jan-3000'))) ,'01-jan-1900')from ais.subdoc where subjisn=sb.isn and classisn=974582025) LicEnd,
 CASE  When RESIDENT  ='N' then Nvl(NAMELAT,Sb.FULLNAME ) else  Sb.FULLNAME end NRezName,
(Select Max(o.Val) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=2453825203)   LikvidStatus,
  (Select Max(o.DATEEND) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=2453825203)   LikvidStatusDTEND,

 (Select Max(o.Val) keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1979965303)   R_BEST,
 (Select Max(o.Val) keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc)from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1979962403)   R_FITCH,
 (Select Max(o.Val) keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc)from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1979960103)   R_MOODYS,
 (Select Max(o.Val) keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc)from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1979958603)   R_SP,
 (Select Max(o.Val)keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1979966903)   R_WEISS,    
 
(Select Max(o.Val)keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=3019835703) VALAAM_NAME,

(Select Max(so.humanisn)keep(dense_rank first  order by so.Updated)
                                                              from subowner so, storage_source.rep_dept rd
                                                                            where  so.subjisn=SB.isn and so.deptisn = rd.deptisn
                                                                              and  (rd.dept1isn =3381054603 or rd.oisn = 1746865203 or rd.dept2isn in(3381054003,1393203203))
                                                                            ) CrIsn,
(Select Max(so.DeptIsn)keep(dense_rank first  order by so.Updated)
                                                              from subowner so, storage_source.rep_dept rd
                                                                            where  so.subjisn=SB.isn and so.deptisn = rd.deptisn
                                                                              and  (rd.dept1isn =3381054603 or rd.oisn = 1746865203 or rd.dept2isn in(3381054003,1393203203))
                                                                             ) CrDeptIsn,

(Select Max(o.Valn)keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1693932103) Dealer,
 Sa.AddrCode,
 sa.AddrType,
 Sa.cityisn,
 Sa.postcode,
 Sa.address,
 Decode(Sb.Parentisn,Null,Null,
( Select /*+ optimizer_features_enable('11.1.0.6') */
    Isn
 from aIS.SubJect_T SSB
  --Where ParentIsn Is null
  Where Connect_By_ISLeaf=1
 Start With SSB.ISn=Sb.Parentisn
 Connect By Prior SSB.Parentisn=SSB.Isn and Nvl(SSB.Resident,'M')=Nvl(Sb.resident,'M')
)) ParentSubj

From TT_ROWID T,
 AIS.SUBJECT_T SB,ais.dicti SC,
( 
 select subjisn,
Max(dc.code) keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) AddrCode,
Max(dc.shortname) keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) AddrType,
Max(countryisn) keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) countryisn,
Max(cityisn)keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) cityisn,
Max(postcode) keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) postcode,
Max(address) keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) address
from TT_ROWID T,Ais.SUBADDR_T sa ,dicti dc
Where sa.classisn=dc.isn
AND T.ISN=SA.subjisn

Group by subjisn
) Sa
 Where  T.ISN=Sb.iSN
 AND Sc.Isn(+)=SB.classisn
 And Sb.isn=sa.subjisn(+)
 ) S,Ais.Subject_T PSb,SubBank Sbb,Dicti PsbCl, Ais.Subject_T UpdBy, Ais.Subject_t Cr
 Where S.ParentSubj=Psb.Isn(+)
 And S.Isn=Sbb.Isn(+)
 and PSb.classisn=PsbCl.Isn(+)
 and S.UpdatedBy=UpdBy.Isn(+)
 and s.CrIsn = Cr.Isn(+)
 )

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_SUBJ_BEST_ADDR" ("SUBJISN", "ADDRISN", "SUBADDR") AS 
  select
-- Источник данных для витрины с оптимальным адресом, выгружаемой функцией Крылова
  S.SUBJISN,
  S.AddrISN,
  AIS.ADDR_UTILS.GetSubAddr(S.AddrISN, 'irdtvmshbf') as SubAddr
from (
  select
    S.SUBJISN,
    TO_NUMBER(AIS.ADDR_UTILS.GetAddrIsn(S.SUBJISN)) as AddrISN
  from (
    select --+ ordered use_nl(T S SA)
    distinct
      SA.SUBJISN  
    from 
      TT_ROWID T,    
      AIS.SUBJECT_T S,  -- для удаления паразитных записей, которые есть в SubAddr и нет в Subject
      AIS.SUBADDR_T SA
    where
      T.ISN = S.ISN
      and S.ISN = SA.SUBJISN
  ) S    
) S

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_SUBJECT_ATTRIB" ("SUBJISN", "HOME_ADDRISN", "HOME_CITYISN", "HOME_REGIONISN", "HOME_ZIP", "POST_ADDRISN", "POST_CITYISN", "POST_REGIONISN", "TEMPORARY_ADDRISN", "TEMPORARY_CITYISN", "TEMPORARY_REGIONISN", "PASSPORT_ADDRISN", "PASSPORT_CITYISN", "PASSPORT_REGIONISN", "PASSPORT_ZIP", "PASSPORT_ADDRESS", "FACT_ADDRISN", "FACT_CITYISN", "FACT_REGIONISN", "JUR_ADDRISN", "JUR_CITYISN", "JUR_REGIONISN", "VIPCLASSISN", "SUBJSECURITYSTR", "ADDRESSSECURITYSTR", "PHONESECURITYSTR", "DRIVINGDATEBEG", "AGEGROUP", "CITIZENSHIP", "N_KIDS", "MARRIAGESTATEISN", "FAMILYSTATEISN", "STOADAYSPAY", "DRIVERST", "MOTIVATION", "NO_MAIL", "EMAIL", "SERV_PHONE", "MOBILEPHONE", "PHONE", "HOME_PHONE", "BIRTHDAY", "ADDRISN", "CITYISN", "REGIONISN", "STO_PRIORITY", "JURIDICAL", "SUBJ_CLASSISN", "SMS_PHONE", "DENY_INFO_SMS", "DENY_PROMO_SMS", "DENY_INFO_EMAIL", "DENY_PROMO_EMAIL", "AGENTCATEGORYISN", "BESTADDRISN", "BESTADDR", "MAINOKVEDISN", "CLIENTISARRESTED", "DENY_INFO_POST", "DENY_INFO_CALL", "MONITORINGISN", "MONITORINGBEG", "MONITORINGEND", "MONITORINGUPD") AS 
  with tt as (select ISN from tt_rowid)

select --+ ordered use_nl(S SJ OptAddr)
  S.SUBJISN,

  S.HOME_ADDRISN,
  S.HOME_CITYISN,
  S.HOME_REGIONISN,
  S.HOME_ZIP,

  S.POST_ADDRISN,
  S.POST_CITYISN,
  S.POST_REGIONISN,

  S.TEMPORARY_ADDRISN,
  S.TEMPORARY_CITYISN,
  S.TEMPORARY_REGIONISN,

  S.PASSPORT_ADDRISN,
  S.PASSPORT_CITYISN,
  S.PASSPORT_REGIONISN,
  S.PASSPORT_ZIP,
  S.PASSPORT_ADDRESS,

  S.FACT_ADDRISN,
  S.FACT_CITYISN,
  S.FACT_REGIONISN,

  S.JUR_ADDRISN,
  S.JUR_CITYISN,
  S.JUR_REGIONISN,

  S.VIPCLASSISN,
  S.SUBJSECURITYSTR,
  S.ADDRESSSECURITYSTR,
  S.PHONESECURITYSTR,
  S.DRIVINGDATEBEG,
  S.AGEGROUP,
  S.CITIZENSHIP,
  S.N_KIDS,
  S.MARRIAGESTATEISN,
  S.FAMILYSTATEISN,
  S.STOADAYSPAY,
  S.DRIVERST,
  S.MOTIVATION,
  S.NO_MAIL,  -- запрет на информационную рассылку: Y - запрещено, N - разрешено
  S.EMAIL,
  S.SERV_PHONE,
  S.MOBILEPHONE,
  S.PHONE,
  S.HOME_PHONE,
  S.BIRTHDAY,
  S.AddrISN,
  S.CITYISN,
  S.REGIONISN,
  S.STO_PRIORITY,
  SJ.JURIDICAL,
  SJ.CLASSISN as SUBJ_CLASSISN,
  S.SMS_Phone,

  -- sts 09.04.2012
  -- Для сотрудников ИГС считается, что они на все согласны :)
  -- См. ф-ию AIS.SMS.pGetSubjConsent - сделал как там
  decode(SJ.CLASSISN, 497, 'N', Deny_Info_SMS) as Deny_Info_SMS,         -- запрет на информационное оповещение по СМС
  decode(SJ.CLASSISN, 497, 'N', Deny_Promo_SMS) as Deny_Promo_SMS,       -- запрет на рекламное оповещение по СМС
  decode(SJ.CLASSISN, 497, 'N', Deny_Info_EMail) as Deny_Info_EMail,     -- запрет на информационное оповещение по email
  decode(SJ.CLASSISN, 497, 'N', Deny_Promo_EMail) as Deny_Promo_EMail,   -- запрет на рекламное оповещение по email
  S.AgentCategoryISN,    -- КАТЕГОРИЯ АГЕНТА

  -- Оптимальный адрес по версии Крылова
  OptAddr.AddrISN as BestAddrISN,  -- FK(SubAddr) - ISN
  OptAddr.SubAddr as BestAddr,     -- Строка адреса

  S.MainOKVEDISN,  -- ОСНОВНОЙ ОКВЭД FK(Dicti)
  Q.CLIENTISARRESTED,  -- Признак клиента под арестом

  -- VAA 11.10.2013 55417293503
  decode(SJ.CLASSISN, 497, 'N', Deny_Info_Post) as Deny_Info_Post,     -- запрет на информационное оповещение в виде бумажной почтовой рассылки
  decode(SJ.CLASSISN, 497, 'N', Deny_Info_Call) as Deny_Info_Call,     -- запрет на информационное оповещение в виде звонка

  -- kuzmin(04.12.2013) task(58045024503)
  -- МОНИТОРИНГ
  S.MonitoringISN,
  S.MonitoringBeg,
  S.MonitoringEnd,
  S.MonitoringUpd

from (
  select
    coalesce(O.O_SUBJISN, ADR.ADR_SUBJISN, PH.PH_SUBJISN, VIP.VIP_SUBJISN, SH.SH_SubjISN) as SUBJISN,

    O.DrivingDateBeg,
    O.AgeGroup,
    O.Citizenship,
    O.N_Kids,
    O.MarriageStateISN,
    O.FamilyStateISN,
    O.STOADaysPay,
    O.DriverSt,
    O.Motivation,
    O.STO_Priority,
    O.AgentCategoryISN,
    O.MainOKVEDISN,  -- ОСНОВНОЙ ОКВЭД

    nvl(O.No_Mail, 'N') as No_Mail,  -- запрет на информационную рассылку: Y - запрещено, N - разрешено (по умолчанию)
    -- доступность SMS рассылок по типам:
    -- запрет на информационное оповещение по СМС (по умолчанию разрешено)
    nvl(O.Deny_Info_SMS, 'N') as Deny_Info_SMS,
    -- запрет на рекламное оповещение по СМС (по умолчанию запрещено)
    nvl(O.Deny_Promo_SMS, 'Y') as Deny_Promo_SMS,
    -- запрет на информационное оповещение по email (по умолчанию разрешено)
    nvl(O.Deny_Info_EMail, 'N') as Deny_Info_EMail,
    -- запрет на рекламное оповещение по email (по умолчанию запрещено)
    nvl(O.Deny_Promo_EMail, 'Y') as Deny_Promo_EMail,

    -- VAA 11.10.2013 55417293503
    -- запрет на информационное оповещение в виде бумажной почтовой рассылки (по умолчанию разрешено)
    nvl(O.Deny_Info_Post, 'N') as Deny_Info_Post,
    -- запрет на информационное оповещение в виде звонка (по умолчанию разрешено)
    nvl(O.Deny_Info_Call, 'N') as Deny_Info_Call,



    ADR.*,
    PH.*,
    VIP.*,

    SH.BIRTHDAY,

    -- kuzmin(04.12.2013) task(58045024503)
    -- МОНИТОРИНГ
    O.MonitoringISN,
    O.MonitoringBeg,
    O.MonitoringEnd,
    O.MonitoringUpd

  from
    TT,
    -- OBJ_ATTRIB
    (select --+ ordered use_nl(tt oa) use_hash(D)
        oa.ObjISN as O_SubjISN,

        max(decode(D.isn, 2647785103, oa.ValD)) as DrivingDateBeg,
        max(decode(D.isn, 1686027703, oa.Val)) as AgeGroup,
        max(decode(D.isn, 2200008503, oa.Val)) as Citizenship,
        max(decode(D.isn, 2626755703, oa.ValN)) as N_Kids,
        max(decode(D.isn, 2626755803, oa.ValN)) as MarriageStateISN,
        max(decode(D.isn, 3028738303, oa.ValN, 2638580803, oa.ValN)) as FamilyStateISN,
        max(decode(D.isn, 1343855503, oa.ValN)) as STOADaysPay,
        max(decode(D.isn, 1686031603, oa.Val)) as DriverSt,
        max(decode(D.isn, 1428587803, oa.ValN)) as Motivation,
        max(decode(D.isn, 1683459803, oa.ValN)) as STO_Priority,
        max(decode(D.isn, 2291578303, oa.ValN)) as AgentCategoryISN,  -- КАТЕГОРИЯ АГЕНТА
        max(decode(D.isn, 3994769103, oa.ValN)) as MainOKVEDISN,  -- ОСНОВНОЙ ОКВЭД

        -- запреты/разрешения без разбивки по типам
        -- sts 09.04.2012 - раньше для поля No_Mail было по другому (см. SYSTEM.DDL_UNDO_TEXT).
        -- Сейчас видимо способ хранения поменялся
        -- А это поле используется в отчетах без разбивки на тип, то так и оставил, только переделал,
        -- чтобы возвращалось корректное значение.
        max(decode(D.isn, 3096320703, nvl2(oa.ValN, 'Y', 'N'))) as No_Mail,  -- запрет на информационную рассылку (по умолчанию разрешена)
        -- А тут возвращаю доступность SMS рассылок по типам:
        -- запрет на информационное оповещение по СМС
        max(decode(D.isn, 3096320703, decode(oa.ValN, 3546162703, 'Y', 'N'))) as Deny_Info_SMS,
        -- запрет на рекламное оповещение по СМС
        max(decode(D.isn, 2896523903, decode(oa.ValN, 3546162703, 'N', 'Y'))) as Deny_Promo_SMS,
        -- запрет на информационное оповещение по email
        max(decode(D.isn, 3096320703, decode(oa.ValN, 3546162503, 'Y', 'N'))) as Deny_Info_EMail,
        -- запрет на рекламное оповещение по email
        max(decode(D.isn, 2896523903, decode(oa.ValN, 3546162503, 'N', 'Y'))) as Deny_Promo_EMail,

        -- VAA 11.10.2013 55417293503
        -- запрет на информационное оповещение в виде бумажной почтовой рассылки
        max(decode(D.isn, 3096320703, decode(oa.ValN, 3546162303, 'Y', 'N'))) as Deny_Info_Post,
        -- запрет на информационное оповещение в виде звонка
        max(decode(D.isn, 3096320703, decode(oa.ValN, 3546162103, 'Y', 'N'))) as Deny_Info_Call,

        -- kuzmin(04.12.2013) task(58045024503)
        -- МОНИТОРИНГ
        max(decode(D.isn, 3002827403, oa.ValN)) as MonitoringISN,
        max(decode(D.isn, 3002827403, oa.datebeg)) as MonitoringBeg,
        max(decode(D.isn, 3002827403, oa.dateend)) as MonitoringEnd,
        max(decode(D.isn, 3002827403, oa.updatedby)) as MonitoringUpd

      from
        tt,
        AIS.obj_attrib oa,
       (select
          connect_by_root d.isn as root,
          d.isn
        from dicti d
        start with d.isn in (
          2647785103,              -- ВОДИТЕЛЬСКИЙ СТАЖ
          1686027703,              -- ВОЗРАСТНАЯ ГРУППА
          2200008503,              -- ГРАЖДАНСТВО
          2626755703,              -- КОЛИЧЕСТВО ДЕТЕЙ
          2626755803,              -- НАХОЖДЕНИЕ В БРАКЕ
          3028738303, 2638580803,  -- СЕМЕЙНОЕ ПОЛОЖЕНИЕ
          1343855503,              -- СРОК ОПЛАТЫ СТОА
          1686031603,              -- ВОДИТЕЛЬСКИЙ СТАЖ (категории),
          1428587803,              -- МОТИВАЦИОННАЯ ГРУППА
          3096320703,              -- ЗАПРЕТ НА ИНФОРМАЦИОННОЕ ОПОВЕЩЕНИЕ - c.get('ATTRNoINFOFLAG')
          2896523903,              -- СОГЛАСИЕ НА РЕКЛАМНОЕ ОПОВЕЩЕНИЕ - c.get('ATTRINFOFLAG')
          1683459803,              -- ПРИОРИТЕТ ПРИ НАПРАВЛЕНИИ
          2291578303,              -- КАТЕГОРИЯ АГЕНТА
          3994769103,              -- ОСНОВНОЙ ОКВЭД
          3002827403               -- МОНИТОРИНГ
        )
        connect by prior d.isn = d.parentisn
        ) D
      where
        tt.isn = oa.ObjISN
        and oa.classisn = D.isn
        and oa.discr = 'C'
        and sysdate between nvl(oa.datebeg, sysdate) and nvl(oa.dateend, sysdate)
      group by oa.ObjISN
      ) O,
      -- SUBADDR
      ( select
          adr.SubjISN as Adr_SubjISN,
          max(decode(adr.ClassISN, 471, adr.AddrISN)) as Home_AddrISN,
          max(decode(adr.ClassISN, 471, adr.cityisn)) as Home_CityISN,
          max(decode(adr.ClassISN, 471, adr.regionisn)) as Home_RegionISN,
          max(decode(adr.ClassISN, 471, adr.postcode)) as Home_ZIP,

          max(decode(adr.ClassISN, 472, adr.AddrISN)) as Post_AddrISN,
          max(decode(adr.ClassISN, 472, adr.cityisn)) as Post_CityISN,
          max(decode(adr.ClassISN, 472, adr.regionisn)) as Post_RegionISN,

          max(decode(adr.ClassISN, 1166402903, adr.AddrISN)) as Temporary_AddrISN,
          max(decode(adr.ClassISN, 1166402903, adr.cityisn)) as Temporary_CityISN,
          max(decode(adr.ClassISN, 1166402903, adr.regionisn)) as Temporary_RegionISN,

          max(decode(adr.ClassISN, 11441319, adr.AddrISN)) as Passport_AddrISN,
          max(decode(adr.ClassISN, 11441319, adr.cityisn)) as Passport_CityISN,
          max(decode(adr.ClassISN, 11441319, adr.regionisn)) as Passport_RegionISN,
          max(decode(adr.ClassISN, 11441319, adr.postcode)) as Passport_ZIP,
          max(decode(adr.ClassISN, 11441319, adr.address)) as Passport_Address,

          max(decode(adr.ClassISN, 15522816, adr.AddrISN)) as Fact_AddrISN,
          max(decode(adr.ClassISN, 15522816, adr.cityisn)) as Fact_CityISN,
          max(decode(adr.ClassISN, 15522816, adr.regionisn)) as Fact_RegionISN,

          max(decode(adr.ClassISN, 470, adr.AddrISN)) as Jur_AddrISN,
          max(decode(adr.ClassISN, 470, adr.cityisn)) as Jur_CityISN,
          max(decode(adr.ClassISN, 470, adr.regionisn)) as Jur_RegionISN,

          -- город и регион для V_BM (тупо берем максимальный)
          max(adr.AddrISN) as AddrISN,
          max(adr.cityisn) as CityISN,
          max(adr.regionisn) as RegionISN
        from (
          select --+ ordered use_nl(adr cty reg)
            adr.AddrISN,
            adr.SubjISN,
            adr.ClassISN,
            adr.CityISN,
            nvl(decode(reg.ParentISN, 0, to_number(null), reg.ParentISN), reg.ISN) as RegionISN,
            adr.postcode,
            adr.address
          from
           (select --+ ordered use_nl(t adr)
              adr.subjisn,
              adr.classisn,
              max(adr.ISN) keep(dense_rank first order by adr.Updated desc, adr.ISN desc) as AddrISN,
              max(adr.cityisn) keep(dense_rank first order by adr.Updated desc, adr.ISN desc) as cityisn,
              max(adr.postcode) keep(dense_rank first order by adr.Updated desc, adr.ISN desc) as postcode,
              max(adr.address) keep(dense_rank first order by adr.Updated desc, adr.ISN desc) as address
            from
              tt t,
              ais.subaddr_t adr
            where
              t.ISN = adr.SubjISN
              and nvl(adr.ACTIVE, 'S') <> 'S'  -- учитываем только активные записи
            group by adr.subjisn, adr.classisn
           ) adr,
             ais.city cty,
             ais.region reg
           where
             adr.cityisn = cty.isn(+)
             and cty.regionisn = reg.isn(+)
        ) adr
        group by adr.SubjISN
      ) Adr,
      -- SUBPHONE
      ( select --+ ordered use_nl(t P)
          Ph.*,
          decode(Ph.SMSPhone,
                 null, null,
                 AIS.SMS.pFormatPhone(Ph.SMSPhone)
          ) as SMS_Phone
        from
          ( select --+ ordered use_nl(t P dx)
              P.SubjISN as PH_SubjISN,
              -- e-mail
              max(decode(P.CLASSISN, 424, P.PHONE)) keep(dense_rank first order by decode(P.CLASSISN, 424, 0, 1), p.Updated desc) as EMAIL,
              -- служ. мобильный
              max(decode(P.CLASSISN, 1482515703, P.PHONE)) keep(dense_rank first order by decode(P.CLASSISN, 1482515703, 0, 1), p.Updated desc) as SERV_PHONE,
              -- мобильный
              max(decode(P.CLASSISN, 25152816, P.PHONE)) keep(dense_rank first order by decode(P.CLASSISN, 25152816, 0, 1), p.Updated desc) as MOBILEPHONE,
              -- телефон
              max(decode(P.CLASSISN, 420, P.PHONE)) keep(dense_rank first order by decode(P.CLASSISN, 420, 0, 1), p.Updated desc) as PHONE,
              -- домашний
              max(decode(P.CLASSISN, 29155416, P.PHONE)) keep(dense_rank first order by decode(P.CLASSISN, 29155416, 0, 1), p.Updated desc) as HOME_PHONE,
              -- Инфа для СМС рассылок
              max(nvl2(dx.classisn1, P.Remark || P.Phone, null)) keep(dense_rank first order by nvl2(dx.classisn1, 0, 1), dx.classisn2, p.Updated desc) as SMSPhone
            from
              tt,
              AIS.subphone_t P,
              AIS.dicx dx
            where
              tt.ISN = P.SubjISN
              and dx.classisn(+) = 3378432503 --c.get('SMS_PHONE_TYPES')   -- Виды телефонов для смс-рассылок
              and p.ClassIsn = dx.classisn1(+)
            group by P.SubjISN
          ) Ph
      ) Ph,
      -- VIP
      ( select
          V.*
        from (
          select --+ ordered use_nl(t s sh sa sp) use_hash(VIP)
            s.ISN as VIP_SubjISN,
            max(VIP.ISN) as VipClassISN,
            max(sh.SecurityStr) as SubjSecurityStr,
            conc(distinct sa.SecurityStr) as AddressSecurityStr,
            conc(distinct sp.SecurityStr) as PhoneSecurityStr
          from
            tt_rowid t,
            ais.subject_t s,
            (select isn from dicti d start with isn in (11634718, 2431326703) connect by prior isn = parentisn) VIP,
            ais.subhuman_t sh,
            ais.subaddr_t sa,
            ais.subphone_t sp
          where
            t.ISN = s.ISN
            and S.ClassISN = VIP.ISN(+)
            and S.ISN = sh.ISN(+)
            and S.ISN = sa.SubjISN(+)
            and S.ISN = sp.SubjISN(+)
          group by
            s.ISN
         ) V
       where
         V.VipClassISN is not null
         or
         coalesce(V.SubjSecurityStr, V.AddressSecurityStr, V.PhoneSecurityStr) is not null
      ) VIP,
      (select --+ ordered use_nl(t SH)
         SH.ISN as SH_SubjISN,
         SH.BIRTHDAY, ROWNUM AS rn
       from
         tt_rowid t,
         AIS.SUBHUMAN_T SH
       where
         t.ISN = SH.ISN
         and SH.BIRTHDAY is not null   -- отбираем только записи со значащими показателями
      ) SH
  where
    TT.ISN = O.O_SUBJISN(+)
    and TT.ISN = ADR.ADR_SUBJISN(+)
    and TT.ISN = PH.PH_SUBJISN(+)
    and TT.ISN = VIP.VIP_SUBJISN(+)
    and TT.ISN = SH.SH_SubjISN(+)
  ) S,
    AIS.SUBJECT_T SJ,
    STORAGE_SOURCE.SUBJ_BEST_ADDR OptAddr,
    (select --+ ordered use_nl(t Q)
       -- sts 13.03.2013
       -- код выдран из АИС: AIS.AGRC.FCUR_WarnN
       -- Правда там возвращается курсор, сортируется по Q.DATESEND и отдается дельфе. Чо там дальше делается - ХЗ
       -- Тут беру max()
       Q.ObjISN as SUBJISN,
       max(decode(Q.REQUEST, '1', 'Y', 'N')) as CLIENTISARRESTED  -- Признак "Арест"
     from tt_rowid t, AIS.QUEUE Q
     where
       t.ISN = Q.ObjISN
       and Q.ClassISN = 1175052903   -- СТРАХОВАТЕЛИ/ПОЛИСЫ ПОД АРЕСТОМ (NEW) / C.Get('qeInArrestedNew')
       and Q.ObjISN2 is null
       and Q.FormISN = 33024916  -- ЮР. ЛИЦО / C.GET('fmLegal')
       and Q.Status = 'W'        -- как в АИС
       and Q.Request = '1'       -- Пока только одно поле про арест, поэтому сразу отбираю только арестованых
     group by Q.ObjISN
    ) Q

where
  S.SUBJISN = SJ.ISN
  and S.SUBJISN = OptAddr.SUBJISN(+)
  and S.SUBJISN = Q.SubjISN(+)


  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BODYDEBCRE" ("BASEISN", "DB", "DE", "BASESALDO", "SUBACCISN", "CODE", "BASEAMOUNTRUB", "BASEDAMOUNTRUB", "BASECAMOUNTRUB", "BASEDATEVAL", "FID", "SUBJISN", "CURRISN", "AGRISN", "BASEAMOUNT", "BASEDAMOUNT", "BASECAMOUNT") AS 
  Select /*+leading(dt) use_nl(b) Index(dt) index(b x_st_buhbody_base)*/
      Dt.BaseIsn, db,de,
      Sum(dAmountrub)-sum(camountrub) as BaseSaldo,
      Max(subaccisn) subaccisn,Max(code) Code,
      Max(nvl(baseDAMOUNTRUB,0)-nvl(baseCAMOUNTRUB,0)) as BaseAmountRub,
      Max(basedamountrub) as BaseDamountrub,
      Max(basecamountrub) as BaseCamountrub,
      Max(basedateval) as BaseDateval,
      Max(fid) as fid, Max(subjisn) as subjisn, Max(currisn) as currisn, Max(agrisn) as agrisn,
      Max(nvl(baseDAMOUNT,0)-nvl(baseCAMOUNT,0)) as BaseAmount,
      Max(basedamount) as BaseDamount,
      Max(basecamount) as BaseCamount
From
(
/*все варианты Dateval и Datequit в один не прирывный столбик - набор интервалов*/
Select /*+Index(dbe ) */
  dbe.BaseIsn,dbe.Dateval Db,Nvl(Lead(dbe.Dateval) over (PARTITION by dbe.BaseIsn Order by dbe.Dateval )-1,'01-jan-3000') De
from(

  Select /*+ index(b x_st_buhbody_base)  */Distinct b.BaseIsn,b.Dateval from STORAGES.st_buhbody b
  Where b.BaseIsn  in (select isn from tt_rowid)   -- baseisn = 397668316)

  Union

  Select /*+ index(b x_st_buhbody_base)  */ Distinct b.BaseIsn,b.Datequit from STORAGES.st_buhbody b
  Where b.BaseIsn in (select isn from tt_rowid)
) dbe
 ) Dt, STORAGES.st_buhbody b
 Where Dt.BaseIsn=b.baseIsn
   and (b.Dateval<=dt.De AND nvl(b.Datequit,'01-jan-3000')>dt.De)
   --and dt.baseisn=397668316   -----
   --and dt.loadisn = 10.2002
 group by Dt.BaseIsn, dt.db, dt.de
 
 
  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUHBODY" ("BASEISN", "PARENTISN", "BODYISN", "CODE", "DAMOUNTRUB", "CAMOUNTRUB", "DATEVAL", "DATEQUIT", "QUITSTATUS", "OPRISN", "SUBACCISN", "BALANCE", "BASEDATEVAL", "FID", "BASEDAMOUNTRUB", "BASECAMOUNTRUB", "SUBJISN", "CURRISN", "AGRISN", "BASEDAMOUNT", "BASECAMOUNT", "DAMOUNT", "CAMOUNT") AS 
  Select --+ Ordered Use_Nl(b bh)
 unique  S."BASEISN",S."PARENTISN",S."BODYISN",S."CODE",S."DAMOUNTRUB",S."CAMOUNTRUB",
 S."DATEVAL",S."DATEQUIT",S."QUITSTATUS",S."OPRISN",S."SUBACCISN",
 Nvl(B.Damountrub,0)-Nvl(B.camountrub,0) as balance,
  b.dateval as basedateval,
  bh.Fid ,
  b.damountrub as basedamountrub,b.camountrub as basecamountrub,b.subjisn as subjisn,b.currisn,b.agrisn,
  b.damount as basedamount,b.camount as basecamount,S."DAMOUNT", S."CAMOUNT"
from(
Select
Decode(D.Isn,null,b.Isn,/*идем вверх только по проводкам операций "автоматические" - 200 02 03 */
Decode(b.Parentisn,null,b.Isn,b.isn,b.isn, /*бывают проводки, ссылающиеся сами на себя*/
( select Max(b1.Isn) KEEP ( dense_rank FIRST ORDER BY decode(b1.subaccisn,b.subaccisn,0,1),Level desc )
   from buhbody b1
   Start with b1.isn=b.isn
   connect  by nocycle  prior  b1.parentisn= b1.isn
   ))) BaseIsn,
     b.ParentIsn,b.Isn BodyIsn,ba.ID Code, Nvl(b.damountrub,0)damountrub ,
  Nvl(b.camountrub,0) camountrub, Nvl(b.damount,0) damount , Nvl(b.camount,0) camount,
  b.Dateval, Nvl(Decode(b.quitstatus,null,b.Datequit,null),'01-jan-3000') Datequit,b.QuitStatus,b.OprIsn,
  b.subaccisn
  from /*(select rowid as RId from buhbody where isn in (8329848903,8492132803,8668517903, 8249586303)) */tt_rowid t, buhbody b,buhsubacc ba,
  (Select * from   dicti d
     Where d.parentisn=759033300 and code in('200','02','03') -- необходимо ограничение по операциям 200 02 03
     ) D
  where t.isn = b.isn
  and status='А'
  And (b.quitstatus is not null or b.dateval<Nvl(b.Datequit,'01-jan-3000'))
  and b.oprisn=d.isn(+)        -- здесь ограничение по целой подветке автоматических операций (200 02 03)
  and b.subaccisn=ba.isn
  and (b.Code Like'77%' or b.Code Like'78%'  Or ba.Id like '7619%') -- ограничение по счетам

) s, buhbody b,buhhead bh
Where s.baseisn=b.isn
and b.headisn=bh.isn

Union all


Select --+ Ordered Use_Nl(b bh)
 unique  S."BASEISN",S."PARENTISN",S."BODYISN",S."CODE",
 Nvl(S."DAMOUNTRUB",0)"DAMOUNTRUB", Nvl(S."CAMOUNTRUB",0) "CAMOUNTRUB",
 S."DATEVAL",S."DATEQUIT",S."QUITSTATUS",S."OPRISN",S."SUBACCISN",
 Nvl(B.Damountrub,0)-Nvl(B.camountrub,0) as balance,
  b.dateval as basedateval,
  bh.Fid ,
  b.damountrub as basedamountrub,b.camountrub as basecamountrub,b.subjisn as subjisn,b.currisn,b.agrisn,
  b.damount as basedamount,b.camount as basecamount,Nvl(S."DAMOUNT",0)"DAMOUNT", Nvl(S."CAMOUNT",0) "CAMOUNT"
from(
Select
Decode(D.Isn,null,b.Isn,/*идем вверх только по проводкам операций "автоматические" - 200 02 03 */
Decode(b.Parentisn,null,b.Isn,b.isn,b.isn, /*бывают проводки, ссылающиеся сами на себя*/
( select Max(b1.Isn) KEEP ( dense_rank FIRST ORDER BY decode(b1.subaccisn,b.subaccisn,0,1),Level desc )
   from buhbody b1
   Start with b1.isn=b.isn
   connect  by nocycle  prior  b1.parentisn= b1.isn
   ))) BaseIsn,
     b.ParentIsn,b.Isn BodyIsn,ba.ID Code,

      Case When dg.QUEISN Is null and quitstatus='Ч' Then decode(dAmountrub,null,0,RemainRub)
      else DAmountrub end
      DAmountrub,
      Case When dg.QUEISN Is null and quitstatus='Ч' Then decode(CAmountrub,null,0,RemainRub)
      else CAmountrub end
      CAmountrub,
      Case When dg.QUEISN Is null and quitstatus='Ч' Then decode(dAmount,null,0,Remain)
      else DAmount end
      DAmount,
      Case When dg.QUEISN Is null and quitstatus='Ч' Then decode(CAmount,null,0,Remain)
      else CAmount end
      CAmount,


     b.Dateval, Nvl(Decode(b.quitstatus,null,b.Datequit,null),'01-jan-3000') Datequit,b.QuitStatus,b.OprIsn,
     b.subaccisn
  from /*(select rowid as RId from buhbody where isn in (8329848903,8492132803,8668517903, 8249586303)) */
  tt_rowid t, buhbody b,buhsubacc ba,
  (Select * from   dicti d
     Where d.parentisn=759033300 and code in('200','02','03') -- необходимо ограничение по операциям 200 02 03
     ) D, docgrp dg
  where t.isn = b.isn
  and b.status='А'
  And (b.quitstatus is not null or Trunc(b.dateval,'mm')<Nvl(trunc(b.Datequit,'mm'),'01-jan-3000'))
  and b.oprisn=d.isn(+)        -- здесь ограничение по целой подветке автоматических операций (200 02 03)
  and b.subaccisn=ba.isn
  and  (b.Code Like'60%' or b.Code Like'71%'  Or  ( ba.Id like '76%' and  not ba.Id like '7619%')) -- ограничение по счетам !!!! Обратное!!!
  and b.groupisn=dg.isn(+)
  

) s, buhbody b,buhhead bh
Where s.baseisn=b.isn
and b.headisn=bh.isn

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUHBODY_NMS" ("BODYISN", "HEADISN", "CURRISN", "SUBACCISN", "DEPTISN", "CODE", "DATEVAL", "DAMOUNT", "DAMOUNTRUB", "DAMOUNTUSD", "CAMOUNT", "CAMOUNTRUB", "CAMOUNTUSD", "OPRISN", "SUBKINDISN", "AGRISN", "DOCITEMISN", "FOBJISN") AS 
  (
SELECT  B.Isn bodyisn, B.headisn, B.currisn, B.subaccisn,
       B.deptisn, B.code, B.dateval, B.damount, B.damountrub,
       B.damountusd, B.camount, B.camountrub, B.camountusd, B.oprisn,
       B.subkindisn, B.agrisn, B.docitemisn, B.fobjisn
  FROM  tt_rowId t, Ais.buhbody B, buhsubacc Bs
  Where T.Isn=B.ISn
  and b.status='А'
  and b.subaccisn=BS.isn
  And Bs.dateend>'01-jan-2012'
  and (( (   Bs.id like '01%' or Bs.id like '02%' or Bs.id like '03%' Or Bs.id like '04%' Or Bs.id like '05%' Or Bs.id like '08%' or Bs.id like '10%' or Bs.id like '19%' OR Bs.Id Like 'Н0%'

 Or bs.ID like '008%'
 
 Or bs.ID like '009%'
 
 Or bs.ID like '003%'

/*GGM 08.07.13*/ OR Bs.Id Like '00К%' --Корректировка аналитики МЦ
   )

/*GGM 08.07.13*/ -- and Bs.id not like '00%'
   --and Nvl(Bs.active,'Y')<>'Z'
   ) )
   and b.dateval>='31-dec-2011'
   
  
  
  )

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUHBODY_REINS" ("PARENTISN", "BODYISN", "CODE", "DAMOUNTRUB", "CAMOUNTRUB", "DATEVAL", "OPRISN", "SUBJISN", "AGRISN", "DSSUBJISN", "AMOUNTSUM", "FULLAMOUNTSUM", "DSKOEF", "DOCSUMCNT", "DSISN", "DISCR", "STATUS", "CLASSISN", "CLASSISN2", "DATEPAY", "DOCISN", "DOCISN2", "SPLITISN", "AMOUNT", "DSCURRISN", "GROUPISN", "STATCODE", "DSAGRISN", "REACCISN", "SUBACCISN", "FID") AS 
  (

Select --+ Ordered Use_Nl(opr)

 s.PARENTISN,
 S.BODYISN,
 S.CODE,
 S.DAMOUNTRUB,
 S.CAMOUNTRUB,
 S.DATEVAL,
 s.OPRISN,


  s.subjisn ,
  s.agrisn,



  s.dssubjisn ,
  s.AmountSum,
  s.FullAmountSum,
  Nvl(s.AmountSum/s.FullAmountSum,1) as DSKoef,
  s.DocSumCnt,
  s.dsisn,
  s.DISCR,
  s.STATUS,
  s.CLASSISN,
  s.CLASSISN2,
  s.DATEPAY,
  s.DOCISN,
  s.DOCISN2,

decode(s.splitisn,null,null,
(
Select DDs.isn
from docsum dds
Where dds.Splitisn is null
start with dds.isn=s.splitisn
connect by prior splitisn=isn
))  SPLITISN,
  s.AMOUNT,
  s.DSCURRISN,
  s.GROUPISN,
  S.Statcode,
  dsagrisn,
  REaccisn,
  SUBACCISN,
  fid
from(
Select --+ Ordered use_nl(b sd ba pc pd bh)
  b.ParentIsn,b.Isn BodyIsn,b.Code, Nvl(b.damountrub,0)damountrub ,
  Nvl(b.camountrub,0) camountrub,b.Dateval, b.OprIsn,
  b.subaccisn,
  b.subjisn,
  b.agrisn,
  SUM (gcc2.gcc2(nvl (pc.Amount, pd.Amount),nvl(pc.CURRISN,pd.CURRISN),35,b.dateval)) OVER (PARTITION BY b.isn) AS FullAmountSum,
  COUNT (*) OVER (PARTITION BY b.isn) AS DocSumCnt,

  gcc2.gcc2(nvl (pc.Amount, pd.Amount),nvl(pc.CURRISN,pd.CURRISN),35,b.dateval) as AmountSum,




  nvl (pc.isn, pd.isn) as dsisn,
  nvl (pc.subjisn, pd.subjisn) as dssubjisn,
  nvl (pc.agrisn, pd.agrisn) as dsagrisn,
  nvl (pc.discr,pd.discr) as discr,
  nvl (pc.status,pd.status) as status,
  nvl (pc.CLASSISN,pd.CLASSISN) as classisn,
  nvl (pc.CLASSISN2,pd.CLASSISN2) as classisn2,
  nvl (pc.DATEPAY,pd.DATEPAY) as datepay,
  nvl (pc.DOCISN,pd.DOCISN) as docisn,
  nvl (pc.DOCISN2,pd.DOCISN2) as docisn2,
  nvl (pc.SPLITISN,pd.SPLITISN) as splitisn,
  nvl (pc.AMOUNT,pd.AMOUNT) as amount,
  nvl (pc.CURRISN,pd.CURRISN) as DScurrisn,
  nvl (pc.GROUPISN,pd.GROUPISN) as groupisn,
 nvl (pc.REaccisn,pd.REaccisn) as REaccisn,

sd.statcode,
bh.fid

  from tt_rowid t, buhbody b, buhhead bh,
  (select Subaccisn,Statcode from storages.V_REP_SUBACC4DEPT where statcode in
   (select statcode from rep_statcode where grp in ('Входящее перестрахование','Исходящее перестрахование'))
   Union 
select Isn,to_Number(Substr(Id,1,3)) from buhsubacc Where (Id Like '913%' Or id Like '914%')
and dateend>='31-dec-2010'
                   ) Sd,
 buhsubacc ba, docsum pc, docsum pd

  where t.isn = b.isn
  and b.subaccisn=ba.isn
  and b.subaccisn =Sd.SubaccIsn
  and b.headisn=bh.isn
  and b.isn = pc.creditisn(+)
  and b.isn = pd.debetisn(+)
  and pc.Discr(+) between 'F' and 'P'
  and pd.Discr(+) between 'F' and 'P'
  and  sd.statcode is not null
 
  and nvl (b.damountrub,b.camountrub) <> 0              -- условие из   VZ_REPBUHBODY_LIST
  and b.status = 'А'                                  -- условие из   VZ_REPBUHBODY_LIST
  and b.oprisn not in (9534516, 24422716)           -- условие из   VZ_REPBUHBODY_LIST
--  and b.dateval<=Ba.dateend

)s
)

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUH_TURN" ("SUBACCISN", "CODE", "SUBKINDISN", "OPRISN", "CURRISN", "DB", "DE", "DAMOUNT", "DAMOUNTRUB", "DAMOUNTUSD", "CAMOUNT", "CAMOUNTRUB", "CAMOUNTUSD", "PRM_KEY", "DEB", "DEE") AS 
  (

Select --+ Ordered Use_Nl(bacc b) Index( b X_BUHBODY_SUBACC_DATE)
b.subaccisn,
B.Code ,
b.SUBKINDISN,
b.oprIsn,
B.CurrIsn,
Trunc(Dateval,'month') DB ,
add_months(Trunc(Dateval,'month'),1)-1 DE,
--Sb.resident,
Sum(b.damount) damount,
Sum(b.damountrub) damountrub,
Sum(b.damountusd) damountusd,
Sum(b.camount) camount,
Sum(b.camountrub) camountrub,
Sum(b.camountusd) camountusd,
to_number(to_char(Trunc(Dateval,'month'),'YYYYMMDD')||To_Char(Subaccisn)) PRM_KEY,
Trunc(Nvl(DATEEVENT,Dateval),'month') DEB ,
add_months(Trunc(Nvl(DATEEVENT,Dateval),'month'),1)-1 DEE
from tt_rowid t,  Ais.buhbody_t b--,subject  sb
where b.subaccisn=Substr(t.Isn,9)
and b.dateval between trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month') and add_months( trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month'),1)-1
and b.Status='А'
--and b.subjisn=sb.isn(+)

group by
b.subaccisn,
B.cODE,
b.SUBKINDISN,
b.oprIsn,
B.CurrIsn,
Trunc(Dateval,'month') ,
add_months(Trunc(Dateval,'month'),1)-1,
Trunc(Nvl(DATEEVENT,Dateval),'month'),
add_months(Trunc(Nvl(DATEEVENT,Dateval),'month'),1)-1
)

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUH_TURN_CONTR" ("SUBACCISN", "CODE", "OPRISN", "DB", "DE", "DAMOUNTRUB", "CAMOUNTRUB", "RESIDENT", "BRANCHISN", "PRM_KEY", "CURRISN", "JURIDICAL") AS 
  (
Select
subaccisn,
Max(Code) Code,
oprIsn,
DB ,
DE,

Nvl(Sum(damountrub),0) damountrub,
Nvl(Sum(camountrub),0) camountrub,
Resident,
branchisn,
to_number(to_char(DB,'YYYYMMDD')||To_Char(Subaccisn)) PRM_KEY,
CURRISN,
JURIDICAL

From (



Select --+ Ordered Use_Nl(b sb ) Index ( b X_BUHBODY_SUBACC_DATE)
b.subaccisn,
B.Code,
b.oprIsn,
Trunc(B.Dateval,'month') DB ,
add_months(Trunc(B.Dateval,'month'),1)-1 DE,
b.damountrub damountrub,
b.camountrub camountrub,
b.CURRISN,
Resident,
branchisn,
sb.juridical
from tt_rowid t, buhsubacc bacc, Ais.buhbody_t b,ais.subject_t sb
where bacc.isn=Substr(t.Isn,9)
and bacc.isn=b.subaccisn
and b.subjisn is not null
and b.dateval between trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month') and add_months( trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month'),1)-1
and b.Status='А'
and Nvl(b.damountrub,b.camountrub)<>0
and b.subjisn=sb.isn(+)

union all

Select --+ Ordered Use_Nl(sb )
subaccisn,
S.Code,
oprIsn,
DB ,
DE,
s.damountrub*Decode(Nvl(FullAmountSum,0),0,1/DsCnt,AmountSum/FullAmountSum) damountrub,
s.camountrub*Decode(Nvl(FullAmountSum,0),0,1/DsCnt,AmountSum/FullAmountSum) camountrub,
s.CURRISN,
Resident,
branchisn,
sb.juridical
From (
Select --+ Ordered Use_Nl(b pc pd ) Index ( b X_BUHBODY_SUBACC_DATE)
b.subaccisn,
B.Code,
b.oprIsn,
Trunc(B.Dateval,'month') DB ,
add_months(Trunc(B.Dateval,'month'),1)-1 DE,
SUM (Nvl(gcc2.gcc2(nvl (pc.Amount, pd.Amount),nvl(pc.CURRISN,pd.CURRISN),35,b.dateval),nvl (pc.AmountRub, pd.AmountRub))) OVER (PARTITION BY b.isn) AS FullAmountSum,
Nvl(gcc2.gcc2(nvl (pc.Amount, pd.Amount),nvl(pc.CURRISN,pd.CURRISN),35,b.dateval),Nvl(pc.AmountRub, pd.AmountRub)) AmountSum,
Count(*) OVER (PARTITION BY b.isn) AS DsCnt,
b.damountrub Damountrub,
b.camountrub camountrub,
Nvl(Pc.subjisn,Pd.SubjIsn) SubjIsn,
b.CURRISN
from tt_rowid t, buhsubacc bacc, Ais.buhbody_t b, docsum pc, docsum pd
where bacc.isn=Substr(t.Isn,9)
and bacc.isn=b.subaccisn
and b.subjisn is  null
and b.dateval between trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month') and add_months( trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month'),1)-1
and b.Status='А'
and Nvl(b.damountrub,b.camountrub)<>0
and b.isn = pc.creditisn(+)
and b.isn = pd.debetisn(+)
and pc.Discr(+) between 'F' and 'P'
and pd.Discr(+) between 'F' and 'P') S, ais.subject_t sb
Where S.SubjIsn=Sb.Isn(+)




)

group by
subaccisn,
oprIsn,
DB ,
DE,
Resident,
branchisn,
CURRISN,
JURIDICAL
)

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_BUH_TURN_CORR" ("SUBACCISN", "CODE", "CORCODE", "OPRISN", "DB", "DE", "DAMOUNTRUB", "CAMOUNTRUB", "PRM_KEY", "SUBKINDISN") AS 
  (
Select 
subaccisn,
Max(Code) Code,
CorCode,
oprIsn,
DB ,
 DE,


Nvl(Sum(damountrub),0) damountrub,
Nvl(Sum(camountrub),0) camountrub,


/*
Nvl(Sum(damountrub*Nvl(CorPc,1)),0) damountrub,
Nvl(Sum(camountrub*Nvl(CorPc,1)),0) camountrub,
*/
to_number(to_char(DB,'YYYYMMDD')||To_Char(Subaccisn)) PRM_KEY,
SubKindISn

From (



Select --+ Ordered Use_Nl(b bc ) Index ( b X_BUHBODY_SUBACC_DATE) Index (bc X_BUHBODY_head)

b.subaccisn,
B.Code,
b.oprIsn,
b.SubKindISn,
Trunc(B.Dateval,'month') DB ,
add_months(Trunc(B.Dateval,'month'),1)-1 DE,



Case
--When Nvl(b.DAmountrub,B.Camountrub)=0 Then 0
When Count(*) over (PARTITION by B.Isn)=1 Then B.DAmountRub
When Nvl(b.DAmountrub,B.Camountrub)<>Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn) then BC.CAmountRub/Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn)
Else
 BC.CAmountRub
End damountrub,


Case
--When Nvl(b.DAmountrub,B.Camountrub)=0 Then 0
When Count(*) over (PARTITION by B.Isn)=1 Then B.CAmountRub
When Nvl(b.DAmountrub,B.Camountrub)<>Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn) then BC.DAmountRub/Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn)
Else
 BC.DAmountRub
End Camountrub,


/*
b.damountrub damountrub,
b.camountrub camountrub,

/*
decode(Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn) ,0,
1/Count(*) over (PARTITION by B.Isn),
Nvl(bc.damountrub,bc.camountrub)/Sum(Nvl(bc.damountrub,bc.camountrub)) over (PARTITION by B.Isn))
CorPc,
*/




bc.Code CorCode
 
from tt_rowid t, buhsubacc bacc, Ais.buhbody_t b,buhbody bc
where bacc.isn=Substr(t.Isn,9)
and bacc.isn=b.subaccisn
and b.dateval between trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month') and add_months( trunc(to_Date(Substr(t.Isn,1,8),'YYYYMMDD'),'Month'),1)-1
and b.Status='А'
and Nvl(b.damountrub,b.camountrub)<>0
and b.headisn=bc.headisn
and bc.status='А'
--and Nvl(bc.damountrub,bc.camountrub)<>0
and decode(b.damountrub,null,'D','C')<> decode(bc.damountrub,null,'D','C')


)

group by
subaccisn,
CorCode,
oprIsn,
SubKindISn,
DB ,
DE
)

  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_TT_DOCSUMBODY" ("BODYISN", "BAMOUNT", "AGRISN", "SUBJISN", "AMOUNTRUB", "DATEPAYLAST", "DSCLASSISN", "DSISN", "DISCR", "SUBACCISN", "SPLITISN", "C_AGR_1", "AGRKOEF", "DSKOEF", "AGRDSKOEF", "DB", "DE", "REMAINDER_1", "REACCISN", "AGENTISN", "AGRDATEBEG") AS 
  Select --+ ORdered Use_Nl(ar)
S."BODYISN","BAMOUNT",S."AGRISN",S."SUBJISN","AMOUNTRUB","DATEPAYLAST","DSCLASSISN","DSISN",S."DISCR","SUBACCISN","SPLITISN","C_AGR","AGRKOEF","DSKOEF","AGRDSKOEF","DB","DE","REMAINDER","REACCISN",
AR.AGENTISN,
AR.DATEBEG
from (
Select d2."BODYISN",d2."BAMOUNT",d2."AGRISN",d2."SUBJISN",d2."AMOUNTRUB",d2."DATEPAYLAST",d2."DSCLASSISN",d2."DSISN",d2."DISCR",d2."SUBACCISN",d2."SPLITISN",d2."C_AGR",d2."AGRKOEF",
  d2.KfBase/sum(d2.KfBase) over(partition by d2.bodyisn,d2.agrisn) as DSKoef,
  d2.AgrKoef*d2.KfBase/sum(d2.KfBase) over(partition by d2.bodyisn,d2.agrisn) as AGRDSKoef,
  Greatest(storage_adm.Load_Storage.gethistdb,BodyStatBeg) db, BodyStatEnd as de,
   d2."REMAINDER",REACCISN

from
(
Select D.*, c_agr/sum(d.KfBase*Sgn) over(partition by bodyisn) as AgrKoef
from
(
Select d.*,
Decode(trunc(sum(remainder) over(partition by bodyisn)),0,
sum( Sgn*AmountRub) over(partition by bodyisn,agrisn),
sum( Sgn*remainder) over(partition by bodyisn,agrisn)) c_agr,


Decode(trunc(sum(remainder) over(partition by bodyisn)),0,AmountRub,Remainder) KfBase


From
(
Select --+ Ordered Use_Nl(b pc pd)
 b.bodyisn,
 b.BASEAMOUNTRUB bamount,
ds.agrisn,
Ds.subjisn,
Case /* если знак remainder и amountrub совпадают или remainder=0*/
When Sign(Ds.amountrub)=Sign(Ds.remainder) Then Gcc2.gcc2(Ds.remainder, Ds.currisn, 35, b.Dateval)
When Sign(Nvl(Ds.remainder,-1))=0 Then 0
Else
Gcc2.Gcc2(Ds.amount,Ds.Currisn,35, b.Dateval)
End   Remainder,
Gcc2.Gcc2(Ds.amount,Ds.currisn,35,b.Dateval) AmountRub,
Nvl(Nvl(Ds.DatePayLast,Ds.DatePay), Ds.DocDate) DatePayLast,
Ds.classisn dsclassisn,
Ds.isn DsIsn,
Ds.discr discr,
Ds.REACCISN REACCISN,
B.subaccisn,
ds.splitisn splitisn,
Decode(Sign(BASEAMOUNTRUB),Sign(Sum(Ds.amount) over (PARTITION by bodyisn,Discr)),-1,1) sgn,
BodyStatEnd,
BodyStatBeg,
Max(Ds.Discr) over (PARTITION by bodyisn) MaxDiscr,
Min(Ds.Discr) over (PARTITION by bodyisn) MinDiscr,
CODE,DAmountrub,CAmountrub

from
(
/*только интересующие нас доксуммы*/
Select --+ Ordered Use_Nl(b b1)
  b.BaseIsn BodyIsn,
  Max(b.BASEAMOUNTRUB) BASEAMOUNTRUB,
  Max(b.subaccisn) subaccisn,
  Max(b.BaseDateval) Dateval,
  Max(b.DE) BodyStatEnd,
  Min(b.DB) BodyStatBeg,
  Max(b.Code) Code,
  Max(BaseDAmountrub) DAmountrub,
 Max(BaseCAmountrub) CAmountrub
from tt_rowid t,STORAGES.ST_BODYDEBCRE B
where
  t.isn=b.baseisn
group by   b.BaseIsn
Having  Max(b.BASEAMOUNTRUB)<>0
)b,ais.docsum  Ds
Where   b.BodyIsn In (ds.DebetIsn,ds.creditisn)
And Ds.Discr In ('F','P')
And Ds.amountrub<>0
) d
where
/* дебильная врезка - для счета 76197 надо отдавать предпочтение фактическим доксуммам, чтобу получить страховщика причинителя вреда по ПВУ*/
/*(Code='76197' and CAmountrub is not null and discr=minDiscr) or
((Code<>'76197' or DAmountrub is not null) and discr=maxDiscr) */
discr=maxDiscr
)D
where
 Sign(d.c_agr) <> Sign(bamount)  And c_agr<>0 -- Заменить на > 0
) d2
where Sign(d2.amountrub*Sgn) <> Sign(d2.bamount)
) S, Storage_source.repagr ar
Where S.agrisn=ar.agrisn(+)
--Where Db<=De


  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_LONGAGRADDENDUM" ("AGRISN", "ADDISN", "DISCR", "DATEBEG", "DATESIGN", "PREMIUMSUM", "CURRISN") AS 
  (select /*+ ALL_ROWS Ordered(a) Use_Nl(a) Index(a) */
       connect_by_root (isn) AS agrisn,
       a.isn AS addisn,
       a.discr,
       a.datebeg,
       a.datesign,
       CASE a.discr
         WHEN 'А' THEN a.premiumsum
         WHEN 'Д' THEN (SELECT A.PremiumSum - NVL(sum(Z.PremiumSum), 0) FROM Agreement Z WHERE  Z.ParentISN = A.ISN and Z.Discr = 'А')
       END AS premiumsum,
       a.currisn
from Agreement A
start  with ISN IN (SELECT --+ ordered use_nl ( t ag )
                         Distinct   t.isn
                    FROM tt_rowid t, agreement ag
                    WHERE ag.isn=t.isn
                      AND sign(months_between (ag.DateEnd,ag.DateBeg)-13)=1
                      AND ag.discr IN ('Д', 'Г')
                      AND ag.classisn IN (SELECT ISN
                                          FROM DICTI D
                                          START WITH D.ISN = 34711216
                                          CONNECT BY PRIOR D.ISN = D.PARENTISN
                                         )
                   )
connect by prior A.ISN = A.PrevISN
       and Nvl(A.Discr,'Y') = 'А'
)


  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_AGENT_RANKS" ("AGRISN", "ADDISN", "IS_MOVE_OBJ_ADDISN", "ADDID", "ORDERNO", "AGR_ID", "AGR_DATEBEG", "AGR_DATEEND", "AGR_DATESIGN", "AGR_RULEISN", "ADD_DATEBEG", "ADD_DATEEND", "ROLE_DATEBEG", "ROLE_DATEEND", "AGENTISN", "AGENTCLASSISN", "ADDRULEISN", "IS_MOVE_OBJ", "SHAREPC_AGENT_BY_ADD", "AGENT_SUMCLASSISN", "AGENT_SUMCLASSISN2", "AGENT_CALCFLG", "AGENT_BASE", "AGENT_BASELOSS", "AGENT_PLANFACT", "AGENT_DEPTISN", "RNK_MOVE_OBJ", "SHAREPC_BY_ADD", "CNT_AGENT_BY_AGR", "CNT_AGENT_BY_ADD", "IS_MOVE_OBJ_ID", "IS_MOVE_OBJ_DATEBEG", "IS_MOVE_OBJ_DATEEND", "SHAREPC_BY_IS_MOVE_OBJ", "CNT_AGENT_BY_IS_MOVE_OBJ", "IS_ADD_CANCEL", "IS_ADD_CANCEL_ADDISN", "SUBJCLASSISN") AS 
  with AGR as (
-- данные по агентам, привязанным (по дате начала) к аддендумам (Тарара)
  select --+ ordered use_nl(RA A)
    RA.AGRISN,
    -- атрибуты договора
    RA.AGR_ID,
    RA.AGR_DATEBEG,
    RA.AGR_DATEEND,
    RA.AGR_DATESIGN,
    RA.AGR_RULEISN,
    -- атрибуты аддендума (при отсутствии такового - поля договора)
    /* sts 01.11.2012 - транкать плохо, ибо есть договоры, у которых аддендумы различаются на минуту: AgrISN = 176551249603
    nvl(trunc(A.DATEBEG), RA.AGR_DATEBEG) as DATEBEG,
    trunc(lead(A.DATEBEG, 1, RA.AGR_DATEEND) over(partition by RA.AGRISN order by A.DATEBEG nulls first)) as DATEEND,
    */
    nvl(A.DATEBEG, RA.AGR_DATEBEG) as DATEBEG,
    lead(A.DATEBEG, 1, RA.AGR_DATEEND) over(partition by RA.AGRISN order by A.DATEBEG nulls first) as DATEEND,

    decode(A.RULEISN, 37564716, 1, 0) as IS_MOVE_OBJ, -- ПЕРЕНОС ТС
    decode(A.RULEISN, 34710416, 1, 0) as IS_ADD_CANCEL, -- ПРЕКРАЩЕНИЕ ДОГОВОРА
    -- ссылка на договор/аддендум, к которому относится агент
    nvl(A.ISN, RA.AGRISN) as ADDISN,
    nvl(A.ID, RA.AGR_ID) as ADDID,
    nvl(A.RULEISN, RA.AGR_RULEISN) as RULEISN

  from
   (select --+ ordered use_nl(T RA) use_hash(CarRules)
      T.ISN as AGRISN,
      -- атрибуты договора
      RA.ID as AGR_ID,
      trunc(RA.DATEBEG) as AGR_DATEBEG,
      trunc(RA.DATEEND) as AGR_DATEEND,
      trunc(RA.DATESIGN) as AGR_DATESIGN,
      RA.RULEISN as AGR_RULEISN,
      nvl2(CarRules.ISN, RA.AGRISN, null) as PARENTISN  -- будем искать аддендумы только для моторного страхования
    from
      TT_ROWID T,
      STORAGE_SOURCE.REPAGR RA,
      MOTOR.V_DICTI_RULE CarRules  -- детализация по аддендумам только для моторного страхования (аналогично REPBUH2OBJ)
    where
      T.ISN = RA.AGRISN
      and RA.STATUS in ('В', 'Д', 'Щ')
      and RA.RULEISN = CarRules.ISN(+)

      --and ra.agrisn = 170415.1158

  ) RA,
    AIS.AGREEMENT A
  where
    (
      (RA.AGRISN = A.ISN and A.DISCR = 'Д')  -- договор
      or
      (RA.PARENTISN = A.PARENTISN and A.DISCR = 'А')  -- аддендумы
    )
),
ADD_MOVE as (
-- определяем первый предыдущий аддендум/договор, имеющий ненулевую сумму
-- нужно для распределения суммы возвратов премии в отчете "Сборы агентов" task(ДИТ-12-2-166348, 32609379103)
  select
    A.AGRISN,
    A.ADDISN,
    A.ADDID,
    A.DATEBEG,
    lead(A.DATEBEG, 1, A.AGR_DATEEND) over(partition by A.AGRISN order by A.DATEBEG) as DATEEND
  from AGR A
  where
    A.IS_MOVE_OBJ = 1
),
ADD_CANCEL as (
  select
    AD.ROOT_ISN,
    max(AD.ADDISN) keep(dense_rank last order by lv) as ADDISN
  from (
    select
      level lv,
      CONNECT_BY_ROOT AD.ISN as ROOT_ISN,
      nvl(AD.PREVISN, AD.ISN) ADDISN
    from AGREEMENT AD
    start with AD.ISN in (select distinct AGR.ADDISN from AGR where AGR.IS_ADD_CANCEL = 1)
    connect by AD.ISN = prior AD.PREVISN and AD.PREMIUMSUM = 0
  ) AD
  group by AD.ROOT_ISN
)


/* примеры
insert into tt_rowid(ISN) values(90913884603);  -- перенос ТС без даты агента
insert into tt_rowid(ISN) values(124407495003);  -- перенос ТС с датой агента
insert into tt_rowid(ISN) values(161772051303);  -- один агент без даты, два адд, один - перенос ТС
insert into tt_rowid(ISN) values(125000002403);  -- перенос ТС с датой агента, два адд-ма с суммами
insert into tt_rowid(ISN) values(21604144103);   -- два агента без дат, два ад-ма на перенос ТС с суммами
-----------
AgrISN:
1. 183965.1158 - договор с одним агентом (Дата окончания) (есть адендум, но без агента)
   170415.1158 - договор с одним агентом (два адендума)
   85490139803 - договор с одним агентом (есть адендум Перенос ТС). Агент начал действовать начиная с адендума

2. 16410501003 - 4 агента, нет адендумов

3. 417636.2692 - два агента (на разные адендумы)
   9542896025  - 4 агента, 2 адендума

4. 62609209507 - два агента с непересекающимися датами, один перенос ТС
   62609209507 - два агента с непересекающимися датами, один перенос ТС (два адендума)
   59276237103 - два агента (+ 1 агент-бонусная комиссия), один Перенос ТС  <<-хороший пример

Один агент несколько раз встречается в договоре:
4603910000 - ипотека
5909640000 - ОСАГО - один агент два раза (второй раз - на ад-м Изменение условий). SharePC = 10
101506.05  - ОСАГО - один агент два раза (два а-ма Изменение периода). SharePC = 10
120823.1498 - ОСАГО, 3 агента, из них два одинаковых. Один адендум Изменение периода
8281029025 - ОСАГО, 3 агента, из них два одинаковых. без адендумов

*/


select --+ ordered use_nl(A S)
  A.AGRISN,
  A.ADDISN,
  -- аддендум "Перенос ТС", к которому относится текущий аддендум (Для первоначального состояния = AgrISN)
  -- Нужно для ТЗ Тарары, т.к. все аддендумы, заключенные после аддендума "Перенос ТС"
  -- считаются новыми условиями договора, определяемые аддендумом "Перенос ТС".
  -- И так - до следующего аддендума "Перенос ТС"
  A.IS_MOVE_OBJ_ADDISN,
  A.ADDID,  -- номер аддендума (номер договора для договора)
  A.ORDERNO,
  A.AGR_ID,       -- номер договора
  A.AGR_DATEBEG,
  A.AGR_DATEEND,
  A.AGR_DATESIGN,
  A.AGR_RULEISN,
  A.ADD_DATEBEG,
  A.ADD_DATEEND,
  A.ROLE_DATEBEG,
  A.ROLE_DATEEND,
  A.AGENTISN,
  A.AGENTCLASSISN,
  A.ADDRULEISN,
  A.IS_MOVE_OBJ,
  A.SHAREPC_AGENT_BY_ADD,  -- процент комиссии по агенту
  A.AGENT_SUMCLASSISN,
  A.AGENT_SUMCLASSISN2,
  A.AGENT_CALCFLG,
  A.AGENT_BASE,
  A.AGENT_BASELOSS,
  A.AGENT_PLANFACT,
  A.AGENT_DEPTISN,

  -- порядковый номер агента по критериям отбора отчета "Сборы агентов" - task(ДИТ-12-2-166347)
  A.RNK_MOVE_OBJ,
  -- сумма % комиссии по аддендуму
  A.SHAREPC_BY_ADD,

  A.CNT_AGENT_BY_AGR,
  A.CNT_AGENT_BY_ADD,

  -- атрибуты для аддендума IS_MOVE_OBJ_ADDISN
  A.IS_MOVE_OBJ_ID,
  A.IS_MOVE_OBJ_DATEBEG,
  A.IS_MOVE_OBJ_DATEEND,

  -- сумма % комиссии по дочерним аддендумам к аддендуму "Перенос ТС"
  A.SHAREPC_BY_IS_MOVE_OBJ,
  -- кол-во агентов по дочерним аддендумам к аддендуму "Перенос ТС"
  A.CNT_AGENT_BY_IS_MOVE_OBJ,

  A.IS_ADD_CANCEL,  -- Признак аддендума "Прекращение договора"
  A.IS_ADD_CANCEL_ADDISN,  -- Ссылка на первый ненулевой аддендум/договор от аддендума "Прекращение договора"
  S.CLASSISN as SUBJCLASSISN
from (
  select
    A.*,
    -- порядковый номер агента по критериям отбора отчета "Сборы агентов" - task(ДИТ-12-2-166347)
    rank() over(partition by A.AGRISN, decode(A.IS_MOVE_OBJ, 1, A.ADDISN, A.AGRISN) order by A.ORDERNO) as RNK_MOVE_OBJ,
    /*
    -- сумма % комиссии по аддендуму/договору
    sum(A.SHAREPC_AGENT_BY_ADD)
      over(partition by A.AGRISN, decode(A.IS_MOVE_OBJ, 1, A.ADDISN, A.AGRISN)
           order by A.ORDERNO ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        as SHAREPC_BY_ADD,  -- todo - уточнить для догвора AgrISN = 5909640000
    */
    -- сумма % комиссии по аддендуму
    sum(A.SHAREPC_AGENT_BY_ADD) over(partition by A.AGRISN, A.ADDISN) as SHAREPC_BY_ADD,

    count(distinct A.ORDERNO || ':' || A.AGENTISN) over(partition by A.AGRISN) as CNT_AGENT_BY_AGR,
    count(distinct A.ORDERNO || ':' || A.AGENTISN) over(partition by A.ADDISN) as CNT_AGENT_BY_ADD,

    -- сумма % комиссии по дочерним аддендумам к аддендуму "Перенос ТС"
    sum(A.SHAREPC_AGENT_BY_ADD) over(partition by A.AGRISN, A.IS_MOVE_OBJ_ADDISN) as SHAREPC_BY_IS_MOVE_OBJ,
    -- кол-во агентов по дочерним аддендумам к аддендуму "Перенос ТС"
    count(distinct A.ORDERNO || ':' || A.AGENTISN) over(partition by A.AGRISN, A.IS_MOVE_OBJ_ADDISN) as CNT_AGENT_BY_IS_MOVE_OBJ
  from (
    select
      -- ключи
      A.AGRISN,
      -- ссылка на договор/аддендум, к которому относится агент (по периоду действия)
      A.ADDISN,
      AR.SUBJISN as AGENTISN,
      AR.ORDERNO,

      -- атрибуты
      max(A.AGR_ID) as AGR_ID,
      max(A.AGR_DATEBEG) as AGR_DATEBEG,
      max(A.AGR_DATEEND) as AGR_DATEEND,
      max(A.AGR_DATESIGN) as AGR_DATESIGN,
      max(A.AGR_RULEISN) as AGR_RULEISN,
      max(A.ADDID) as ADDID,
      max(A.DATEBEG) as ADD_DATEBEG,
      max(A.DATEEND) as ADD_DATEEND,
      max(A.RULEISN) as ADDRULEISN,
      max(A.IS_MOVE_OBJ) as IS_MOVE_OBJ, -- ПЕРЕНОС ТС
      max(A.IS_MOVE_OBJ_ADDISN) as IS_MOVE_OBJ_ADDISN,   -- аддендум "Перенос ТС", к которому относится текущий аддендум (Для первоначального состояния = AgrISN)
      -- атрибуты для аддендума IS_MOVE_OBJ_ADDISN
      max(A.IS_MOVE_OBJ_ID) as IS_MOVE_OBJ_ID,
      max(A.IS_MOVE_OBJ_DATEBEG) as IS_MOVE_OBJ_DATEBEG,
      max(A.IS_MOVE_OBJ_DATEEND) as IS_MOVE_OBJ_DATEEND,

      -- атрибуты для аддендума IS_ADD_CANCEL
      max(A.IS_ADD_CANCEL) as IS_ADD_CANCEL,
      max(A.IS_ADD_CANCEL_ADDISN) as IS_ADD_CANCEL_ADDISN,

      -- поля из AgrRole. Группировка до AgentISN в рамках аддендума/договора для фильтрации "грязных" данных
      /* плохо, т.к. на AgrISN = 13141920003 один агент участвует два раза с разными суммами и действует как на аддендум,
      так и на договор. Из за этого, если схлопывать до SubjISN получаются разные промежутки действия для Ад и Д:
      -- начало - от Ад/Д, конец - дата окончания Д
      -- Поэтому вынес в group by
      --min(abs(AR.ORDERNO)) as ORDERNO,
      */
      min(nvl(AR.DATEBEG, '01-jan-1900')) as ROLE_DATEBEG,
      max(nvl(AR.DATEEND, '01-jan-3000')) as ROLE_DATEEND,
      max(AR.CLASSISN) as AGENTCLASSISN,
      sum(AR.SHAREPC) as SHAREPC_AGENT_BY_ADD,
      max(AR.SUMCLASSISN) as AGENT_SUMCLASSISN,
      max(AR.SUMCLASSISN2) as AGENT_SUMCLASSISN2,
      max(AR.CALCFLG) as AGENT_CALCFLG,
      sum(AR.BASE) as AGENT_BASE,
      sum(AR.BASELOSS) as AGENT_BASELOSS,
      min(AR.PLANFACT) as AGENT_PLANFACT,  -- min - приоритет F - факт
      max(AR.DEPTISN) as AGENT_DEPTISN

    from
    ( select
        AGR.*,
        /* sts 01.11.2012
        из за того, что есть договоры, у которых аддендумы различаются на минуту (AgrISN = 176551249603)
        и чтобы (на примере договора) агент Калиберда Юлия Петровна с датой начала действия 30.08.2012 не
        определялся как действующий на аддендум Д (у которого окончание 30.08.2012 15:13:00)
        определяем дату окончания периода следующим образом:
        Если период действия аддендума меньше одного дня, то дату окончания не усекаем
        (как раз для того, чтобы к агенту Калиберде подтянуть аддендум 1).
        Иначе - усекаем до дня чтобы агент Калиберда не подтягивался в аддендум Д
        И уже с этой полученной датой сравниваем период действия роли агента
        */
        case when AGR.DATEEND - AGR.DATEBEG < 1 then AGR.DATEEND else trunc(AGR.DATEEND) end as DATEEND_CALC,
        -- определяем аддендум "Перенос ТС", к которому относится текущий аддендум.
        -- Нужно для ТЗ Тарары, т.к. все аддендумы, заключенные после аддендума "Перенос ТС"
        -- считаются новыми условиями договора, определяемые аддендумом "Перенос ТС".
        -- И так - до следующего аддендума "Перенос ТС"
        nvl(ADD_MOVE.ADDISN, AGR.AGRISN) as IS_MOVE_OBJ_ADDISN,
        nvl(ADD_MOVE.ADDID, AGR.AGR_ID) as IS_MOVE_OBJ_ID,
        nvl(ADD_MOVE.DATEBEG, AGR.AGR_DATEBEG) as IS_MOVE_OBJ_DATEBEG,
        first_value(AGR.DATEEND - decode(AGR.DATEEND, AGR.AGR_DATEEND, 0, 1)) over(partition by nvl(ADD_MOVE.ADDISN, AGR.AGRISN) order by AGR.DATEBEG desc) as IS_MOVE_OBJ_DATEEND,
        -- определяем первый предыдущий аддендум/договор, имеющий ненулевую сумму
        -- нужно для распределения суммы возвратов премии в отчете "Сборы агентов" task(ДИТ-12-2-166348, 32609379103)
        nvl(ADD_CANCEL.ADDISN, AGR.ADDISN) as IS_ADD_CANCEL_ADDISN
      from
        AGR,
        ADD_MOVE,
        ADD_CANCEL
      where
        AGR.AGRISN = ADD_MOVE.AGRISN(+)
        and AGR.DATEBEG >= ADD_MOVE.DATEBEG(+)
        and AGR.DATEBEG < ADD_MOVE.DATEEND(+)
        and AGR.ADDISN = ADD_CANCEL.ROOT_ISN(+)
    ) A,
      AIS.AGRROLE AR
    where
      -- 1. сделал закрытый джойн. Из за этого не попадают договоры/адендумы без агентов
      -- нпр: AgrISN = 183965.1158 имеет одного агента и один аддендум, но срок действия агента в адендум не попадает,
      -- поэтому по такому договору одна запись (на первоначальное состояние договора)
      -- Наверное это правильно...
      -- 2. Для прекращенного договора AgrISN = 133574884903 есть недействующий (в рамках договора) агент.
      -- Но т.к. дату окончания аддендума я считаю как дату начала следующего адендума, то по этому договору
      -- агент получается действующим на аддендум 1 (Изменение условий)
      -- Наверное это тоже правильно, т.к. собственно агент и "добавил" аддендум
      A.AGRISN = AR.AGRISN
      and AR.CLASSISN in (
        437,   -- АГЕНТ
        2481446203, -- АГЕНТ (БОНУСНАЯ КОМИССИЯ)
        2530118403  -- ГЕНЕРАЛЬНЫЙ АГЕНТ
        /* В итоге - пока убрал - решил оставить таблицу MOTOR.AGRAGENT_RANKS (для отчетов ниже) ибо она сильно другая (в частности по группировкам)
           Сделал только догрузку по логам аналогично табл. REP_AGENT_RANKS

        -- sts 12.10.2012 - нужно для отчетов "Кол-во выданных смет" и "Агентский отчет (от ДРС)" в Cognos
        -- для отчета "Сквитованная премия (Регионы)" не нужен, но в отчете классы агентов отфильтровываются,
        -- так что добавление Брокера не должно повлиять на отчет
        438   -- БРОКЕР
        */
      )
      /* sts 01.11.2012 - транкать плохо, ибо есть договоры, у которых аддендумы различаются на минуту: AgrISN = 176551249603
      and nvl(trunc(AR.DATEEND), '01-jan-3000') >= trunc(A.DATEBEG)
      and nvl(trunc(AR.DATEBEG), '01-jan-1900') < trunc(A.DATEEND)
      */
      and nvl(AR.DATEEND, '01-jan-3000') >= A.DATEBEG
      and nvl(AR.DATEBEG, '01-jan-1900') < A.DATEEND_CALC
    group by
      A.AGRISN,
      A.ADDISN,
      AR.SUBJISN,
      AR.ORDERNO
  ) A
) A,
  AIS.SUBJECT_T S
where
  A.AGENTISN = S.ISN(+)
  
  
  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPAGR" ("AGRISN", "ID", "DATEBEG", "DATEEND", "DATESIGN", "CLASSISN", "RULEISN", "DEPTISN", "DEPT0ISN", "FILISN", "RULEDEPT", "EMPLISN", "CLIENTISN", "CURRISN", "PREMIUMSUM", "PREMUSD", "PREMRUB", "PREMEUR", "INCOMERATE", "STATUS", "DISCR", "APPLISN", "SHAREPC", "REINSPC", "GROUPISN", "BIZFLG", "PARENTISN", "INSURANTISN#", "INSURANTCOUNT#", "AGENTISN", "AGENTCOUNT", "EMITISN", "EMITCOUNT", "COMISSION", "BUHDATE", "LIMITSUM", "LIMITSUMUSD", "INSUREDSUM", "INSUREDSUMUSD", "AGRCREATED", "AGENTJURIDICAL", "FIRMISN", "AGENTCLASSISN", "SALERGOISN", "SALERFISN", "OWNERDEPTISN", "CLIENTJURIDICAL", "FILCOMMISION", "BEMITISN", "BFILISN", "CALCBIZFLG", "PREVISN", "CROSSALERISN", "TRANSFERCOMISSION", "BENEFICIARYISN", "PARTNERISN", "LIMITSUMRUB", "INSUREDSUMRUB", "OLDDATEEND", "CALCEMITISN", "CALCFILISN", "GMISN", "ADDRISN", "AGENTDEPTISN", "DATECALC", "BROKERISN", "LEASEFICIARYISN", "AGENTCOLLECTFLG", "AGRDETAILISN", "PAWNBROKERISN", "SALESCHANNELISN", "DATEBASE", "RECOMMENDERISN", "FORMISN", "CREATEDBY", "UPRISN", "INCOMESUM", "INCOMESUMUSD", "INCOMESUMRUB", "DISCOUNT", "DATEISSUE", "CREATEDATE") AS 
  (select --+ ordered use_hash(sd  fil bfil) Use_Nl(ar)
       S.AGRISN, S.ID, S.DATEBEG, S.DATEEND, S.DATESIGN, S.CLASSISN, S.RULEISN, S.DEPTISN, nvl(SD.DEPT0ISN, 0) DEPT0ISN,
       FIL.FILISN, S.RULEDEPT, S.EMPLISN, S.CLIENTISN, S.CURRISN, S.PREMIUMSUM, S.PREMUSD, S.PREMRUB, S.PREMEUR, S.INCOMERATE,
       S.STATUS, S.DISCR, S.APPLISN, S.SHAREPC, S.REINSPC, S.GROUPISN, S.BIZFLG, S.PARENTISN, S.INSURANTISN#, S.INSURANTCOUNT#,
       Nvl(AR.AGENTISN,AR.BROKERISN) AGENTISN/* KGR 10.05.2011*/, AR.AGENTCOUNT, AR.EMITISN, AR.EMITCOUNT, S.COMISSION, S.BUHDATE, S.LIMITSUM, S.LIMITSUMUSD, S.INSUREDSUM,
       S.INSUREDSUMUSD, S.AGRCREATED, NVL(AR.AGENTJURIDICAL,AR.BROKERJURIDICAL) AGENTJURIDICAL /* KGR 10.05.2011*/,
       S.FIRMISN, 437 AGENTCLASSISN, AR.SALERGOISN, AR.SALERFISN, S.OWNERDEPTISN,
       S.CLIENTJURIDICAL, AR.FILCOMMISION, AR.BEMITISN, BFIL.FILISN BFILISN,
       decode(decode(BEMITISN, null, FIL.RCISN, BFIL.RCISN), null, 'Ц', 'Ф') CALCBIZFLG,
       S.PREVISN, AR.CROSSALERISN, AR.TRANSFERCOMISSION, AR.BENEFICIARYISN, AR.PARTNERISN, S.LIMITSUMRUB, S.INSUREDSUMRUB,
       S.OLDDATEEND, nvl(BEMITISN, EMITISN) CALCEMITISN,
       decode(BEMITISN, null, FIL.FILISN, BFIL.FILISN) CALCFILISN,
       S.GMISN, S.ADDRISN, AR.AGENTDEPTISN, S.DATECALC, AR.BROKERISN, AR.LEASEFICIARYISN, AR.AGENTCOLLECTFLG, S.AGRDETAILISN,
       AR.PAWNBROKERISN, -- OD 6.05.2010 ДИТ-10-2-098743 Залогодержатель
       S.SALESCHANNELISN,
       S.DATEBASE,
       AR.RECOMMENDERISN, -- OD 07.09.2010 ДИТ-10-3-117604
       s.formisn, -- EGAO 04.03.2011
       S.CREATEDBY, -- OD 22.03.2011
       ar.uprisn, -- EGAO 18.04.2012
       -- sts 19.10.2012 - task(38397275003)
       S.INCOMESUM,
       S.INCOMESUMUSD,
       S.INCOMESUMRUB,
       -- sts 06.11.2012 - скидка для туристов
       S.DISCOUNT,
       s.dateissue, -- EGAO 08.05.2013
       s.CREATEDATE
  from ( select --+ ordered use_nl(r a ar dr drp ad sd sdf sdf1 arc clnt) use_hash(agnt gm)
                A.ISN AGRISN,A.ID, A.DATEBEG, A.DATEEND, A.DATESIGN, A.CLASSISN, A.RULEISN, A.DEPTISN,
                DR.FILTERISN RULEDEPT, A.EMPLISN, A.CLIENTISN, A.CURRISN, A.PREMIUMSUM,
                decode(A.CURRISN, 53, A.PREMIUMSUM, gcc2.gcc2(A.PREMIUMSUM, A.CURRISN, 53, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) PREMUSD,
                decode(A.CURRISN, 35, A.PREMIUMSUM, gcc2.gcc2(A.PREMIUMSUM, A.CURRISN, 35, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) PREMRUB,
                decode(A.CURRISN, 29448516, A.PREMIUMSUM, gcc2.gcc2(A.PREMIUMSUM, A.CURRISN, 29448516, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) PREMEUR,
                decode(A.PREMIUMSUM, 0, 0, A.INCOMESUM / A.PREMIUMSUM) INCOMERATE,
                A.STATUS, A.DISCR, A.APPLISN, A.SHAREPC, A.REINSPC, A.GROUPISN, A.BIZFLG, A.PARENTISN,
                /*INSURANTISN переименован в INSURANTISN# для того, чтобы выяснить где использовались эти поля.
                В дальнейшем, эти два поля надо из repagr убрать (вемсте с agrrole) */
                -- nvl(min(decode(AR.CLASSISN, 430, AR.SUBJISN)), CLIENTISN) INSURANTISN#,
                -- count(decode(AR.CLASSISN, 430, 1)) INSURANTCOUNT#,
                to_number(null) INSURANTISN#,
                to_number(null) INSURANTCOUNT#,
                A.COMISSION COMISSION,
                null BUHDATE,
                A.LIMITSUM,
                decode(A.CURRISN, 53, A.LIMITSUM, gcc2.gcc2(A.LIMITSUM, A.CURRISN, 53, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) LIMITSUMUSD,
                decode(A.CURRISN, 35, A.LIMITSUM, gcc2.gcc2(A.LIMITSUM, A.CURRISN, 35, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) LIMITSUMRUB,
                A.INSUREDSUM,
                decode(A.CURRISN, 53, A.INSUREDSUM, gcc2.gcc2(A.INSUREDSUM, A.CURRISN, 53, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) INSUREDSUMUSD,
                decode(A.CURRISN, 35, A.INSUREDSUM, gcc2.gcc2(A.INSUREDSUM, A.CURRISN, 35, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) INSUREDSUMRUB,
                A.CREATED AGRCREATED,
                A.FIRMISN,
                A.OWNERDEPTISN OWNERDEPTISN,
                CLNT.JURIDICAL CLIENTJURIDICAL,
                A.PREVISN PREVISN,
                A.OLDDATEEND OLDDATEEND,
                GM.GMISN GMISN,
                A.ADDRISN,
                A.DATECALC,
                AD.AGRDETAILISN AGRDETAILISN, -- OD 11.11.09 детализация дог-ра
                A.SALESCHANNELISN SALESCHANNELISN,
                a.datebase AS datebase, -- EGAO 05.07.2010
                a.formisn AS formisn, -- EGAO 0.03.2011
                A.CREATEDBY CREATEDBY, -- OD 22.03.2011
                -- sts 19.10.2012 - task(38397275003)
                A.INCOMESUM,
                system.gcc2.gcc2(A.INCOMESUM, A.CURRISN, 53, least(A.DATESIGN, trunc(sysdate), A.DATEBEG)) as INCOMESUMUSD,
                system.gcc2.gcc2(A.INCOMESUM, A.CURRISN, 35, least(A.DATESIGN, trunc(sysdate), A.DATEBEG)) as INCOMESUMRUB,
                -- sts 06.11.2012 - скидка для туристов
                A.DISCOUNT,
                a.dateissue, -- EGAO 08.05.2013
                null CREATEDATE
           from TT_ROWID R,
                AIS.AGREEMENT A,
                AIS.SUBJECT_T CLNT,
                -- AIS.AGRROLE AR,
                DICTI DR,
                AGR_DETAIL_AGRHASH AD, -- OD 11.11.09 Детализация договора
                ( select --+ ordered use_nl(x) index(x x_agrext_agr) use_hash(d)
                         AGRISN,
                         max(X1) GMISN
                    from TT_ROWID R,
                         AGREXT X,
                         ( select ISN
                             from DICTI Z
                            start with ISN = 2255842303 -- продукты gm
                          connect by prior ISN = PARENTISN ) D
                   where X.AGRISN   = R.ISN
                     and X.CLASSISN = 1071774425
                     and X.X1       = D.ISN
                   group by AGRISN ) GM
          where A.ISN          = R.ISN
            -- and A.ISN          = AR.AGRISN(+)
            and A.RULEISN      = DR.ISN(+)
            and A.ISN          = AD.AGRISN(+)
            and R.ISN          = GM.AGRISN(+)
            -- and AR.CLASSISN(+) = 430 -- страхователь
            and A.DISCR in ('Д', 'Г')
            and A.CLASSISN in ( select ISN
                                  from DICTI
                                 start with ISN = 34711216 -- тип договора страхования
                               connect by prior ISN = PARENTISN )
            AND  A.CLIENTISN=CLNT.ISN(+)
/* KGS  24.10.11 Нафиг не нужен тут этот гроупбай
          group by A.ISN, A.ID, A.DATEBEG, A.DATEEND, A.DATESIGN, A.CLASSISN, A.RULEISN,
                   A.DEPTISN, DR.FILTERISN, A.EMPLISN, A.CLIENTISN, A.CURRISN, A.INCOMESUM,
                   A.PREMIUMSUM, A.STATUS, A.DISCR, A.APPLISN, A.SHAREPC, A.REINSPC, A.GROUPISN,
                   A.BIZFLG, A.PARENTISN, LIMITSUM, INSUREDSUM, A.CREATED, A.FIRMISN, A.ADDRISN,
                   A.DATECALC */) S,
      REPAGRROLEAGR AR,
      REP_DEPT SD,
      REP_DEPT FIL,
      REP_DEPT BFIL
where S.DEPTISN   = SD.DEPTISN(+)
  and AR.EMITISN  = FIL.DEPTISN(+)
  and AR.BEMITISN = BFIL.DEPTISN(+)
  and S.AGRISN    = AR.AGRISN(+)
)


  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPAGR_ECONOMIC" ("AGRISN", "ID", "DATEBEG", "DATEEND", "DATESIGN", "CLASSISN", "RULEISN", "DEPTISN", "DEPT0ISN", "FILISN", "RULEDEPT", "EMPLISN", "CLIENTISN", "CURRISN", "PREMIUMSUM", "PREMUSD", "PREMRUB", "PREMEUR", "INCOMERATE", "STATUS", "DISCR", "APPLISN", "SHAREPC", "REINSPC", "GROUPISN", "BIZFLG", "PARENTISN", "INSURANTISN", "INSURANTCOUNT", "AGENTISN", "AGENTCOUNT", "EMITISN", "EMITCOUNT", "COMISSION", "BUHDATE", "LIMITSUM", "LIMITSUMUSD", "INSUREDSUM", "INSUREDSUMUSD", "AGRCREATED", "AGENTJURIDICAL", "FIRMISN") AS 
  (select --+ ordered use_nl( r a ar dr drp sd sdf sdf1) no_merge ( zdept ) no_merge( zdept2 ) use_hash (zdept zdept2 )
      a.isn agrisn,
      a.id,
      a.datebeg,
      a.dateend,
      a.datesign,
      a.classisn,
      a.ruleisn,
      a.deptisn,
      sd.isn dept0Isn,
      Min(Nvl(sdf.isn,sdf1.isn)) filisn,
      dr.filterisn ruledept,
      a.emplisn,
      a.clientisn,
      a.currisn,
      a.premiumsum,
      decode(a.currisn,53,a.premiumsum,gcc2.gcc2(a.premiumsum,a.currisn,53,a.datebeg)) PremUSD,
      decode(a.currisn,35,a.premiumsum,gcc2.gcc2(a.premiumsum,a.currisn,35,a.datebeg)) PremRUB,
      decode(a.currisn,29448516,a.premiumsum,gcc2.gcc2(a.premiumsum,a.currisn,29448516,a.datebeg)) PremEUR,
      decode(a.premiumsum,0,0,a.incomesum/a.premiumsum) IncomeRate,
      a.status,
      a.discr,
      a.applisn,
      a.sharepc,
      a.reinspc,
      a.groupisn,
      a.bizflg,
      a.parentisn,
      nvl(min(decode(ar.classisn,430 /*c.get('Insured')*/, subjisn)), clientisn) InsurantIsn,
      count(decode(ar.classisn,430 /*c.get('Insured')*/, 1)) InsurantCount,
      min(decode(ar.classisn,437 /*c.get('Agent')*/, subjisn, 438, subjisn)) AgentIsn,
      sum(decode(ar.classisn,437 /*c.get('Agent')*/, 1, 438,1)) AgentCount,
      min(decode(ar.classisn,13157916 /*c.get('Emittent')*/, subjisn)) EmitIsn,
      count(decode(ar.classisn,13157916 /*c.get('Emittent')*/ ,1)) EmitCount,
      Max(a.comission) comission,
      to_date(Null) BuhDate,
      a.limitsum,
      decode(a.currisn,53,a.limitsum,gcc2.gcc2(a.limitsum,a.currisn,53,a.datebeg)) limitsumUsd,
      a.insuredsum,
      decode(a.currisn,53,a.insuredsum,gcc2.gcc2(a.insuredsum,a.currisn,53,a.datebeg)) insuredsumUsd,
      a.created agrcreated,
      min(case when ar.classisn in(437,438) then (Select JURIDICAL From Ais.Subject Where Isn=Ar.subjisn)end) AGENTJURIDICAL,
      a.firmisn
FROM tt_rowId t,
     ais.agreement a,
     ais.agrrole ar,
     dicti dr,
     ais.subdept sd,
     ais.subdept sdf,
     ais.subdept sdf1,
     (SELECT isn,
             (SELECT DISTINCT first_value(case when x.classisn=956867125 or x.parentisn = 28763316/*c.get('SubsideDept')*/ then isn end)
                      over (order by case when x.classisn=956867125 then Level end desc nulls last,
                            CASE when x.parentisn = 28763316/*c.get('SubsideDept')*/ then Level end desc nulls last
                            ROWS BETWEEN unbounded preceding and unbounded following)
              FROM ais.subdept x
              CONNECT BY PRIOR x.parentisn = x.isn
              START WITH x.isn=z.isn
            ) AS filisn
      FROM ais.subdept z
     ) zdept,
     (SELECT isn,
             (SELECT DISTINCT first_value(case when x.classisn=956867125 or x.parentisn = 28763316/*c.get('SubsideDept')*/ then isn end)
                      over (order by case when x.classisn=956867125 then Level end desc nulls last,
                            CASE when x.parentisn = 28763316/*c.get('SubsideDept')*/ then Level end desc nulls last
                            ROWS BETWEEN unbounded preceding and unbounded following)
              FROM ais.subdept x
              CONNECT BY PRIOR x.parentisn = x.isn
              START WITH x.isn=z.isn
            ) AS filisn
      FROM ais.subdept z
     ) zdept2
WHERE t.isn=a.isn
  and a.discr in('Д','Г')
  and a.classisn  in (select isn from dicti start with isn = 12415216 connect  by prior isn = parentisn)
  and ar.agrisn(+)=a.isn
  and dr.isn(+)=a.ruleisn
  and sd.rowid=(select rowid from ais.subdept z where parentisn=0 start with isn=a.deptisn connect by prior parentisn = isn)

  /*and sdf.isn  --(+) MSerp 26.10.2009. Убрал открытый join, т.к. в 10g этот фокус больше не проходит. Если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. Насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
            = Nvl(
            (SELECT distinct first_value(case when classisn=956867125 or parentisn = 28763316\*c.get('SubsideDept')*\ then isn end)
                    over (order by case when classisn=956867125 then Level end desc nulls last,
                             case when parentisn = 28763316\*c.get('SubsideDept')*\ then Level end desc nulls last
                    rows between unbounded preceding and unbounded following )
            from subdept z
            start with isn=a.deptisn
            connect by prior parentisn = isn
            ),0*a.isn)*/
  -- EGAO 27.10.2009 Глюки появились
  AND zdept.isn(+)=a.deptisn
  AND sdf.isn(+)=zdept.filisn

  /*and sdf1.isn  --(+) MSerp 26.10.2009. Убрал открытый join, т.к. в 10g этот фокус больше не проходит. Если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. Насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
              =Nvl(
              (select
              distinct first_value(case when classisn=956867125 or parentisn = 28763316\*c.get('SubsideDept')*\ then isn end)
              over (order by case when classisn=956867125 then Level end desc nulls last,
                       case when parentisn = 28763316\*c.get('SubsideDept')*\ then Level end desc nulls last
              rows between unbounded preceding and unbounded following )
              from subdept z start with isn=decode(ar.classisn,13157916 \*c.get('Emittent')*\, subjisn) connect by prior parentisn = isn
              ),0*a.isn)*/
  -- EGAO 27.10.2009 Глюки появились
  AND zdept2.isn(+)=CASE ar.classisn WHEN 13157916 THEN ar.subjisn END
  AND sdf1.isn(+)=zdept2.filisn
group by
        a.isn,
        a.id,
        a.datebeg,
        a.dateend,
        a.datesign,
        a.classisn,
        a.ruleisn,
        a.deptisn,
        dr.filterisn,
        a.emplisn,
        a.clientisn,
        a.currisn,
        a.incomesum,
        a.premiumsum,
        a.status,
        a.discr,
        a.applisn,
        a.sharepc,
        a.reinspc,
        a.groupisn,
        a.bizflg,
        a.parentisn,
        sd.isn,
        LimitSum,
        insuredsum,
        a.created,
        a.firmisn
)

(Select --+ use_nl(t l c) ordered
       l.AgrIsn, Max(decode(c.classisn,10908816,1,0)) Sea,
       Decode(count(distinct c.classisn),0,0,1,0,1) MORE1
     from tt_rowid t,Ais.agrlimit l,Ais.crgroute c
     where t.isn=L.agrisn And l.isn = c.isn
     Group by l.AgrIsn)