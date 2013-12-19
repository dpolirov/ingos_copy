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
 group by R.AGRISN;