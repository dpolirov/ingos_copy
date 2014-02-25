create or replace view storage_adm.v_repagrroleagr (
   agrisn,
   agentisn,
   salergoisn,
   salerfisn,
   crossalerisn,
   cardealerisn,
   brokerisn,
   partnerisn,
   leaseficiaryisn,
   pawnbrokerisn,
   recommenderisn,
   irecommenderisn,
   emitisn,
   bemitisn,
   agentdeptisn,
   agentjuridical,
   agentcollectflg,
   agentcount,
   emitcount,
   filcommision,
   transfercomission,
   beneficiaryisn,
   brokerdeptisn,
   brokerjuridical,
   brokercollectflg,
   brokercount,
   headclient,
   agentsharepc,
   brokersharepc,
   reinclientisn,
   salergodeptisn,
   salerfdeptisn,
   managerkkisn,
   empopgoisn,
   empopgodeptisn,
   uprisn,
   empoperu,
   salergoclassisn,
   salerfclassisn,
   crossalerdeptisn,
   avtodillerisn,
   admcuratorisn,
   doctorcuratorisn,
   underwriterisn,
   underwriteroldisn,
   representativeisn,
   crossalerfisn,
   crossalerfdeptisn,
   agent_maxcomission_isn,
   contractorisn,
   contrcomission,
   contrcount,
   agent_maxcomission_sharepc )
as
(
select distinct R.AGRISN,

       -- sts 20.09.2011 - добавил nvl() на даты и привел сортировку связанных полей к одному виду
       -- нпр, поля агента имели разную сортировку для AGENTISN и AGENTJURIDICAL
       -- kgs агентам и брокерам и их аттрибутам поставил FIST, R.DATEBEG asc

       first_value(case R.CLASSISN when 437 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 437 then 1 else 0 end desc, R.CALCFLG desc, R.ORDERNO desc, R.DATEBEG, R.ISN, case R.CLASSISN when 437 then R.SUBJISN end desc) as AGENTISN,  -- агент
       first_value(case R.CODE when 'SALES_G' then R.SUBJISN end) over (partition by R.AGRISN order by case R.CODE when 'SALES_G' then 1 else 0 end desc, /*sts 10.10.2012 - флаг не учитывать (by Гоша) R.CALCFLG, */ R.DATEEND desc, R.ISN desc, case R.CODE when 'SALES_G' then R.SUBJISN end desc) as SALERGOISN,                -- продавец головного офиса
       first_value(case R.CODE when 'SALES_F' then R.SUBJISN end) over (partition by R.AGRISN order by case R.CODE when 'SALES_F' then 1 else 0 end desc, /* R.CALCFLG, */ R.DATEEND desc, R.ISN desc, case R.CODE when 'SALES_F' then R.SUBJISN end desc) as SALERFISN,                 -- продавец филиала
       first_value(case R.CLASSISN when 1738886903 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 1738886903 then 1 else 0 end desc, /* R.CALCFLG, */ R.DATEEND desc, R.ISN desc, case R.CLASSISN when 1738886903 then R.SUBJISN end desc) as CROSSALERISN,    -- кросс-продавец головного офиса
       first_value(case R.CLASSISN when 731194000 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 731194000 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 731194000 then R.SUBJISN end desc) as CARDEALERISN,      -- автосалон
       first_value(case R.CLASSISN when 438 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 438 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 438 then R.SUBJISN end desc) as BROKERISN,                     -- брокер
       first_value(case R.CLASSISN when 13381416 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 13381416 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 13381416 then R.SUBJISN end desc) as PARTNERISN,          -- партнер
       first_value(case R.CLASSISN when 682566316 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 682566316 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 682566316 then R.SUBJISN end desc) as LEASEFICIARYISN,   -- лизингополучатель
       first_value(case R.CLASSISN when 1064403825 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 1064403825 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 1064403825 then R.SUBJISN end desc) as PAWNBROKERISN,   -- залогодержатель
       first_value(case R.CLASSISN when 47228116 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 47228116 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 47228116 then R.SUBJISN end desc) as RECOMMENDERISN,      -- рекомендатель
       first_value(case R.CLASSISN when 1574889603 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 1574889603 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 1574889603 then R.SUBJISN end desc) as IRECOMMENDERISN, -- внутренний рекомендатель
       first_value(case R.CLASSISN when 13157916 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 13157916 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 13157916 then R.SUBJISN end desc) as EMITISN,             -- эмитент полиса
       first_value(case R.CLASSISN when 1617366603 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 1617366603 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 1617366603 then R.SUBJISN end desc) as BEMITISN,        -- эмитент бизнеса
       first_value(case R.CLASSISN when 437 then R.ROLEDEPTISN end) over (partition by R.AGRISN order by case R.CLASSISN when 437 then 1 else 0 end desc, R.CALCFLG desc, R.ORDERNO desc, R.DATEBEG, R.ISN, case R.CLASSISN when 437 then R.ROLEDEPTISN end desc) as AGENTDEPTISN,
       first_value(case R.CLASSISN when 437 then R.JURIDICAL end) over (partition by R.AGRISN order by case R.CLASSISN when 437 then 1 else 0 end desc, R.CALCFLG desc, R.ORDERNO desc, R.DATEBEG, R.ISN, case R.CLASSISN when 437 then R.JURIDICAL end desc) as AGENTJURIDICAL,
       first_value(case R.CLASSISN when 437 then R.COLLECTFLG end) over (partition by R.AGRISN order by case R.CLASSISN when 437 then 1 else 0 end desc, R.CALCFLG desc, R.ORDERNO desc, R.DATEBEG, R.ISN, case R.CLASSISN when 437 then R.COLLECTFLG end desc) as AGENTCOLLECTFLG,
       count(case R.CLASSISN when 437 then R.SUBJISN end) over (partition by R.AGRISN) as AGENTCOUNT,
       count(case R.CLASSISN when 13157916 then R.SUBJISN end) over (partition by R.AGRISN) as EMITCOUNT,
       min(greatest(coalesce(case R.CLASSISN when 13157916/*c.get('emittent')*/ then R.SHAREPC end, 0),
                    coalesce(case R.CLASSISN when 1617366603/*bemittent*/ then R.SHAREPC end, 0))) over (partition by R.AGRISN) as FILCOMMISION,
       min(case R.CLASSISN when 1738886903 then R.SHAREPC end) over (partition by R.AGRISN) as TRANSFERCOMISSION,
       /* sts 12.07.2012 task(34327332503)
       count(decode(R.CLASSISN, 430, 1)) INSURANTCOUNT,
       */
       first_value(case R.CLASSISN when 433 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 433 then 1 else 0 end desc, R.CALCFLG desc, R.DATEBEG desc, R.ISN desc, case R.CLASSISN when 433 then R.SUBJISN end desc) as BENEFICIARYISN,            -- выгодоприобретатель
       first_value(case R.CLASSISN when 438 then R.ROLEDEPTISN end) over (partition by R.AGRISN order by case R.CLASSISN when 438 then 1 else 0 end desc, R.CALCFLG desc, R.DATEBEG desc, R.ISN desc, case R.CLASSISN when 438 then R.ROLEDEPTISN end desc) as BROKERDEPTISN,
       first_value(case R.CLASSISN when 438 then R.JURIDICAL end) over (partition by R.AGRISN order by case R.CLASSISN when 438 then 1 else 0 end desc, R.CALCFLG desc, R.DATEBEG desc, R.ISN desc, case R.CLASSISN when 438 then R.JURIDICAL end desc) as BROKERJURIDICAL,
       first_value(case R.CLASSISN when 438 then R.COLLECTFLG end) over (partition by R.AGRISN order by case R.CLASSISN when 438 then 1 else 0 end desc, R.CALCFLG desc, R.DATEBEG desc, R.ISN desc, case R.CLASSISN when 438 then R.COLLECTFLG end desc) as BROKERCOLLECTFLG,
       count(case R.CLASSISN when 438 then R.SUBJISN end) over (partition by R.AGRISN) as BROKERCOUNT,
       first_value(case R.CLASSISN when 2616961403 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 2616961403 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 2616961403 then R.SUBJISN end desc) as HEADCLIENT,       -- головной клиент
       first_value(case R.CLASSISN when 437 then R.SHAREPC end) over (partition by R.AGRISN order by case R.CLASSISN when 437 then 1 else 0 end desc, R.CALCFLG desc, R.ORDERNO desc, R.DATEBEG, R.ISN, case R.CLASSISN when 437 then R.SHAREPC end desc) as AGENTSHAREPC,
       first_value(case R.CLASSISN when 438 then R.SHAREPC end) over (partition by R.AGRISN order by case R.CLASSISN when 438 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 438 then R.SHAREPC end desc) as BROKERSHAREPC,
       first_value(case classisn when 434 then SubjIsn when 435 then SubjIsn end) over (partition by R.AGRISN order by case classisn when 434 then 1 when 435 then 1 else 0 end desc, case SUBJCLASSISN when 12212016 then 0 when 658813916 then 0 else 1 end desc, case CalcFlg when 'Y' then 1 else 0 end desc, coalesce(R.SHAREPC,0) desc, SubjIsn desc, case classisn when 434 then SubjIsn when 435 then SubjIsn end desc) as REINCLIENTISN, -- перестрахователь или перестраховщик из участников , брокеров не в приоритет
       -- sts 20.09.2011 - добавил подразделения продавцов
       first_value(case R.CODE when 'SALES_G' then R.ROLEDEPTISN end) over (partition by R.AGRISN order by case R.CODE when 'SALES_G' then 1 else 0 end desc, /* R.CALCFLG, */ R.DATEEND desc, R.ISN desc, case R.CODE when 'SALES_G' then R.ROLEDEPTISN end desc) as SALERGODEPTISN,                -- подразделение продавца головного офиса
       first_value(case R.CODE when 'SALES_F' then R.ROLEDEPTISN end) over (partition by R.AGRISN order by case R.CODE when 'SALES_F' then 1 else 0 end desc, /* R.CALCFLG, */ R.DATEEND desc, R.ISN desc, case R.CODE when 'SALES_F' then R.ROLEDEPTISN end desc) as SALERFDEPTISN,                  -- подразделение продавца филиала
       first_value(case R.CLASSISN when 1943199903 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 1943199903 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 1943199903 then R.SUBJISN end desc) as MANAGERKKISN,          --распорядитель кк(комерч. кредит)
       -- {EGAO 13.04.2012
       first_value(case R.CLASSISN when 2846444203 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 2846444203 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 2846444203 then R.SUBJISN end desc) as EMPOPGOISN,  --сотрудник оп го
       first_value(case R.CLASSISN when 2846444203 then R.ROLEDEPTISN end) over (partition by R.AGRISN order by case R.CLASSISN when 2846444203 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 2846444203 then R.ROLEDEPTISN end desc) as EMPOPGODEPTISN,
       --}
       --{EGAO 17.04.2012
       first_value(CASE WHEN r.code='SALES_G' OR R.CLASSISN=2846444203 THEN uprisn END) over (partition by R.AGRISN ORDER BY CASE WHEN r.code='SALES_G' THEN 3 WHEN R.CLASSISN=2846444203 THEN 2 ELSE 1 END desc, case r.code when 'SALES_G' then 'Y' else R.CALCFLG end desc, R.DATEEND desc, R.ISN desc, CASE WHEN r.code='SALES_G' OR R.CLASSISN=2846444203 THEN uprisn END desc) as uprisn, -- EGAO 17.04.2012 Виды аналитики "Подразделения Ингосстрах"
       --}
       -- EGAO 21.05.2012 в рамках ДИТ-12-2-167253
       first_value(case r.classisn when 1101648603 then r.subjisn end) over (partition by R.AGRISN ORDER BY case r.classisn when 1101648603 then 1 else 0 end desc, r.calcflg desc, r.dateend desc, r.isn desc, case r.classisn when 1101648603 then r.subjisn end desc) AS empoperu,
       -- sts 20.09.2012 - добавил роли продавцов
       first_value(case R.CODE when 'SALES_G' then R.CLASSISN end) over (partition by R.AGRISN order by case R.CODE when 'SALES_G' then 1 else 0 end desc, /* R.CALCFLG, */ R.DATEEND desc, R.ISN desc, case R.CODE when 'SALES_G' then R.CLASSISN end desc) as SALERGOCLASSISN,        -- класс (роль в договоре) продавца головного офиса
       first_value(case R.CODE when 'SALES_F' then R.CLASSISN end) over (partition by R.AGRISN order by case R.CODE when 'SALES_F' then 1 else 0 end desc, /* R.CALCFLG, */ R.DATEEND desc, R.ISN desc, case R.CODE when 'SALES_F' then R.CLASSISN end desc) as SALERFCLASSISN ,         -- класс (роль в договоре) продавца филиала
       first_value(case R.CLASSISN when 1738886903 then R.ROLEDEPTISN end) over (partition by R.AGRISN order by case R.CLASSISN when 1738886903 then 1 else 0 end desc, /* R.CALCFLG, */ R.DATEEND desc, R.ISN desc, case R.CLASSISN when 1738886903 then R.ROLEDEPTISN end desc) as CROSSALERDEPTISN,
       -- sts 07.12.2012 - добавил роль автодиллер
       first_value(case R.CLASSISN when 3081540003 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 3081540003 then 1 else 0 end desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 3081540003 then R.SUBJISN end desc) as AvtoDillerISN,   -- АВТОДИЛЕР (CarDealerISN уже есть выше как "Автосалон", поэтому AvtoDillerISN)
       -- sts 07.12.2012 - task(40524747403)
       first_value(case R.CLASSISN when 2626553403 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 2626553403 then 1 else 0 end desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 2626553403 then R.SUBJISN end desc) as AdmCuratorISN,   -- Административный куратор
       first_value(case R.CLASSISN when 693962316 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 693962316 then 1 else 0 end desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 693962316 then R.SUBJISN end desc) as DoctorCuratorISN,   -- Врач-куратор
       first_value(case R.CLASSISN when 693962016 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 693962016 then 1 else 0 end desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 693962016 then R.SUBJISN end desc) as UnderwriterISN,   -- Андеррайтер
       first_value(case R.CLASSISN when 444 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 444 then 1 else 0 end desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 444 then R.SUBJISN end desc) as UnderwriterOldISN,   -- Андеррайтер(старый)
       -- sts 05.01.2013 - task(ДИТ-12-4-173936)
       first_value(case R.CLASSISN when 35435216 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 35435216 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 3676722703 then R.SUBJISN end desc) as REPRESENTATIVEISN,      -- ПРЕДСТАВИТЕЛЬ
       -- VAA 07.05.2013 ДИТ-13-2-199474
       first_value(case R.CLASSISN when 3676722703 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 3676722703 then 1 else 0 end desc, /* R.CALCFLG, */ R.DATEEND desc, R.ISN desc, case R.CLASSISN when 3676722703 then R.SUBJISN end desc) as CROSSALERFISN,    -- кросс-продавец филиала
       first_value(case R.CLASSISN when 3676722703 then R.ROLEDEPTISN end) over (partition by R.AGRISN order by case R.CLASSISN when 3676722703 then 1 else 0 end desc, /* R.CALCFLG, */ R.DATEEND desc, R.ISN desc, case R.CLASSISN when 3676722703 then R.ROLEDEPTISN end desc) as CROSSALERFDEPTISN, -- подразделение кросс-продавца филиала
       -- kds (06.08.2013) task(52120579303)
       first_value(case R.CLASSISN when 437 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 437 then 1 else 0 end desc, R.SHAREPC desc, R.DATEEND desc, R.datebeg desc, R.isn desc, case R.CLASSISN when 437 then R.SUBJISN end desc) as AGENT_MAXCOMISSION_ISN,         -- агент с максимальной комиссией
       -- kds (30.09.2013) task(???)
       first_value(case R.CLASSISN when 4207938903 then R.SUBJISN end) over (partition by R.AGRISN order by case R.CLASSISN when 4207938903 then 1 else 0 end desc, R.CALCFLG desc, R.DATEEND desc, R.ISN desc, case R.CLASSISN when 4207938903 then R.SUBJISN end desc) as CONTRACTORISN, -- Подрядчик
       avg(case R.CLASSISN when 4207938903 then R.SHAREPC end) over (partition by R.AGRISN) as CONTRCOMISSION, -- Процент подрядчика
       count(case R.CLASSISN when 4207938903 then 1 else null end) over (partition by R.AGRISN) as CONTRCOUNT, -- Количество подрядчиков
       -- VAA 14.01.2014
       first_value(case R.CLASSISN when 437 then R.SHAREPC end) over (partition by R.AGRISN order by case R.CLASSISN when 437 then 1 else 0 end desc, R.SHAREPC desc, R.DATEEND desc, r.datebeg desc, r.isn desc, case R.CLASSISN when 437 then R.SHAREPC end desc) as AGENT_MAXCOMISSION_SHAREPC
  from ( select 
                R.AGRISN,
                R.CLASSISN,
                R.SUBJISN,
                coalesce(R.DATEBEG, TO_DATE('01-01-1900','DD-MM-YYYY')) as DATEBEG,
                coalesce(R.DATEEND, TO_DATE('01-01-3000','DD-MM-YYYY')) as DATEEND,
                R.SHAREPC,
                R.SHARESUM,
                coalesce(R.CALCFLG, 'N') as CALCFLG,
                R.DEPTISN as ROLEDEPTISN,
                D.CODE,
                R.COLLECTFLG,
                SH.DEPTISN,
                S.JURIDICAL,
                R.ISN,
                S.CLASSISN as SUBJCLASSISN,
                rdt.uprisn, -- EGAO 13.04.2012
                abs(R.OrderNO) as OrderNO  -- sts 22.05.2012 - сортировка по модулю
           from storage_adm.tt_rowid t 
			join 
                ais.AGRROLE R 
                 on t.ISN = R.AGRISN 
			left join
                ais.SUBJECT S 
                 on R.SUBJISN = S.ISN
			left join
                ais.SUBHUMAN SH 
                 on R.SUBJISN = SH.ISN
			left join
               (select
                         D.ISN,
                         D.CODE
                    from ais.DICTI D
                   where D.CODE in ('SALES_G', 'SALES_F')
                     and D.ISN <> 1738886903) as D -- кросс-продавец головного офиса (sts - т.к. кросс продавец выше идет отдельно)
                 on R.CLASSISN  = D.ISN
			left join
                storage_source.rep_dept rdt -- EGAO 13.04.2012
                 on rdt.deptisn = R.deptisn
            ) as R
 where R.CLASSISN <> 430; -- страхователь
);
