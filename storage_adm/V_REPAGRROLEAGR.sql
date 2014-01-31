create or replace view v_repagrroleagr (
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
    select r.agrisn,
           max(decode(r.classisn, 437, r.subjisn))keep(dense_rank last order by decode(r.classisn, 437, 1, 0), r.calcflg, r.orderno,r.datebeg desc, r.isn desc) agentisn,  -- агент
           max(decode(r.code, 'sales_g', r.subjisn))keep(dense_rank last order by decode(r.code, 'sales_g', 1, 0),r.dateend, r.isn) salergoisn,                -- продавец головного офиса
           max(decode(r.code, 'sales_f', r.subjisn))keep(dense_rank last order by decode(r.code, 'sales_f', 1, 0), r.dateend, r.isn) salerfisn,                 -- продавец филиала
           max(decode(r.classisn, 1738886903, r.subjisn))keep(dense_rank last order by decode(r.classisn, 1738886903, 1, 0), r.dateend, r.isn) crossalerisn,    -- кросс-продавец головного офиса
           max(decode(r.classisn, 731194000, r.subjisn))keep(dense_rank last order by decode(r.classisn, 731194000, 1, 0), r.calcflg, r.dateend, r.isn) cardealerisn,      -- автосалон
           max(decode(r.classisn, 438, r.subjisn))keep(dense_rank last order by decode(r.classisn, 438, 1, 0), r.calcflg, r.dateend, r.isn) brokerisn,                     -- брокер
           max(decode(r.classisn, 13381416, r.subjisn))keep(dense_rank last order by decode(r.classisn, 13381416, 1, 0), r.calcflg, r.dateend, r.isn) partnerisn,          -- партнер
           max(decode(r.classisn, 682566316, r.subjisn))keep(dense_rank last order by decode(r.classisn, 682566316, 1, 0), r.calcflg, r.dateend, r.isn) leaseficiaryisn,   -- лизингополучатель
           max(decode(r.classisn, 1064403825, r.subjisn))keep(dense_rank last order by decode(r.classisn, 1064403825, 1, 0), r.calcflg, r.dateend, r.isn) pawnbrokerisn,   -- залогодержатель
           max(decode(r.classisn, 47228116, r.subjisn))keep(dense_rank last order by decode(r.classisn, 47228116, 1, 0), r.calcflg, r.dateend, r.isn) recommenderisn,      -- рекомендатель
           max(decode(r.classisn, 1574889603, r.subjisn))keep(dense_rank last order by decode(r.classisn, 1574889603, 1, 0), r.calcflg, r.dateend, r.isn) irecommenderisn, -- внутренний рекомендатель
           max(decode(r.classisn, 13157916, r.subjisn))keep(dense_rank last order by decode(r.classisn, 13157916, 1, 0), r.calcflg, r.dateend, r.isn) emitisn,             -- эмитент полиса
           max(decode(r.classisn, 1617366603, r.subjisn))keep(dense_rank last order by decode(r.classisn, 1617366603, 1, 0), r.calcflg, r.dateend, r.isn) bemitisn,        -- эмитент бизнеса
           max(decode(r.classisn, 437, r.roledeptisn))keep (dense_rank last order by decode(r.classisn, 437, 1, 0), r.calcflg, r.orderno,r.datebeg desc, r.isn desc) agentdeptisn,
           max(decode(r.classisn, 437, r.juridical))keep (dense_rank last order by decode(r.classisn, 437, 1, 0), r.calcflg, r.orderno,r.datebeg desc, r.isn desc) agentjuridical,
           max(decode(r.classisn, 437, r.collectflg))keep (dense_rank last order by decode(r.classisn, 437, 1, 0), r.calcflg, r.orderno,r.datebeg desc, r.isn desc) agentcollectflg,
           count(decode(r.classisn, 437, r.subjisn)) agentcount,
           count(decode(r.classisn, 13157916, r.subjisn)) emitcount,
           min(greatest(oracompat.nvl(decode(r.classisn, 13157916, r.sharepc), 0),
                        oracompat.nvl(decode(r.classisn, 1617366603/*bemittent*/, r.sharepc), 0))) filcommision,
           min(decode(r.classisn, 1738886903, r.sharepc)) transfercomission,
           max(decode(r.classisn, 433, r.subjisn))keep(dense_rank last order by decode(r.classisn, 433, 1, 0), r.calcflg,r.datebeg asc, r.isn desc) beneficiaryisn,            -- выгодоприобретатель
           max(decode(r.classisn, 438, r.roledeptisn))keep(dense_rank last order by decode(r.classisn, 438, 1, 0), r.calcflg, r.datebeg asc, r.isn) brokerdeptisn,
           max(decode(r.classisn, 438, r.juridical))keep(dense_rank last order by decode(r.classisn, 438, 1, 0), r.calcflg, r.datebeg asc, r.isn) brokerjuridical,
           max(decode(r.classisn, 438, r.collectflg))keep(dense_rank last order by decode(r.classisn, 438, 1, 0), r.calcflg, r.datebeg asc , r.isn) brokercollectflg,
           count(decode(r.classisn, 438, r.subjisn)) brokercount,
           max(decode(r.classisn, 2616961403, r.subjisn))keep(dense_rank last order by decode(r.classisn, 2616961403, 1, 0), r.calcflg, r.dateend, r.isn) headclient,       -- головной клиент
           max(decode(r.classisn, 437, r.sharepc))keep (dense_rank last order by decode(r.classisn, 437, 1, 0), r.calcflg, r.orderno, r.datebeg desc, r.isn desc) agentsharepc,
           max(decode(r.classisn, 438, r.sharepc))keep(dense_rank last order by decode(r.classisn, 438, 1, 0), r.calcflg, r.dateend, r.isn) brokersharepc,
           max(decode(classisn,434,subjisn,435,subjisn,null)) keep(dense_rank last order by decode(classisn,434,1,435,1,0), decode(subjclassisn,12212016,0,658813916,0,1), decode(calcflg,'y',1,0),oracompat.nvl(r.sharepc,0),subjisn) reinclientisn, -- перестрахователь или перестраховщик из участников , брокеров не в приоритет
           max(decode(r.code, 'sales_g', r.roledeptisn))keep(dense_rank last order by decode(r.code, 'sales_g', 1, 0), /* r.calcflg, */ r.dateend, r.isn) salergodeptisn,                -- подразделение продавца головного офиса
           max(decode(r.code, 'sales_f', r.roledeptisn))keep(dense_rank last order by decode(r.code, 'sales_f', 1, 0), /* r.calcflg, */ r.dateend, r.isn) salerfdeptisn,                  -- подразделение продавца филиала
           max(decode(r.classisn, 1943199903, r.subjisn))keep(dense_rank last order by decode(r.classisn, 1943199903, 1, 0), r.calcflg, r.dateend, r.isn) managerkkisn,          --распорядитель кк(комерч. кредит)
           max(decode(r.classisn, 2846444203, r.subjisn))keep(dense_rank last order by  decode(r.classisn, 2846444203, 1, 0), r.calcflg, r.dateend, r.isn) empopgoisn,  --сотрудник оп го
           max(decode(r.classisn, 2846444203, r.roledeptisn))keep(dense_rank last order by  decode(r.classisn, 2846444203, 1, 0), r.calcflg, r.dateend, r.isn) empopgodeptisn,
           max(case 
                    when r.code = 'sales_g' or r.classisn = 2846444203 
                        then uprisn 
               end) keep (dense_rank last order by case 
                                                           when r.code = 'sales_g' 
                                                                then 3 
                                                           when r.classisn = 2846444203 
                                                                then 2 
                                                           else 1 
                                                      end, decode(r.code, 'sales_g', 'y', r.calcflg), r.dateend, r.isn) uprisn, -- egao 17.04.2012 виды аналитики "подразделения ингосстрах"
           max(decode(r.classisn,1101648603,r.subjisn))keep(dense_rank last order by decode(r.classisn,1101648603,1,0), r.calcflg, r.dateend, r.isn) as empoperu,
           max(decode(r.code, 'sales_g', r.classisn))keep(dense_rank last order by decode(r.code, 'sales_g', 1, 0), /* r.calcflg, */ r.dateend, r.isn) as salergoclassisn,        -- класс (роль в договоре) продавца головного офиса
           max(decode(r.code, 'sales_f', r.classisn))keep(dense_rank last order by decode(r.code, 'sales_f', 1, 0), /* r.calcflg, */ r.dateend, r.isn) as salerfclassisn ,         -- класс (роль в договоре) продавца филиала
           max(decode(r.classisn, 1738886903, r.roledeptisn))keep(dense_rank last order by decode(r.classisn, 1738886903, 1, 0), /* r.calcflg, */ r.dateend, r.isn) as crossalerdeptisn,
           max(decode(r.classisn, 3081540003, r.subjisn)) keep(dense_rank last order by decode(r.classisn, 3081540003, 1, 0), r.dateend, r.isn) as avtodillerisn,   -- автодилер (cardealerisn уже есть выше как "автосалон", поэтому avtodillerisn)
           max(decode(r.classisn, 2626553403, r.subjisn)) keep(dense_rank last order by decode(r.classisn, 2626553403, 1, 0), r.dateend, r.isn) as admcuratorisn,   -- административный куратор
           max(decode(r.classisn, 693962316, r.subjisn)) keep(dense_rank last order by decode(r.classisn, 693962316, 1, 0), r.dateend, r.isn) as doctorcuratorisn,   -- врач-куратор
           max(decode(r.classisn, 693962016, r.subjisn)) keep(dense_rank last order by decode(r.classisn, 693962016, 1, 0), r.dateend, r.isn) as underwriterisn,   -- андеррайтер
           max(decode(r.classisn, 444, r.subjisn)) keep(dense_rank last order by decode(r.classisn, 444, 1, 0), r.dateend, r.isn) as underwriteroldisn,   -- андеррайтер(старый)
           max(decode(r.classisn, 35435216, r.subjisn))keep(dense_rank last order by decode(r.classisn, 35435216, 1, 0), r.calcflg, r.dateend, r.isn) as representativeisn,      -- представитель
           max(decode(r.classisn, 3676722703, r.subjisn))keep(dense_rank last order by decode(r.classisn, 3676722703, 1, 0), /* r.calcflg, */ r.dateend, r.isn) as crossalerfisn,    -- кросс-продавец филиала
           max(decode(r.classisn, 3676722703, r.roledeptisn))keep(dense_rank last order by decode(r.classisn, 3676722703, 1, 0), /* r.calcflg, */ r.dateend, r.isn) as crossalerfdeptisn, -- подразделение кросс-продавца филиала
           max(decode(r.classisn, 437, r.subjisn))keep(dense_rank last order by decode(r.classisn, 437, 1, 0), r.sharepc, r.dateend, r.datebeg, r.isn) as agent_maxcomission_isn,         -- агент с максимальной комиссией
           max(decode(r.classisn, 4207938903, r.subjisn))keep(dense_rank last order by decode(r.classisn, 4207938903, 1, 0), r.calcflg, r.dateend, r.isn) contractorisn, -- подрядчик
           avg(decode(r.classisn, 4207938903, r.sharepc)) contrcomission, -- процент подрядчика
           count(decode(r.classisn, 4207938903, 1, null)) contrcount, -- количество подрядчиков
           max(decode(r.classisn, 437, r.sharepc))keep(dense_rank last order by decode(r.classisn, 437, 1, 0), r.sharepc, r.dateend, r.datebeg, r.isn) as agent_maxcomission_sharepc
      from ( select --+ ordered use_nl(t r s sh) use_hash(d rdt) index(r x_repagrrole_agr) index ( sh x_subhuman )
                    r.agrisn,
                    r.classisn,
                    r.subjisn,
                    oracompat.nvl(r.datebeg, to_date('01-01-1900','dd-mm-yyyy')) datebeg,
                    oracompat.nvl(r.dateend, to_date('01-01-3000','dd-mm-yyyy')) dateend,
                    r.sharepc,
                    r.sharesum,
                    oracompat.nvl(r.calcflg, 'n') calcflg,
                    r.deptisn roledeptisn,
                    d.code,
                    r.collectflg,
                    sh.deptisn,
                    s.juridical,
                    r.isn,
                    s.classisn subjclassisn,
                    rdt.uprisn, -- egao 13.04.2012
                    abs(r.orderno) as orderno  -- sts 22.05.2012 - сортировка по модулю
               from tt_rowid t
                        inner join ais.agrrole r
                        on t.isn = r.agrisn
                        left join ( select
                                             d.isn,
                                             d.code
                                        from dicti d
                                        where d.code in ('sales_g', 'sales_f')
                                            and d.isn <> 1738886903 ) d
                        on r.classisn = d.isn
                        left join subject s
                        on r.subjisn = s.isn
                        left join subhuman sh
                        on r.subjisn = sh.isn
                        left join rep_dept rdt
                        on rdt.deptisn = r.deptisn
                ) r
     where r.classisn <> 430 -- страхователь
     group by r.agrisn
);
