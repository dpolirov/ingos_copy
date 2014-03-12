create or replace view storage_adm.v_rep_agr_salers_max_dates
(
AGRISN, 
PMINDATE, 
PMAXDATE
)
as (
--v_rep_agr_salers_max_dates

-- sts 24.07.2012 - вьюха возвращает список AgrISN для нарезалки по продавцам (для вьюхи V_REP_AGR_SALERS)
-- также расчитывает максимальную дату окончания промежутка в зависимости от наличия/отсутствия
-- урегулированных убытков по договору (в случае "открытой" правой границы отрезка продавец-роль-мотивационная группа)

select --+ ordered use_nl(ra rb)
 ra.AgrISN,
 --max(ra.pLoadIsn) as pLoadIsn,
 max(ra.pMinDate) as pMinDate,
 -- максимальная дата проводки либо квитовки для закончившихся урегулированных договоров
 -- (если нет проводок, то дата окончания договора - для урегулированных)
 -- 01-jan-3000 для неурегулированных
 max(decode(ra.EndAgrISN,NULL, ra.pMaxDate,
	-- максимальная правая граница интервала = 01.01.3000
	-- если задано, то нет неурегулированных убытков
          greatest(oracompat.nvl(rb.BuhQuitDate, oracompat.nvl(rb.DateVal, ra.AgrDateEnd)),
                   ra.AgrDateEnd) -- максимальная дата проводки для закончившихся урегулированных договоров
          )) as pMaxDate
  from (select --+ ordered use_nl(ra r)
         ra.AgrISN,
         --max(ra.pLoadIsn) as pLoadIsn,
         max(ra.pMinDate) as pMinDate,
         max(ra.pMaxDate) as pMaxDate,
         max(ra.dateend) as AgrDateEnd,
         
         -- сделал логику определения урегулированного убытка, ч/з DateRefund из претензии,
         -- т.к. если определять по статусам, то нужно перебирать несколько (Y, R)
         case
           when max(r.isn) is null then
            ra.AgrISN -- нет убытков
           when min(decode(r.DateRefund, null, 0, 1)) = 1 then
            ra.AgrISN -- урегулированный убыток (нет претензий без даты урегулирования)
         end as EndAgrISN -- признак оконченного договора с урегулированными убытками (либо без убытков)
          from (select --+ ordered use_nl(t ra)
                 to_date('01-01-1900','dd-mm-yyyy') as pMinDate,
                 to_date('01-01-3000','dd-mm-yyyy') as pMaxDate,
                 ra.AgrISN,
                 ra.dateend,
                 case
                   when ra.dateend < CURRENT_TIMESTAMP then
                    ra.AgrISN
                 end as EndAgrISN -- признак оконченного договора
                  from STORAGE_ADM.tt_rowid t, storage_source.repagr ra
                 where t.isn = ra.AgrISN) ra
                 left join AIS.AgrRefund r
                  on ra.EndAgrISN = r.AgrISN
               --reprefund r  14.05.2013 - sts - перевел на AgrRefund
         group by ra.AgrISN) ra
         left join storage_source.repbuhbody rb
          on ra.EndAgrISN = rb.AgrISN
 group by ra.AgrISN
 );