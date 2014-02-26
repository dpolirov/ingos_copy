CREATE VIEW storage_adm.v_rep_agr_salers_max_dates (
   agrisn,
   pmindate,
   pmaxdate )
AS
(select --+ ordered use_nl(ra rb)
  ra.AgrISN,
  --max(ra.pLoadIsn) as pLoadIsn,
  max(ra.pMinDate) as pMinDate,
  -- максимальная дата проводки либо квитовки для закончившихся урегулированных договоров
  -- (если нет проводок, то дата окончания договора - для урегулированных)
  -- 01-jan-3000 для неурегулированных
  max(case 
    when ra.EndAgrISN is not null  -- если задано, то нет неурегулированных убытков
    then greatest(coalesce(rb.BuhQuitDate, coalesce(rb.DateVal, ra.AgrDateEnd)), ra.AgrDateEnd)  -- максимальная дата проводки для закончившихся урегулированных договоров
    else ra.pMaxDate end-- максимальная правая граница интервала = 01.01.3000
  ) as pMaxDate
from
 (
  select 
    ra.AgrISN,
    --max(ra.pLoadIsn) as pLoadIsn,
    max(ra.pMinDate) as pMinDate,
    max(ra.pMaxDate) as pMaxDate,
    max(ra.dateend) as AgrDateEnd,
    
    -- сделал логику определения урегулированного убытка, ч/з DateRefund из претензии,
    -- т.к. если определять по статусам, то нужно перебирать несколько (Y, R)
    case
      when max(r.isn) is null then ra.AgrISN    -- нет убытков
      when min(case when r.DateRefund is null then 0 else 1 end) = 1 then ra.AgrISN   -- урегулированный убыток (нет претензий без даты урегулирования)
    end as EndAgrISN  -- признак оконченного договора с урегулированными убытками (либо без убытков)
  from
   (select 
      timestamp '01-jan-1900' as pMinDate,
      timestamp '01-jan-3000' as pMaxDate,
      ra.AgrISN,
      ra.dateend,
      case when ra.dateend < current_timestamp then ra.AgrISN end as EndAgrISN  -- признак оконченного договора
    from 
      STORAGE_ADM.tt_rowid t,
      storage_source.repagr ra
    where
      t.isn = ra.AgrISN
   ) ra
        left join
     --reprefund r  14.05.2013 - sts - перевел на AgrRefund
     AIS.AgrRefund r
   on 
     ra.EndAgrISN = r.AgrISN
   group by ra.AgrISN
 ) ra left join
   storage_source.repbuhbody rb
 on
   ra.EndAgrISN = rb.AgrISN
 group by 
   ra.AgrISN
)
