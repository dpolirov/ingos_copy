create or replace view storage_adm.v_rep_subacc4dept
(
   CODE, 
   DEPTISN, 
   SUBACCISN, 
   SHEETTYPE, 
   STATCODE, 
   UPDATED, 
   UPDATEDBY, 
   ISN, 
   RPTGROUPISN, 
   SAGROUP, 
   DATEEND
)
as 
(
  select s.code,
       s.deptisn,
       s.subaccisn,
       s.sheettype,
       Round(s.statcode) statcode,
       s.updated,
       s.updatedby,
       s.isn,
       s.rptgroupisn, /*10*(Statcode-Round(Statcode))+1 */
       sagroup,
       Case
         When Code Like '913%' and statcode = 32 then
          to_date('31-12-2005','dd-mm-yyyy')
         else
          null
       end dateend
/*913 счета содержали комиссию до 31.12.2005 далее она стала начислятся на 263 счета
согласно письму самохвалова от 12.04.2006*/
  from ais.subacc4dept s
	);