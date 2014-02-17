CREATE VIEW storages.v_rep_subacc4dept (
   code,
   deptisn,
   subaccisn,
   sheettype,
   statcode,
   updated,
   updatedby,
   isn,
   rptgroupisn,
   sagroup )
AS
select s.code, s.deptisn, s.subaccisn, s.sheettype, Round(s.statcode),
       s.updated, s.updatedby, s.isn, s.rptgroupisn, 10*(Statcode-Round(Statcode))+1 sagroup
    from ais.subacc4dept s
union 
--776
select id as code,
     0 as deptisn,-- ?
     isn as subaccisn,
     0 as sheettype,
     776 as statcode,
     updated, updatedby,
     0 as isn, --?
     0 as rptgroupisn, --?
     0 as sagroup
from ais.buhsubacc where id like '776%'




   --select oracompat.nvl2(timestamp '2013-10-23',3,3)