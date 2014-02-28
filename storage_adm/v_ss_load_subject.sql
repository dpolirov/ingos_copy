CREATE OR REPLACE VIEW storage_adm.v_ss_load_subject (
   subjisn )
AS
(
Select --+ Ordered Use_Nl(sb)
 S.FindIsn SubjIsn
from storage_adm.ss_histlog s
where Upper(TABLE_NAME)='SUBJECT_T'
 And  PROCISN=6
 AND LOADISN is null
Union all


Select --+ Ordered Use_Nl(sb)
 Sb.Isn
from storage_adm.ss_histlog s, ais.subject_t sb
where Upper(TABLE_NAME)='SUBJECT_T'
 And  PROCISN=6
 AND LOADISN is null
 AND s.findisn=sb.ParentIsn
 )