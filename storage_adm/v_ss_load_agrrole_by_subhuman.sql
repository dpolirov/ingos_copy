CREATE OR REPLACE VIEW storage_adm.v_ss_load_agrrole_by_subhuman (
   agrisn )
AS
select --+ parallel(h 30) ordered use_nl(ar)
-- вьюха преобразования записей из ss_histlog, относящихся к SubHuman в AgrISN
-- Используется в разрузке ролей договора (RepAgrRoleAgr)
-- ss_histlog.FindISN = SubjISN
    distinct 
      ar.AgrISN
    from storage_adm.ss_histlog h, AIS.AgrRole ar
    Where 
      ProcIsn = 27
      and table_name='SUBHUMAN_T' 
      and LoadIsn=STORAGE_ADM.getloadisn()
      and h.FindISN = ar.SubjISN