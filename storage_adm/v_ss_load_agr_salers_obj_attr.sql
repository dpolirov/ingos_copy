CREATE OR REPLACE VIEW storage_adm.v_ss_load_agr_salers_obj_attr (
   agrisn )
AS
select 
-- вьюха преобразования записей из ss_histlog, относящихся к Obj_Attrib в AgrISN
-- Используется в разрузке продавцов (Rep_Agr_Salers)
-- ss_histlog.FindISN = Obj_Attrib.ISN для скорости отбора

-- Здесь отбирается ч/з связку SS_HISTLOG -> OBJ_ATTRIB, а не напрямую из OBJ_ATTRIB
-- для наложения ограничения по дате из договора, чтобы ограничить число записей.
-- Т.к. у продавцов может быть куча договоров, а нужны только те,
-- у которых смена мотивационной группы попадает в дату договора
  A.ISN as AGRISN
from
  ais.AGRROLE AR,
( select 
    D.ISN,
    D.CODE
  from ais.DICTI D
  where D.CODE in ('SALES_G', 'SALES_F')
) D,
( select --+ ordered use_nl(S OA) index(OA X_ATTRIB)
    OA.ObjISN as SubjISN,
    coalesce(oracompat.add_months(max(OA.DATEBEG)::date, -1), timestamp '01-jan-1900') as DATE_BEG_LAST  -- отбираем последнюю дату минус месяц
  from 
   (select --+ full(s) parallel(s 32) cardinality (s 1000000) 
    distinct 
      FindIsn
    from storage_adm.ss_histlog s
    Where 
      ProcIsn = 19 
      and table_name='OBJ_ATTRIB' 
      and LoadIsn =STORAGE_ADM.getloadisn()
  ) S,
    ais.OBJ_ATTRIB OA
  where   
    S.FindISN = OA.ISN
    and OA.classisn = 1428587803    
    and OA.discr = 'C' 
  group by OA.ObjISN  
  having coalesce(oracompat.add_months(max(OA.DATEBEG)::date, -1), timestamp '01-jan-3000') >= current_timestamp
) OA,
  ais.AGREEMENT A
where
  OA.SubjISN = AR.SUBJISN
  and AR.CLASSISN = D.ISN
  and AR.AGRISN = A.ISN
  and A.DATEEND >= OA.DATE_BEG_LAST