  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_SUBJ_BEST_ADDR" ("SUBJISN", "ADDRISN", "SUBADDR") AS 
  select
-- �������� ������ ��� ������� � ����������� �������, ����������� �������� �������
  S.SUBJISN,
  S.AddrISN,
  AIS.ADDR_UTILS.GetSubAddr(S.AddrISN, 'irdtvmshbf') as SubAddr
from (
  select
    S.SUBJISN,
    TO_NUMBER(AIS.ADDR_UTILS.GetAddrIsn(S.SUBJISN)) as AddrISN
  from (
    select --+ ordered use_nl(T S SA)
    distinct
      SA.SUBJISN  
    from 
      TT_ROWID T,    
      AIS.SUBJECT_T S,  -- ��� �������� ���������� �������, ������� ���� � SubAddr � ��� � Subject
      AIS.SUBADDR_T SA
    where
      T.ISN = S.ISN
      and S.ISN = SA.SUBJISN
  ) S    
) S;