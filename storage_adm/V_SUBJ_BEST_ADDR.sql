create or replace view v_subj_best_addr (
   subjisn,
   addrisn,
   subaddr )
as
(
    select
    -- �������� ������ ��� ������� � ����������� �������, ����������� �������� �������
      s.subjisn,
      s.addrisn,
      ais.addr_utils.getsubaddr(s.addrisn, 'irdtvmshbf') as subaddr
    from (
          select
                s.subjisn,
                cast(ais.addr_utils.getaddrisn(s.subjisn) as numeric) as addrisn
              from (
                    select distinct sa.subjisn  
                        from 
                          tt_rowid t,    
                          ais.subject_t s,  -- ��� �������� ���������� �������, ������� ���� � subaddr � ��� � subject
                          ais.subaddr_t sa
                        where
                              t.isn = s.isn
                              and s.isn = sa.subjisn
                ) s    
        ) s
);
