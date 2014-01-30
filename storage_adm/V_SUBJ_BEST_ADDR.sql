create or replace view v_subj_best_addr (
   subjisn,
   addrisn,
   subaddr )
as
(
    select
    -- источник данных для витрины с оптимальным адресом, выгружаемой функцией крылова
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
                          ais.subject_t s,  -- для удаления паразитных записей, которые есть в subaddr и нет в subject
                          ais.subaddr_t sa
                        where
                              t.isn = s.isn
                              and s.isn = sa.subjisn
                ) s    
        ) s
);
