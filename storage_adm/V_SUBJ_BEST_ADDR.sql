create or replace view storage_adm.v_subj_best_addr (
   subjisn,
   addrisn,
   subaddr )
as
(
    select
    -- источник данных для витрины с оптимальным адресом, выгружаемой функцией крылова
      s.subjisn,
      s.addrisn,
      shared_system.addr_utils_getsubaddr(s.addrisn::numeric, 'irdtvmshbf'::varchar) as subaddr
    from (
          select
                s.subjisn,
                cast(shared_system.addr_utils_getaddrisn(s.subjisn) as numeric) as addrisn
              from (
                    select distinct sa.subjisn  
                        from 
                          storage_adm.tt_rowid t,    
                          ais.subject_t s,  -- для удаления паразитных записей, которые есть в subaddr и нет в subject
                          ais.subaddr_t sa
                        where
                              t.isn = s.isn
                              and s.isn = sa.subjisn
                ) s    
        ) s
);
