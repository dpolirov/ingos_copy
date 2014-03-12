create or replace view storage_adm.v_rep_agrext (
   agrisn,
   classisn,
   x1,
   x2,
   x3,
   x4,
   x5 )
as
(select agrisn,classisn,x1,x2,x3,x4,x5
  from ais.agrext a
  where a.agrisn in (select tt.isn from storage_adm.tt_rowid tt)
        and a.x1 in (select 1283165703 isn -- международная программа
                    union all
                    select nh.isn 
                      from ais.dicti_nh nh
                      where shared_system.is_subtree(nh.__hier, 1071775625))
                      --Start With isn=1071775625 -- СТРАХОВАНИЕ ЗАЛОГОВОГО ИМУЩЕСТВА
                      --connect by prior Isn=Parentisn)
);

