create or replace view v_rep_agrext (
   agrisn,
   classisn,
   x1,
   x2,
   x3,
   x4,
   x5 )
as
(select agrisn,classisn,x1,x2,x3,x4,x5
  from agrext
  where agrisn in (select isn from tt_rowid)
        and x1 in (select 1283165703 isn -- ìåæäóíàğîäíàÿ ïğîãğàììà
                    union all
                    select isn 
                      from dicti_nh
                      where hierarchies.is_subtree(__hier, 1071775625))
                      --Start With isn=1071775625 -- ÑÒĞÀÕÎÂÀÍÈÅ ÇÀËÎÃÎÂÎÃÎ ÈÌÓÙÅÑÒÂÀ
                      --connect by prior Isn=Parentisn)
);

