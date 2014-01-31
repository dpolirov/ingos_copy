create or replace view v_repagrtur (
   agrisn,
   isrussia,
   isshengen)
as
(select --+ use_nl (a l) ordered
       a.agrisn,
       max(decode(subclassisn,2570/*c.get('russia')*/,1,2458716 /*c.get('ter_russia')*/,1,0))isrussia, 
       max(decode(subclassisn,13310216,1,0)) isshengen
    from tt_rowid t, ais.agrlimit a, ais.agrlimitem l
    where a.agrisn = t.isn
      and a.isn = l.limisn
      and l.subclassisn in (2570/*c.get('russia')*/,2458716 /*c.get('ter_russia')*/,13310216/*shengem*/)
     group by a.agrisn
);
