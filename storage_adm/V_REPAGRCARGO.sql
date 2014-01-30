create or replace view v_repagrcargo (
   agrisn,
   sea,
   more1 )
as
(select --+ use_nl(t l c) ordered
       l.agrisn, 
       max(decode(c.classisn,10908816,1,0)) sea,
       decode(count(distinct c.classisn),0,0,1,0,1) more1
    from tt_rowid t,ais.agrlimit l,ais.crgroute c
    where t.isn = l.agrisn and l.isn = c.isn
    group by l.agrisn
);