CREATE OR REPLACE VIEW "STORAGE_ADM"."V_REPAGRCARGO" (
   agrisn,
   sea,
   more1 )
AS
(Select --+ use_nl(t l c) ordered
       l.AgrIsn, Max(decode(c.classisn,10908816,1,0)) Sea,
       Decode(count(distinct c.classisn),0,0,1,0,1) MORE1
     from tt_rowid t,Ais.agrlimit l,Ais.crgroute c
     where t.isn=L.agrisn And l.isn = c.isn
     Group by l.AgrIsn;