CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPAGRTUR" ("AGRISN", "ISRUSSIA", "ISSHENGEN") AS 
  (select --+ use_nl (a l) ordered
       a.agrisn,
       Max(Decode(subclassisn,2570/*c.get('Russia')*/,1,2458716 /*c.get('Ter_Russia')*/,1,0))isrussia, 
       Max(Decode(subclassisn,13310216,1,0)) isshengen
     from tt_rowId t, Ais.agrlimit a, Ais.agrlimitem l
     where a.agrisn = T.Isn
       and a.isn = l.limisn
       and l.subclassisn in (2570/*c.get('Russia')*/,2458716 /*c.get('Ter_Russia')*/,13310216/*Shengem*/)
     Group by a.agrisn
);
