 CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPCRGDOC" ("AGRISN", "CLASSISN", "OBJISN", "SUBJISN", "JURIDICAL") AS 
  (Select /*+ Ordered USe_Nl(sb) */ S."AGRISN",S."CLASSISN",S."OBJISN",S."SUBJISN",sb.juridical
       from
         (Select --+ Index_Asc(d X_CRGDOC_AGR) ordered use_Nl(d sb)
            d.agrisn, d.classisn, Max(d.objisn) objisn, Max(d.subjisn) subjisn
          from tt_rowid t,ais.CrgDoc d
          Where t.isn=d.agrisn
            and d.classisn=34709216 -- œ¿—œŒ–“ “–¿Õ—œŒ–ÕŒ√Œ —–≈ƒ—“¬¿
          Group by d.agrisn,d.classisn) S,subject sb
       Where sb.isn(+)=S.subjisn
);