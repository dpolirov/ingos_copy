create or replace view v_repcrgdoc (
   agrisn,
   classisn,
   objisn,
   subjisn,
   juridical )
as
(select s.agrisn,s.classisn,s.objisn,s.subjisn,sb.juridical
    from (
            select d.agrisn, 
                    d.classisn, 
                    max(d.objisn) objisn, 
                    max(d.subjisn) subjisn
                from tt_rowid t,ais.crgdoc d
                where t.isn = d.agrisn
                    and d.classisn = 34709216
            group by d.agrisn, d.classisn
          ) s
        left join subject sb
        on sb.isn = s.subjisn
);
