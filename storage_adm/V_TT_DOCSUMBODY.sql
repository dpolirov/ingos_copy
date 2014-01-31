create or replace view v_tt_docsumbody (
   bodyisn,
   bamount,
   agrisn,
   subjisn,
   amountrub,
   datepaylast,
   dsclassisn,
   dsisn,
   discr,
   subaccisn,
   splitisn,
   c_agr_1,
   agrkoef,
   dskoef,
   agrdskoef,
   db,
   de,
   remainder_1,
   reaccisn,
   agentisn,
   agrdatebeg )
as
(
    select --+ ordered use_nl(ar)
        s.bodyisn,bamount,s.agrisn,
        s.subjisn,amountrub,datepaylast,
        dsclassisn,dsisn,s.discr,
        subaccisn,splitisn,c_agr,
        agrkoef,dskoef,agrdskoef,
        db,de,remainder,
        reaccisn,ar.agentisn,ar.datebeg
    from (
            select d2.bodyisn,d2.bamount,d2.agrisn,
                    d2.subjisn,d2.amountrub,d2.datepaylast,
                    d2.dsclassisn,d2.dsisn,d2.discr,
                    d2.subaccisn,d2.splitisn,d2.c_agr,
                    d2.agrkoef, 
                    d2.kfbase/sum(d2.kfbase) over(partition by d2.bodyisn,d2.agrisn) as dskoef,
                    d2.agrkoef*d2.kfbase/sum(d2.kfbase) over(partition by d2.bodyisn,d2.agrisn) as agrdskoef,
                    greatest(storage_adm.load_storage.gethistdb,bodystatbeg) db, 
                    bodystatend as de,
                    d2.remainder,
                    reaccisn
                from
                    (
                        select d.*, 
                                c_agr/sum(d.kfbase*sgn) over(partition by bodyisn) as agrkoef
                            from
                            (
                                select d.*,
                                        decode(oracompat.trunc(sum(remainder) over(partition by bodyisn)),0,
                                        sum( sgn*amountrub) over(partition by bodyisn,agrisn),
                                        sum( sgn*remainder) over(partition by bodyisn,agrisn)) c_agr,
                                        decode(oracompat.trunc(sum(remainder) over(partition by bodyisn)),0,amountrub,remainder) kfbase
                                    from
                                    (
                                        select --+ ordered use_nl(b pc pd)
                                                b.bodyisn,
                                                b.baseamountrub bamount,
                                                ds.agrisn,
                                                ds.subjisn,
                                                case /* если знак remainder и amountrub совпадают или remainder=0*/
                                                    when sign(ds.amountrub) = sign(ds.remainder) then gcc2.gcc2(ds.remainder, ds.currisn, 35, b.dateval)
                                                    when sign(oracompat.nvl(ds.remainder,-1)) = 0 then 0
                                                else
                                                    gcc2.gcc2(ds.amount,ds.currisn,35, b.dateval)
                                                end remainder,
                                                gcc2.gcc2(ds.amount,ds.currisn,35,b.dateval) amountrub,
                                                oracompat.nvl(oracompat.nvl(ds.datepaylast,ds.datepay), ds.docdate) datepaylast,
                                                ds.classisn dsclassisn,
                                                ds.isn dsisn,
                                                ds.discr discr,
                                                ds.reaccisn reaccisn,
                                                b.subaccisn,
                                                ds.splitisn splitisn,
                                                decode(sign(baseamountrub),sign(sum(ds.amount) over (partition by bodyisn,discr)),-1,1) sgn,
                                                bodystatend,
                                                bodystatbeg,
                                                max(ds.discr) over (partition by bodyisn) maxdiscr,
                                                min(ds.discr) over (partition by bodyisn) mindiscr,
                                                code,
                                                damountrub,
                                                camountrub
                                            from
                                            (
                                                /*только интересующие нас доксуммы*/
                                                select --+ ordered use_nl(b b1)
                                                      b.baseisn bodyisn,
                                                      max(b.baseamountrub) baseamountrub,
                                                      max(b.subaccisn) subaccisn,
                                                      max(b.basedateval) dateval,
                                                      max(b.de) bodystatend,
                                                      min(b.db) bodystatbeg,
                                                      max(b.code) code,
                                                      max(basedamountrub) damountrub,
                                                      max(basecamountrub) camountrub
                                                    from tt_rowid t,storages.st_bodydebcre b
                                                    where
                                                      t.isn = b.baseisn
                                                group by b.baseisn
                                                having max(b.baseamountrub) <> 0
                                            )b,
                                            ais.docsum ds
                                            where b.bodyisn in (ds.debetisn,ds.creditisn)
                                                and ds.discr in ('f','p')
                                                and ds.amountrub <> 0
                                    ) d
                                    where
                                    /* дебильная врезка - для счета 76197 надо отдавать предпочтение фактическим доксуммам, чтобу получить страховщика причинителя вреда по пву*/
                                    /*(code='76197' and camountrub is not null and discr=mindiscr) or
                                    ((code<>'76197' or damountrub is not null) and discr=maxdiscr) */
                                    discr = maxdiscr
                            )d
                            where
                            sign(d.c_agr) <> sign(bamount) and c_agr <> 0 -- заменить на > 0
                        ) d2
                    where sign(d2.amountrub*sgn) <> sign(d2.bamount)
        ) s 
        left join storage_source.repagr ar
        on s.agrisn = ar.agrisn
    --where db<=de
);
