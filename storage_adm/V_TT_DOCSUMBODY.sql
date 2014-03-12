create or replace view storage_adm.v_tt_docsumbody 
(
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
   agrdatebeg
)
as
(
with tmain as
(	select --+ ordered use_nl(b b1)
            b.baseisn bodyisn,
            max(b.baseamountrub) baseamountrub,
            max(b.subaccisn) subaccisn,
            max(b.basedateval) dateval,
            max(b.de) bodystatend,
            min(b.db) bodystatbeg,
            max(b.code) code,
            max(basedamountrub) damountrub,
            max(basecamountrub) camountrub
		from storage_adm.tt_rowid t,storages.st_bodydebcre b
		where t.isn = b.baseisn
		group by b.baseisn
		having max(b.baseamountrub) <> 0
)
    select --+ ordered use_nl(ar)
        s.bodyisn,bamount,s.agrisn,
        s.subjisn,amountrub,datepaylast,
        dsclassisn,dsisn,s.discr,
        subaccisn,splitisn,c_agr,
        agrkoef,dskoef,agrdskoef,
        db,de,remainder,
        reaccisn,ar.agentisn,ar.datebeg
    from (
            select	d2.bodyisn,d2.bamount,d2.agrisn,
                    d2.subjisn,d2.amountrub,d2.datepaylast,
                    d2.dsclassisn,d2.dsisn,d2.discr,
                    d2.subaccisn,d2.splitisn,d2.c_agr,
                    d2.agrkoef, 
                    d2.kfbase/sum(d2.kfbase) over(partition by d2.bodyisn,d2.agrisn) as dskoef,
                    d2.agrkoef*d2.kfbase/sum(d2.kfbase) over(partition by d2.bodyisn,d2.agrisn) as agrdskoef,
                    greatest(shared_system.load_storage_gethistdb(),bodystatbeg) db, 
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
                                        decode(trunc(sum(remainder) over(partition by bodyisn)),0,
                                        sum( sgn*amountrub) over(partition by bodyisn,agrisn),
                                        sum( sgn*remainder) over(partition by bodyisn,agrisn)) c_agr,
                                        decode(trunc(sum(remainder) over(partition by bodyisn)),0,amountrub,remainder) kfbase
                                    from
                                    (
									--==--
                                        select --+ ordered use_nl(b pc pd)
                                                foo.bodyisn,
                                                foo.baseamountrub bamount,
                                                foo.agrisn,
                                                foo.subjisn,
                                                case /* если знак remainder и amountrub совпадают или remainder=0*/
                                                    when sign(foo.amountrub) = sign(foo.remainder) then shared_system.gcc2(foo.remainder, foo.currisn, 35, foo.dateval)
                                                    when sign(oracompat.nvl(foo.remainder,-1 :: numeric)) = 0 then 0
                                                else
                                                    shared_system.gcc2(foo.amount,foo.currisn,35, foo.dateval)
                                                end remainder,
                                                SHARED_SYSTEM.GCC2(FOO.AMOUNT,FOO.CURRISN,35,FOO.DATEVAL) AMOUNTRUB,
                                                oracompat.nvl(oracompat.nvl(foo.datepaylast,foo.datepay), foo.docdate) datepaylast,
                                                foo.classisn dsclassisn,
                                                foo.isn dsisn,
                                                foo.discr discr,
                                                foo.reaccisn reaccisn,
                                                foo.subaccisn,
                                                foo.splitisn splitisn,
                                                decode(sign(baseamountrub),sign(sum(foo.amount) over (partition by bodyisn,discr)),-1,1) sgn,
                                                bodystatend,
                                                bodystatbeg,
                                                max(foo.discr) over (partition by bodyisn) maxdiscr,
                                                min(foo.discr) over (partition by bodyisn) mindiscr,
                                                foo.code,
                                                foo.damountrub,
                                                foo.camountrub
												from 
												(	select	b.bodyisn,b.baseamountrub,ds.agrisn,code,damountrub,camountrub,
															ds.subjisn,ds.amountrub,ds.remainder,ds.currisn, b.dateval,
															ds.amount,ds.datepaylast,ds.datepay,ds.docdate,ds.classisn,ds.isn,
															ds.discr,ds.reaccisn,b.subaccisn,ds.splitisn,bodystatend,bodystatbeg
														from
															tmain b,
															ais.docsum ds
														where b.bodyisn = ds.debetisn
														and ds.discr in ('F','P')
														and ds.amountrub <> 0
													---
													 union
													---
													select  b.bodyisn,b.baseamountrub,ds.agrisn,code,damountrub,camountrub,
															ds.subjisn,ds.amountrub,ds.remainder,ds.currisn, b.dateval,
															ds.amount,ds.datepaylast,ds.datepay,ds.docdate,ds.classisn,ds.isn,
															ds.discr,ds.reaccisn,b.subaccisn,ds.splitisn,bodystatend,bodystatbeg
														from
															tmain b,
															ais.docsum ds
														where b.bodyisn = ds.creditisn
														and ds.discr in ('F','P')
														and ds.amountrub <> 0
												) as foo
									--==--
									) d
                                    where discr = maxdiscr
                                    /* дебильная  врезка - для счета 76197 надо отдавать предпочтение фактическим доксуммам, чтобу получить страховщика причинителя вреда по пву*/
                                    /*(code='76197' and camountrub is not null and discr=mindiscr) or
                                    ((code<>'76197' or damountrub is not null) and discr=maxdiscr) */
                                    --discr = maxdiscr
                            )d
                            where sign(d.c_agr) <> sign(bamount) and c_agr <> 0 -- заменить на > 0
                        ) d2
                    where sign(d2.amountrub*sgn) <> sign(d2.bamount)
        ) s 
        left join storage_source.repagr ar
        on s.agrisn = ar.agrisn
    --where db<=de
);