create or replace view v_repcond (
   condisn,
   datebeg,
   dateend,
   parentisn,
   agrisn,
   addisn,
   addstatus,
   addno,
   addbeg,
   addsign,
   parentaddisn,
   newaddisn,
   objisn,
   parentobjisn,
   riskisn,
   parentriskisn,
   limitisn,
   rptclassisn,
   limclassisn,
   currisn,
   premcurrisn,
   franchcurrisn,
   franchtype,
   premiumsum,
   premusd,
   premrub,
   premeur,
   limitsum,
   limitusd,
   limitrub,
   limiteur,
   franchsum,
   franchusd,
   franchrub,
   francheur,
   objclassisn,
   objrptclassisn,
   descisn,
   objprnclassisn,
   objprnrptclassisn,
   riskclassisn,
   riskprnclassisn,
   riskrptclassisn,
   riskprnrptclassisn,
   riskruleisn,
   riskprnruleisn,
   limitclassisn,
   agrdatebeg,
   agrdateend,
   agrruleisn,
   agrclassisn,
   agrcomission,
   agrdiscr,
   newaddsign,
   quantity,
   franchtariff,
   objregionisn,
   objcountryisn,
   clientisn,
   agrolddateend,
   addpremiumsum,
   agrcurrisn,
   agrdetailisn,
   premagr,
   carrptclass,
   discount,
   discount2,
   agrsharepc,
   cost,
   tariff,
   yeartariff )
as
(/* 10.01.2013 kgs !!!!!! ахтунг !!!! для медиков конды с 0 плановой премией не грузим!!!!! */
    select /*+ all_rows */
            condisn,datebeg,dateend,parentisn,agrisn,addisn,addstatus,addno,addbeg,
            addsign,parentaddisn,newaddisn,objisn,parentobjisn,riskisn,parentriskisn,limitisn,rptclassisn,
            limclassisn,currisn,premcurrisn,franchcurrisn,franchtype,premiumsum,premusd,premrub,premeur,limitsum,
            limitusd,limitrub,limiteur,franchsum,franchusd,franchrub,francheur,objclassisn,objrptclassisn,descisn,
            objprnclassisn,objprnrptclassisn,riskclassisn,riskprnclassisn,riskrptclassisn,riskprnrptclassisn,riskruleisn,
            riskprnruleisn,limitclassisn,agrdatebeg,agrdateend,agrruleisn,agrclassisn,agrcomission,agrdiscr,newaddsign,
            quantity,franchtariff,objregionisn,objcountryisn,clientisn,agrolddateend,addpremiumsum,agrcurrisn,agrdetailisn,
            premagr,carrptclass,
            discount,discount2,agrsharepc,
            cost,  -- sts 14.03.2013 - страховая стоимость
            --kds(14.11.2013) task(57056418903)
            tariff,
            yeartariff
    from
        (select --+ ordered  no_merge(acpx) no_merge(aopx) use_nl(t ac  acp ar ao arp aop al ad adn a city city1 addr adh) use_hash(acpx aopx  carrules)
               ac.isn condisn,
               oracompat.trunc(ac.datebeg) datebeg,
               oracompat.trunc(ac.dateend) dateend,
               ac.parentisn,
               ac.agrisn,
               ac.addisn,
               ad.status addstatus,
               ad.no addno,
               ad.datebeg addbeg,
               ad.datesign addsign,
               acp.addisn parentaddisn,
               ac.newaddisn,
               ac.objisn,
               aop.isn parentobjisn,
               ac.riskisn,
        /*       arp.isn parentriskisn, */
        /*kgs 13.07.12 по письму дмитревской. искуственный групповой риск для правильного рассчета перестрахования*/
               case 
                   when ar.parentisn is null and ar.ruleisn in (707051716 ,2381976903,206916 ,207016,207116)
                   then
                        (select max(ar1.isn) 
                            from agrrisk ar1 
                            where ar1.agrisn = ar.agrisn and ar1.ruleisn = 707045516
                                and ar1.parentisn is null )
                   else
                       ar.parentisn 
               end parentriskisn,
               ac.limitisn,
               ac.rptclassisn,
               ac.limclassisn,
               ac.currisn,
               ac.premcurrisn,
               ac.franchcurrisn,
               ac.franchtype,
               ac.premiumsum,
               gcc2.gcc2(ac.premiumsum,ac.premcurrisn, 53, coalesce(a.datebeg,ad.datesign,oracompat.trunc(current_timestamp))/*egao 20.05.2011 least(ad.datesign, oracompat.trunc(sysdate), ac.datebeg)*/) premusd,
               gcc2.gcc2(ac.premiumsum,ac.premcurrisn, 35, least(ad.datesign, oracompat.trunc(current_timestamp), ac.datebeg)) premrub,
               gcc2.gcc2(ac.premiumsum,ac.premcurrisn, 29448516, least(ad.datesign, oracompat.trunc(current_timestamp), ac.datebeg)) premeur,
               ac.limitsum,
               gcc2.gcc2(ac.limitsum,ac.currisn, 53, least(ad.datesign, oracompat.trunc(current_timestamp), ac.datebeg)) limitusd,
               gcc2.gcc2(ac.limitsum,ac.currisn, 35, least(ad.datesign, oracompat.trunc(current_timestamp), ac.datebeg)) limitrub,
               gcc2.gcc2(ac.limitsum,ac.currisn, 29448516, least(ad.datesign, oracompat.trunc(current_timestamp), ac.datebeg)) limiteur,
               ac.franchsum,
               gcc2.gcc2(ac.franchsum,ac.franchcurrisn, 53, least(ad.datesign, oracompat.trunc(current_timestamp), ac.datebeg)) franchusd,
               gcc2.gcc2(ac.franchsum,ac.franchcurrisn, 35, least(ad.datesign, oracompat.trunc(current_timestamp), ac.datebeg)) franchrub,
               gcc2.gcc2(ac.franchsum,ac.franchcurrisn, 29448516, least(ad.datesign, oracompat.trunc(current_timestamp), ac.datebeg)) francheur,
               ao.classisn objclassisn,
               ao.rptclassisn objrptclassisn,
               ao.descisn,
               aop.classisn objprnclassisn,
               aop.rptclassisn objprnrptclassisn,
               ar.classisn riskclassisn,
               arp.classisn riskprnclassisn,
               ar.rptclassisn riskrptclassisn,
               arp.rptclassisn riskprnrptclassisn,
               ar.ruleisn riskruleisn,
               arp.ruleisn riskprnruleisn,
               al.classisn limitclassisn,
               a.datebeg agrdatebeg,
               a.dateend agrdateend,
               a.ruleisn agrruleisn,
               a.classisn agrclassisn,
               a.comission agrcomission,
               a.discr agrdiscr,
               adn.datesign newaddsign,
               ac.quantity,
               ac.franchtariff,
               oracompat.nvl(oracompat.nvl(city.parentregionisn,city1.parentregionisn),(select c.parentregionisn
                                                                                            from (select row_number() over () rn, c.parentregionisn 
                                                                                                        from ais.agraddr adr,
                                                                                                              rep_city c 
                                                                                                        where adr.agrisn = ac.agrisn 
                                                                                                            and adr.cityisn = c.cityisn) as t
                                                                                            where rn <= 1)) objregionisn,
               oracompat.nvl(oracompat.nvl(city.parentcountryisn,city1.parentcountryisn),(select c.parentcountryisn
                                                                                                from (select row_number() over () rn, c.parentcountryisn 
                                                                                                            from ais.agraddr adr,
                                                                                                                  rep_city c 
                                                                                                            where adr.agrisn = ac.agrisn  
                                                                                                                and adr.cityisn = c.cityisn) as t
                                                                                                where rn <= 1)) objcountryisn,
               a.clientisn,
               a.olddateend agrolddateend,
               ad.premiumsum addpremiumsum,
               a.currisn agrcurrisn, --egao 14.07.2010
               adh.agrdetailisn, -- od 25.10.2010
               case
                    when ac.premcurrisn = a.currisn then ac.premiumsum
                    else gcc2.gcc2(ac.premiumsum,ac.premcurrisn, a.currisn, coalesce(ac.datebeg, a.datebeg,oracompat.trunc(current_timestamp)))
               end as premagr,-- egao 31.08.2011 в рамках дит-07-1-027944
                /* sts - old 13.01.2012 -- в качестве второго параметра ф-ии должен быть ruleisn договора, а не риска!
                   корректная версия ниже
                decode(carrules.isn,null,null,motor.f_get_rptclass(ar.classisn, ar.ruleisn, ac.rptclassisn))  carrptclass
                */
               decode(carrules.isn,null,null,753518300,'го',motor.f_get_rptclass(ar.classisn, a.ruleisn, ac.rptclassisn)) as carrptclass,
               ac.discount,
               ac.discount2,
               oracompat.nvl(a.sharepc,100) as agrsharepc, -- egao 19.03.2012
               ac.cost, -- sts 14.03.2013 - страховая стоимость
               --kds(14.11.2013) task(57056418903)
               ac.tariff,
               ac.yeartariff
          from tt_rowid t
                left join agrcond ac
                on t.isn = ac.agrisn
                left join agrrisk ar
                on ar.isn = ac.riskisn
                left join agrobject ao
                on ao.isn = ac.objisn
                left join agrrisk arp
                on arp.isn = ar.parentisn
                left join ( select --+ ordered use_nl(zt zao)
                                    distinct
                                    zao.isn,
                                    ( select
                                             max(rowid) rid
                                        from agrobject zz
                                        where zz.parentisn is null
                                        start with zz.isn = zao.parentisn
                                        connect by nocycle prior zz.parentisn = zz.isn ) rid
                                from tt_rowid zt,
                                     agrobject zao
                               where zao.agrisn = zt.isn ) aopx
                on aopx.isn = ao.isn   
                left join agrobject aop
                on aop.rowid = aopx.rid
                left join agrlimit al
                on al.isn = ac.limitisn
                left join agreement ad
                on ad.isn = ac.addisn
                left join agreement adn
                on adn.isn = ac.newaddisn
                left join ( select --+ ordered use_nl(zt zac)
                                    distinct
                                    zac.isn,
                                    ( select
                                             max(rowid)
                                        from agrcond zz
                                        where zz.parentisn is null
                                        start with zz.isn = zac.parentisn
                                        connect by nocycle prior zz.parentisn = zz.isn ) rid /*nocycle - kgs 08.10.11 не нужен. надо данные в аис вычистить*/
                                from tt_rowid zt,
                                     agrcond zac
                                where zac.agrisn = zt.isn ) acpx
                on acpx.isn = ac.isn
                left join agrcond acp
                on acp.rowid = acpx.rid
                left join rep_city city
                on aop.cityisn = city.cityisn
                left join agreement a
                on t.isn = a.isn
                left join ais.agraddr addr
                on aop.cityisn = addr.isn
                left join rep_city city1
                on addr.cityisn = city1.cityisn
                left join agr_detail_agrhash adh
                on ac.agrisn = adh.agrisn
                left join ( select r.* from  motor.v_dicti_rule r ) carrules
                on a.ruleisn = carrules.isn
        )
    where agrruleisn  not in ( select d.isn
                                   from dicti_nh d
                                   where hierarchies.is_subtree(__hier, 686160416)
                                   --from dicti d
                                   --start with d.isn = 686160416
                                   --connect by prior d.isn = d.parentisn  
                               )
        or oracompat.nvl(premiumsum,0) > 0
)
/
