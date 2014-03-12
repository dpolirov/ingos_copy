create or replace view storage_adm.v_repbuhbody (
   deptisn,
   statcode,
   classisn,
   bodyisn,
   dateval,
   currisn,
   deptisnbuh,
   subjisn,
   subaccisn,
   buhamount,
   buhamountrub,
   docsumisn,
   datepaylast,
   agrisn,
   deptisnan,
   reprdeptisn,
   bizflg,
   addisn,
   refundisn,
   docsumpc,
   buhquitbodyisn,
   buhquitbodycnt,
   acccurrisn,
   quitdebetisn,
   quitcreditisn,
   quitdateval,
   datequit,
   quitcurrisn,
   quitdebetsubaccisn,
   quitcreditsubaccisn,
   quitdebetbuhamount,
   quitcreditbuhamount,
   amountclosedquit,
   buhquitamount,
   buhquitpartamount,
   buhquitdate,
   parentisn,
   amountclosingquit,
   fullamountclosingquit,
   agrbuhdate,
   factisn,
   buhquitisn,
   buhheadfid,
   buhamountusd,
   oprisn,
   oprdeptisn,
   docisn,
   datepay,
   headisn,
   docsumsubj,
   docisn2,
   sagroup,
   corsubaccisn,
   dsdatebeg,
   dsdateend,
   adeptisn,
   dsclassisn,
   dsclassisn2,
   factpc,
   buhpc,
   amount,
   amountrub,
   amountusd,
   remark,
   dsstatus,
   incomeoutlaykindisn,
   budgetgroupkindisn)
as
(
    
    with 
    repbuhbody_list as
    (select z.*,
         --поля корреспонденции
         max(b.isn) buhquitbodyisn,
         count(b.headisn) buhquitbodycnt
        from storage_adm.vz_repbuhbody_list z
        left join ais.buhbody_t b
        on z.headisn = b.headisn
                and b.status = 'А'
                and decode(z.damountrub,null,b.damountrub,b.camountrub) is not null
        group by 
        z.bodyisn,
        z.deptisn,
        z.statcode,
        z.classisn,
        z.sagroup,
        z.dsdiscr,
        z.damountrub,
        z.camountrub,
        z.headisn),
        
    docsumlist as (select --+ use_concat
           l.bodyisn,
           ds.*
         from repbuhbody_list l, ais.docsum ds
         where l.bodyisn = ds.debetisn and l.dsdiscr = ds.discr
         ---
         union
         ---
         select --+ use_concat
           l.bodyisn,
           ds.*
         from repbuhbody_list l, ais.docsum ds
         where l.bodyisn = ds.creditisn and l.dsdiscr = ds.discr ),

        quitbodylist as (
        select
          b.bodyisn,
          oracompat.nvl(bb.damount,-bb.camount) buhquitamount,
          oracompat.nvl(bb1.damount,-bb1.camount) buhquitpartamount,
          bb1.datequit buhquitdate,
          oracompat.nvl(bb1.isn,bb.isn) buhquitisn,
          bb.subaccisn corsubaccisn
        from repbuhbody_list b,
             ais.buhbody_t bb, 
             ais.buhbody_t bb1
        where b.buhquitbodyisn = bb.isn
              and bb.isn = bb1.isn
              and decode(bb1.isn,bb.isn,686696616,bb1.oprisn) = 686696616
              and bb1.status = 'А'
	---
	union
	---
	select
          b.bodyisn,
          oracompat.nvl(bb.damount,-bb.camount) buhquitamount,
          oracompat.nvl(bb1.damount,-bb1.camount) buhquitpartamount,
          bb1.datequit buhquitdate,
          oracompat.nvl(bb1.isn,bb.isn) buhquitisn,
          bb.subaccisn corsubaccisn
        from repbuhbody_list b,
             ais.buhbody_t bb, 
             ais.buhbody_t bb1
        where b.buhquitbodyisn = bb.isn
              and bb.isn = bb1.parentisn
              and decode(bb1.isn,bb.isn,686696616,bb1.oprisn) = 686696616
              and bb1.status = 'А'
    )
    
    select deptisn, statcode, classisn, bodyisn, dateval, currisn, deptisnbuh,  subjisn, subaccisn,
           buhamount, buhamountrub, docsumisn, datepaylast, agrisn, 
           deptisnan, reprdeptisn, 
           bizflg, addisn, refundisn,
           docsumpc, buhquitbodyisn, buhquitbodycnt, acccurrisn, quitdebetisn, quitcreditisn, quitdateval, datequit, quitcurrisn,
           quitdebetsubaccisn, quitcreditsubaccisn, quitdebetbuhamount, quitcreditbuhamount, amountclosedquit, buhquitamount,
           buhquitpartamount, buhquitdate, parentisn, amountclosingquit, fullamountclosingquit, agrbuhdate, factisn, buhquitisn,
           buhheadfid, buhamountusd, oprisn,oprdeptisn, docisn, datepay, headisn, docsumsubj,
           docisn2, sagroup, corsubaccisn, dsdatebeg, dsdateend, adeptisn, dsclassisn, dsclassisn2,
           decode(oracompat.nvl(fullamountclosingquit,0::numeric),0,1,abs(amountclosingquit)/fullamountclosingquit) as factpc,
           decode(oracompat.nvl(buhquitamount,0::numeric),0,1,oracompat.nvl(buhquitpartamount,buhquitamount)/buhquitamount) as buhpc,
           buhamount*decode (oracompat.nvl(fullamountclosingquit,0::numeric),0,1,abs(amountclosingquit)/fullamountclosingquit)*
           decode(oracompat.nvl(buhquitamount,0::numeric),0,1,oracompat.nvl(buhquitpartamount,buhquitamount)/buhquitamount)*docsumpc as amount, -- egao 02.02.2010
           buhamountrub*decode(oracompat.nvl(fullamountclosingquit,0::numeric),0,1,abs(amountclosingquit)/fullamountclosingquit)*
           decode(oracompat.nvl(buhquitamount,0::numeric),0,1,oracompat.nvl(buhquitpartamount,buhquitamount)/buhquitamount)*docsumpc as amountrub, -- egao 02.02.2010
           buhamountusd*decode(oracompat.nvl(fullamountclosingquit,0::numeric),0,1,abs(amountclosingquit)/fullamountclosingquit)*
           decode(oracompat.nvl(buhquitamount,0::numeric),0,1,oracompat.nvl(buhquitpartamount,buhquitamount)/buhquitamount)*docsumpc as amountusd, -- egao 02.02.2010
           remark,
           dsstatus /*kgs 01.08.2011 статут доксуммы для дебиторки*/,
		   incomeoutlaykindisn, -- EGAO 31.04.2014
		   budgetgroupkindisn -- EGAO 31.04.2014
        from (
                select --+ use_nl (b d dp s d2 a aa r f b1 b2 bb bb1 opr) ordered index (bb ) use_concat index (bb1)
                         b.deptisn, b.statcode, b.classisn,
                         b.bodyisn, b.dateval, b.currisn, b.deptisnbuh,  b.subjisn, b.subaccisn,
                         b.buhamount, b.buhamountrub, b.docsumisn, b.datepaylast,oracompat.nvl(aa.isn,0::numeric) agrisn,
                         b.deptisnan, b.reprdeptisn, 
                         b.bizflg, a.isn addisn, r.isn refundisn,
                         cast(decode(oracompat.nvl(fullamountdoc,0::numeric),0,decode(docsumcnt,0,0+null,1/docsumcnt),b.amountdoc/fullamountdoc) as numeric) docsumpc,
                         b.buhquitbodyisn, b.buhquitbodycnt, s.currisn acccurrisn,
                         b1.isn quitdebetisn, b2.isn quitcreditisn,
                         oracompat.nvl(oracompat.nvl(b1.dateval, b2.dateval),f.datepay) quitdateval,
                         oracompat.nvl(b1.datequit, b2.datequit) datequit,
                         oracompat.nvl(f.currisn,oracompat.nvl(b1.currisn, b2.currisn)) quitcurrisn,
                         b1.subaccisn quitdebetsubaccisn,
                         b2.subaccisn quitcreditsubaccisn,
                         oracompat.nvl(b1.damount, -b1.camount) quitdebetbuhamount,
                         oracompat.nvl(b2.camount, -b2.damount) quitcreditbuhamount,
                         f.amount amountclosedquit,
                         buhquitamount,
                         buhquitpartamount,
                         buhquitdate,
                         b.parentisn,
                         f.amountdoc amountclosingquit,
                         sum(abs(f.amountdoc)) over (partition by b.docsumisn, buhquitisn) as fullamountclosingquit,
                         cast(decode(b.statcode,38,decode(b.deptisn,707480016,dp.signed)) as timestamp) agrbuhdate,
                         f.isn factisn,  buhquitisn,
                         b.buhheadfid, b.buhamountusd,b.oprisn,opr.classisn1 oprdeptisn,
                         b.docisn,
                         b.datepay,
                         b.headisn,
                         oracompat.nvl(dssubjisn,f.subjisn) docsumsubj,
                         b.docisn2,
                         sagroup,
                          corsubaccisn,
                         dsdatebeg,
                         dsdateend,
                         b.adeptisn,   -- egao 29.04.2009  в рамках дит-09-1-083535
                         b.dsclassisn, -- egao 02.02.2010
                         b.dsclassisn2 ,  -- egao 02.02.2010
                         b.remark,
                         b.dsstatus,
						 b.incomeoutlaykindisn, -- EGAO 31.01.2014
						 b.budgetgroupkindisn
                --     decode (decode (b.statcode,38,1,34,1),1,decode (oracompat.nvl (a.isn,aa.isn),null,null,ais.get_agr_buhdate (oracompat.nvl (a.isn,aa.isn), b.docsumisn))) agrbuhdate
                    from (select --+ use_nl (r b pc pd h adept ) ordered index ( adept x_kindaccset_acc_kind ) use_hash(ds) index (b)
                                --поля из report_body_list
                                 r.deptisn, r.statcode, r.classisn,
                                --поля проводки
                                 b.isn bodyisn, b.dateval dateval, b.currisn currisn, h.fid buhheadfid,
                                 b.deptisn deptisnbuh, b.subjisn, b.subaccisn, b.parentisn,h.isn headisn,
                                 oracompat.nvl(b.camount, -b.damount) buhamount,
                                 oracompat.nvl(b.camountrub, -b.damountrub) buhamountrub,
                                 oracompat.nvl(b.camountusd, -b.damountusd) buhamountusd,
                                 --поля аналитики
                                 --getdeptfromkindacc(b.subkindisn) deptisnan,
                                 --getreprdeptfromkindacc (b.subkindisn) reprdeptisn,
                                 gd.GetDeptFromKindAcc_res deptisnan,
                                 grd.GetReprDeptFromKindAcc_res reprdeptisn,
                                 '' bizflg,
                                 ds.isn  docsumisn,
                                 ds.datepay datepay,
                                 ds.datepaylast datepaylast,
                                 ds.datebeg dsdatebeg,
                                 ds.dateend dsdateend,
                                 ds.classisn dsclassisn,
                                 ds.classisn2 dsclassisn2,
                                 ds.status dsstatus,
                                 ds.subjisn dssubjisn,
                                 oracompat.nvl(ds.agrisn, b.agrisn) agrisn,
                                 ds.refundisn refundisn,
                                 shared_system.gcc2(ds.amount,ds.currisn,b.currisn,b.dateval) amountdoc,
                                 ds.docisn docisn,
                                 ds.docisn2 docisn2,
                                 b.oprisn,
                                 sum(shared_system.gcc2(ds.amount,ds.currisn,b.currisn,b.dateval)) over (partition by b.isn) as fullamountdoc,
                                 count(*) over (partition by b.isn) as docsumcnt,
                                 buhquitbodyisn,
                                 buhquitbodycnt,
                            --        decode(pc.isn,null,pd.creditisn,pc.debetisn) pdsbuhquitisn,
                                 r.sagroup,
                                 adept.classisn as adeptisn, -- egao 29.04.2009  в рамках дит-09-1-083535
                                 b.remark,
								 ex.classisn AS incomeoutlaykindisn,
								 bg.classisn AS budgetgroupkindisn
                            from repbuhbody_list r
                                    inner join ais.buhbody_t b
                                    on r.bodyisn = b.isn
                                    left join storage_adm.GetDeptFromKindAcc gd
                                    on b.subkindisn = gd.KindAccIsn
                                    left join storage_adm.GetReprDeptFromKindAcc grd
                                    on b.subkindisn = grd.KindAccIsn
                                    inner join ais.buhhead_t h
                                    on b.headisn = h.isn
                                    left join docsumlist ds
                                    on r.bodyisn = ds.bodyisn 
									left join ais.kindaccset adept
                                    on adept.kindaccisn = b.subkindisn
									and adept.kindisn = 56645916 -- egao 29.04.2009  в рамках дит-09-1-083535 (c.get('ckinddeptfull')-подразделения сао "ингосстрах"0
									left join ais.kindaccset ex
									on b.SubKindIsn = ex.kindaccisn
									and ex.kindisn=48739116
									left join ais.kindaccset bg
									on b.SubKindIsn = bg.kindaccisn
									and bg.kindisn=1832007003
                        ) b
                            left join ais.docs d
                            on b.docisn = d.isn
                            left join ais.docs dp
                            on d.parentisn = dp.isn
                            left join ais.subacc s
                            on d.accisn =  s.isn
                            left join ais.docs d2
                            on b.docisn2 = d2.isn
                            left join ais.agreement a
                            on b.refundisn = a.isn
                            left join ais.agreement aa
                            on b.agrisn = aa.isn
                            left join ais.agrrefund r
                            on b.refundisn = r.isn
                            left join ais.docsum f
                            on b.docsumisn = f.parentisn 
				and f.discr = 'F'
				and f.status is null
				and f.amount <> 0
                            left join ais.buhbody_t b1
                            on f.debetisn = b1.isn
                            left join ais.buhbody_t b2
                            on f.creditisn = b2.isn
                            left join quitbodylist qbl
                            on b.bodyisn = qbl.bodyisn
                            left join (select x.* 
                                            from ais.dicx x,
                                                 ais.dicti d 
                                            where x.classisn = shared_system.get('xdeptreproper')
                                                and d.isn = x.classisn1 
                                                and oracompat.nvl(d.active,'S') <> '') opr
                            on opr.classisn2 = b.oprisn                   
                    ) as q

);
