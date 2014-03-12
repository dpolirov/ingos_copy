create or replace view storage_adm.v_repbuhquit (
   deptisn,
   statcode,
   classisn,
   bodyisn,
   dateval,
   currisn,
   buhheadfid,
   deptisnbuh,
   subjisn,
   subaccisn,
   parentisn,
   headisn,
   buhamount,
   buhamountrub,
   buhamountusd,
   oprisn,
   buhquitbodyisn,
   buhquitbodycnt,
   sagroup,
   buhpc,
   buhquitpc,
   buhquitdate,
   groupisn,
   buhquitisn,
   queisn,
   corsubaccisn,
   quitsum,
   quitpc,
   fact,
   quitbodyisn,
   quitdateval,
   repcursdiff )
as
(

select --+ use_nl (bbq) ordered no_merge ( b )
           b.deptisn, 
           b.statcode, 
           b.classisn, 
           b.bodyisn,
           b.dateval, 
           b.currisn, 
           b.buhheadfid, 
           b.deptisnbuh, 
           b.subjisn,
           b.subaccisn, 
           b.parentisn, 
           b.headisn, 
           b.buhamount, 
           b.buhamountrub,
           b.buhamountusd, 
           b.oprisn, 
           b.buhquitbodyisn, 
           b.buhquitbodycnt,
           b.sagroup,
           b.buhpc,-- коэф. частичной квитовки
           b.buhquitpc, -- коэф. корреспонденции
           b.buhquitdate,-- дата операции квитовки
           b.groupisn,
           b.buhquitisn, 
           b.queisn, 
           b.corsubaccisn, 
           sum(b.quitsum) quitsum,
           sum( b.quitpc) quitpc,
           b.fact,
           max(b.quitbodyisn) quitbodyisn ,/*kgs 04.10.11 чтобы было много места*/
           bbq.dateval quitdateval,-- дата начисления проводки, с которой квитовали
           case when b.currisn = 35 then 0
           else
             sum
                 (
                   oracompat.nvl(( - shared_system.gcc2(oracompat.nvl(b.quitsum,0::numeric)*b.buhquitsumpc,b.currisn,35,oracompat.nvl(b.buhquitdate, b.dateval))+ -- валютируем зачет на дату начисления
                         shared_system.gcc2(oracompat.nvl(b.quitsum,0::numeric)*b.buhquitsumpc,b.currisn,35, decode(b.fact,'y',bbq.dateval,oracompat.nvl(b.buhquitdate,b.dateval))) -- валютируем поступление на дату оплаты
                   ),0::numeric)+
                    oracompat.nvl(( -b.buhamountrub*buhpc*quitpc*buhquitpc +  -- сумма в рублях
                         shared_system.gcc2(b.buhamount*buhpc*quitpc*buhquitpc,b.currisn,35, oracompat.nvl(b.buhquitdate,b.dateval))
                      ) ,0::numeric)   -- разница валютирования даты начисления и даты квитовки
                  )
           end repcursdiff /* курсовая разница (в руб) валютирования не рублевого поступления в зависимости от типа потом ее к сквитованной сумме прибавлять просто надо*/
        from
            (
                select --+ use_nl (qd1  qd2) ordered no_merge ( b ) index (qd1 x_quedecode_objisn ) index ( qd2 x_quedecode_refisn ) index (bb) index (bb1 x_buhbody_parent)
                        b.*,
                        oracompat.nvl(oracompat.nvl(qd1.objparam2,-qd2.refparam2),0::numeric) quitsum,
                        oracompat.nvl(oracompat.nvl(qd1.objparam2,-qd2.refparam2),1::numeric)/decode(oracompat.nvl((sum(oracompat.nvl(qd1.objparam2,-qd2.refparam2)) over (partition by bodyisn,buhquitisn)),1::numeric),0,1,
                        oracompat.nvl((sum(oracompat.nvl(qd1.objparam2,-qd2.refparam2)) over (partition by bodyisn,buhquitisn)),1::numeric)) quitpc,
                        decode(oracompat.nvl(qd1.refisn,qd2.objisn),null,null,
                        (select oracompat.nvl(max('Y'),'N') from ais.docsum where  b.buhquitisn in (debetisn,creditisn)/*(creditisn=oracompat.nvl(qd1.refisn,qd2.objisn) or debetisn=oracompat.nvl(qd1.refisn,qd2.objisn)) */and discr='f'  )) fact,
                        oracompat.nvl(qd1.refisn,qd2.objisn) quitbodyisn
                    from(
                                                              select --+ use_nl (r bb bb1 dg) ordered
                                  b.*,
                                  decode( oracompat.nvl (bb1.damount,-bb1.camount),0,1,oracompat.nvl (bb1.damount,-bb1.camount)/ oracompat.nvl (bb.damount,-bb.camount)) buhpc,
                                        /*decode(oracompat.nvl (bb.damount,-bb.camount),0,1,buhamount/oracompat.nvl (bb.damount,-bb.camount))*/
                                  case
                                      when oracompat.nvl (bb.damount,-bb.camount)=0 then 1
                                      when abs(buhamount/oracompat.nvl (bb.damount,-bb.camount))>1 then 1 -- если сумма корресп проводки меньше нашей - то 1, иначе - коэффициент
                                      else
                                         abs(buhamount/oracompat.nvl (bb.damount,-bb.camount))
                                  end buhquitsumpc,
                                  case
                                      when oracompat.nvl (bb.damount,-bb.camount)=0 then 1
                                      when abs(buhamount/oracompat.nvl (bb.damount,-bb.camount))<1 then 1 -- если сумма корресп проводки больше нашей - то 1, иначе - коэффициент
                                      else
                                         oracompat.nvl (bb.damount,-bb.camount)/buhamount
                                  end buhquitpc,
                                  bb1.datequit buhquitdate,
                                  bb1.groupisn,
                                  bb1.isn buhquitisn,
                                  dg.queisn,
                                  bb.subaccisn corsubaccisn
                             from
                                 (
                                     select --+ use_nl (r b pc pd h) ordered index (b) index (cb)
                                            --поля из report_body_list
                                             r.deptisn, 
                                             r.statcode, 
                                             r.classisn,
                                            --поля проводки
                                             b.isn bodyisn, 
                                             b.dateval dateval,
                                             b.currisn currisn, 
                                             h.fid buhheadfid,
                                             b.deptisn deptisnbuh, 
                                             b.subjisn,
                                             b.subaccisn, 
                                             b.parentisn,
                                             h.isn headisn,
                                             oracompat.nvl (b.camount, -b.damount) buhamount,
                                             oracompat.nvl (b.camountrub, -b.damountrub) buhamountrub,
                                             oracompat.nvl (b.camountusd, -b.damountusd) buhamountusd,
                                             --поля аналитики
                                             b.oprisn,
                                             --поля корреспонденции
                                        /*     (select max (isn)
                                              from ais.buhbody_t
                                              where headisn = b.headisn
                                                and status = 'а'
                                                and decode (b.damount,null,damount,camount) is not null)*/
                                             cb.isn buhquitbodyisn,
                                             count (bbb.headisn) buhquitbodycnt,
                                                r.sagroup
                                        from storage_adm.vz_repbuhbody_list r
                                        inner join ais.buhbody_t b on r.bodyisn = b.isn
                                        inner join ais.buhhead_t h on b.headisn = h.isn 
                                        inner join ais.buhbody_t cb on
                                               b.headisn = cb.headisn
                                            and cb.status = 'А'
                                            and decode(b.damount,null,cb.damount,cb.camount) is not null
                                        left join ais.buhbody_t bbb
                                        on bbb.headisn = b.headisn
                                                    and bbb.status = 'А'
                                                    and decode(b.damount,null,bbb.damount,bbb.camount) is not null
                                                    group by

                                                    r.deptisn, 
                                             r.statcode, 
                                             r.classisn,
                                            
                                             b.isn , 
                                             b.dateval ,
                                             b.currisn , 
                                             h.fid ,
                                             b.deptisn , 
                                             b.subjisn,
                                             b.subaccisn, 
                                             b.parentisn,
                                             h.isn ,
                                             oracompat.nvl (b.camount, -b.damount),
                                             oracompat.nvl (b.camountrub, -b.damountrub) ,
                                             oracompat.nvl (b.camountusd, -b.damountusd) ,
                                             
                                             b.oprisn,
                                             cb.isn ,
                                             r.sagroup
                                ) b 
                                    left join ais.buhbody_t bb
                                    on b.buhquitbodyisn = bb.isn,
                                ais.buhbody_t bb1
                                    left join ais.docgrp dg
                                    on bb1.groupisn = dg.isn
                             where bb.isn = bb1.isn
                                   
                        ---
                        union 
                        ---
                        select --+ use_nl (r bb bb1 dg) ordered
                                  b.*,
                                  decode( oracompat.nvl (bb1.damount,-bb1.camount),0,1,oracompat.nvl (bb1.damount,-bb1.camount)/ oracompat.nvl (bb.damount,-bb.camount)) buhpc,
                                        /*decode(oracompat.nvl (bb.damount,-bb.camount),0,1,buhamount/oracompat.nvl (bb.damount,-bb.camount))*/
                                  case
                                      when oracompat.nvl (bb.damount,-bb.camount)=0 then 1
                                      when abs(buhamount/oracompat.nvl (bb.damount,-bb.camount))>1 then 1 -- если сумма корресп проводки меньше нашей - то 1, иначе - коэффициент
                                      else
                                         abs(buhamount/oracompat.nvl (bb.damount,-bb.camount))
                                  end buhquitsumpc,
                                  case
                                      when oracompat.nvl (bb.damount,-bb.camount)=0 then 1
                                      when abs(buhamount/oracompat.nvl (bb.damount,-bb.camount))<1 then 1 -- если сумма корресп проводки больше нашей - то 1, иначе - коэффициент
                                      else
                                         oracompat.nvl (bb.damount,-bb.camount)/buhamount
                                  end buhquitpc,
                                  bb1.datequit buhquitdate,
                                  bb1.groupisn,
                                  bb1.isn buhquitisn,
                                  dg.queisn,
                                  bb.subaccisn corsubaccisn
                             from
                                 (
                                     select --+ use_nl (r b pc pd h) ordered index (b) index (cb)
                                            --поля из report_body_list
                                             r.deptisn, 
                                             r.statcode, 
                                             r.classisn,
                                            --поля проводки
                                             b.isn bodyisn, 
                                             b.dateval dateval,
                                             b.currisn currisn, 
                                             h.fid buhheadfid,
                                             b.deptisn deptisnbuh, 
                                             b.subjisn,
                                             b.subaccisn, 
                                             b.parentisn,
                                             h.isn headisn,
                                             oracompat.nvl (b.camount, -b.damount) buhamount,
                                             oracompat.nvl (b.camountrub, -b.damountrub) buhamountrub,
                                             oracompat.nvl (b.camountusd, -b.damountusd) buhamountusd,
                                             --поля аналитики
                                             b.oprisn,
                                             --поля корреспонденции
                                        /*     (select max (isn)
                                              from ais.buhbody_t
                                              where headisn = b.headisn
                                                and status = 'а'
                                                and decode (b.damount,null,damount,camount) is not null)*/
                                             cb.isn buhquitbodyisn,
                                             count (bbb.headisn) buhquitbodycnt,
                                                r.sagroup
                                        from storage_adm.vz_repbuhbody_list r
                                        inner join ais.buhbody_t b on r.bodyisn = b.isn
                                        inner join ais.buhhead_t h on b.headisn = h.isn 
                                        inner join ais.buhbody_t cb on
                                               b.headisn = cb.headisn
                                            and cb.status = 'А'
                                            and decode(b.damount,null,cb.damount,cb.camount) is not null
                                        left join ais.buhbody_t bbb
                                        on bbb.headisn = b.headisn
                                                    and bbb.status = 'А'
                                                    and decode(b.damount,null,bbb.damount,bbb.camount) is not null
                                                    group by

                                                    r.deptisn, 
                                             r.statcode, 
                                             r.classisn,
                                            
                                             b.isn , 
                                             b.dateval ,
                                             b.currisn , 
                                             h.fid ,
                                             b.deptisn , 
                                             b.subjisn,
                                             b.subaccisn, 
                                             b.parentisn,
                                             h.isn ,
                                             oracompat.nvl (b.camount, -b.damount),
                                             oracompat.nvl (b.camountrub, -b.damountrub) ,
                                             oracompat.nvl (b.camountusd, -b.damountusd) ,
                                             
                                             b.oprisn,
                                             cb.isn ,
                                             r.sagroup
                                ) b 
                                    left join ais.buhbody_t bb
                                    on b.buhquitbodyisn = bb.isn,
                                ais.buhbody_t bb1
                                    left join ais.docgrp dg
                                    on bb1.groupisn = dg.isn
                             where bb.isn = bb1.parentisn
                                   and bb1.status = 'А'
                                   and bb1.oprisn = shared_system.get('opartquit')
                                   and oracompat.nvl(bb1.camount,bb1.damount) <> 0 -- у операций частичной квитовки сумм 0 не рассматриваем
                        
                        ) b
                            left join ais.quedecode qd1
                            on (b.queisn = qd1.queisn and b.buhquitisn = qd1.objisn)
                            left join ais.quedecode qd2
                            on (b.queisn = qd2.queisn and b.buhquitisn = qd2.refisn)
            ) b
                left join ais.buhbody_t bbq
                on b.quitbodyisn = bbq.isn
    /*kgs 04.10.11 чтобы было много места*/
    group by
               b.deptisn, b.statcode, b.classisn, b.bodyisn,
               b.dateval, b.currisn, b.buhheadfid, b.deptisnbuh, 
               b.subjisn, b.subaccisn, b.parentisn, b.headisn, 
               b.buhamount, b.buhamountrub, b.buhamountusd, b.oprisn, 
               b.buhquitbodyisn, b.buhquitbodycnt,b.sagroup, b.buhpc, 
               b.buhquitpc,b.buhquitdate,b.groupisn, b.buhquitisn, 
               b.queisn, b.corsubaccisn, b.fact, bbq.dateval
			   );