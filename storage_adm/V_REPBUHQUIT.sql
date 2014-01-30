create or replace view v_repbuhquit (
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
           b.buhpc,-- ����. ��������� ��������
           b.buhquitpc, -- ����. ���������������
           b.buhquitdate,-- ���� �������� ��������
           b.groupisn,
           b.buhquitisn, 
           b.queisn, 
           b.corsubaccisn, 
           sum(b.quitsum) quitsum,
           sum( b.quitpc) quitpc,
           b.fact,
           max(b.quitbodyisn) quitbodyisn ,/*kgs 04.10.11 ����� ���� ����� �����*/
           bbq.dateval quitdateval,-- ���� ���������� ��������, � ������� ���������
           case when b.currisn = 35 then 0
           else
             sum
                 (
                   oracompat.nvl(( - gcc2.gcc2(oracompat.nvl(b.quitsum,0)*b.buhquitsumpc,b.currisn,35,oracompat.nvl(b.buhquitdate, b.dateval))+ -- ���������� ����� �� ���� ����������
                         gcc2.gcc2(oracompat.nvl(b.quitsum,0)*b.buhquitsumpc,b.currisn,35, decode(b.fact,'y',bbq.dateval,oracompat.nvl(b.buhquitdate,b.dateval))) -- ���������� ����������� �� ���� ������
                   ),0)+
                    oracompat.nvl(( -b.buhamountrub*buhpc*quitpc*buhquitpc +  -- ����� � ������
                         gcc2.gcc2(b.buhamount*buhpc*quitpc*buhquitpc,b.currisn,35, oracompat.nvl(b.buhquitdate,b.dateval))
                      ) ,0)   -- ������� ������������� ���� ���������� � ���� ��������
                  )
           end repcursdiff /* �������� ������� (� ���) ������������� �� ��������� ����������� � ����������� �� ���� ����� �� � ������������ ����� ���������� ������ ����*/
        from
            (
                select --+ use_nl (qd1  qd2) ordered no_merge ( b ) index (qd1 x_quedecode_objisn ) index ( qd2 x_quedecode_refisn ) index (bb) index (bb1 x_buhbody_parent)
                        b.*,
                        oracompat.nvl(oracompat.nvl(qd1.objparam2,-qd2.refparam2),0) quitsum,
                        oracompat.nvl(oracompat.nvl(qd1.objparam2,-qd2.refparam2),1)/decode(oracompat.nvl((sum(oracompat.nvl(qd1.objparam2,-qd2.refparam2)) over (partition by bodyisn,buhquitisn)),1),0,1,
                        oracompat.nvl((sum(oracompat.nvl(qd1.objparam2,-qd2.refparam2)) over (partition by bodyisn,buhquitisn)),1)) quitpc,
                        decode(oracompat.nvl(qd1.refisn,qd2.objisn),null,null,
                        (select oracompat.nvl(max('y'),'n') from docsum where  b.buhquitisn in (debetisn,creditisn)/*(creditisn=oracompat.nvl(qd1.refisn,qd2.objisn) or debetisn=oracompat.nvl(qd1.refisn,qd2.objisn)) */and discr='f'  )) fact,
                        oracompat.nvl(qd1.refisn,qd2.objisn) quitbodyisn
                    from(
                         select --+ use_nl (r bb bb1 dg) ordered
                                  b.*,
                                  decode( oracompat.nvl (bb1.damount,-bb1.camount),0,1,oracompat.nvl (bb1.damount,-bb1.camount)/ oracompat.nvl (bb.damount,-bb.camount)) buhpc,
                                        /*decode(oracompat.nvl (bb.damount,-bb.camount),0,1,buhamount/oracompat.nvl (bb.damount,-bb.camount))*/
                                  case
                                      when oracompat.nvl (bb.damount,-bb.camount)=0 then 1
                                      when abs(buhamount/oracompat.nvl (bb.damount,-bb.camount))>1 then 1 -- ���� ����� ������� �������� ������ ����� - �� 1, ����� - �����������
                                      else
                                         abs(buhamount/oracompat.nvl (bb.damount,-bb.camount))
                                  end buhquitsumpc,
                                  case
                                      when oracompat.nvl (bb.damount,-bb.camount)=0 then 1
                                      when abs(buhamount/oracompat.nvl (bb.damount,-bb.camount))<1 then 1 -- ���� ����� ������� �������� ������ ����� - �� 1, ����� - �����������
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
                                            --���� �� report_body_list
                                             r.deptisn, 
                                             r.statcode, 
                                             r.classisn,
                                            --���� ��������
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
                                             --���� ���������
                                             b.oprisn,
                                             --���� ���������������
                                        /*     (select max (isn)
                                              from ais.buhbody_t
                                              where headisn = b.headisn
                                                and status = '�'
                                                and decode (b.damount,null,damount,camount) is not null)*/
                                             cb.isn buhquitbodyisn,
                                             (select --+ index(bbb)
                                                    count (*)
                                                from ais.buhbody_t bbb
                                                where bbb.headisn = b.headisn
                                                    and bbb.status = '�'
                                                    and decode(b.damount,null,damount,camount) is not null) buhquitbodycnt,
                                             r.sagroup
                                        from vz_repbuhbody_list r, 
                                              ais.buhbody_t b,  
                                              ais.buhhead_t h, 
                                              ais.buhbody_t cb
                                        where r.bodyisn = b.isn
                                            and b.headisn = h.isn 
                                            and b.headisn = cb.headisn
                                            and cb.status = '�'
                                            and decode(b.damount,null,cb.damount,cb.camount) is not null
                                ) b 
                                    left join ais.buhbody_t bb
                                    on b.buhquitbodyisn = bb.isn,
                                ais.buhbody_t bb1
                                    left join ais.docgrp dg
                                    on bb1.groupisn = dg.isn
                             where bb.isn = bb1.isn
                                   or bb.isn = bb1.parentisn
                                   and bb1.status = '�'
                                   and bb1.oprisn = c.get('opartquit')
                                   and oracompat.nvl(bb1.camount,bb1.damount) <> 0 -- � �������� ��������� �������� ���� 0 �� �������������
                        ) b
                            left join ais.quedecode qd1
                            on (b.queisn = qd1.queisn and b.buhquitisn = qd1.objisn)
                            left join ais.quedecode qd2
                            on (b.queisn = qd2.queisn and b.buhquitisn = qd2.refisn)
            ) b
                left join ais.buhbody_t bbq
                on b.quitbodyisn = bbq.isn
    /*kgs 04.10.11 ����� ���� ����� �����*/
    group by
               b.deptisn, b.statcode, b.classisn, b.bodyisn,
               b.dateval, b.currisn, b.buhheadfid, b.deptisnbuh, 
               b.subjisn, b.subaccisn, b.parentisn, b.headisn, 
               b.buhamount, b.buhamountrub, b.buhamountusd, b.oprisn, 
               b.buhquitbodyisn, b.buhquitbodycnt,b.sagroup, b.buhpc, 
               b.buhquitpc,b.buhquitdate,b.groupisn, b.buhquitisn, 
               b.queisn, b.corsubaccisn, b.fact, bbq.dateval
);
