CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPBUHQUIT" ("DEPTISN", "STATCODE", "CLASSISN", "BODYISN", "DATEVAL", "CURRISN", "BUHHEADFID", "DEPTISNBUH", "SUBJISN", "SUBACCISN", "PARENTISN", "HEADISN", "BUHAMOUNT", "BUHAMOUNTRUB", "BUHAMOUNTUSD", "OPRISN", "BUHQUITBODYISN", "BUHQUITBODYCNT", "SAGROUP", "BUHPC", "BUHQUITPC", "BUHQUITDATE", "GROUPISN", "BUHQUITISN", "QUEISN", "CORSUBACCISN", "QUITSUM", "QUITPC", "FACT", "QUITBODYISN", "QUITDATEVAL", "REPCURSDIFF") AS 
  Select --+ use_nl (bbq) ordered no_merge ( b )
  b.deptisn, b.statcode, b.classisn, b.bodyisn,
       b.dateval, b.currisn, b.buhheadfid, b.deptisnbuh, b.subjisn,
       b.subaccisn, b.parentisn, b.headisn, b.buhamount, b.buhamountrub,
       b.buhamountusd, b.oprisn, b.buhquitbodyisn, b.buhquitbodycnt,
       b.sagroup,
       b.buhpc,-- коэф. частичной квитовки
       b.buhquitpc, -- коэф. корреспонденции
       b.buhquitdate,-- дата операции квитовки
        b.groupisn,
       b.buhquitisn, b.queisn, b.corsubaccisn, Sum(b.quitsum) quitsum,Sum( b.quitpc) quitpc,
       b.fact,
       Max(b.quitbodyisn) quitbodyisn ,/*KGS 04.10.11 чтобы было много места*/
       bbq.dateval QuitDateval ,-- Дата начисления проводки, с которой квитовали

       Case When b.currisn=35 Then 0
       else
         Sum
             (
        
               NVL((  - gcc2.gcc2(Nvl(b.quitsum,0)*B.BuhQuitSumPc,b.currisn,35,Nvl(B.BUHQUITDATE, B.Dateval))+ -- валютируем зачет на дату начисления
                     gcc2.gcc2(Nvl(b.quitsum,0)*B.BuhQuitSumPc,B.currisn,35, Decode(B.FACT,'Y',bbq.dateval,Nvl(B.BUHQUITDATE,B.Dateval))) -- валютируем поступление на дату оплаты
               ),0)+
               
                Nvl(( -b.buhamountrub*BUHPC*QUITPC*BuhQuitPc +  -- Сумма в рублях
                     gcc2.gcc2(b.buhamount*BUHPC*QUITPC*BuhQuitPc,B.currisn,35, Nvl(B.BUHQUITDATE,B.Dateval))
                  ) ,0)   -- разница валютирования даты начисления и даты квитовки
               
              )
               
             end RepCursDiff /* курсовая разница (в руб) валютирования не рублевого поступления в зависимости от типа потом ее к сквитованной сумме прибавлять просто надо*/
from
(
Select --+ use_nl (qd1  qd2) ordered no_merge ( b ) index (qd1 X_QUEDECODE_OBJISN ) index ( qd2 X_QUEDECODE_REFISN ) Index (bb) Index (bb1 X_BUHBODY_PARENT)
 B.*,
 Nvl(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2),0) quitSum,
Nvl(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2),1)/decode(Nvl((Sum(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2)) over (Partition by bodyisn,BuhQuitIsn)),1),0,1,
Nvl((Sum(Nvl(qd1.OBJPARAM2,-qd2.RefPARAM2)) over (Partition by bodyIsn,BuhQuitIsn)),1)) quitPc,
Decode(Nvl(qd1.REFISN,qd2.ObjIsn),null,null,
(Select Nvl(Max('Y'),'N') from docsum where  b.buhquitisn in (debetisn,creditisn)/*(creditisn=Nvl(qd1.REFISN,qd2.ObjIsn) or debetisn=Nvl(qd1.REFISN,qd2.ObjIsn)) */and discr='F'  )) Fact,
Nvl(qd1.REFISN,qd2.ObjIsn) QuitBodyIsn
from(
 Select --+ use_nl (r bb bb1 dg) ordered
     b.*,
    decode( nvl (bb1.damount,-bb1.camount),0,1,nvl (bb1.damount,-bb1.camount)/ nvl (bb.damount,-bb.camount)) BuhPc,
    /*decode(nvl (bb.damount,-bb.camount),0,1,BuhAmount/nvl (bb.damount,-bb.camount))*/

  Case
      When nvl (bb.damount,-bb.camount)=0 Then 1
      When Abs(BuhAmount/nvl (bb.damount,-bb.camount))>1 then 1 -- если сумма корресп проводки меньше нашей - то 1, иначе - коэффициент
      else
         Abs(BuhAmount/nvl (bb.damount,-bb.camount))
       end    BuhQuitSumPc,
       
  Case
      When nvl (bb.damount,-bb.camount)=0 Then 1
      When Abs(BuhAmount/nvl (bb.damount,-bb.camount))<1 then 1 -- если сумма корресп проводки больше нашей - то 1, иначе - коэффициент
      else
         nvl (bb.damount,-bb.camount)/BuhAmount
       end    BuhQuitPc,

       
     bb1.datequit BuhQuitDate,
     bb1.groupisn,
     bb1.isn BuhQuitIsn,
     dg.queisn,
     bb.subaccisn CorSubAccIsn

 from
 (
 select --+ use_nl (r b pc pd h) ordered Index (b) Index (cb)
    --Поля из report_body_list
     r.Deptisn, r.StatCode, r.ClassIsn,
    --Поля проводки
     b.isn BodyIsn, b.dateval DateVal, b.currisn CurrIsn, h.fid BuhHeadFid,
     b.deptisn DeptIsnBuh, b.SubjIsn, b.SubAccIsn, b.parentisn,H.Isn HeadIsn,
     nvl (b.camount, -b.damount) BuhAmount,
     nvl (b.camountrub, -b.damountrub) BuhAmountRub,
     nvl (b.camountusd, -b.damountusd) BuhAmountUsd,
     --Поля аналитики
     B.oprisn,
     --Поля корреспонденции
/*     (select max (isn)
      from ais.buhbody_t
      where headisn = b.headisn
        and status = 'А'
        and decode (b.damount,null,damount,camount) is not null)*/
        Cb.Isn BuhQuitBodyIsn,
     (select --+ Index(bbb)
       count (*)
      from ais.buhbody_t bbb
      where bbb.headisn = b.headisn
        and bbb.status = 'А'
        and decode (b.damount,null,damount,camount) is not null) BuhQuitBodyCnt,
        r.sagroup
    from VZ_REPBUHBODY_LIST r, ais.buhbody_t b,  ais.buhhead_t h, ais.buhbody_t cb
    where r.bodyisn = b.isn
      and b.headisn = h.isn
      
        and b.headisn = cb.headisn
        and cb.status = 'А'
        and decode (b.damount,null,Cb.damount,Cb.camount) is not null

) b,ais.buhbody_t bb, ais.buhbody_t bb1,ais.DOCGRP dg

    where b.BuhQuitBodyIsn = bb.isn (+)
       and (bb.isn = bb1.isn
        or bb.isn = bb1.parentisn
        and bb1.status = 'А'
        and bb1.oprisn = c.get('oPartQuit')
        and Nvl(bb1.camount,bb1.damount)<>0 -- у операций частичной квитовки сумм 0 не рассматриваем
        )
        And  bb1.groupisn=dg.isn(+)


        ) b,ais.quedecode qd1,ais.quedecode qd2
        Where b.queisn=qd1.queisn(+)
        And  b.BuhQuitIsn=qd1.ObjIsn(+)

        and b.queisn=qd2.queisn(+)
        And  b.BuhQuitIsn=qd2.REFISN(+)
) b,ais.buhbody_t bbq
Where b.QuitBodyIsn=bbq.isn(+)
/*KGS 04.10.11 чтобы было много места*/
Group by
  b.deptisn, b.statcode, b.classisn, b.bodyisn,
       b.dateval, b.currisn, b.buhheadfid, b.deptisnbuh, b.subjisn,
       b.subaccisn, b.parentisn, b.headisn, b.buhamount, b.buhamountrub,
       b.buhamountusd, b.oprisn, b.buhquitbodyisn, b.buhquitbodycnt,
       b.sagroup, b.buhpc, b.buhquitpc,
       b.buhquitdate,
        b.groupisn,
       b.buhquitisn, b.queisn, b.corsubaccisn,
       b.fact,
       bbq.dateval;   