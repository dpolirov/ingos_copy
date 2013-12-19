
  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_OBJCLASS_DOMESTIC" ("AGRISN", "OBJCLASSISN", "DOMESTIC", "PARENTOBJCLASSISN") AS 
  (SELECT a.AgrIsn,a.ObjClassIsn,
       Max(decode(dt.parentisn,36776016 /*c.get('CarClassForeign') - КЛАСС МАШИН ИНОСТРАННОГО ПРОИЗВОДСТВА*/,'N',
                               36775916 /*c.get('CarClassLocal') - КЛАСС МАШИН ОТЕЧЕСТВЕННОГО ПРОИЗВОДСТВА*/,'Y')) domestic,
       a.ParentObjClassIsn
FROM (Select --+ Ordered Use_Nl (a o op oc t )
              A.Isn AgrIsn,o.ClassIsn ObjClassIsn,
              nvl((select /*+index(rt x_rultariff_x1)*/ max(rt.x2)
                   from ais.rultariff rt
                   where rt.TariffISN=703301816 -- C.Get('TRF_TariffGroup') - Тарифная группа для модификации ТС. (...ГО регион-кредит)
                     and x1=t.modelisn
                     AND (rt.DateBeg<=a.datebeg Or rt.DateBeg Between a.datebeg and a.dateend)),oc.tariffgroupisn) tariffgroupisn, -- EGAO 27.10.2009
              /*Max(decode(dt.parentisn,36776016 \*c.get('CarClassForeign') - КЛАСС МАШИН ИНОСТРАННОГО ПРОИЗВОДСТВА*\,'N',
                                      36775916 \*c.get('CarClassLocal') - КЛАСС МАШИН ОТЕЧЕСТВЕННОГО ПРОИЗВОДСТВА*\,'Y')) domestic, -- EGAO 27.10.2009*/
              op.ClassIsn ParentObjClassIsn
      From  tt_rowid tt,AGREEMENT a,
            (select Isn
             from dicti
             start with isn=683209116 -- КОМПЛЕКСНОЕ СТРАХОВАНИЕ
             connect by prior isn=parentisn
            ) rl,
            (select Isn
             from dicti  -- ТИП ДОГОВОРА СТРАХОВАНИЯ
             start with isn=34711216
             connect by prior isn=parentisn
            ) ac,
            Ais.AGROBJECT o, AGROBJECT op, OBJCAR oc, CARTARIF t--EGAO 27.10.2009 Глюки начались, DICTI dt
      where a.Isn=tt.isn
        and ruleisn =rl.isn
        and a.classisn =ac.isn
        and not exists (select /*+ index(j x_subject_class) */ isn from subject j where isn=a.emplisn and classisn=491)
        and o.AgrIsn=A.Isn
        ANd op.rowid=(-- для группировки по родительским объектам
            select rowid from agrobject where parentisn is null
            start with isn=o.Isn
            connect by prior parentisn=isn and prior parentisn is not null)
        and op.descisn=oc.isn(+)
        and oc.tarifisn=t.isn(+)
        /*EGAO 27.10.2009 Глюки начались Вынес dt в самый внешний join, а select max(rt.x2) внес во from внутреннего запроса. До моих изменений был тока внутренний запрос, без поля tariffgroupisn
        and dt.isn  --(+) MSerp 26.10.2009. Убрал открытый join, т.к. в 10g этот фокус больше не проходит. Если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. Насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
            = nvl(
        (select \*+index(rt x_rultariff_x1)*\ max(rt.x2)
         from ais.rultariff rt
         where rt.TariffISN=703301816 -- C.Get('TRF_TariffGroup') - Тарифная группа для модификации ТС. (...ГО регион-кредит)
           and x1=t.modelisn
           AND (rt.DateBeg<=a.datebeg Or rt.DateBeg Between a.datebeg and a.dateend))
         ,oc.tariffgroupisn)*/
      --EGAO 27.10.2009 Глюки начались  Group by A.Isn, o.ClassIsn, op.ClassIsn
     ) a, dicti dt
WHERE dt.isn(+)=a.tariffgroupisn
Group by A.agrisn, a.ObjClassIsn, a.ParentObjClassIsn
);