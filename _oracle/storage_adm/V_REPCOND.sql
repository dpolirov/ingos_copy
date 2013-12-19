 CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPCOND" ("CONDISN", "DATEBEG", "DATEEND", "PARENTISN", "AGRISN", "ADDISN", "ADDSTATUS", "ADDNO", "ADDBEG", "ADDSIGN", "PARENTADDISN", "NEWADDISN", "OBJISN", "PARENTOBJISN", "RISKISN", "PARENTRISKISN", "LIMITISN", "RPTCLASSISN", "LIMCLASSISN", "CURRISN", "PREMCURRISN", "FRANCHCURRISN", "FRANCHTYPE", "PREMIUMSUM", "PREMUSD", "PREMRUB", "PREMEUR", "LIMITSUM", "LIMITUSD", "LIMITRUB", "LIMITEUR", "FRANCHSUM", "FRANCHUSD", "FRANCHRUB", "FRANCHEUR", "OBJCLASSISN", "OBJRPTCLASSISN", "DESCISN", "OBJPRNCLASSISN", "OBJPRNRPTCLASSISN", "RISKCLASSISN", "RISKPRNCLASSISN", "RISKRPTCLASSISN", "RISKPRNRPTCLASSISN", "RISKRULEISN", "RISKPRNRULEISN", "LIMITCLASSISN", "AGRDATEBEG", "AGRDATEEND", "AGRRULEISN", "AGRCLASSISN", "AGRCOMISSION", "AGRDISCR", "NEWADDSIGN", "QUANTITY", "FRANCHTARIFF", "OBJREGIONISN", "OBJCOUNTRYISN", "CLIENTISN", "AGROLDDATEEND", "ADDPREMIUMSUM", "AGRCURRISN", "AGRDETAILISN", "PREMAGR", "CARRPTCLASS", "DISCOUNT", "DISCOUNT2", "AGRSHAREPC", "COST", "TARIFF", "YEARTARIFF") AS 
  (/* 10.01.2013 KGS !!!!!! АХТУНГ !!!! ДЛЯ МЕДИКОВ КОНДЫ С 0 ПЛАНОВОЙ ПРЕМИЕЙ НЕ ГРУЗИМ!!!!! */
select /*+ ALL_ROWS */
"CONDISN","DATEBEG","DATEEND","PARENTISN","AGRISN","ADDISN","ADDSTATUS","ADDNO","ADDBEG",
"ADDSIGN","PARENTADDISN","NEWADDISN","OBJISN","PARENTOBJISN","RISKISN","PARENTRISKISN","LIMITISN","RPTCLASSISN",
"LIMCLASSISN","CURRISN","PREMCURRISN","FRANCHCURRISN","FRANCHTYPE","PREMIUMSUM","PREMUSD","PREMRUB","PREMEUR","LIMITSUM",
"LIMITUSD","LIMITRUB","LIMITEUR","FRANCHSUM","FRANCHUSD","FRANCHRUB","FRANCHEUR","OBJCLASSISN","OBJRPTCLASSISN","DESCISN",
"OBJPRNCLASSISN","OBJPRNRPTCLASSISN","RISKCLASSISN","RISKPRNCLASSISN","RISKRPTCLASSISN","RISKPRNRPTCLASSISN","RISKRULEISN",
"RISKPRNRULEISN","LIMITCLASSISN","AGRDATEBEG","AGRDATEEND","AGRRULEISN","AGRCLASSISN","AGRCOMISSION","AGRDISCR","NEWADDSIGN",
"QUANTITY","FRANCHTARIFF","OBJREGIONISN","OBJCOUNTRYISN","CLIENTISN","AGROLDDATEEND","ADDPREMIUMSUM","AGRCURRISN","AGRDETAILISN",
"PREMAGR","CARRPTCLASS",
"DISCOUNT","DISCOUNT2","AGRSHAREPC",
  Cost,  -- sts 14.03.2013 - Страховая стоимость
  --kds(14.11.2013) task(57056418903)
  "TARIFF",
  "YEARTARIFF"
from
(select --+ ordered  no_merge(acpx) no_merge(aopx) use_nl(t ac  acp ar ao arp aop al ad adn a city city1 addr adh) use_hash(acpx aopx  CarRules)
       AC.ISN CONDISN,
       trunc(AC.DATEBEG)DATEBEG,
       trunc(AC.DATEEND)DATEEND,
       AC.PARENTISN,
       AC.AGRISN,
       AC.ADDISN,
       AD.STATUS ADDSTATUS,
       AD.NO ADDNO,
       AD.DATEBEG ADDBEG,
       AD.DATESIGN ADDSIGN,
       ACP.ADDISN PARENTADDISN,
       AC.NEWADDISN,
       AC.OBJISN,
       AOP.ISN PARENTOBJISN,
       AC.RISKISN,

/*       ARP.ISN PARENTRISKISN, */
/*kgs 13.07.12 по письму дмитревской. искуственный групповой риск для правильного рассчета перестрахования*/

           Case When AR.PARENTISN is null and AR.RULEISN in (707051716 ,2381976903,206916 ,207016,207116)
           then
             (Select max(Ar1.Isn) from agrrisk ar1 Where ar1.agrisn=ar.agrisn and ar1.ruleisn=707045516
             and ar1.parentisn is null )
                else
             AR.PARENTISN end PARENTRISKISN,

       AC.LIMITISN,
       AC.RPTCLASSISN,
       AC.LIMCLASSISN,
       AC.CURRISN,
       AC.PREMCURRISN,
       AC.FRANCHCURRISN,
       AC.FRANCHTYPE,
       AC.PREMIUMSUM,
       gcc2.gcc2(AC.PREMIUMSUM,AC.PREMCURRISN, 53, coalesce(a.datebeg,ad.datesign,trunc(SYSDATE))/*EGAO 20.05.2011 least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)*/) PREMUSD,
       gcc2.gcc2(AC.PREMIUMSUM,AC.PREMCURRISN, 35, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) PREMRUB,
       gcc2.gcc2(AC.PREMIUMSUM,AC.PREMCURRISN, 29448516, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) PREMEUR,
       AC.LIMITSUM,
       gcc2.gcc2(AC.LIMITSUM,AC.CURRISN, 53, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) LIMITUSD,
       gcc2.gcc2(AC.LIMITSUM,AC.CURRISN, 35, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) LIMITRUB,
       gcc2.gcc2(AC.LIMITSUM,AC.CURRISN, 29448516, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) LIMITEUR,
       AC.FRANCHSUM,
       gcc2.gcc2(AC.FRANCHSUM,AC.FRANCHCURRISN, 53, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) FRANCHUSD,
       gcc2.gcc2(AC.FRANCHSUM,AC.FRANCHCURRISN, 35, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) FRANCHRUB,
       gcc2.gcc2(AC.FRANCHSUM,AC.FRANCHCURRISN, 29448516, least(AD.DATESIGN, trunc(sysdate), AC.DATEBEG)) FRANCHEUR,
       AO.CLASSISN OBJCLASSISN,
       AO.RPTCLASSISN OBJRPTCLASSISN,
       AO.DESCISN,
       AOP.CLASSISN OBJPRNCLASSISN,
       AOP.RPTCLASSISN OBJPRNRPTCLASSISN,
       AR.CLASSISN RISKCLASSISN,
       ARP.CLASSISN RISKPRNCLASSISN,
       AR.RPTCLASSISN RISKRPTCLASSISN,
       ARP.RPTCLASSISN RISKPRNRPTCLASSISN,
       AR.RULEISN RISKRULEISN,
       ARP.RULEISN RISKPRNRULEISN,
       AL.CLASSISN LIMITCLASSISN,
       A.DATEBEG AGRDATEBEG,
       A.DATEEND AGRDATEEND,
       A.RULEISN AGRRULEISN,
       A.CLASSISN AGRCLASSISN,
       A.COMISSION AGRCOMISSION,
       A.DISCR AGRDISCR,
       ADN.DATESIGN NEWADDSIGN,
       AC.QUANTITY,
       AC.FRANCHTARIFF,
       nvl(nvl(CITY.PARENTREGIONISN,CITY1.PARENTREGIONISN),(select C.PARENTREGIONISN from AIS.AGRADDR ADR,REP_CITY C where ADR.AGRISN=AC.AGRISN and ROWNUM<=1 and ADR.CITYISN=C.CITYISN)) OBJREGIONISN,
       nvl(nvl(CITY.PARENTCOUNTRYISN,CITY1.PARENTCOUNTRYISN),(select C.PARENTCOUNTRYISN from AIS.AGRADDR ADR,REP_CITY C where ADR.AGRISN=AC.AGRISN and ROWNUM<=1 and ADR.CITYISN=C.CITYISN)) OBJCOUNTRYISN,
       A.CLIENTISN,
       A.OLDDATEEND AGROLDDATEEND,
       AD.PREMIUMSUM ADDPREMIUMSUM,
       A.CURRISN AGRCURRISN, --egao 14.07.2010
       ADH.AGRDETAILISN -- OD 25.10.2010
       ,CASE
          WHEN ac.premcurrisn=a.currisn THEN ac.premiumsum
          ELSE gcc2.gcc2(ac.premiumsum,ac.premcurrisn, a.currisn, coalesce(ac.datebeg, a.datebeg,trunc(SYSDATE)))
        END AS premagr ,-- EGAO 31.08.2011 в рамках ДИТ-07-1-027944

        /* sts - old 13.01.2012 -- в качестве второго параметра ф-ии должен быть RULEISN договора, а не риска!
           корректная версия ниже
        Decode(CarRules.Isn,null,null,motor.f_get_rptclass(AR.CLASSISN, AR.RULEISN, AC.RPTCLASSISN))  Carrptclass
        */

        Decode(CarRules.Isn,null,null,753518300,'ГО',motor.f_get_rptclass(AR.CLASSISN, A.RULEISN, AC.RPTCLASSISN)) as Carrptclass,
        AC.Discount,
        AC.Discount2,
        NVL(a.sharepc,100) AS agrsharepc, -- EGAO 19.03.2012
        ac.Cost, -- sts 14.03.2013 - Страховая стоимость
        --kds(14.11.2013) task(57056418903)
        AC.TARIFF,
        AC.YEARTARIFF
  from TT_ROWID T,
       AGRCOND AC,
       ( select --+ ordered use_nl(zt zac)
                distinct
                ZAC.ISN,
                ( select
                         max(ROWID)
                    from AGRCOND ZZ
                   where ZZ.PARENTISN is null
                   start with ZZ.ISN = ZAC.PARENTISN
                 connect by NoCYCLE prior ZZ.PARENTISN = ZZ.ISN ) RID /*NoCYCLE - KGS 08.10.11 не нужен. надо данные в АИС вычистить*/
           from TT_ROWID ZT,
                AGRCOND ZAC
          where ZAC.AGRISN = ZT.ISN ) ACPX,
       AGRCOND ACP,
       AGRRISK AR,
       AGROBJECT AO,
       AGRRISK ARP,
       ( select --+ ordered use_nl(zt zao)
                distinct
                ZAO.ISN,
                ( select
                         max(ROWID) RID
                    from AGROBJECT ZZ
                   where ZZ.PARENTISN is null
                   start with ZZ.ISN = ZAO.PARENTISN
                 connect by NoCYCLE prior ZZ.PARENTISN = ZZ.ISN ) RID
           from TT_ROWID ZT,
                AGROBJECT ZAO
          where ZAO.AGRISN = ZT.ISN ) AOPX,
       AGROBJECT AOP,
       AGRLIMIT AL,
       AGREEMENT AD,
       AGREEMENT ADN,
       AGREEMENT A,
       REP_CITY CITY,
       REP_CITY CITY1,
       AIS.AGRADDR ADDR,
       AGR_DETAIL_AGRHASH ADH,
       /*KGS 19.12.2011 Простановка поля RPTCLASS для автострахования. Далее будем пользовать отсюда */
       ( Select r.* from  motor.v_dicti_rule  r ) CarRules
 where T.ISN        = AC.AGRISN
   and AR.ISN(+)    = AC.RISKISN
   and AO.ISN(+)    = AC.OBJISN
   and ARP.ISN(+)   = AR.PARENTISN
   and AOPX.ISN(+)  = AO.ISN
   and AOP.ROWID(+) = AOPX.RID --mserp 27.10.2009 глюки начались.
   --(+) mserp 26.10.2009. убрал открытый join, т.к. в 10g этот фокус больше не проходит. если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
   --= nvl((select/*+rule*/ decode(count(*), 0, null, max(rowid))  from agrobject where parentisn is null  start with isn = ao.parentisn connect by prior parentisn = isn),ao.rowid)
   and AL.ISN(+)    = AC.LIMITISN
   and AD.ISN(+)    = AC.ADDISN
   and ADN.ISN(+)   = AC.NEWADDISN
   and ACPX.ISN(+)  = AC.ISN
   and ACP.ROWID(+) = ACPX.RID -- mserp 27.10.2009 глюки начались.
   --(+) mserp 26.10.2009. убрал открытый join, т.к. в 10g этот фокус больше не проходит. если начнутся глюки, в чем я сомневаюсь, надо будет переписывать. насколько я помню, в 9i (+) можно было написать, но join всё равно получался закрытым.
   --= nvl((select/*+rule*/ decode(count(*), 0, null, max(rowid)) from agrcond where parentisn is null  start with isn = ac.parentisn connect by prior parentisn = isn),ac.rowid)
   and T.ISN        = A.ISN(+)
   and AOP.CITYISN  = CITY.CITYISN(+)
   and AOP.CITYISN  = ADDR.ISN(+)
   and ADDR.CITYISN = CITY1.CITYISN(+)
   and AC.AGRISN    = ADH.AGRISN(+)

   and A.ruleisn =CarRules.Isn(+)
)
Where AGRRULEISN  NOT IN ( select D.ISN
                           from DICTI D
                           start with D.ISN = 686160416
                           connect by prior D.ISN = D.PARENTISN  )
OR NVL(PREMIUMSUM,0)>0
);