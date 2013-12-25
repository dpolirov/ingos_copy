  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_BUH_LINECODE" ("BODYISN", "STATCODE", "LINECODE") AS 
  with Prm as (
-- sts 25.07.2012 - загрузка данных для RepBuh2Obj
-- part - расшифровка 48 статкодов
  select
    max(pParam.GetParamD('pDB')) as DB,
    max(pParam.GetParamD('pDE')) as DE
  from dual
)

select

  B.BodyISN,
  B.STATCODE,
  Max(nvl(RS.STATCODE, B.STATCODE)) keep (dense_rank first  order by  Rs.ANALITIKISN nulls last) as LINECODE
from 
 (select --+ parallel(RB 28) ordered use_nl(Prm RB)
  distinct
    RB.BodyISN,          
    RB.SubAccISN,
    RB.CorSubAccISN,
    RB.STATCODE
  from  
    Prm,
    repbuhbody RB
  where  
    RB.DateVal >= Prm.DB and RB.DateVal < Prm.DE
    and RB.Sagroup in (1, 3, 5)
    and RB.StatCode = 48
  UNION 
  select --+ parallel(RB 28) ordered use_nl(Prm RB)
  distinct
    RB.BodyISN,          
    RB.SubAccISN,
    RB.CorSubAccISN,
    RB.STATCODE
  from  
    Prm,
    STORAGE_SOURCE.RepBuhQuit RB
  where  
    RB.BuhQuitDate >= Prm.DB and RB.BuhQuitDate < Prm.DE
    and RB.Sagroup in (1, 3, 5)
    and RB.StatCode = 48
 ) B,
   REP_BUDGET_STATCODE RS
where
  B.SubAccISN = RS.SUBACCISN(+)
  and nvl(RS.CORSUBACCISN(+), B.CorSubAccISN) = B.CorSubAccISN
  AND CASE
        WHEN RS.ANALITIKISN(+) IS NULL THEN 1
        ELSE
          CASE RS.ANALITIKISN(+)
            WHEN (SELECT KS.CLASSISN
                  FROM AIS.BUHBODY_T BB,
                       AIS.KINDACCSET KS
                  WHERE BB.ISN        = B.BODYISN
                    AND KS.KINDACCISN = BB.SUBKINDISN
                    AND KS.CLASSISN   = RS.ANALITIKISN
                 ) THEN 1
          END
      END = 1
group by B.BodyISN,
  B.STATCODE;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_MV_REPBUH2OBJ_SMALL" ("DB", "DE", "AGRISN", "REFUNDISN", "REAGRISN", "STATCODE", "SAGROUP", "LINECODE", "DEPTISN", "BUHSUBJISN", "RPTCLASS", "RPTGROUPISN", "MOTIVGROUPISN", "CARRPTCLASS", "NAMOUNTRUB", "NAMOUNTUSD", "QAMOUNTRUB", "QAMOUNTUSD", "DATEVAL", "DATEQUIT", "IS_NACH", "IS_QUIT", "SALERGOISN", "SALERGODEPT", "SALERCRGOISN", "SALERCRGODEPT", "SALERFISN", "SALERFDEPT", "FIRST_DATEPAY", "LAST_DATEPAY", "DATEPAYLAST", "OBJISN", "OBJCLASSISN") AS 
  select /*+ ordered use_nl( ags B RA AD) Index(ags) */
/*$
sts 15.02.2013 - загрузка данных для материализованного представления MV_REPBUH2OBJ_SMALL
$*/
  b.db,
  b.de, 
  b.agrisn, 
  b.refundisn, 
  b.reagrisn, 
  b.statcode,
  b.sagroup, 
  b.linecode, 
  b.deptisn, 
  b.buhsubjisn, 
  b.rptclass,
  b.rptgroupisn, 
  b.motivgroupisn, 
  b.carrptclass,
  sum(b.namountrub) as namountrub,
  sum(b.namountusd) as namountusd,
  sum(b.qamountrub) as qamountrub, 
  sum(b.qamountusd) as qamountusd, 
  min(b.dateval) as dateval, 
  min(b.datequit) as datequit,
  b.is_nach, 
  b.is_quit, 
  b.salergoisn, 
  b.salergodept,
  b.salercrgoisn, 
  b.salercrgodept, 
  b.salerfisn, 
  b.salerfdept,
  min(b.first_datepay) as first_datepay, 
  max(b.last_datepay) as last_datepay,
  max(b.datepaylast) as datepaylast,
  b.objisn,
  b.objclassisn
from storages.repbuh2obj b
/*$
   sts - т.к. текст этой вьюхи используется в пакете REP_MOTIVATION как текст мат. представления,
   то тут в комментариях прописан текст, который будет заменятся на даты.
$*/
 /*$MAT_VIEW_DATE_SECTION$*/
group by
  b.db, 
  b.de, 
  b.agrisn, 
  b.refundisn, 
  b.reagrisn, 
  b.statcode,
  b.sagroup, 
  b.linecode, 
  b.deptisn, 
  b.buhsubjisn, 
  b.rptclass,
  b.rptgroupisn, 
  b.motivgroupisn, 
  b.carrptclass,
  b.is_nach, 
  b.is_quit, 
  b.salergoisn, 
  b.salergodept,
  b.salercrgoisn, 
  b.salercrgodept, 
  b.salerfisn, 
  b.salerfdept,
  b.objisn,
  b.objclassisn;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_REPBUH2OBJ_FINAL_PART" ("DB", "DE", "AGRISN", "REFUNDISN", "REAGRISN", "STATCODE", "SAGROUP", "LINECODE", "DEPTISN", "SUBACCISN", "BUHSUBJISN", "RULEISN", "OBJISN", "OBJCLASSISN", "RPTCLASS", "RPTGROUPISN", "MOTIVGROUPISN", "CARRPTCLASS", "NAMOUNTRUB", "NAMOUNTUSD", "QAMOUNTRUB", "QAMOUNTUSD", "DATEVAL", "DATEQUIT", "ADDISN", "IS_NACH", "IS_QUIT", "AGRDATEBEG", "AGRDATEEND", "AGRDATESIGN", "AGRRULEISN", "AGRID", "ADDDATEBEG", "ADDDATEEND", "ADDDATESIGN", "ADDRULEISN", "ADDID", "EXCLUDE_MEDIC", "SALERGOISN", "SALERGODEPT", "SALERCRGOISN", "SALERCRGODEPT", "SALERFISN", "SALERFDEPT", "FIRST_DATEPAY", "LAST_DATEPAY", "DATEPAYLAST") AS 
  select --+ ordered use_nl( ags B RA AD agr) Index(ags X_REP_AGR_SALERS_LINE_AGR_DB)
-- sts 25.07.2012 - загрузка данных для RepBuh2Obj
-- part - "схлопываем" строки начислений и квитовок
-- kgs 28.08.2012 - добавил определения продавцов (строчки из rep_agr_salers_Line)
-- kgs 26.03.2013 - если продавца нет в rep_agr_salers_Line - берем последнего из repagroleagr - для исходящих проводок которые делают хер знает когда
  B.DB,
  B.DE,
  B.agrisn,
  B.refundisn,
  B.ReAgrisn,
  B.statcode,
  B.sagroup,
  nvl(B.LineCode, B.statcode) as LineCode,
  B.deptisn,
  B.subaccisn,
  B.BuhSubjIsn,
  B.ruleisn,
  B.ObjIsn,
  B.objclassisn,
  B.rptclass,
  B.rptgroupisn,
  B.motivgroupisn,
  B.carrptclass,
  B.NAmountrub,
  B.NAmountusd,
  B.qamountrub,
  B.qamountusd,
  B.dateval,
  B.datequit,
  B.addisn,
  B.IS_NACH,       -- признак наличия начислений
  B.IS_QUIT,       -- признак наличия квитовок
  /* убрано для скорости
  -- некоторые атрибуты договора
  RA.DATEBEG as AGRDATEBEG,
  RA.DATEEND as AGRDATEEND,
  RA.DATESIGN as AGRDATESIGN,
  RA.RULEISN as AGRRULEISN,
  RA.ID as AGRID,
  -- некоторые атрибуты адендума
  AD.DATEBEG as ADDDATEBEG,
  AD.DATEEND as ADDDATEEND,
  AD.DATESIGN as ADDDATESIGN,
  AD.RULEISN as ADDRULEISN,
  AD.ID as ADDID,
  */
  -- некоторые атрибуты договора
  to_date(null) as AGRDATEBEG,
  to_date(null) as AGRDATEEND,
  to_date(null) as AGRDATESIGN,
  to_number(null) as AGRRULEISN,
  cast(null as varchar2(20)) as AGRID,
  -- некоторые атрибуты адендума
  to_date(null) as ADDDATEBEG,
  to_date(null) as ADDDATEEND,
  to_date(null) as ADDDATESIGN,
  to_number(null) as ADDRULEISN,
  cast(null as varchar2(20)) as ADDID,

  B.Exclude_Medic, -- признак исключения строк из мотивационного отчета (Y - исключить/N - оставить)
  
  Nvl(AGS.SALERGOISN,Agr.SALERGOISN) SALERGOISN,
  Nvl(AGS.SALERGODEPT,AGR.SALERGODEPTISN) SALERGODEPT,
  
  Nvl(AGS.SALERCRGOISN,AGR.crossalerisn) SALERCRGOISN,
  Nvl(AGS.SALERCRGODEPT,AGR.crossalerdeptisn) SALERCRGODEPT,
  
  Nvl(AGS.SALERFISN,AGR.SALERFISN) SALERFISN,
  Nvl(AGS.SALERFDEPT,AGR.SALERFDEPTISN) SALERFDEPT,
  
  B.first_datepay,
  B.last_datepay,
  B.datepaylast

from
 (select
    b.DB,
    b.DE,
    b.agrisn,
    b.refundisn,
    b.ReAgrisn,
    b.statcode,
    b.sagroup,
    b.LineCode,
    b.deptisn,
    b.subaccisn,
    b.BuhSubjIsn,
    b.ruleisn,
    b.ObjIsn,
    b.objclassisn,
    b.rptclass,
    b.rptgroupisn,
    b.motivgroupisn,
    b.carrptclass,
    sum(b.NAmountrub) as NAmountRUB,
    sum(b.NAmountusd) as NAmountUSD,
    sum(b.qamountrub) as QAmountRUB,
    sum(b.qamountusd) as QAmountUSD,
    b.dateval,
    b.datequit,
    b.addisn,
    max(b.IS_NACH) as IS_NACH,       -- признак наличия начислений
    max(b.IS_QUIT) as IS_QUIT,       -- признак наличия квитовок
    b.Exclude_Medic,
    min(b.first_datepay) as first_datepay,
    max(b.last_datepay) as last_datepay,
    max(b.datepaylast) as datepaylast

  from tt_repbuh2obj b
  group by
    b.DB,
    b.DE,
    b.agrisn,
    b.refundisn,
    b.ReAgrisn,
    b.statcode,
    b.sagroup,
    b.LineCode,
    b.deptisn,
    b.subaccisn,
    b.BuhSubjIsn,
    b.ruleisn,
    b.ObjIsn,
    b.objclassisn,
    b.rptclass,
    b.rptgroupisn,
    b.motivgroupisn,
    b.carrptclass,
    b.dateval,
    b.datequit,
    b.addisn,
    b.Exclude_Medic
 ) B,Storages.rep_agr_salers_Line ags, repagrroleagr agr
 
 Where B.agrisn=Ags.agrisn(+)
 and Nvl(B.datequit,B.Dateval) between ags.datebeg(+)  and Ags.Dateend(+)
 and b.agrisn=agr.agrisn(+)
 /* убрано для скорости
   REPAGR RA,
   AGREEMENT AD
where
  B.AGRISN = RA.AGRISN
  and B.ADDISN = AD.ISN(+)
*/;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_REPBUH2OBJ_NACH_DIRECT" ("DB", "DE", "AGRISN", "REFUNDISN", "REAGRISN", "STATCODE", "SAGROUP", "LINECODE", "DEPTISN", "SUBACCISN", "BUHSUBJISN", "RULEISN", "OBJISN", "OBJCLASSISN", "RPTCLASS", "RPTGROUPISN", "MOTIVGROUPISN", "CARRPTCLASS", "NAMOUNTRUB", "NAMOUNTUSD", "QAMOUNTRUB", "QAMOUNTUSD", "DATEVAL", "DATEQUIT", "ADDISN", "IS_NACH", "IS_QUIT", "EXCLUDE_MEDIC", "FIRST_DATEPAY", "LAST_DATEPAY", "DATEPAYLAST") AS 
  with Prm as (
-- sts 25.07.2012 - загрузка данных для RepBuh2Obj
-- part - данные по прямым начислениям
  select
    max(pParam.GetParamD('pDB')) as DB,
    max(pParam.GetParamD('pDE')) as DE
  from dual
)

Select
   A.DB,
   A.DE - 1 as DE,
   a.agrisn,
   a.refundisn,
   0 ReAgrisn,

   statcode,
   sagroup,
   LineCode,

   deptisn,
   subaccisn,
   buhsubjisn,

   ruleisn,
   objisn,
   objclassisn,
   rptclass,
   rptgroupisn,

   motivgroupisn,
   Carrptclass,

   amountrub as NAmountrub,
   amountusd as NAmountusd,
   0 as qamountrub,
   0 as qamountusd,

   a.dateval,
   a.datequit,
   a.addisn,

   'Y' as IS_NACH,       -- признак наличия начислений
   cast(null as char(1)) as IS_QUIT,  -- признак наличия квитовок
   A.Exclude_Medic,  -- признак исключения строк из мотивационного отчета
   a.first_datepay,
   a.last_datepay,
   a.datepaylast

from (
  select --+ ordered use_nl(Prm a) use_hash(ST CarRules DKBRules LC EXCL_BS) parallel(a 28)
    Prm.DB,
    Prm.DE as DE,
    a.agrisn,
    a.refundisn,
    a.statcode,
    LC.LINECODE, -- расшифровка 48 статкода по начислениям нужна для отчета "Бюджет по договорам"
    a.deptisn,
    a.subaccisn,
    Nvl(a.bodysubjisn, a.subjisn) as BuhSubjIsn,
    a.ruleisn,
    Case When CarRules.Isn is not null or DKBRules.Isn is not null Then Nvl(ParentObjIsn,a.objisn) end ObjIsn,
    Case When CarRules.Isn is not null or DKBRules.Isn is not null Then Nvl(a.parentobjclassisn, a.objclassisn) end objclassisn,
    a.rptclass,
    a.rptgroupisn,
    Sum(a.amountrub) as amountrub,
    Sum(a.amountusd) as amountusd,
    a.sagroup,
    a.motivgroupisn,
    CarRules.Isn as CarRulesIsn,
    a.Carrptclass,
    trunc(a.dateval) as dateval,
    trunc(a.datequit) as datequit,
    to_number(Case When CarRules.Isn is not null then a.addisn else a.agrisn end) as addisn,
    case
      when a.deptisn = 23735116 and EXCL_BS.SubAccISN is not null then 'Y'
      else 'N'
    end as Exclude_Medic, -- признак исключения строк из мотивационного отчета
    min(a.DatePay) as first_datepay,
    max(a.DatePay) as last_datepay,
    max(a.datepaylast) as datepaylast
  from
     Prm,
     RepBuh2Cond a,
    (select statcode from rep_statcode Where grp <> 'Исходящее перестрахование') ST,
    --(select r.* from  motor.v_dicti_rule r Where isn <> 753518300) CarRules,
    motor.v_dicti_rule CarRules,
    (-- Продукты ДКБ
     select
       ISN,PARENTISN,CODE,SHORTNAME,FULLNAME,CONSTNAME,ACTIVE
     from dicti
     start with parentisn = 24890816 and Filterisn = 2553627303
     connect by prior isn=parentisn
    ) DKBRules,
    TT_BUH_LINECODE LC,
   (select Isn as SubAccISN, 220 as statcode from buhsubacc where Id Like '60%' -- исключаем Расчеты с поставщиками и подрядчиками
    UNION ALL
    select SubAccISN, 60 as statcode from subacc4dept sb where sb.statcode = 220  -- по мед убыткам исключаем корреспонденцию с авансами
   ) EXCL_BS -- счета и статкоды для определения признака исключения проводок из мотивационного отчета

  where
    a.DateVal >= Prm.DB and a.DateVal < Prm.DE
    and a.Sagroup in (1, 3, 5)
    and a.statcode = ST.statcode
    --and a.statcode in (select statcode from rep_statcode Where grp <> 'Исходящее перестрахование')
    and a.ruleisnagr = CarRules.Isn(+)
    and a.ruleisnagr = DKBRules.Isn(+)
    and a.BodyISN = LC.BodyISN(+)
    and a.StatCode = LC.StatCode(+)
    and a.corsubaccisn = EXCL_BS.SubAccISN(+)
    and a.Statcode = EXCL_BS.Statcode(+)
  group by
    Prm.DB,
    Prm.DE,
    a.agrisn,
    a.refundisn,
    a.statcode,
    LC.LineCode,
    a.deptisn,
    a.subaccisn,
    Nvl(a.bodysubjisn,a.subjisn),
    a.ruleisn,
    Case When CarRules.Isn is not null or DKBRules.Isn is not null Then Nvl(a.ParentObjIsn, a.objisn) end,
    Case When CarRules.Isn is not null or DKBRules.Isn is not null Then Nvl(a.Parentobjclassisn, a.objclassisn) end,
    a.rptclass,
    a.rptgroupisn,
    a.sagroup,
    a.motivgroupisn,
    A.RULEISN,
    CarRules.Isn,
    a.Carrptclass,
    trunc(a.dateval),
    trunc(a.datequit),
    to_number(Case When CarRules.Isn is not null then a.addisn else a.agrisn end),
    case
      when a.deptisn = 23735116 and EXCL_BS.SubAccISN is not null then 'Y'
      else 'N'
    end

) A;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_REPBUH2OBJ_NACH_RE" ("DB", "DE", "AGRISN", "REFUNDISN", "REAGRISN", "STATCODE", "SAGROUP", "LINECODE", "DEPTISN", "SUBACCISN", "BUHSUBJISN", "RULEISN", "OBJISN", "OBJCLASSISN", "RPTCLASS", "RPTGROUPISN", "MOTIVGROUPISN", "CARRPTCLASS", "NAMOUNTRUB", "NAMOUNTUSD", "QAMOUNTRUB", "QAMOUNTUSD", "DATEVAL", "DATEQUIT", "ADDISN", "IS_NACH", "IS_QUIT", "EXCLUDE_MEDIC", "FIRST_DATEPAY", "LAST_DATEPAY", "DATEPAYLAST") AS 
  with Prm as (
-- sts 25.07.2012 - загрузка данных для RepBuh2Obj
-- part - данные по исходящим начислениям
  select
    max(pParam.GetParamD('pDB')) as DB,
    max(pParam.GetParamD('pDE')) as DE
  from dual  
)

select  --+ Parallel(a 28)
  Prm.DB,
  Prm.DE - 1 as DE,
  a.agrisn,
  0 as refundisn,
  ReIsn as ReAgrisn,

  a.statcode,
  a.sagroup,
  a.statcode as LineCode,
  a.deptisn,
  a.subaccisn,
  0 as buhsubjisn,

  0 as ruleisn,
  0 as objisn,
  0 as objclassisn,
  a.rptclass,
  a.rptgroupisn,

  a.motivgroupisn,
  null as carrptclass,
  a.amountrub NAmountrub,
  a.amountusd  NAmountusd,
  0 as qamountrub,
  0 as qamountusd,
  
  trunc(a.dateval) as dateval,
  trunc(a.dateval) as datequit,
  a.agrisn as addisn,
  
  'Y' as IS_NACH,       -- признак наличия начислений
  cast(null as char(1)) as IS_QUIT,  -- признак наличия квитовок
  'N' as Exclude_Medic, -- признак исключения строк из мотивационного отчета (для данного случая всегда N)
  
  to_date(null) as first_datepay,
  to_date(null) as last_datepay,
  to_date(null) as datepaylast

from
  Prm,
  repbuhre2directanalytics a,
 (select statcode from rep_statcode Where grp = 'Исходящее перестрахование') ST
where
  a.DateVal >= Prm.DB and a.DateVal < Prm.DE
  and a.Sagroup in (1, 3, 5)
  and a.statcode  = ST.statcode
  --and a.statcode in (select statcode from rep_statcode Where grp = 'Исходящее перестрахование');

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_REPBUH2OBJ_QUITPC_KASSA" ("BODYISN", "DATEVAL", "QUITPC", "LINECODE", "STATCODE", "DATEQUIT", "EXCLUDE_MEDIC") AS 
  (-- sts 25.07.2012 - загрузка данных для RepBuh2Obj
-- part - проводки, сквитованные или начисленные в периоде (для кассовых операций), с коэффициентом квитовки
-- Версия из мотивации:
-- в отчете по мотивации для статкодов 220, 60, 221 учитывается курсовая разница, а тут - нет
-- Из за этого расходятся суммы для: BodyISN = 15484201903, StatCode = 221, DB = '01-jan-2011'
select
  b.bodyisn,
  b.Dateval,
  decode(
    b.max_BuhAmountRub,
    0, 0,
    b.row_BuhAmountRub / b.max_BuhAmountRub
  ) as QuitPC,
  b.LineCode,
  b.Statcode,
  b.DateVal as DateQuit,  --< для кассового метода
  b.Exclude_Medic -- признак исключения строк из мотивационного отчета
from (
  select --+ ordered use_nl(t b) use_hash(EXCL_BS) Full(t) Parallel(t,32)
    b.bodyisn,
    b.Dateval,
    b.BuhQuitDate as DateQuit,  -- агрегировать нельзя, иначе некорректно расчитывается QuitPC в разрезе DateQuit
    case
      when b.deptisn = 23735116 and EXCL_BS.SubAccISN is not null then 'Y'
      else 'N'
    end as Exclude_Medic,
    sum((b.BuhAmountRub * b.BuhPC * b.QuitPC * b.BuhQuitPC) + b.RepCursDiff) as row_BuhAmountRub,
    max(b.BuhAmountRub) as max_BuhAmountRub,
    max(b.StatCode) as LineCode,
    max(b.Statcode) as Statcode
  from
    tt_rowid t,
    STORAGE_SOURCE.RepBuhQuit b,
   (select Isn as SubAccISN, 220 as statcode from buhsubacc where Id Like '60%' -- исключаем Расчеты с поставщиками и подрядчиками
    UNION ALL
    select SubAccISN, 60 as statcode from subacc4dept sb where sb.statcode = 220  -- по мед убыткам исключаем корреспонденцию с авансами
   ) EXCL_BS -- счета и статкоды для определения признака исключения проводок из мотивационного отчета
  where
    t.ISN = b.bodyisn
    and b.corsubaccisn = EXCL_BS.SubAccISN(+)
    and b.Statcode = EXCL_BS.Statcode(+)
    /* данное условие нужно только в мотивационном отчете. В витрине же должны быть все данные.
    Поэтому вынесено отдельным признаком выше

    and (
      not (b.deptisn = 23735116 and b.Statcode = 220) or
        b.corsubaccisn not in (select Isn from buhsubacc where Id Like '60%') -- исключаем Расчеты с поставщиками и подрядчиками
    )
    -- по мед убыткам исключаем корреспонденцию с авансами
    and (
      not (deptisn = 23735116 and Statcode = 60) or
        b.corsubaccisn not in (select subaccisn from subacc4dept sb where sb.statcode = 220)
    )
    */
  group by
    b.bodyisn,
    b.Dateval,
    b.BuhQuitDate,
    case
      when b.deptisn = 23735116 and EXCL_BS.SubAccISN is not null then 'Y'
      else 'N'
    end
) b
);

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_REPBUH2OBJ_QUITPC_NORMAL" ("BODYISN", "DATEVAL", "QUITPC", "LINECODE", "STATCODE", "DATEQUIT", "EXCLUDE_MEDIC") AS 
  with Prm as (
-- sts 25.07.2012 - загрузка данных для RepBuh2Obj
-- part - проводки, сквитованные или начисленные в периоде (для кассовых операций), с коэффициентом квитовки
  select
    max(pParam.GetParamD('pDB')) as DB,
    max(pParam.GetParamD('pDE')) as DE
  from dual  
)

select
  b.bodyisn,
  b.Dateval,
  decode(
    b.max_BuhAmountRub,
    0, 0,
    b.row_BuhAmountRub / b.max_BuhAmountRub
  ) as QuitPC,
  b.LineCode, 
  b.Statcode,
  b.DateQuit,
  'N' as Exclude_Medic -- признак исключения строк из мотивационного отчета (для данного случая всегда N)
from (
  select --+ ordered use_nl(Prm B) use_hash(LC)
    b.bodyisn,
    --max(b.Dateval) as DateVal,
    b.Dateval,
    b.BuhQuitDate as DateQuit,  -- агрегировать нельзя, иначе некорректно расчитывается QuitPC в разрезе DateQuit
    sum((b.BuhAmountRub * b.BuhPC * b.QuitPC * b.BuhQuitPC) + b.RepCursDiff) as row_BuhAmountRub,
    max(b.BuhAmountRub) as max_BuhAmountRub,
    max(LC.LineCode) as LineCode,
    max(b.Statcode) as Statcode
  from --+ Parallel(b 28)
    Prm,
    STORAGE_SOURCE.RepBuhQuit b,
    TT_BUH_LINECODE LC
  where              
    b.BuhQuitDate >= Prm.DB and b.BuhQuitDate < Prm.DE
    and b.sagroup in (1, 3, 5)
    and b.statcode not in (220, 60, 221)
    and b.BodyISN = LC.BodyISN(+)
    and b.StatCode = LC.StatCode(+)
  group by b.bodyisn, b.Dateval, b.BuhQuitDate
) b;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_REPBUH2OBJ_QUIT_DIRECT" ("DB", "DE", "AGRISN", "REFUNDISN", "REAGRISN", "STATCODE", "SAGROUP", "LINECODE", "DEPTISN", "SUBACCISN", "BUHSUBJISN", "RULEISN", "OBJISN", "OBJCLASSISN", "RPTCLASS", "RPTGROUPISN", "MOTIVGROUPISN", "CARRPTCLASS", "NAMOUNTRUB", "NAMOUNTUSD", "QAMOUNTRUB", "QAMOUNTUSD", "DATEVAL", "DATEQUIT", "ADDISN", "IS_NACH", "IS_QUIT", "EXCLUDE_MEDIC", "FIRST_DATEPAY", "LAST_DATEPAY", "DATEPAYLAST") AS 
  with Prm as (
-- sts 25.07.2012 - загрузка данных для RepBuh2Obj
-- part - проводки, сквитованные или начисленные в периоде (для кассовых операций), с коэффициентом квитовки
-- sts 17.08.2012 - версия, разбитая на два подзапроса - первый - по отобранным записям по партиции в том же периоде
-- и второй - по оставшимся записям в остальных партициях.
-- Сделано для скорости, части отличаются способом доступа к таблице
  select
    max(pParam.GetParamD('pDB')) as DB,
    max(pParam.GetParamD('pDE')) as DE
  from dual
),
ST as (select statcode from rep_statcode Where grp <> 'Исходящее перестрахование')

select --+ ordered use_hash(rb CarRules DKBRules)
-- sts 25.07.2012 - загрузка данных для RepBuh2Obj
-- part - по прямым договорам квитовки на аналитику договоров
  rb.DB,
  rb.DE - 1 as DE,
  rb.agrisn,
  rb.refundisn,
  0 as ReAgrisn,
  rb.statcode,
  rb.sagroup,
  rb.LineCode,
  rb.deptisn,
  rb.subaccisn,
  rb.BuhSubjIsn,
  rb.ruleisn,
  to_number(Case When CarRules.Isn is not null or DKBRules.Isn is not null Then rb.ParentObjIsn end ) as ObjIsn,
  to_number(Case When CarRules.Isn is not null or DKBRules.Isn is not null Then rb.parentobjclassisn end ) as objclassisn ,
  rb.rptclass,
  rb.rptgroupisn,
  rb.motivgroupisn,
  rb.Carrptclass,
  0 as NAmountrub,
  0 as NAmountusd,
  Sum(rb.qamountrub) as qamountrub,
  0 as qamountusd,
  rb.dateval,
  rb.datequit,
  to_number(Case When CarRules.Isn is not null then rb.addisn else rb.agrisn end) as addisn,
  cast(null as char(1)) as IS_NACH,       -- признак наличия начислений
  'Y' as IS_QUIT,  -- признак наличия квитовок
  rb.Exclude_Medic, -- признак исключения строк из мотивационного отчета
  min(rb.DatePay) as first_datepay,
  max(rb.DatePay) as last_datepay,
  max(rb.datepaylast) as datepaylast

from
   (
    select --+ ordered  use_hash(ST bq) no_parallel(bq) 
      Rb.DB,
      Rb.DE,
      rb.agrisn,
      rb.refundisn,
      bq.statcode,
      rb.sagroup,
      bq.LineCode,
      rb.deptisn,
      rb.subaccisn,
      rb.BuhSubjIsn,
      rb.ruleisn,
      rb.ruleisnagr,
      rb.ParentObjIsn,
      rb.parentobjclassisn,
      rb.rptclass,
      rb.rptgroupisn,
      rb.motivgroupisn,
      rb.Carrptclass,
      rb.amountrub * bq.quitpc as qamountrub,
      rb.dateval,
      trunc(bq.datequit) as datequit,
      rb.AddISN,
      nvl(Bq.Exclude_Medic, 'N') as Exclude_Medic, -- признак исключения строк из мотивационного отчета
      rb.DatePay,
      rb.datepaylast
    from
      (-- доступ фулсканом к партиции
      Select /*+ Ordered Use_Nl(rb) Full(rb) parallel(rb 28) */
      Prm.DB,
      Prm.DE,
      rb.bodyIsn,
      rb.statcode,
      rb.agrisn,
      rb.refundisn,
      rb.sagroup,
      rb.deptisn,
      rb.subaccisn,
      nvl(rb.bodysubjisn, rb.subjisn) as BuhSubjIsn,
      rb.ruleisn,
      rb.ruleisnagr,
      nvl(rb.ParentObjIsn, rb.objisn) as ParentObjIsn,
      nvl(rb.parentobjclassisn, rb.objclassisn) as parentobjclassisn,
      rb.rptclass,
      rb.rptgroupisn,
      rb.motivgroupisn,
      rb.Carrptclass,
      Sum(rb.amountrub) amountrub,
      trunc(rb.dateval) as dateval,
      rb.AddISN,
      rb.DatePay,
      rb.datepaylast
    from      Prm,
      repbuh2cond rb
    Where
      rb.DateVal >= Prm.DB and rb.DateVal < Prm.DE
group by
      Prm.DB,
      Prm.DE,
      rb.bodyIsn,
      rb.statcode,
      rb.agrisn,
      rb.refundisn,
      rb.sagroup,
      rb.deptisn,
      rb.subaccisn,
      nvl(rb.bodysubjisn, rb.subjisn) ,
      rb.ruleisn,
      rb.ruleisnagr,
      nvl(rb.ParentObjIsn, rb.objisn) ,
      nvl(rb.parentobjclassisn, rb.objclassisn) ,
      rb.rptclass,
      rb.rptgroupisn,
      rb.motivgroupisn,
      rb.Carrptclass,
      trunc(rb.dateval),
      rb.AddISN,
      rb.DatePay,
      rb.datepaylast

   ) rb,
   tt_quitpc Bq,
      ST
      
    where
          rb.statcode = ST.statcode
      and Bq.BodyIsn = rb.BodyIsn
  UNION ALL
    -- доступ через индекс
    select --+ ordered use_nl(Prm bq rb) use_hash(ST)
      Prm.DB,
      Prm.DE,
      rb.agrisn,
      rb.refundisn,
      bq.statcode,
      rb.sagroup,
      bq.LineCode,
      rb.deptisn,
      rb.subaccisn,
      nvl(rb.bodysubjisn, rb.subjisn) as BuhSubjIsn,
      rb.ruleisn,
      rb.ruleisnagr,
      nvl(rb.ParentObjIsn, rb.objisn) as ParentObjIsn,
      nvl(rb.parentobjclassisn, rb.objclassisn) as parentobjclassisn,
      rb.rptclass,
      rb.rptgroupisn,
      rb.motivgroupisn,
      rb.Carrptclass,
      rb.amountrub * bq.quitpc as qamountrub,
      trunc(rb.dateval) as dateval,
      trunc(bq.datequit) as datequit,
      rb.AddISN,
      nvl(Bq.Exclude_Medic, 'N') as Exclude_Medic, -- признак исключения строк из мотивационного отчета
      rb.DatePay,
      rb.datepaylast
    from
      Prm,
      tt_quitpc Bq,
      repbuh2cond rb,
      ST
    where
      not (bq.DateVal >= Prm.DB and bq.DateVal < Prm.DE)
      and Bq.BodyIsn = rb.BodyIsn
      and rb.statcode = ST.statcode
   ) rb,
  --( select r.* from  motor.v_dicti_rule r Where isn <> 753518300) CarRules,
  motor.v_dicti_rule CarRules,
  (-- Продукты ДКБ
   select
     ISN,PARENTISN,CODE,SHORTNAME,FULLNAME,CONSTNAME,ACTIVE
   from dicti
   start with parentisn = 24890816 and Filterisn = 2553627303
   connect by prior isn=parentisn
  ) DKBRules
where
  rb.ruleisnagr = CarRules.Isn(+)
  and rb.ruleisnagr = DKBRules.Isn(+)
group by
  rb.DB,
  rb.DE - 1,
  rb.agrisn,
  rb.refundisn,
  rb.statcode,
  rb.sagroup,
  rb.LineCode,
  rb.deptisn,
  rb.subaccisn,
  rb.BuhSubjIsn,
  rb.ruleisn,
  to_number(Case When CarRules.Isn is not null or DKBRules.Isn is not null Then rb.ParentObjIsn end ),
  to_number(Case When CarRules.Isn is not null or DKBRules.Isn is not null Then rb.parentobjclassisn end ),
  rb.rptclass,
  rb.rptgroupisn,
  rb.motivgroupisn,
  rb.Carrptclass,
  rb.dateval,
  rb.datequit,
  to_number(Case When CarRules.Isn is not null then rb.addisn else rb.agrisn end),
  rb.Exclude_Medic;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_REPBUH2RESECTION" ("AGRISN", "BODYISN", "STATCODE", "DATEVAL", "RPTGROUPISN", "MOTIVGROUPISN", "BUDGETGROUPISN", "RPTCLASS", "RPTCLASSISN", "REFUNDISN", "REFUNDEXTISN", "BUHCURRISN", "SAGROUP", "BUHAMOUNTRUB", "BUHAMOUNTUSD", "BUHAMOUNT", "AMOUNTRUB", "AMOUNTUSD", "AMOUNT", "REISN", "REAGRCLASSISN", "SECTISN", "SHARENEORIG", "REINSURANCEPERIODPC", "SECTPC", "BUHISN") AS 
  SELECT a.agrisn, a.bodyisn, max(a.statcode) AS statcode, max(a.dateval) AS dateval, a.rptgroupisn, a.motivgroupisn,
       a.budgetgroupisn, a.rptclass, a.rptclassisn, a.refundisn, a.refundextisn,
       max(a.buhcurrisn) AS buhcurrisn, max(a.sagroup) AS sagroup, max(a.buhamountrub) AS buhamountrub,
       max(a.buhamountusd) AS buhamountusd, max(a.buhamount) AS buhamount,
       sum(a.amountrub*a.sectpc) AS amountrub,
       sum(a.amountusd*a.sectpc)AS amountusd,
       sum(a.amount*a.sectpc) AS amount,
       max(a.reisn) AS reisn,
       max(a.reagrclassisn) AS reagrclassisn,
       a.sectisn,
       a.shareneorig,
       max(a.reinsuranceperiodpc) AS reinsuranceperiodpc,
       max(a.sectpc) AS sectpc,
       MAX(a.buhisn) AS buhisn
FROM (
      SELECT --+ ordered use_nl ( a re ra s ) index ( re X_REPAGR_AGR ) index ( ra  X_REPAGR_AGR )
             a.agrisn, a.rptgroupisn, a.amountrub, a.amountusd, a.amount, a.statcode,
             a.refundisn, a.refundextisn, a.bodyisn, a.dateval, a.motivgroupisn,
             a.reinsuranceperiodpc, a.shareneorig, a.reagrclassisn, a.sectisn,
             a.reisn, /*1/COUNT(sectisn) over (PARTITION BY a.buhisn) EGAO 26.04.2013 */1 AS sectpc, a.buhamountrub, a.buhamountusd, a.buhamount,
             a.buhcurrisn, a.sagroup, a.budgetgroupisn, a.rptclass, a.rptclassisn, a.buhisn
      FROM (
            WITH
                 x AS (
                       SELECT x.*
                       FROM (
                             SELECT --+ index ( x X_REP_AGRRE_AGR ) ordered use_nl ( x re ra )
                                    DISTINCT x.agrisn, x.condisn, x.reisn, x.sectisn,
                                             CASE
                                               WHEN re.datebase='I' THEN 1
                                               ELSE (least(trunc(x.sdateend), trunc(ra.dateend))-greatest(trunc(x.sdatebeg), trunc(ra.datebeg))+1)/(trunc(ra.dateend)-trunc(ra.datebeg)+1)
                                             END AS reinsuranceperiodpc,
                                             CASE re.classisn
                                               WHEN 9058 THEN nvl(x.shareneorig/100, 1)
                                               ELSE 1
                                             END AS shareneorig,
                                             re.classisn AS reagrclassisn

                             FROM rep_agrre x, repagr re, repagr ra
                             WHERE x.agrisn>pparam.GetParamN('minagrisn') AND x.agrisn<=pparam.GetParamN('maxagrisn')
                               AND re.agrisn=x.reisn
                               AND ra.agrisn=x.agrisn
                            ) x
                       WHERE x.reinsuranceperiodpc>0
                      ),
                 a AS (
                       SELECT --+ index ( bc X_REPBUH2COND_AGRISN ) ordered use_nl ( x bc ) no_merge ( x )
                              bc.agrisn, bc.condisn, bc.rptgroupisn, bc.refundisn, bc.statcode,
                              bc.refundextisn, bc.bodyisn, bc.motivgroupisn, bc.budgetgroupisn, bc.rptclass, bc.rptclassisn,
                              MAX(bc.isn) AS buhisn,
                              MAX(bc.dateval) AS dateval,
                              MAX(bc.buhamountrub) AS buhamountrub,
                              MAX(bc.buhamountusd) AS buhamountusd,
                              MAX(bc.buhamount) AS buhamount,
                              MAX(bc.buhcurrisn) AS buhcurrisn,
                              MAX(bc.sagroup) AS sagroup,
                              SUM(bc.amountusd) AS amountusd,
                              SUM(bc.amount) AS amount,
                              SUM(bc.amountrub) AS amountrub
                       FROM (SELECT DISTINCT agrisn FROM x) x, repbuh2cond bc
                       WHERE bc.agrisn=x.agrisn
                         AND bc.statcode IN (38, 34, 220, 24)
                         AND bc.sagroup IN (1, 3)
                         AND NVL(bc.amountrub,0)<>0
                       GROUP BY bc.agrisn, bc.condisn, bc.rptgroupisn, bc.refundisn, bc.statcode,
                                bc.refundextisn, bc.bodyisn, bc.motivgroupisn, bc.budgetgroupisn, bc.rptclass, bc.rptclassisn
                       HAVING SUM(bc.amountrub)<>0
                      )

            SELECT --+ use_hash ( a x )
                   a.*, x.sectisn, x.reisn, x.shareneorig, x.reagrclassisn, x.reinsuranceperiodpc
            FROM a, x
            WHERE x.agrisn=a.agrisn
              AND x.condisn IS NULL
            UNION
            SELECT --+ use_hash ( a x )
                   a.*, x.sectisn, x.reisn, x.shareneorig, x.reagrclassisn, x.reinsuranceperiodpc
            FROM a, x
            WHERE x.agrisn=a.agrisn
              AND x.condisn IS NOT NULL
              AND x.condisn=a.condisn

           ) a) a
GROUP BY a.agrisn,
         a.bodyisn,
         a.rptgroupisn,
         a.motivgroupisn,
         a.budgetgroupisn, a.rptclass, a.rptclassisn, a.refundisn, a.refundextisn,
         a.sectisn,
         a.shareneorig
;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_REPBUHRE2DIRECTANALYTICS" ("BODYISN", "DATEVAL", "STATCODE", "SAGROUP", "BUHCURRISN", "BUHAMOUNT", "BUHAMOUNTUSD", "BUHAMOUNTRUB", "AGRISN", "DOCSUMISN", "RPTGROUPISN", "DIRECTPC", "AMOUNT", "AMOUNTUSD", "AMOUNTRUB", "MOTIVGROUPISN", "DEPTISN", "REISN", "RPTCLASSISN", "RPTCLASS", "BUDGETGROUPISN", "SUBACCISN", "SECTISN") AS 
  SELECT a.bodyisn, -- исх. проводка
       a.dateval, -- дата исх. проводки
       a.statcode, -- тип исх. проводки
       a.sagroup,  -- тип суммы исх. проводки
       a.buhcurrisn, -- валюта исх. проводки
       a.buhamount, -- сумма исх. прводки (неаддитивная)
       a.buhamountusd,
       a.buhamountrub,
       a.agrisn,
       a.docsumisn,
       NVL(a.rptgroupisn,0) AS rptgroupisn, -- УГ (аналитика прямой проводки)
       directpc, -- доля суммы исх. проводки, приходящаяся на текущию строку
       a.amount*a.directpc AS amount, -- сумма исх. проводки (аддитивная)
       a.amountusd*a.directpc AS amountusd,
       a.amountrub*a.directpc AS amountrub,
       NVL(a.motivgroupisn,0) AS motivgroupisn, -- мотивационная группа (аналитика прямой проводки)
       a.deptisn,
       a.reisn, -- договор исх. проводки
       a.rptclassisn, a.rptclass, a.budgetgroupisn, a.subaccisn, a.sectisn
FROM (
      SELECT a.bodyisn, a.docsumisn, a.sectisn, a.dateval, a.statcode, a.sagroup, a.buhcurrisn,
             a.amount, a.amountrub, a.amountusd,
             a.buhamount, a.buhamountrub, a.buhamountusd,
             a.rptgroupisn, a.agrisn,
             a.motivgroupisn, a.deptisn, a.reisn,
             a.rptclassisn, a.rptclass, a.budgetgroupisn, a.subaccisn,
             CASE WHEN nvl(directamountruball,0)=0 THEN CASE WHEN a.statcode=35 THEN 0 ELSE 1/allcnt END/*EGAO 20.06.2013 1/allcnt*/ ELSE directamountrub/directamountruball END AS directpc
      from (
            SELECT a.bodyisn, a.docsumisn, a.dateval, a.statcode, a.sagroup, a.buhcurrisn,
                   a.amount, a.amountrub, a.amountusd,
                   a.buhamount, a.buhamountrub, a.buhamountusd,
                   a.agrisn, a.rptgroupisn,
                   a.sectisn,
                   a.deptisn,
                   a.motivgroupisn, a.directamountrub, a.reisn,
                   a.rptclassisn, a.rptclass, a.budgetgroupisn, a.subaccisn,
                   SUM(a.directamountrub) over (PARTITION BY a.bodyisn, a.sectisn, a.docsumisn) AS directamountruball,
                   COUNT(1) over (PARTITION BY a.bodyisn, a.sectisn, a.docsumisn) AS allcnt -- EGAO 27.07.2011
            FROM (
                  SELECT --+ no_merge ( a ) use_nl ( a c ) index ( c x_directsum2resection_reisn )
                         a.docsumisn, a.bodyisn,
                         a.sectisn,
                         max(a.dateval) AS dateval, max(a.statcode) AS statcode, max(a.sagroup) AS sagroup,
                         MAX(a.buhcurrisn) AS buhcurrisn,
                         max(a.amount) AS amount, MAX(a.amountrub) AS amountrub, MAX(a.amountusd) AS amountusd,
                         max(a.agrisn) AS reisn, c.rptgroupisn, max(a.deptisn) AS deptisn,
                         c.motivgroupisn,
                         SUM(CASE
                               WHEN c.statcode IN (220, 24) THEN
                                 (SELECT c.amountrub*tre.repc
                                  FROM storages.tt_pay_re tre
                                  WHERE tre.refundisn=c.refundisn
                                    AND nvl(tre.refundextisn,0)=nvl(c.refundextisn,0)
                                    and tre.bodyisn=c.bodyisn
                                    AND CASE c.reagrclassisn
                                          WHEN 9018 THEN 1
                                          WHEN 9058 THEN
                                            CASE
                                              WHEN sign(a.reaccperiod-90)=-1 AND trunc(tre.dateloss) BETWEEN a.reaccdatebeg AND a.reaccdateend THEN 1
                                              WHEN sign(a.reaccperiod-90)>=0 AND trunc(c.dateval) BETWEEN a.reaccdatebeg AND a.reaccdateend THEN 1
                                            END
                                          ELSE 0
                                        END = 1
                                 )
                               WHEN c.statcode IN (38, 34) THEN c.amountrub*c.reinsuranceperiodpc*c.shareneorig
                             END) AS directamountrub,
                        nvl(c.agrisn, a.agrisn) AS agrisn,
                        C.rptclassisn,
                        C.rptclass,
                        c.budgetgroupisn,
                        max(a.subaccisn) AS subaccisn,
                        MAX(a.buhamount) AS buhamount, max(a.buhamountrub) AS buhamountrub, max(a.buhamountusd) AS buhamountusd
                  FROM (
                        SELECT --+ ordered use_nl ( a b ) index ( a X_TT_BUHRE2DIRANALYTICS_BODY )
                               a.bodyisn, a.dateval, a.statcode,
                               a.sagroup, a.deptisn, a.subaccisn, a.buhcurrisn,
                               a.buhamount, a.buhamountrub, a.buhamountusd,
                               a.amount*NVL(b.sectpc,1)*NVL(b.docsumpc,1) AS amount,
                               a.amountrub*NVL(b.sectpc,1)*NVL(b.docsumpc,1) AS amountrub,
                               a.amountusd*NVL(b.sectpc,1)*NVL(b.docsumpc,1) AS amountusd,
                               b.docsumisn, b.sectisn, b.agrisn,
                               b.reaccdateend-b.reaccdatebeg+1 AS reaccperiod,
                               b.reaccdatebeg,
                               b.reaccdateend
                        FROM tt_buhre2directanalytics a,
                             repbuhre2resection_new b -- EGAO 18.12.2013 repbuhre2resection b
                        WHERE a.bodyisn>pparam.GetParamN('MinIsn') AND a.bodyisn<=pparam.GetParamN('MaxIsn')
                          AND b.bodyisn(+)=a.bodyisn
                       ) a, repbuh2resection c
                  WHERE a.agrisn=c.reisn(+)
                    AND CASE WHEN a.sectisn IS NULL THEN 1 WHEN a.sectisn=c.sectisn(+) THEN 1 END =1
                    AND CASE
                          WHEN a.statcode IN (27, 33, 351, 924) THEN
                            CASE c.statcode(+)
                              WHEN 38 THEN 1
                              WHEN 34 THEN 1
                              ELSE 0
                            END
                          WHEN a.statcode IN (35) THEN
                            CASE c.statcode(+)
                              WHEN 220 THEN 1
                              WHEN 24 THEN 1
                              ELSE 0
                            END
                        END = 1
                  GROUP BY a.docsumisn, a.bodyisn,
                           a.sectisn,
                           c.rptgroupisn,
                           c.motivgroupisn,
                           NVL(c.agrisn, a.agrisn),
                           c.rptclassisn,
                           c.rptclass,
                           c.budgetgroupisn

                 ) a
           ) a
     ) a
WHERE NVL(a.amount*a.directpc,0)<>0
;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_REPBUHRE2RESECTION" ("BODYISN", "DATEVAL", "STATCODE", "SUBACCISN", "BUHCURRISN", "BUHDEPTISN", "BUHAMOUNT", "AMOUNT", "ISREVALUATION", "DOCSUMISN", "DOCISN", "DSSUBJISN", "DSCURRISN", "DSCLASSISN", "DSCLASSISN2", "DOCSUMPC", "SECTISN", "SECTTYPE", "SECTDATEBEG", "SECTDATEEND", "SECTCURRISN", "SECTPC", "REACCISN", "REACCDATEBEG", "REACCDATEEND", "AGRISN", "AGRCLASSISN", "DATEBASE", "AGRDATEBEG", "AGRDATEEND") AS 
  WITH cl AS (SELECT d.isn, connect_by_root(isn) AS rootisn
            FROM dicti d
            START WITH d.isn IN (414, 427, 445)
            CONNECT BY PRIOR d.isn=d.parentisn
           ),
     re AS (
           SELECT --+ no_merge ( s )
                  s.*,
                  cl.rootisn AS classisn, ROWNUM rn
           FROM (SELECT --+ index ( s X_REACCSUM_ACC )
                        s.agrisn, s.amount, s.sectisn, s.reaccisn, s.subjisn, s.currisn,
                        ais.recalc_utils.reclass2sumclass(s.classisn, s.classisn2) AS classisn2
                 FROM reaccsum s
                 WHERE s.reaccisn>pparam.GetParamN('MinIsn') AND s.reaccisn<=pparam.GetParamN('MaxIsn')
                   AND s.subjisn IS NOT NULL
                ) s, cl
           WHERE cl.isn=s.classisn2
          ),
    x AS  (
            SELECT --+ index ( ds X_DOCSUM_REACC )
                   ds.*, ROWNUM rn
            FROM docsum ds
            WHERE ds.reaccisn>pparam.GetParamN('MinIsn') AND ds.reaccisn<=pparam.GetParamN('MaxIsn')
              AND ds.classisn IN (414, 427, 445)
              AND ds.discr='P'
              AND ds.classisn2<>2265208403
          ),
     a AS (SELECT reaccisn, subjisn, currisn, classisn, classisn2
           FROM (SELECT DISTINCT 1 AS ssource,ds.reaccisn, ds.subjisn, ds.currisn, ds.classisn, ds.classisn2
                 FROM x ds
                 UNION ALL
                 SELECT DISTINCT 2, re.reaccisn, re.subjisn, re.currisn, re.classisn, re.classisn2
                 FROM re
                )
           GROUP BY reaccisn, subjisn, currisn, classisn, classisn2
           HAVING SUM(DECODE(ssource,1,1,0))<>SUM(DECODE(ssource,2,1,0))
          ),
    ds AS (
            SELECT --+ use_hash ( ds a )
                   DISTINCT ds.*, a.reaccisn AS diffreaccisn
            FROM a,
                 x ds
            WHERE ds.classisn=a.classisn(+) AND ds.subjisn=a.subjisn(+) AND ds.currisn=a.currisn(+) AND ds.reaccisn=a.reaccisn(+)
          )

SELECT --+ ordered use_nl ( re s acc agr ) no_merge ( re )

       -- проводка
       re.bodyisn,
       re.dateval,
       re.statcode,
       re.subaccisn,
       re.buhcurrisn,
       re.buhdeptisn,
       re.buhamount,
       re.amount,
       re.isrevaluation,

       -- доксумма
       re.docsumisn,
       re.docisn,
       re.dssubjisn,
       re.dscurrisn,
       re.dsclassisn,
       re.dsclassisn2,
       re.docsumpc,

       -- секция
       re.sectisn,
       s.secttype,
       trunc(s.datebeg) AS sectdatebeg,
       trunc(s.dateend) AS sectdateend,
       s.currisn AS sectcurrisn,
       re.sectpc,

       -- 100% счет
       re.reaccisn,
       trunc(acc.datebeg) AS reaccdatebeg,
       trunc(acc.dateend) AS reaccdateend,

       -- исходящий договор перестрахования
       re.agrisn,
       agr.classisn AS agrclassisn,
       agr.datebase,
       trunc(agr.datebeg) AS agrdatebeg,
       trunc(agr.dateend) AS agrdateend
FROM (
      SELECT --+ no_merge ( ds ) no_merge ( re )
             re.sectisn,
             ds.reaccisn,
             ds.subjisn AS dssubjisn,
             ds.currisn AS dscurrisn,
             ds.classisn AS dsclassisn,
             ds.classisn2 AS dsclassisn2,
             re.agrisn,
             ds.bodyisn,
             MAX(ds.subaccisn) AS subaccisn,
             MAX(ds.dateval) AS dateval,
             ds.docsumisn,
             MAX(ds.docsumpc) AS docsumpc,
             SUM(ds.amount) AS buhamount,
             MAX(re.sectpc) AS sectpc,
             SUM(ds.amount*re.sectpc*ds.docsumpc) AS amount,
             MAX(ds.buhcurrisn) AS buhcurrisn,
             MAX(ds.docisn) AS docisn,
             MAX(ds.statcode) AS statcode,
             MAX(ds.buhdeptisn)AS buhdeptisn,
             MAX(ds.isrevaluation) AS isrevaluation
      FROM  (
             SELECT reaccisn, subjisn, currisn, classisn, classisn2,
                    amount,
                    CASE WHEN NVL(docsumAll,0)=0 THEN 1/docsumCnt ELSE AmountDoc/docsumAll END AS docsumpc,
                    bodyisn,
                    subaccisn,
                    dateval,
                    docsumisn,
                    buhcurrisn,
                    docisn,
                    statcode,
                    buhdeptisn,
                    isrevaluation
             FROM (
                   SELECT --+ no_merge ( ds ) index ( bb X_REPBUHBODY_BODYISN ) ordered use_nl ( ds bb r )
                          ds.isn AS docsumisn,
                          ds.reaccisn,
                          ds.subjisn,
                          ds.currisn,
                          ds.classisn,
                          ds.classisn2,
                          NVL(gcc2.gcc2(ds.amount,ds.currisn,bb.currisn,bb.dateval),0) AS AmountDoc,
                          bb.bodyisn,
                          bb.amount,
                          bb.subaccisn,
                          trunc(bb.dateval) AS dateval,
                          NVL(SUM(gcc2.gcc2(ds.amount,ds.currisn,bb.currisn,bb.dateval)) over (PARTITION BY bb.isn),0) AS docsumAll,
                          COUNT(1) OVER (PARTITION BY bb.isn) AS docsumCnt,
                          bb.currisn AS buhcurrisn,
                          nvl(ds.docisn,ds.docisn2) docisn,
                          bb.statcode,
                          bb.deptisnbuh AS buhdeptisn,
                          --{ EGAO 05.06.2012
                          /*
                          CASE
                            WHEN NVL(bb.agrisn,0)=0 THEN
                              CASE
                                WHEN NVL(bb.currisn,-1)=35 THEN 0
                                ELSE 1
                              END
                            ELSE
                              CASE
                                WHEN r.agrisn IS NULL THEN 0
                                ELSE 1
                              END
                          END
                          */
                          CASE WHEN NVL(bb.currisn,-1)=35 THEN 0 ELSE 1 END AS isrevaluation
                          --}
                   FROM ds, repbuhbody bb --EGAO 05.06.2012 , rep_isreval r
                   WHERE ds.diffreaccisn IS NULL
                     AND bb.bodyisn IN (ds.creditisn, ds.debetisn)
                     AND NVL(bb.amount,0)<>0
                     --EGAO 05.06.2012 AND r.agrisn(+)=bb.agrisn
                  )
             WHERE CASE WHEN docsumAll=0 THEN 1/docsumCnt ELSE AmountDoc/docsumAll END<>0
            ) ds,
            (
             SELECT re.subjisn, re.currisn, re.reaccisn, re.sectisn, re.classisn, re.agrisn, classisn2,
                    CASE WHEN re.allpremsum<>0 THEN re.sectpremsum/re.allpremsum ELSE 1/re.allcnt END AS sectpc
             FROM (SELECT --+ ordered use_nl ( s ag )
                          s.subjisn, s.currisn, s.reaccisn, s.sectisn, s.classisn, s.agrisn, classisn2,
                          NVL(SUM(s.amount),0)  AS sectpremsum,
                          NVL(SUM(SUM(s.amount)) OVER (PARTITION BY s.subjisn, s.currisn, s.reaccisn, s.classisn, s.agrisn, classisn2),0) AS allpremsum,
                          COUNT(1) over (PARTITION BY s.subjisn, s.currisn, s.reaccisn, s.classisn, s.agrisn, classisn2) AS allcnt
                   FROM re s
                   GROUP BY s.subjisn, s.currisn, s.reaccisn, s.sectisn, s.classisn, s.agrisn, classisn2
                  ) re
             WHERE CASE WHEN re.allpremsum<>0 THEN re.sectpremsum/re.allpremsum ELSE 1/re.allcnt END <> 0
            ) re
      WHERE ds.reaccisn=re.reaccisn
        AND ds.subjisn=re.subjisn
        AND ds.currisn=re.currisn
        AND ds.classisn=re.classisn
        AND ds.classisn2=re.classisn2
      GROUP BY re.sectisn, ds.reaccisn, ds.subjisn, ds.currisn, ds.classisn, ds.classisn2, re.agrisn, ds.docsumisn, ds.bodyisn

      UNION ALL

      SELECT --+ no_merge ( ds ) no_merge ( re ) no_merge ( a )
             re.sectisn,
             ds.reaccisn,
             ds.subjisn AS dssubjisn,
             ds.currisn AS dscurrisn,
             ds.classisn AS dsclassisn,
             NULL AS dsclassisn2,
             re.agrisn,
             ds.bodyisn,
             MAX(ds.subaccisn) AS subaccisn,
             MAX(ds.dateval) AS dateval,
             ds.docsumisn,
             MAX(ds.docsumpc) AS docsumpc,
             SUM(ds.amount) AS buhamount,
             MAX(re.sectpc) AS sectpc,
             SUM(ds.amount*re.sectpc*ds.docsumpc) AS amount,
             MAX(ds.buhcurrisn) AS buhcurrisn,
             MAX(ds.docisn) AS docisn,
             MAX(ds.statcode) AS statcode,
             MAX(ds.buhdeptisn)AS buhdeptisn,
             MAX(ds.isrevaluation) AS isrevaluation
      FROM (
            SELECT reaccisn, subjisn, currisn, classisn,
                   CASE WHEN NVL(docsumAll,0)=0 THEN 1/docsumCnt ELSE AmountDoc/docsumAll END AS docsumpc,
                   amount,
                   bodyisn,
                   subaccisn,
                   dateval,
                   docsumisn,
                   buhcurrisn,
                   docisn,
                   statcode,
                   buhdeptisn,
                   isrevaluation
             FROM (
                   SELECT --+ no_merge ( ds ) index ( bb X_REPBUHBODY_BODYISN ) ordered use_nl ( ds bb r ) index ( r X_REP_ISREVAL )
                          ds.isn AS docsumisn,
                          ds.reaccisn,
                          ds.subjisn,
                          ds.currisn,
                          ds.classisn,
                          NVL(gcc2.gcc2(ds.amount,ds.currisn,bb.currisn,bb.dateval),0) AmountDoc,
                          bb.bodyisn,
                          bb.amount,
                          bb.subaccisn,
                          trunc(bb.dateval) AS dateval,
                          NVL(SUM(gcc2.gcc2(ds.amount,ds.currisn,bb.currisn,bb.dateval)) over (PARTITION BY bb.isn),0) AS docsumAll,
                          COUNT(1) OVER (PARTITION BY bb.isn) AS docsumCnt,
                          bb.currisn AS buhcurrisn,
                          nvl(ds.docisn,ds.docisn2) docisn,
                          bb.statcode,
                          bb.deptisnbuh AS buhdeptisn,
                          --{EGAO 05.06.2012
                          /*CASE
                            WHEN NVL(bb.agrisn,0)=0 THEN
                              CASE
                                WHEN NVL(bb.currisn,-1)=35 THEN 0
                                ELSE 1
                              END
                            ELSE
                              CASE
                                WHEN r.agrisn IS NULL THEN 0
                                ELSE 1
                              END
                          END*/
                          CASE WHEN NVL(bb.currisn,-1)=35 THEN 0 ELSE 1 END isrevaluation
                          --}
                   FROM ds, repbuhbody bb-- EGAO 05.06.2012 , rep_isreval r
                   WHERE ds.diffreaccisn IS NOT NULL
                     AND bb.bodyisn IN (ds.creditisn, ds.debetisn)
                     AND NVL(bb.amount,0)<>0
                     --AND r.agrisn(+)=bb.bodyisn
                  )
             WHERE CASE WHEN docsumAll=0 THEN 1/docsumCnt ELSE AmountDoc/docsumAll END<>0
            ) ds,
            (SELECT re.subjisn, re.currisn, re.reaccisn, re.sectisn, re.classisn, re.agrisn,
                    CASE WHEN re.allpremsum<>0 THEN re.sectpremsum/re.allpremsum ELSE 1/allcnt END AS sectpc
             FROM (
                   SELECT --+ ordered use_nl ( s ag )
                          s.subjisn, s.currisn, s.reaccisn, s.sectisn, s.classisn, s.agrisn,
                          NVL(SUM(s.amount),0) AS sectpremsum,
                          NVL(SUM(SUM(s.amount)) OVER (PARTITION BY s.subjisn, s.currisn, s.reaccisn, s.classisn, s.agrisn),0) AS allpremsum,
                          COUNT(1) over (PARTITION BY s.subjisn, s.currisn, s.reaccisn, s.classisn, s.agrisn) AS allcnt
                   FROM re s
                   GROUP BY s.subjisn, s.currisn, s.reaccisn, s.sectisn, s.classisn, s.agrisn
                  ) re
             WHERE CASE WHEN re.allpremsum<>0 THEN re.sectpremsum/re.allpremsum ELSE 1/allcnt END <> 0
            ) re
      WHERE ds.reaccisn=re.reaccisn
        AND ds.subjisn=re.subjisn
        AND ds.currisn=re.currisn
        AND ds.classisn=re.classisn
      GROUP BY re.sectisn, ds.reaccisn, ds.subjisn, ds.currisn, ds.classisn, re.agrisn, ds.docsumisn, ds.bodyisn
     ) re,
     repagr agr,
     resection s,
     reacc100 acc
WHERE agr.agrisn=re.agrisn
  AND s.isn=re.sectisn
  AND acc.isn=re.reaccisn
;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_RESUBJPERIOD" ("SECTISN", "CONDISN", "PARENTISN", "ISN", "SUBJISN", "SHAREPC", "DATEENTRY", "DATESWITCH", "DATEBEG", "DATEEND") AS 
  SELECT --+ use_hash ( a s )
      a.sectisn,
      a.condisn,
      a.parentisn,
      a.isn,
      a.subjisn,
      nvl(a.sharepc,0) AS sharepc,
      nvl(trunc(a.dateentry),trunc(s.datebeg)) AS dateentry,
      nvl(trunc(a.dateswitch),to_date('01.01.3000','dd.mm.yyyy')) AS dateswitch,
      nvl(trunc(a.datebeg),TRUNC(s.datebeg)) AS datebeg,
      nvl(trunc(a.dateend),TRUNC(s.dateend)) AS dateend
FROM resubjperiod a, resection s
WHERE s.isn=a.sectisn
;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_RNPREPCLASS" ("SAGROUP", "REPCLASSISN") AS 
  SELECT 1 AS sagroup, 1 AS repclassisn FROM dual
UNION ALL
SELECT 2 AS sagroup, 2 AS repclassisn FROM dual
UNION ALL
SELECT 3 AS sagroup, 1 AS repclassisn FROM dual
;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_RNP_RE_MSFO_VIRTUALCOND" ("AGRISN", "CONDISN", "DATEBEG", "DATEEND", "VIRTUALDATEBEG", "VIRTUALDATEEND", "SHAREPC") AS 
  WITH sect AS (
              SELECT --+ use_hash ( pr x ) no_merge ( pr ) index ( x  X_REP_AGRRE_AGRXISN )
                      x.condisn,
                      x.agrisn,
                      trunc(x.sdatebeg) AS sdatebeg,
                      trunc(x.sdateend) AS sdateend,
                      pr.priority AS sectpriority,
                      MAX(pr.priority) over (PARTITION BY x.agrisn, x.condisn) AS maxpriority
              FROM rep_agrre x, v_resection_priority_msfo pr
              WHERE x.agrisn>pparam.GetParamN('MinAgrIsn') AND x.agrisn<=pparam.GetParamN('MaxAgrIsn')
                AND x.datebase='C'
                AND pr.secttype=x.secttype
                AND pr.agrclassisn=x.reclassisn
             )
SELECT a.agrisn,
       a.condisn,
       a.datebeg,
       a.dateend,
       a.dt AS virtualdatebeg,
       a.dte AS virtualdateend,
       CASE WHEN (a.dateend-a.datebeg+1)=0 THEN 0
         ELSE (a.dte-a.dt+1)/(a.dateend-a.datebeg+1)
       END AS sharepc
FROM (
      SELECT a.*, lead(dt) over (PARTITION BY agrisn, condisn order by dt)-1 AS dte
            FROM (
                  SELECT --+ ordered use_hash ( sect cd )
                         DISTINCT cd.*,
                         decode(n.n, 1, cd.datebeg,
                                     2, cd.dateend+1,
                                     3, sect.sdatebeg,
                                     4, sect.sdateend+1
                               ) dt
                  FROM (SELECT --+ index ( rc X_REPCOND_AGR )
                               rc.condisn,
                               max(rc.agrisn) AS agrisn,
                               trunc(max(rc.datebeg)) AS datebeg,
                               trunc(max(rc.dateend)) AS dateend
                        FROM repcond rc
                        WHERE rc.agrisn>pparam.GetParamN('MinAgrIsn') AND rc.agrisn<=pparam.GetParamN('MaxAgrIsn')
                        GROUP BY rc.condisn
                       ) cd,
                       sect,
                       (SELECT ROWNUM n FROM dual CONNECT BY ROWNUM<=4) n
                  WHERE EXISTS (SELECT 'x' FROM sect x
                                WHERE x.sectpriority<x.maxpriority
                                  AND x.agrisn=cd.agrisn
                                  AND (x.condisn IS NULL OR x.condisn=cd.condisn)
                                  AND (x.sdatebeg>cd.datebeg AND x.sdatebeg<cd.dateend OR x.sdateend>cd.datebeg AND x.sdateend<cd.dateend)
                               )
                    AND sect.agrisn=cd.agrisn
                    AND (sect.condisn IS NULL OR sect.condisn=cd.condisn)
                    AND (sect.sdatebeg>cd.datebeg AND sect.sdatebeg<cd.dateend OR sect.sdateend>cd.datebeg AND sect.sdateend<cd.dateend)
                 ) a
     ) a
WHERE a.dt+ (a.dte-a.dt)/2 between a.datebeg and a.dateend
;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_RNP_RE_SUBJECT_BY_AGRROLE" ("SSOURCE", "LOADISN", "DATEREP", "AGRISN", "REINSISN", "REINSISNNAME", "REINSISNPC", "SHORTNAME", "FULLNAME", "LATNAME", "COUNTRY", "INGO", "RECODE") AS 
  WITH ag AS (
             SELECT --+ materialize
             a.*
             FROM (
                   SELECT --+ full ( a ) parallel (a 32)
                           a.agrisn, a.loadisn, daterep
                   FROM rnp_re_rsbu a
                   WHERE a.loadisn=pparam.getParamN('LoadIsn')
                     AND a.reclassisn=-1
                   UNION
                   SELECT --+ full ( a ) parallel (a 32)
                           a.agrisn, a.loadisn, daterep
                   FROM rnp_re_msfo_final a
                   WHERE a.loadisn=pparam.getParamN('LoadIsn')
                     AND a.reclassisn=-1
                  ) a

           ),
    rl AS (
            SELECT --+ index ( rl X_AGRROLE_CLASS )
                   rl.agrisn,
                   rl.subjisn,
                   rl.sharepc  AS sharepc
            FROM agrrole rl
            WHERE 1=1
              AND rl.classisn=435 -- перестраховщик
              AND rl.sumclassisn=414 -- премия
              AND rl.sumclassisn2 IN (414, 8133016)
              AND rl.orderno>0
              AND NVL(rl.sharepc,0)<>0
              AND rl.calcflg='Y'
          ),
     a AS (SELECT --+ use_hash ( ag a )
                   ag.agrisn,
                   max(ag.loadisn) AS loadisn,
                   max(ag.daterep) AS daterep,
                   rl.subjisn,
                   NVL(SUM(rl.sharepc),0)  AS sharepc,
                   NVL(SUM(SUM(rl.sharepc)) over (PARTITION BY ag.agrisn),0) AS sharepcall
            FROM ag, rl
            WHERE rl.agrisn(+)=ag.agrisn
            GROUP BY ag.agrisn, rl.subjisn
          )
SELECT --+ ordered use_nl ( a s ing c ) n_merge ( x )
       1 AS ssource,
       a.loadisn,
       a.daterep,
       a.agrisn,
       a.subjisn AS reinsisn,
       decode(s.code, NULL, '',s.code||' - ')||initcap(s.shortname) AS reinsisnname,
       CASE WHEN sharepcall=0 THEN 0 ELSE sharepc/sharepcall END AS reinsisnpc,
       s.shortname,
       s.fullname,
       s.namelat AS latname,
       c.shortname AS country,
       nvl2(ing.isn,1,0) AS ingo,
       x.reform||x.retype||x.remethod AS recode
FROM a, ais.subject_t s, Rep_IngoGrp ing, country c,
     (SELECT x.* FROM v_rnp_re_subject_recode x WHERE x.agrclassisn=-1 AND x.secttype='QS') x
 WHERE a.sharepcall<>0
   AND s.isn(+)=a.subjisn
   AND ing.isn(+)=a.subjisn
   AND c.isn(+)=s.countryisn
UNION ALL
SELECT --+ use_hash ( a )
       2 AS ssource,
       a.loadisn,
       a.daterep,
       a.agrisn,
       0 AS reisnisn,
       to_char(NULL) AS reisnisnname,
       1 AS reisnisnpc,
       to_char(NULL) AS shortname,
       to_char(NULL) AS fullname,
       to_char(NULL) AS latname,
       to_char(NULL) AS country,
       0 AS ingo,
       to_char(NULL) AS recode
FROM a
WHERE a.sharepcall=0
;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_RNP_RE_SUBJECT_BY_SECTION" ("SSOURCE", "LOADISN", "DATEREP", "SECTISN", "SECTTYPE", "SECTDATEBEG", "SECTDATEEND", "REISN", "REDATEBASE", "RECLASSISN", "REINSISN", "REINSISNPC", "REINSISNNAME", "LATNAME", "SHORTNAME", "FULLNAME", "COUNTRY", "INGO", "REINSDATEENTRY", "REINSDATESWITCH", "REINSDATEBEG", "REINSDATEEND", "RECODE") AS 
  WITH x AS (
           SELECT --+ materialize
                  a.*
           FROM (
           SELECT --+ full ( a ) parallel (a 32)
                   a.sectisn,
                  trunc(a.sectdatebeg) AS sectdatebeg, TRUNC(a.sectdateend) AS sectdateend,
                  a.secttype,
                  a.reisn, a.reclassisn, a.redatebase,
                  a.loadisn, a.daterep
           FROM rnp_re_msfo_final a
           WHERE a.loadisn=pparam.getParamN('LoadIsn')
           AND a.reclassisn<>-1
           UNION
           SELECT --+ full ( a ) parallel (a 32)
                  a.sectisn,
                  trunc(a.sectdatebeg) AS sectdatebeg, TRUNC(a.sectdateend) AS sectdateend,
                  a.secttype,
                  a.reisn, a.reclassisn, a.redatebase,
                  a.loadisn, a.daterep
           FROM rnp_re_rsbu a
           WHERE a.loadisn=pparam.getParamN('LoadIsn')
             AND a.reclassisn<>-1) a
          )
          --SELECT * FROM s;
          ,
   sbj AS (
           SELECT --+ materialize
                  a.sectisn, a.condisn, a.subjisn, a.sharepc, condsharepc, a.parentisn, a.parentsharepc,
                  CASE WHEN nvl(a.condsharepc,0)=0 THEN 0 ELSE a.sharepc*a.parentsharepc/a.condsharepc END AS subjsharepc,
                  a.dateentry,
                  a.dateswitch,
                  a.datebeg,
                  a.dateend
           FROM (

                 SELECT a.sectisn,
                        a.condisn,
                        a.subjisn,
                        connect_by_root(nvl(a.sharepc,0)) AS parentsharepc,
                        connect_by_root(a.isn) AS parentisn,
                        nvl(a.sharepc,0) AS sharepc,
                        trunc(a.dateentry) AS dateentry,
                        trunc(a.dateswitch) AS dateswitch,
                        trunc(a.datebeg) AS datebeg,
                        trunc(a.dateend) AS dateend,
                        SUM(a.sharepc) over (PARTITION BY a.condisn, connect_by_root(a.isn)) AS condsharepc
                 FROM resubjperiod a
                 WHERE CONNECT_BY_ISLEAF=1 AND nvl(a.sharepc,0)<>0
                 CONNECT BY PRIOR a.isn=a.parentisn
                 START WITH a.parentisn IS NULL
                ) a
          )
          --SELECT * FROM sbj;
          ,
    cd AS (
           SELECT --+ use_hash ( x cd ) materialize
                  cd.isn AS layerisn,
                  cd.sectisn,
                  cd.name AS layername,
                  nvl(cd.rate,0) AS rate,
                  nvl(cd.depospremsum,0) AS depospremsum,
                  COUNT(CASE WHEN nvl(cd.rate,0)<>0 THEN 1 END) over (PARTITION BY cd.sectisn) AS FullRateLayerCnt,
                  COUNT(CASE WHEN nvl(cd.depospremsum,0)<>0 THEN 1 END) over (PARTITION BY cd.sectisn) AS FullDepospremLayerCnt,
                  COUNT(1) over (PARTITION BY cd.sectisn) AS condcnt
           FROM recond cd
           WHERE cd.isn NOT IN (119901171303, 111824735603) -- EGAO 17.01.2012 Лейеры заведены некорректно, исправлению в АИС не подлежат. Исключаем их обработку в коде
          )
          --SELECT * FROM cd;
          ,
     a AS (
           SELECT --+ materialize
                  a.*,
                  SUM(a.reshare) over (PARTITION BY a.sectisn) AS sectsharepc
           FROM (
                 SELECT x.loadisn, x.daterep, x.reisn, x.reclassisn, x.sectisn, x.secttype, x.sectdatebeg, x.sectdateend, x.redatebase,
                        cd.layerisn, cd.layername, cd.rate,
                        cd.depospremsum, sbj.subjisn, sbj.parentisn, sbj.parentsharepc,
                        sbj.sharepc,
                        sbj.subjsharepc,
                        NVL(CASE
                              WHEN x.secttype IN ('XL', 'RX', 'SL') THEN
                                CASE
                                  WHEN cd.condcnt=1 THEN sbj.subjsharepc
                                  WHEN cd.FullRateLayerCnt>0 THEN sbj.subjsharepc*cd.rate
                                  WHEN cd.FullDepospremLayerCnt>0 THEN sbj.subjsharepc*cd.depospremsum
                                  ELSE 0
                                END
                              WHEN x.secttype IN ('QS', 'SP') THEN subjsharepc
                            END,0) AS reshare, sbj.dateentry, sbj.dateswitch, sbj.datebeg, sbj.dateend
                 FROM x, cd, sbj
                 WHERE cd.sectisn(+)=x.sectisn
                   AND sbj.sectisn(+)=cd.sectisn
                   AND sbj.condisn(+)=cd.layerisn
                ) a
          )
          --SELECT COUNT(1) FROM a; -- 39 409
SELECT --+ ordered use_nl ( a s c ) no_merge ( a )
       1 AS ssource,
       a.loadisn,
       a.daterep,
       --Секция
       a.sectisn,
       a.secttype,
       a.sectdatebeg,
       a.sectdateend,
       a.reisn,
       a.redatebase,
       a.reclassisn,
       --Участник
       a.reinsisn,
       a.reinsisnpc,
       decode(s.code, NULL, '',s.code||' - ')||InitCap(s.shortname) AS reinsisnname,
       s.namelat AS latname,
       s.shortname,
       s.fullname,
       c.shortname AS country,
       nvl2(ing.isn,1,0) AS ingo,
       a.reinsdateentry,
       a.reinsdateswitch,
       a.reinsdatebeg,
       a.reinsdateend,
       --
       x.reform||x.retype||x.remethod AS recode



FROM (
      SELECT a.sectisn,
             a.subjisn AS reinsisn,
             MAX(a.reisn) AS reisn,
             MAX(a.reclassisn) AS reclassisn,
             MAX(a.redatebase) AS redatebase,
             MAX(a.secttype) AS secttype,
             MAX(a.sectdatebeg) AS sectdatebeg,
             MAX(a.sectdateend) AS sectdateend,
             SUM(CASE WHEN NVL(a.sectsharepc,0)=0 THEN 0 ELSE a.reshare/a.sectsharepc END) AS reinsisnpc,
             max(a.dateentry) AS reinsdateentry,
             max(a.dateswitch) AS reinsdateswitch,
             max(a.datebeg) AS reinsdatebeg,
             max(a.dateend) AS reinsdateend,
             MAX(a.loadisn) AS loadisn,
             MAX(a.daterep) AS daterep
      FROM  a
      GROUP BY a.sectisn, a.subjisn
      HAVING SUM(CASE WHEN NVL(a.sectsharepc,0)=0 THEN 0 ELSE a.reshare/a.sectsharepc END)<>0
     ) a,
     ais.subject_t s,
     country c,
     Rep_IngoGrp ing,
     v_rnp_re_subject_recode x
WHERE s.isn(+)=a.reinsisn
  AND c.isn(+)=s.countryisn
  AND ing.isn(+)=s.isn
  AND x.agrclassisn(+)=a.reclassisn
  AND x.secttype(+)=a.secttype

UNION ALL
SELECT 2 AS ssource,
       --
       max(a.loadisn) AS loadisn,
       max(a.daterep) AS daterep,
       --секция
       a.sectisn,
       max(a.secttype) AS secttype,
       MAX(a.sectdatebeg) AS sectdatebeg,
       MAX(a.sectdateend) AS sectdateend,
       MAX(a.reisn) AS reisn,
       MAX(a.redatebase) AS redatebase,
       MAX(a.reclassisn) AS reclassisn,
       --участник
       0 AS reinsisn,
       1 AS reinsisnpc,

       to_char(NULL) AS reinsisnname,
       to_char(NULL) AS latname,
       to_char(NULL) AS shortname,
       to_char(NULL) AS fullname,
       to_char(NULL) AS country,
       0 AS ingo,
       to_date(NULL) AS reinsdateentry,
       to_date(NULL) AS reinsdateswitch,
       to_date(NULL) AS reinsdatebeg,
       to_date(NULL) AS reinsdateend,
       --
       to_char(NULL) AS recode
FROM a
GROUP BY a.sectisn
HAVING SUM(CASE WHEN NVL(a.sectsharepc,0)=0 THEN 0 ELSE a.reshare/a.sectsharepc END)=0
;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_RPTGROUP2AGRRULE" ("RPTGROUPISN", "RPTGROUPCODE", "RPTGROUPNAME", "AGRRULEISN", "AGRRULENAME") AS 
  SELECT /*+ ordered use_nl ( a d1 d2 ) */a.rptgroupisn, d2.code AS rptgroupcode, d2.shortname AS rptgroupname, a.agrruleisn, d1.shortname AS agrrulename
FROM (
      SELECT rootisn AS agrruleisn, MAX(groupisn) AS rptgroupisn
      FROM (
            SELECT r.lv, MIN(r.lv) over (PARTITION BY r.rootisn) AS minlv, t.ruleisn,t.groupisn, r.rootisn
            FROM (SELECT LEVEL lv, isn, connect_by_root(isn) AS rootisn
                  FROM (SELECT a.isn, a.parentisn
                        FROM dicti a
                        START WITH a.parentisn =24890816
                        CONNECT BY PRIOR a.isn=a.parentisn
                       )
                  CONNECT BY PRIOR parentisn = isn
                 ) r, (SELECT classisn1 groupisn,classisn2 ruleisn FROM dicx WHERE classisn=2031719303) t
            WHERE r.isn = t.ruleisn
           )
      WHERE lv=minlv
      GROUP BY rootisn
     )a, dicti d1, dicti d2
WHERE d1.isn(+)=a.agrruleisn
  AND d2.isn(+)=a.rptgroupisn
;

  CREATE OR REPLACE FORCE VIEW "STORAGES"."V_RPTGROUP2RULE" ("RPTGROUPISN", "RPTGROUPCODE", "RPTGROUPNAME", "RULEISN", "RULENAME") AS 
  SELECT /*+ ordered use_nl ( a d1 d2 ) */a.rptgroupisn, d2.code AS rptgroupcode, d2.shortname AS rptgroupname, a.ruleisn, d1.shortname AS rulename
FROM (
      SELECT rootisn AS ruleisn, MAX(groupisn) AS rptgroupisn
      FROM (
            SELECT r.lv, MIN(r.lv) over (PARTITION BY r.rootisn) AS minlv, t.ruleisn,t.groupisn, r.rootisn
            FROM (SELECT LEVEL lv, isn, connect_by_root(isn) AS rootisn
                  FROM ais.rule
                  CONNECT BY PRIOR parentisn = isn
                 ) r, (SELECT classisn1 groupisn,classisn2 ruleisn FROM dicx WHERE classisn=2031712503) t
            WHERE r.isn = t.ruleisn
           ) a
      WHERE a.lv=a.minlv
      GROUP BY rootisn
     )a, dicti d1, dicti d2
WHERE d1.isn(+)=a.ruleisn
  AND d2.isn(+)=a.rptgroupisn
;