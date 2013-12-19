 CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_AGENT_RANKS" ("AGRISN", "ADDISN", "IS_MOVE_OBJ_ADDISN", "ADDID", "ORDERNO", "AGR_ID", "AGR_DATEBEG", "AGR_DATEEND", "AGR_DATESIGN", "AGR_RULEISN", "ADD_DATEBEG", "ADD_DATEEND", "ROLE_DATEBEG", "ROLE_DATEEND", "AGENTISN", "AGENTCLASSISN", "ADDRULEISN", "IS_MOVE_OBJ", "SHAREPC_AGENT_BY_ADD", "AGENT_SUMCLASSISN", "AGENT_SUMCLASSISN2", "AGENT_CALCFLG", "AGENT_BASE", "AGENT_BASELOSS", "AGENT_PLANFACT", "AGENT_DEPTISN", "RNK_MOVE_OBJ", "SHAREPC_BY_ADD", "CNT_AGENT_BY_AGR", "CNT_AGENT_BY_ADD", "IS_MOVE_OBJ_ID", "IS_MOVE_OBJ_DATEBEG", "IS_MOVE_OBJ_DATEEND", "SHAREPC_BY_IS_MOVE_OBJ", "CNT_AGENT_BY_IS_MOVE_OBJ", "IS_ADD_CANCEL", "IS_ADD_CANCEL_ADDISN", "SUBJCLASSISN") AS 
  with AGR as (
-- данные по агентам, привязанным (по дате начала) к аддендумам (Тарара)
  select --+ ordered use_nl(RA A)
    RA.AGRISN,
    -- атрибуты договора
    RA.AGR_ID,
    RA.AGR_DATEBEG,
    RA.AGR_DATEEND,
    RA.AGR_DATESIGN,
    RA.AGR_RULEISN,
    -- атрибуты аддендума (при отсутствии такового - поля договора)
    /* sts 01.11.2012 - транкать плохо, ибо есть договоры, у которых аддендумы различаются на минуту: AgrISN = 176551249603
    nvl(trunc(A.DATEBEG), RA.AGR_DATEBEG) as DATEBEG,
    trunc(lead(A.DATEBEG, 1, RA.AGR_DATEEND) over(partition by RA.AGRISN order by A.DATEBEG nulls first)) as DATEEND,
    */
    nvl(A.DATEBEG, RA.AGR_DATEBEG) as DATEBEG,
    lead(A.DATEBEG, 1, RA.AGR_DATEEND) over(partition by RA.AGRISN order by A.DATEBEG nulls first) as DATEEND,

    decode(A.RULEISN, 37564716, 1, 0) as IS_MOVE_OBJ, -- ПЕРЕНОС ТС
    decode(A.RULEISN, 34710416, 1, 0) as IS_ADD_CANCEL, -- ПРЕКРАЩЕНИЕ ДОГОВОРА
    -- ссылка на договор/аддендум, к которому относится агент
    nvl(A.ISN, RA.AGRISN) as ADDISN,
    nvl(A.ID, RA.AGR_ID) as ADDID,
    nvl(A.RULEISN, RA.AGR_RULEISN) as RULEISN

  from
   (select --+ ordered use_nl(T RA) use_hash(CarRules)
      T.ISN as AGRISN,
      -- атрибуты договора
      RA.ID as AGR_ID,
      trunc(RA.DATEBEG) as AGR_DATEBEG,
      trunc(RA.DATEEND) as AGR_DATEEND,
      trunc(RA.DATESIGN) as AGR_DATESIGN,
      RA.RULEISN as AGR_RULEISN,
      nvl2(CarRules.ISN, RA.AGRISN, null) as PARENTISN  -- будем искать аддендумы только для моторного страхования
    from
      TT_ROWID T,
      STORAGE_SOURCE.REPAGR RA,
      MOTOR.V_DICTI_RULE CarRules  -- детализация по аддендумам только для моторного страхования (аналогично REPBUH2OBJ)
    where
      T.ISN = RA.AGRISN
      and RA.STATUS in ('В', 'Д', 'Щ')
      and RA.RULEISN = CarRules.ISN(+)

      --and ra.agrisn = 170415.1158

  ) RA,
    AIS.AGREEMENT A
  where
    (
      (RA.AGRISN = A.ISN and A.DISCR = 'Д')  -- договор
      or
      (RA.PARENTISN = A.PARENTISN and A.DISCR = 'А')  -- аддендумы
    )
),
ADD_MOVE as (
-- определяем первый предыдущий аддендум/договор, имеющий ненулевую сумму
-- нужно для распределения суммы возвратов премии в отчете "Сборы агентов" task(ДИТ-12-2-166348, 32609379103)
  select
    A.AGRISN,
    A.ADDISN,
    A.ADDID,
    A.DATEBEG,
    lead(A.DATEBEG, 1, A.AGR_DATEEND) over(partition by A.AGRISN order by A.DATEBEG) as DATEEND
  from AGR A
  where
    A.IS_MOVE_OBJ = 1
),
ADD_CANCEL as (
  select
    AD.ROOT_ISN,
    max(AD.ADDISN) keep(dense_rank last order by lv) as ADDISN
  from (
    select
      level lv,
      CONNECT_BY_ROOT AD.ISN as ROOT_ISN,
      nvl(AD.PREVISN, AD.ISN) ADDISN
    from AGREEMENT AD
    start with AD.ISN in (select distinct AGR.ADDISN from AGR where AGR.IS_ADD_CANCEL = 1)
    connect by AD.ISN = prior AD.PREVISN and AD.PREMIUMSUM = 0
  ) AD
  group by AD.ROOT_ISN
)


/* примеры
insert into tt_rowid(ISN) values(90913884603);  -- перенос ТС без даты агента
insert into tt_rowid(ISN) values(124407495003);  -- перенос ТС с датой агента
insert into tt_rowid(ISN) values(161772051303);  -- один агент без даты, два адд, один - перенос ТС
insert into tt_rowid(ISN) values(125000002403);  -- перенос ТС с датой агента, два адд-ма с суммами
insert into tt_rowid(ISN) values(21604144103);   -- два агента без дат, два ад-ма на перенос ТС с суммами
-----------
AgrISN:
1. 183965.1158 - договор с одним агентом (Дата окончания) (есть адендум, но без агента)
   170415.1158 - договор с одним агентом (два адендума)
   85490139803 - договор с одним агентом (есть адендум Перенос ТС). Агент начал действовать начиная с адендума

2. 16410501003 - 4 агента, нет адендумов

3. 417636.2692 - два агента (на разные адендумы)
   9542896025  - 4 агента, 2 адендума

4. 62609209507 - два агента с непересекающимися датами, один перенос ТС
   62609209507 - два агента с непересекающимися датами, один перенос ТС (два адендума)
   59276237103 - два агента (+ 1 агент-бонусная комиссия), один Перенос ТС  <<-хороший пример

Один агент несколько раз встречается в договоре:
4603910000 - ипотека
5909640000 - ОСАГО - один агент два раза (второй раз - на ад-м Изменение условий). SharePC = 10
101506.05  - ОСАГО - один агент два раза (два а-ма Изменение периода). SharePC = 10
120823.1498 - ОСАГО, 3 агента, из них два одинаковых. Один адендум Изменение периода
8281029025 - ОСАГО, 3 агента, из них два одинаковых. без адендумов

*/


select --+ ordered use_nl(A S)
  A.AGRISN,
  A.ADDISN,
  -- аддендум "Перенос ТС", к которому относится текущий аддендум (Для первоначального состояния = AgrISN)
  -- Нужно для ТЗ Тарары, т.к. все аддендумы, заключенные после аддендума "Перенос ТС"
  -- считаются новыми условиями договора, определяемые аддендумом "Перенос ТС".
  -- И так - до следующего аддендума "Перенос ТС"
  A.IS_MOVE_OBJ_ADDISN,
  A.ADDID,  -- номер аддендума (номер договора для договора)
  A.ORDERNO,
  A.AGR_ID,       -- номер договора
  A.AGR_DATEBEG,
  A.AGR_DATEEND,
  A.AGR_DATESIGN,
  A.AGR_RULEISN,
  A.ADD_DATEBEG,
  A.ADD_DATEEND,
  A.ROLE_DATEBEG,
  A.ROLE_DATEEND,
  A.AGENTISN,
  A.AGENTCLASSISN,
  A.ADDRULEISN,
  A.IS_MOVE_OBJ,
  A.SHAREPC_AGENT_BY_ADD,  -- процент комиссии по агенту
  A.AGENT_SUMCLASSISN,
  A.AGENT_SUMCLASSISN2,
  A.AGENT_CALCFLG,
  A.AGENT_BASE,
  A.AGENT_BASELOSS,
  A.AGENT_PLANFACT,
  A.AGENT_DEPTISN,

  -- порядковый номер агента по критериям отбора отчета "Сборы агентов" - task(ДИТ-12-2-166347)
  A.RNK_MOVE_OBJ,
  -- сумма % комиссии по аддендуму
  A.SHAREPC_BY_ADD,

  A.CNT_AGENT_BY_AGR,
  A.CNT_AGENT_BY_ADD,

  -- атрибуты для аддендума IS_MOVE_OBJ_ADDISN
  A.IS_MOVE_OBJ_ID,
  A.IS_MOVE_OBJ_DATEBEG,
  A.IS_MOVE_OBJ_DATEEND,

  -- сумма % комиссии по дочерним аддендумам к аддендуму "Перенос ТС"
  A.SHAREPC_BY_IS_MOVE_OBJ,
  -- кол-во агентов по дочерним аддендумам к аддендуму "Перенос ТС"
  A.CNT_AGENT_BY_IS_MOVE_OBJ,

  A.IS_ADD_CANCEL,  -- Признак аддендума "Прекращение договора"
  A.IS_ADD_CANCEL_ADDISN,  -- Ссылка на первый ненулевой аддендум/договор от аддендума "Прекращение договора"
  S.CLASSISN as SUBJCLASSISN
from (
  select
    A.*,
    -- порядковый номер агента по критериям отбора отчета "Сборы агентов" - task(ДИТ-12-2-166347)
    rank() over(partition by A.AGRISN, decode(A.IS_MOVE_OBJ, 1, A.ADDISN, A.AGRISN) order by A.ORDERNO) as RNK_MOVE_OBJ,
    /*
    -- сумма % комиссии по аддендуму/договору
    sum(A.SHAREPC_AGENT_BY_ADD)
      over(partition by A.AGRISN, decode(A.IS_MOVE_OBJ, 1, A.ADDISN, A.AGRISN)
           order by A.ORDERNO ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        as SHAREPC_BY_ADD,  -- todo - уточнить для догвора AgrISN = 5909640000
    */
    -- сумма % комиссии по аддендуму
    sum(A.SHAREPC_AGENT_BY_ADD) over(partition by A.AGRISN, A.ADDISN) as SHAREPC_BY_ADD,

    count(distinct A.ORDERNO || ':' || A.AGENTISN) over(partition by A.AGRISN) as CNT_AGENT_BY_AGR,
    count(distinct A.ORDERNO || ':' || A.AGENTISN) over(partition by A.ADDISN) as CNT_AGENT_BY_ADD,

    -- сумма % комиссии по дочерним аддендумам к аддендуму "Перенос ТС"
    sum(A.SHAREPC_AGENT_BY_ADD) over(partition by A.AGRISN, A.IS_MOVE_OBJ_ADDISN) as SHAREPC_BY_IS_MOVE_OBJ,
    -- кол-во агентов по дочерним аддендумам к аддендуму "Перенос ТС"
    count(distinct A.ORDERNO || ':' || A.AGENTISN) over(partition by A.AGRISN, A.IS_MOVE_OBJ_ADDISN) as CNT_AGENT_BY_IS_MOVE_OBJ
  from (
    select
      -- ключи
      A.AGRISN,
      -- ссылка на договор/аддендум, к которому относится агент (по периоду действия)
      A.ADDISN,
      AR.SUBJISN as AGENTISN,
      AR.ORDERNO,

      -- атрибуты
      max(A.AGR_ID) as AGR_ID,
      max(A.AGR_DATEBEG) as AGR_DATEBEG,
      max(A.AGR_DATEEND) as AGR_DATEEND,
      max(A.AGR_DATESIGN) as AGR_DATESIGN,
      max(A.AGR_RULEISN) as AGR_RULEISN,
      max(A.ADDID) as ADDID,
      max(A.DATEBEG) as ADD_DATEBEG,
      max(A.DATEEND) as ADD_DATEEND,
      max(A.RULEISN) as ADDRULEISN,
      max(A.IS_MOVE_OBJ) as IS_MOVE_OBJ, -- ПЕРЕНОС ТС
      max(A.IS_MOVE_OBJ_ADDISN) as IS_MOVE_OBJ_ADDISN,   -- аддендум "Перенос ТС", к которому относится текущий аддендум (Для первоначального состояния = AgrISN)
      -- атрибуты для аддендума IS_MOVE_OBJ_ADDISN
      max(A.IS_MOVE_OBJ_ID) as IS_MOVE_OBJ_ID,
      max(A.IS_MOVE_OBJ_DATEBEG) as IS_MOVE_OBJ_DATEBEG,
      max(A.IS_MOVE_OBJ_DATEEND) as IS_MOVE_OBJ_DATEEND,

      -- атрибуты для аддендума IS_ADD_CANCEL
      max(A.IS_ADD_CANCEL) as IS_ADD_CANCEL,
      max(A.IS_ADD_CANCEL_ADDISN) as IS_ADD_CANCEL_ADDISN,

      -- поля из AgrRole. Группировка до AgentISN в рамках аддендума/договора для фильтрации "грязных" данных
      /* плохо, т.к. на AgrISN = 13141920003 один агент участвует два раза с разными суммами и действует как на аддендум,
      так и на договор. Из за этого, если схлопывать до SubjISN получаются разные промежутки действия для Ад и Д:
      -- начало - от Ад/Д, конец - дата окончания Д
      -- Поэтому вынес в group by
      --min(abs(AR.ORDERNO)) as ORDERNO,
      */
      min(nvl(AR.DATEBEG, '01-jan-1900')) as ROLE_DATEBEG,
      max(nvl(AR.DATEEND, '01-jan-3000')) as ROLE_DATEEND,
      max(AR.CLASSISN) as AGENTCLASSISN,
      sum(AR.SHAREPC) as SHAREPC_AGENT_BY_ADD,
      max(AR.SUMCLASSISN) as AGENT_SUMCLASSISN,
      max(AR.SUMCLASSISN2) as AGENT_SUMCLASSISN2,
      max(AR.CALCFLG) as AGENT_CALCFLG,
      sum(AR.BASE) as AGENT_BASE,
      sum(AR.BASELOSS) as AGENT_BASELOSS,
      min(AR.PLANFACT) as AGENT_PLANFACT,  -- min - приоритет F - факт
      max(AR.DEPTISN) as AGENT_DEPTISN

    from
    ( select
        AGR.*,
        /* sts 01.11.2012
        из за того, что есть договоры, у которых аддендумы различаются на минуту (AgrISN = 176551249603)
        и чтобы (на примере договора) агент Калиберда Юлия Петровна с датой начала действия 30.08.2012 не
        определялся как действующий на аддендум Д (у которого окончание 30.08.2012 15:13:00)
        определяем дату окончания периода следующим образом:
        Если период действия аддендума меньше одного дня, то дату окончания не усекаем
        (как раз для того, чтобы к агенту Калиберде подтянуть аддендум 1).
        Иначе - усекаем до дня чтобы агент Калиберда не подтягивался в аддендум Д
        И уже с этой полученной датой сравниваем период действия роли агента
        */
        case when AGR.DATEEND - AGR.DATEBEG < 1 then AGR.DATEEND else trunc(AGR.DATEEND) end as DATEEND_CALC,
        -- определяем аддендум "Перенос ТС", к которому относится текущий аддендум.
        -- Нужно для ТЗ Тарары, т.к. все аддендумы, заключенные после аддендума "Перенос ТС"
        -- считаются новыми условиями договора, определяемые аддендумом "Перенос ТС".
        -- И так - до следующего аддендума "Перенос ТС"
        nvl(ADD_MOVE.ADDISN, AGR.AGRISN) as IS_MOVE_OBJ_ADDISN,
        nvl(ADD_MOVE.ADDID, AGR.AGR_ID) as IS_MOVE_OBJ_ID,
        nvl(ADD_MOVE.DATEBEG, AGR.AGR_DATEBEG) as IS_MOVE_OBJ_DATEBEG,
        first_value(AGR.DATEEND - decode(AGR.DATEEND, AGR.AGR_DATEEND, 0, 1)) over(partition by nvl(ADD_MOVE.ADDISN, AGR.AGRISN) order by AGR.DATEBEG desc) as IS_MOVE_OBJ_DATEEND,
        -- определяем первый предыдущий аддендум/договор, имеющий ненулевую сумму
        -- нужно для распределения суммы возвратов премии в отчете "Сборы агентов" task(ДИТ-12-2-166348, 32609379103)
        nvl(ADD_CANCEL.ADDISN, AGR.ADDISN) as IS_ADD_CANCEL_ADDISN
      from
        AGR,
        ADD_MOVE,
        ADD_CANCEL
      where
        AGR.AGRISN = ADD_MOVE.AGRISN(+)
        and AGR.DATEBEG >= ADD_MOVE.DATEBEG(+)
        and AGR.DATEBEG < ADD_MOVE.DATEEND(+)
        and AGR.ADDISN = ADD_CANCEL.ROOT_ISN(+)
    ) A,
      AIS.AGRROLE AR
    where
      -- 1. сделал закрытый джойн. Из за этого не попадают договоры/адендумы без агентов
      -- нпр: AgrISN = 183965.1158 имеет одного агента и один аддендум, но срок действия агента в адендум не попадает,
      -- поэтому по такому договору одна запись (на первоначальное состояние договора)
      -- Наверное это правильно...
      -- 2. Для прекращенного договора AgrISN = 133574884903 есть недействующий (в рамках договора) агент.
      -- Но т.к. дату окончания аддендума я считаю как дату начала следующего адендума, то по этому договору
      -- агент получается действующим на аддендум 1 (Изменение условий)
      -- Наверное это тоже правильно, т.к. собственно агент и "добавил" аддендум
      A.AGRISN = AR.AGRISN
      and AR.CLASSISN in (
        437,   -- АГЕНТ
        2481446203, -- АГЕНТ (БОНУСНАЯ КОМИССИЯ)
        2530118403  -- ГЕНЕРАЛЬНЫЙ АГЕНТ
        /* В итоге - пока убрал - решил оставить таблицу MOTOR.AGRAGENT_RANKS (для отчетов ниже) ибо она сильно другая (в частности по группировкам)
           Сделал только догрузку по логам аналогично табл. REP_AGENT_RANKS

        -- sts 12.10.2012 - нужно для отчетов "Кол-во выданных смет" и "Агентский отчет (от ДРС)" в Cognos
        -- для отчета "Сквитованная премия (Регионы)" не нужен, но в отчете классы агентов отфильтровываются,
        -- так что добавление Брокера не должно повлиять на отчет
        438   -- БРОКЕР
        */
      )
      /* sts 01.11.2012 - транкать плохо, ибо есть договоры, у которых аддендумы различаются на минуту: AgrISN = 176551249603
      and nvl(trunc(AR.DATEEND), '01-jan-3000') >= trunc(A.DATEBEG)
      and nvl(trunc(AR.DATEBEG), '01-jan-1900') < trunc(A.DATEEND)
      */
      and nvl(AR.DATEEND, '01-jan-3000') >= A.DATEBEG
      and nvl(AR.DATEBEG, '01-jan-1900') < A.DATEEND_CALC
    group by
      A.AGRISN,
      A.ADDISN,
      AR.SUBJISN,
      AR.ORDERNO
  ) A
) A,
  AIS.SUBJECT_T S
where
  A.AGENTISN = S.ISN(+);
 