CREATE OR REPLACE FORCE VIEW "COGNOS"."V_CITY_REGION_WITH_STRUCTURE" ("CITYISN", "CITYNAME", "REGIONISN", "REGIONNAME", "RC_ISN", "RC_FULLNAME", "FIL_ISN", "FIL_FULLNAME") AS 
  select
  t.CityISN,
  t.CityName,
  t.regionisn,
  rg.ShortName as RegionName,
  t.RC_ISN,
  t.RC_FULLNAME,
  t.FIL_ISN,
  t.FIL_FULLNAME
from (
  select
    t.CityISN,
    c.ShortName as CityName,
    nvl(c.RegionISN, t.regionisn) as regionisn,
    t.RC_ISN,
    t.RC_FULLNAME,
    t.FIL_ISN,
    t.FIL_FULLNAME
  from (
    select --+ ordered use_nl(t sa)
      nvl(sa.jur_cityisn, sa.cityisn) as cityisn,
      nvl(sa.jur_regionisn, sa.regionisn) as regionisn,
      max(t.RC_ISN) as RC_ISN,
      max(t.RC_FULLNAME) as RC_FULLNAME,
      max(t.FIL_ISN) as FIL_ISN,
      max(t.FIL_FULLNAME) as FIL_FULLNAME
    from
      V_DEPT_WITH_STRUCTURE t,
      STORAGE_SOURCE.SUBJECT_ATTRIB sa
    where
      t.ISN = sa.SUBJISN
      and t.active <> 'S'
      and t.Dept0_ISN = 28763316  -- РЕГИОНАЛЬНЫЕ ОРГАНИЗАЦИИ
      and coalesce(t.RC_ISN, t.FIL_ISN) is not null
      and coalesce(sa.jur_cityisn, sa.cityisn, sa.jur_regionisn, sa.regionisn) is not null
    group by
      nvl(sa.jur_cityisn, sa.cityisn),
      nvl(sa.jur_regionisn, sa.regionisn)
  ) t, AIS.City c
  where
    t.CityISN = c.ISN(+)
) t, AIS.Region rg
where
  t.RegionISN = rg.ISN(+);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DEPT_BOSSES" ("DEPTISN", "DEPTBNAME", "SUBJISN_1", "SUBJNAME_1", "SUBJFULLNAME_1", "DUTYNAME_1", "DUTYFULLNAME_1", "EMAIL_1", "SUBJISN_2", "SUBJNAME_2", "SUBJFULLNAME_2", "DUTYNAME_2", "DUTYFULLNAME_2", "EMAIL_2") AS 
  select --+ ordered use_nl(s ph)
-- sts - Добавил поля с инфой по следующему сабжу по должности. Ранжирование по должности - поле RNK
-- Поля с постфиксом _1 - инфа по начальнику, _2 - следующего по должности
  s.deptisn,
  s.deptbname,
  -- Начальник
  max(decode(s.RNK_BOSS, 1, s.SubjISN)) as SubjISN_1,
  max(decode(s.RNK_BOSS, 1, s.SubjName)) as SubjName_1,
  max(decode(s.RNK_BOSS, 1, s.SubjFullName)) as SubjFullName_1,
  max(decode(s.RNK_BOSS, 1, s.DutyName)) as DutyName_1,
  max(decode(s.RNK_BOSS, 1, s.DutyFullName)) as DutyFullName_1,
  max(decode(s.RNK_BOSS, 1, ph.phone)) keep(dense_rank first order by decode(s.RNK_BOSS, 1, 0, 1) asc, ph.updated desc) as EMail_1,
  -- Заместитель (следующий по должности)
  max(decode(s.RNK, 1, s.SubjISN)) as SubjISN_2,
  max(decode(s.RNK, 1, s.SubjName)) as SubjName_2,
  max(decode(s.RNK, 1, s.SubjFullName)) as SubjFullName_2,
  max(decode(s.RNK, 1, s.DutyName)) as DutyName_2,
  max(decode(s.RNK, 1, s.DutyFullName)) as DutyFullName_2,
  max(decode(s.RNK, 1, ph.phone)) keep(dense_rank first order by decode(s.RNK, 1, 0, 1) asc, ph.updated desc) as EMail_2
from
 (select
    s.*
  from
    ( select
        s.*,
        -- при поиске заместителя начальника оперируем сабжами до ведущих специалистов включительно
        case
          when s.sdty_rank <= 90 then
            rank()
              over(
                partition by s.deptisn
                order by
                  s.IS_OTPUSK asc,  -- сабж не в отпуске
                  decode(s.RNK_BOSS, 1, 1, 0) asc,  -- сабж - не начальник
                  case
                    -- сабж не входит в число игнорируемых. При поиске следующего начальника (если самый главный начальник в отпуске),
                    -- главбуху незачем получать отчет из Когноса чтобы не пугался.
                    -- Если нужно будет - case убрать
                    when s.dutyisn not in (
                      30336016  -- ГЛАВНЫЙ БУХГАЛТЕР
                    ) then 0
                    else 1
                  end asc,
                  s.sdty_rank asc,
                  s.BegCareerDate asc, -- при прочих равных отбираем того, кто раньше начал работать
                  s.SubjISN   -- для исключения замножений на всякий случай
              )
            end as RNK
         from
          (select --+ ordered use_nl(s O CR sh) use_hash(rd sdty)
              rd.deptisn,
              rd.deptbname,
              rank() over(partition by rd.deptisn order by sdty.rank asc, CR.BegCareerDate asc, s.ISN) as RNK_BOSS,
              nvl2(O.EmplISN, 1, 0) as IS_OTPUSK,  -- сабж не в отпуске (признак)
              sdty.rank as sdty_rank,
              CR.BegCareerDate,
              s.isn as SubjISN,
              s.shortname as SubjName,
              s.fullname as SubjFullName,
              sdty.shortname as DutyName,
              sh.dutyisn,
              sdty.fullname as DutyFullName
            from
              ais.subject_t s,
              ais.subhuman_t sh,
             (select
                O.EmplISN,
                min(O.DateBeg) as Otpusk_DateBeg,  -- агрегация, т.к. есть сабжи с пересекающимися отпусками
                max(O.DateEnd) as Otpusk_DateEnd
              from
                AIS.Emp_Otpusk O
              where
                sysdate between O.datebeg and O.dateend
              group by O.EmplISN
            ) O,
            ( select
                CR.EmplISN,
                min(CR.BeginDate) as BegCareerDate
              from AIS.EMP_CAREER CR
              group by CR.EmplISN
            ) CR,
              storages.rep_dept rd,
              ais.subduty sdty
            where
              s.classisn = 497  -- СОТРУДНИК ОСАО "ИНГОССТРАХ"
              and nvl(s.active, 'S') <> 'S'
              and s.isn = O.EmplISN(+)
              and s.isn = CR.EmplISN(+)
              and s.isn = sh.isn
              and sh.deptisn = rd.deptisn
              and sh.dutyisn = sdty.ISN
              ---and rd.deptisn = 37492616 --504
           ) s
         ) s
         where
           s.RNK_BOSS = 1 or s.RNK = 1
) s,
  ais.subphone_t ph
where
  s.SubjISN = ph.SubjISN(+)
  and ph.ClassISN(+) = 424  -- E-MAIL
group by
  s.deptisn,
  s.deptbname;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DEPT_WITH_STRUCTURE" ("ISN", "SHORTNAME", "FULLNAME", "CLASSISN", "ACTIVE", "RC_ISN", "RC_NAME", "RC_FULLNAME", "RC_ACTIVE", "FIL_ISN", "FIL_NAME", "FIL_FULLNAME", "FIL_ACTIVE", "O_ISN", "O_NAME", "O_FULLNAME", "O_ACTIVE", "UPR_ISN", "UPR_NAME", "UPR_FULLNAME", "UPR_ACTIVE", "DO_ISN", "DO_NAME", "DO_FULLNAME", "DO_ACTIVE", "DEPT0_ISN", "DEPT0_NAME", "DEPT0_FULLNAME", "DEPT0_ACTIVE", "DEPT1_ISN", "DEPT1_NAME", "DEPT1_FULLNAME", "DEPT1_ACTIVE") AS 
  WITH ISNS AS (
  SELECT
    s.ISN
  FROM AIS.SUBDEPT_T s
  START WITH s.PARENTISN=0
  CONNECT BY PRIOR ISN = ParentIsn
)

select
  S.ROOT_ISN as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  S.ROOT_FULLNAME as FULLNAME,
  S.ROOT_CLASSISN as CLASSISN,
  S.ROOT_ACTIVE as ACTIVE,
  max(decode(S.IS_RC, 1, S.ISN))          keep(dense_rank last order by S.IS_RC, S.IS_ACTIVE, S.LEVL desc) as RC_ISN,
  max(decode(S.IS_RC, 1, S.SHORTNAME))    keep(dense_rank last order by S.IS_RC, S.IS_ACTIVE, S.LEVL desc) as RC_NAME,
  max(decode(S.IS_RC, 1, S.FULLNAME))     keep(dense_rank last order by S.IS_RC, S.IS_ACTIVE, S.LEVL desc) as RC_FULLNAME,
  max(decode(S.IS_RC, 1, S.ACTIVE))       keep(dense_rank last order by S.IS_RC, S.IS_ACTIVE, S.LEVL desc) as RC_ACTIVE,

  max(decode(S.IS_FIL, 1, S.ISN))         keep(dense_rank last order by S.IS_FIL, S.IS_ACTIVE, S.LEVL desc) as FIL_ISN,
  max(decode(S.IS_FIL, 1, S.SHORTNAME))   keep(dense_rank last order by S.IS_FIL, S.IS_ACTIVE, S.LEVL desc) as FIL_NAME,
  max(decode(S.IS_FIL, 1, S.FULLNAME))    keep(dense_rank last order by S.IS_FIL, S.IS_ACTIVE, S.LEVL desc) as FIL_FULLNAME,
  max(decode(S.IS_FIL, 1, S.ACTIVE))      keep(dense_rank last order by S.IS_FIL, S.IS_ACTIVE, S.LEVL desc) as FIL_ACTIVE,

  max(decode(S.IS_OTDEL, 1, S.ISN))       keep(dense_rank last order by S.IS_OTDEL, S.IS_ACTIVE, S.LEVL desc) as O_ISN,
  max(decode(S.IS_OTDEL, 1, S.SHORTNAME)) keep(dense_rank last order by S.IS_OTDEL, S.IS_ACTIVE, S.LEVL desc) as O_NAME,
  max(decode(S.IS_OTDEL, 1, S.FULLNAME))  keep(dense_rank last order by S.IS_OTDEL, S.IS_ACTIVE, S.LEVL desc) as O_FULLNAME,
  max(decode(S.IS_OTDEL, 1, S.ACTIVE))    keep(dense_rank last order by S.IS_OTDEL, S.IS_ACTIVE, S.LEVL desc) as O_ACTIVE,

  max(decode(S.IS_UPR, 1, S.ISN))         keep(dense_rank last order by S.IS_UPR, S.IS_ACTIVE, S.LEVL desc) as UPR_ISN,
  max(decode(S.IS_UPR, 1, S.SHORTNAME))   keep(dense_rank last order by S.IS_UPR, S.IS_ACTIVE, S.LEVL desc) as UPR_NAME,
  max(decode(S.IS_UPR, 1, S.FULLNAME))    keep(dense_rank last order by S.IS_UPR, S.IS_ACTIVE, S.LEVL desc) as UPR_FULLNAME,
  max(decode(S.IS_UPR, 1, S.ACTIVE))      keep(dense_rank last order by S.IS_UPR, S.IS_ACTIVE, S.LEVL desc) as UPR_ACTIVE,

  max(decode(S.IS_DO, 1, S.ISN))          keep(dense_rank last order by S.IS_DO, S.IS_ACTIVE, S.LEVL desc) as DO_ISN,
  max(decode(S.IS_DO, 1, S.SHORTNAME))    keep(dense_rank last order by S.IS_DO, S.IS_ACTIVE, S.LEVL desc) as DO_NAME,
  max(decode(S.IS_DO, 1, S.FULLNAME))     keep(dense_rank last order by S.IS_DO, S.IS_ACTIVE, S.LEVL desc) as DO_FULLNAME,
  max(decode(S.IS_DO, 1, S.ACTIVE))       keep(dense_rank last order by S.IS_DO, S.IS_ACTIVE, S.LEVL desc) as DO_ACTIVE,

  max(decode(S.LEV, 1, S.ISN))            keep(dense_rank last order by decode(S.LEV, 1, 1, 0), S.IS_ACTIVE) as DEPT0_ISN,
  max(decode(S.LEV, 1, S.SHORTNAME))      keep(dense_rank last order by decode(S.LEV, 1, 1, 0), S.IS_ACTIVE) as DEPT0_NAME,
  max(decode(S.LEV, 1, S.FULLNAME))       keep(dense_rank last order by decode(S.LEV, 1, 1, 0), S.IS_ACTIVE) as DEPT0_FULLNAME,
  max(decode(S.LEV, 1, S.ACTIVE))         keep(dense_rank last order by decode(S.LEV, 1, 1, 0), S.IS_ACTIVE) as DEPT0_ACTIVE,

  -- sts - и т.д. по аналогии. Мне нужен был только верхний уровень (DEPT0_ISN). Dept1_ISN сделал как пример...
  max(decode(S.LEV, 2, S.ISN))            keep(dense_rank last order by decode(S.LEV, 2, 1, 0), S.IS_ACTIVE) as DEPT1_ISN,
  max(decode(S.LEV, 2, S.SHORTNAME))      keep(dense_rank last order by decode(S.LEV, 2, 1, 0), S.IS_ACTIVE) as DEPT1_NAME,
  max(decode(S.LEV, 2, S.FULLNAME))       keep(dense_rank last order by decode(S.LEV, 2, 1, 0), S.IS_ACTIVE) as DEPT1_FULLNAME,
  max(decode(S.LEV, 2, S.ACTIVE))         keep(dense_rank last order by decode(S.LEV, 2, 1, 0), S.IS_ACTIVE) as DEPT1_ACTIVE

from
 (select
    S.ISN,
    S.SHORTNAME,
    S.FULLNAME,
    S.levl,
    S.LEV,
    S.ROOT_ISN, -- PK подразделения, т.к. "connect by" идет снизу вверх
    S.ROOT_SHORTNAME,
    S.ROOT_FULLNAME,
    S.ROOT_CLASSISN,
    S.ROOT_ACTIVE,

    S.ACTIVE,
    S.IS_ACTIVE,
    S.IS_RC,
    S.IS_FIL,
    S.IS_OTDEL,
    S.IS_UPR,

    max(case when 956868025 in (S.CLASSISN, SE.CLASSISNOLD) then 1 else 0 end) as IS_DO  -- признак Доп. офиса

  from (
    select
          S.*,
          ABS (levl - 1 - MAX (levl) OVER (PARTITION BY ROOT_ISN)) as lev,
          decode(S.ACTIVE, 'S', 0, 1) as IS_ACTIVE,
          case when S.CLASSISN = 956866825 or S.ISN = 667199016 /* врезка, чтобы спб был и филиалом и РЦ */ then 1 else 0 end as IS_RC,
          case when S.CLASSISN = 956867125 then 1 else 0 end as IS_FIL,
          case when  ' '||upper(s.fullname)||' ' like '% ОТДЕЛ %' then 1 else 0 end as IS_OTDEL,
          case when lower(s.fullname) like 'управление %' then 1 else 0 end as IS_UPR
    from
         ( SELECT
              D.ISN,
              D.SHORTNAME,
              D.FULLNAME,
              D.CLASSISN,
              nvl(D.ACTIVE, 'S') as ACTIVE,
              LEVEL as levl,
              CONNECT_BY_ROOT (D.ISN) as ROOT_ISN, -- PK подразделения, т.к. "connect by" идет снизу вверх
              CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME,
              CONNECT_BY_ROOT (D.FULLNAME) as ROOT_FULLNAME,
              CONNECT_BY_ROOT (D.CLASSISN) as ROOT_CLASSISN,
              CONNECT_BY_ROOT (D.ACTIVE) as ROOT_ACTIVE
            FROM AIS.SUBDEPT_T D
            START WITH isn IN (SELECT ISN from ISNS)
            CONNECT
              BY PRIOR D.PARENTISN = D.ISN
         ) S, ISNS
    WHERE S.ROOT_ISN = ISNS.ISN
  ) S, AIS.SUBJCLASSEXT SE
  where
    S.ISN = SE.SUBJISN(+)
  group by
    S.ISN,
    S.SHORTNAME,
    S.FULLNAME,
    S.levl,
    S.LEV,
    S.ROOT_ISN,
    S.ROOT_SHORTNAME,
    S.ROOT_FULLNAME,
    S.ROOT_CLASSISN,
    S.ROOT_ACTIVE,
    S.ACTIVE,
    S.IS_ACTIVE,
    S.IS_RC,
    S.IS_FIL,
    S.IS_OTDEL,
    S.IS_UPR

) S
group by
  S.ROOT_ISN,
  S.ROOT_SHORTNAME,
  S.ROOT_FULLNAME,
  S.ROOT_CLASSISN,
  S.ROOT_ACTIVE;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DICT_CF_SUBJECT" ("ISN", "SHORTNAME") AS 
  select distinct
       S.ISN,
       S.SHORTNAME
  from SUBJECT S,
       STORAGES.TT_CF_NEW_FINAL T
 where S.ISN = T.SUBJISN

 union all

select 0,
       'Мелочь'
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DICT_COLOR" ("COLORISN", "COLOR") AS 
  select ISN COLORISN,
        SHORTNAME COLOR
   from DICTI
  start with PARENTISN = 12444716
connect by prior ISN = PARENTISN

union all

select 0,
       ' '
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DICT_FAMILYSTATE" ("FAMILYSTATEISN", "FAMILYSTATE") AS 
  select ISN FAMILYSTATEISN,
        SHORTNAME FAMILYSTATE
   from DICTI
  start with PARENTISN = 11275519
connect by prior ISN = PARENTISN

union all

 select 0,
        ' '
 from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DICT_MOTOR_RPTCLASS" ("RPTCLASS", "SHORTNAME") AS 
  select '??' RPTCLASS,
       'Не определен' SHORTNAME
  from dual

union all

select 'ГО' RPTCLASS,
       'ГО' SHORTNAME
  from dual

union all

select 'ДО' RPTCLASS,
       'ДО' SHORTNAME
  from dual

union all

select 'КА-угон' RPTCLASS,
       'КА-угон' SHORTNAME
  from dual

union all

select 'КА-ущерб' RPTCLASS,
       'КА-ущерб' SHORTNAME
  from dual

union all

select 'НС' RPTCLASS,
       'НС' SHORTNAME
  from dual

union all

select ' ',
       'Не указан'
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DICT_RISKCLASS" ("RISKCLASSISN", "RISKCLASSNAME") AS 
  Select Isn RISKCLASSISN,SHORTNAME RISKCLASSNAME
from dicti
start with parentisn=28966016
connect by prior isn=parentisn
union all
Select 0 ,' '
from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DICT_RISKSTEAL" ("RISKSTEAL", "SHORTNAME") AS 
  select 0 RISKSTEAL,'Угон не застрахован' shortname  from dual
union all
select 1 ,'Угон застрахован' shortname  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DICT_SEX" ("SEX", "SHORTNAME") AS 
  (
Select 'М' SEX,'Мужчина' shortname  from dual
 union all
Select 'Ж', 'Женщина' from dual
 union all
Select '?','Нет данных'  from dual
 union all
Select ' ','Нет данных'  from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_ADD_ACCRUAL" ("ADDACCRUAL") AS 
  select 0 addaccrual from dual
union all
select 1 addaccrual from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGENTCLASSISN" ("AGENTCLASSISN", "SHORTNAME") AS 
  select 0 AGENTCLASSISN, 'Нет данных' shortname
from dual
union all
select isn AGENTCLASSISN, shortname
from dicti where isn in (437,438);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGENTJURIDICAL" ("AGENTJURIDICAL", "SHORTNAME") AS 
  select 'Y' AGENTJURIDICAL, 'Юр.лицо' shortname from dual
union all
select 'N' AGENTJURIDICAL, 'Физ.лицо' shortname from dual
union all
select '0','Не указано' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGENTS" ("AGENTISN", "SHORTNAME", "FULLNAME") AS 
  select "ISN" AGENTISN,
       "SHORTNAME",
       "FULLNAME"
  from ais.subject_t
 where classisn = 437

union all

select 0,
       cognos_const.GET_VALUE('AGENT_CHAR'),
       cognos_const.GET_VALUE('AGENT_CHAR')
from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGENTSTATUS" ("AGENTSTATUSISN", "AGENTSTATUS") AS 
  select 1 AGENTSTATUSISN, 'Прекращен' AGENTSTATUS from dual
union all
 select 2, 'Действует' from dual
union all
 select 0, cognos_const.get_value('EMPTY_CHAR') from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGRCLASSISN" ("AGRCLASSISN", "SHORTNAME") AS 
  select ISN AGRCLASSISN,
        SHORTNAME
   from DICTI
  start with PARENTISN = 34711216
connect by prior ISN = PARENTISN

  union all

 select 0,
        cognos_const.get_value('EMPTY_CHAR')
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGRINSTALMENT" ("AGRINSTALMENT", "SHORTNAME") AS 
  select 'Y' AGRINSTALMENT,
        'Рассрочка' SHORTNAME
   from dual

union all

 select 'N' AGRINSTALMENT,
        'Нет рассрочки' SHORTNAME
   from dual

union all

 select ' ',
        'Не указано'
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGRPRODUCT" ("ISN", "GRISN", "ISNNAME", "GRISNNAME") AS 
  select D.ISN,
       D.GRISN,
       A.SHORTNAME ISNNAME,
       B.SHORTNAME GRISNNAME
  from ( select D.ISN,
                ( case to_number(substr(D.PATH, instr(D.PATH, '#', 1, 2) + 1, instr(D.PATH, '#', 1, 3) - instr(D.PATH, '#', 1, 2) - 1))
                   when 683209116  -- комплексное страхование
                    then nvl(to_number(substr(D.PATH, instr(D.PATH, '#', 1, 3) + 1, instr(D.PATH, '#', 1, 4) - instr(D.PATH, '#', 1, 3) - 1)), d.isn)
                   else nvl(to_number(substr(D.PATH, instr(D.PATH, '#', 1, 2) + 1, instr(D.PATH, '#', 1, 3) - instr(D.PATH, '#', 1, 2) - 1)), d.isn)
                  end ) GRISN
           from ( select D.ISN,
                         sys_connect_by_path(D.ISN, '#') PATH
                    from DICTI D
                   where D.ISN <> 24890816
                   start with D.ISN = 24890816 -- классификация продуктов договоров страхования
                 connect by prior D.ISN = D.PARENTISN
                     and upper(D.SHORTNAME) <> 'АВАНС' ) D ) D,
       DICTI A,
       DICTI B
 where A.ISN(+) = D.ISN
   and B.ISN(+) = D.GRISN

union all

select 0,
       0,
       'Нет данных',
       'Нет данных'
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGRROLECLASS" ("AGRROLECLASSISN", "SHORTNAME", "CODE") AS 
  select isn, shortname ,code from dicti where parentisn = 402
union all 
select 0 isn,  'Не указано','Не указано' shortname from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGRRULE" ("GROUPNAME", "GROUPISN", "AGRRULEISN", "SHORTNAME") AS 
  select nvl(g.groupname, cognos_const.GET_VALUE('EMPTY_CHAR')) groupname,
       nvl(g.groupisn,0) groupisn,
       a.agrruleisn,
       a.shortname
from dic_agrrulegroup g,
     (select *
      from dic_agrrulegroup_map
      where active = 1) gm,
     (select ISN AGRRULEISN,
        SHORTNAME
      from DICTI
           start with PARENTISN = 24890816
           connect by prior ISN = PARENTISN

      union all

      select 0,
             cognos_const.get_value('EMPTY_CHAR')
      from dual) a
where a.AGRRULEISN = gm.ruleisn(+)
  and gm.groupisn = g.groupisn(+);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGRSTATUS" ("CODE", "SHORTNAME", "DESCRIPTION") AS 
  select code, shortname, fullname description  from dicti where parentisn = 601
UNION ALL
select '0', cognos_const.get_value('EMPTY_CHAR'), cognos_const.get_value('EMPTY_CHAR')  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGRSTATUS2" ("SHORTSTATUS", "SHORTNAME") AS 
  select --+ CARDINALITY (S 100)
  SHORTSTATUS,
  SHORTNAME
from (
  select 'В' SHORTSTATUS, 'ВЫПУЩЕН' SHORTNAME
     from dual
  union all
  select 'Д', 'ПРЕКРАЩЕН СТРАХОВАТЕЛЕМ'
     from dual
  union all
  select 'Щ', 'ПРЕКРАЩЕН СТРАХОВЩИКОМ'
     from dual
  union all
  select 'С', 'ЗАЯВЛЕНИЕ'
     from dual
  union all
  select 'З', 'ЗАПРОС'
     from dual
  union all
  select 'Р', 'ЗАРЕЗЕРВИРОВАН'
     from dual
  union all
  select 'А', 'АННУЛИРОВАН'
     from dual
  union all
   select '0', cognos_const.get_value('EMPTY_CHAR')
     from dual
)    S;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_AGRTERM" ("AGRTERM", "CLASSISN", "SHORTNAME") AS 
  select TERM AGRTERM,
       CLASSISN,
       SHORTNAME
from STORAGES.REP_ISLONGAGR_R

union all

select 0,
       0,
       cognos_const.get_value('EMPTY_CHAR')
from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_BIZFLG" ("BIZFLG", "SHORTNAME") AS 
  select 'Ц' BIZFLG,
        'Бизнес ЦО' SHORTNAME
   from dual

union all

 select 'Ф',
        'Бизнес филиалов' SHORTNAME
   from dual

union all

 select '0',
        cognos_const.get_value('EMPTY_CHAR')
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_BONUS" ("ISN", "BONUS", "SHORTNAME") AS 
  select isn, shortname bonus, shortname from dicti start with  parentisn  = 24037516 connect by prior isn = parentisn

union all

select
  t.ISN,
  t.SHORTNAME,
  t.SHORTNAME
from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_BSO_OPER" ("BSO_OPERISN", "CODE", "SHORTNAME", "ACTIVE") AS 
  select isn bso_operisn,
       code,
       shortname,
       active
  from DICTI
 start with parentisn = 768497800
connect by prior isn = parentisn
 union
select 0,
       null,
       cognos_const.get_value('EMPTY_CHAR'),
       null
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_BSO_OPER_TYPE" ("CODE", "SHORTNAME") AS 
  select -3 code, 'ОПЕРАЦИЯ БЕЗ ОРГАНИЗАЦИИ ШАПКИ' shortname from dual
union all
select -2 code, 'УДАЛЕНИЕ' shortname from dual
union all
select -1 code, 'СТОРНИРУЕМАЯ ОПЕРАЦИЯ' shortname from dual
union all
select 0 code, 'НОРМАЛЬНАЯ' shortname from dual
union all
select 1 code, 'СТОРНИРУЮЩАЯ ОПЕРАЦИЯ' shortname from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_BSO_STATUS" ("BSO_STATUSISN", "SHORTNAME", "FULLNAME") AS 
  select isn bso_statusisn,
       shortname,
       fullname
  from DICTI
 start with parentisn = 767682600
connect by prior isn = parentisn
 union
select 0,
       cognos_const.get_value('EMPTY_CHAR'),
       cognos_const.get_value('EMPTY_CHAR')
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_BSO_TYPE" ("BSO_TYPEISN", "CODE", "SHORTNAME", "ACTIVE") AS 
  select isn bso_typeisn,
       code,
       shortname,
       active
  from DICTI
 start with parentisn = 767683700
connect by prior isn = parentisn
 union
select 0,
       null,
       cognos_const.get_value('EMPTY_CHAR'),
       null
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_BUDGETGROUP" ("BUDGETGROUPISN", "CODE", "SHORTNAME") AS 
  select ISN BUDGETGROUPISN,
       CODE,
        SHORTNAME
   from DICTI
  start with PARENTISN = 1342981303
connect by prior ISN = PARENTISN

union all

select 0,
        cognos_const.get_value('EMPTY_CHAR'),
        cognos_const.get_value('EMPTY_CHAR')
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_BUHOPER" ("LEV1_ISN", "LEV1_NAME", "LEV2_ISN", "LEV2_NAME", "LEVLNK_ISN") AS 
  (
 
Select Lev1_Isn,Lev1_Name,
       Nvl(Lev2_Isn,Lev1_Isn)  Lev2_Isn,
       Nvl(Lev2_Name,Lev1_Name) Lev2_Name,
       Nvl(Lev2_Isn,Lev1_Isn)   LevLnk_Isn
from (      

 Select Max(Decode(Lev,1,Isn)) Lev1_Isn,
        Max(Decode(Lev,1,ShortName)) Lev1_Name,
        Max(Decode(Lev,2,Isn)) Lev2_Isn,
        Max(Decode(Lev,2,ShortName)) Lev2_Name,
        Max(Decode(Lev,3,Isn)) Lev3_Isn,
        Max(Decode(Lev,3,ShortName)) Lev3_Name



from 
(
Select S.Isn,S.ShortName, Connect_By_Root (Isn) RootIsn,Lv Lev
from 
(
 Select S.Isn,ParentIsn, Decode(Code,Null,S.ShortName,Code||' '||S.ShortName) ShortName,CONNECT_BY_ISLEAF IsLast,Level Lv
 from dicti s
 Where Nvl(Active,'S')<>'S' 
 Start With  ParentIsn=9533816
 Connect By Prior Isn=ParentIsn
) s 
Start With IsLast=1
Connect By Prior ParentIsn=Isn               
 )            

 
group by  RootIsn

)


);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_CAR_BASETARIFF_GROUP" ("ISN", "SHORTNAME", "FULLNAME") AS 
  select isn, shortname, fullname from dicti start with  parentisn  = 2759406903 connect by prior isn = parentisn

union all

select
  t.ISN,
  t.SHORTNAME,
  t.SHORTNAME
from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_CAR_BODYTYPE" ("ISN", "SHORTNAME", "FULLNAME") AS 
  select /*+ use_nl(d) */
  d.ISN,
  d.SHORTNAME,
  d.FULLNAME
from dicti d
where d.parentisn = 1857696603

union all

select
  t.ISN,
  t.SHORTNAME,
  t.SHORTNAME
from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_CLAIMINVOICE_CLASS" ("ISN", "SHORTNAME", "FULLNAME") AS 
  select /*+ use_nl(d) */
  d.ISN,
  d.SHORTNAME,
  d.FULLNAME
from dicti d
where d.parentisn = 2149993403

union all

select
  t.ISN,
  t.SHORTNAME,
  t.SHORTNAME
from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_CLAIMINVOICE_STATUS" ("STATUS", "STATUSNAME") AS 
  select 'Y' as STATUS, 'Закрыт' as STATUSNAME from dual
union all
select 'N', 'Открыт' from dual
union all
select 'D', 'В работе' from dual
union all
select 'A', 'Аннулирован' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_CLAIMSTATUS" ("STATUSCODE", "SHORTNAME") AS 
  (-- sts 13.12.2012 - сделал как в АИС: ф-ия AIS.AGRN.DecodeRefundStatus
Select code STATUSCODE, shortname
From DICTI
where
  parentisn = 54057116 -- C.Get('PrClaimStatus')
  and Active is not null
union all
Select '0' ClaimStatus, cognos_const.get_value('EMPTY_CHAR')  From dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_CLIENTGROUP" ("CLIENTGROUP", "SHORTNAME") AS 
  select 'Y' CLIENTGROUP,
'Сотрудник(родственник сотрудника,внештатник) ИГС' shortname from dual
union all
select 'N' , 'Не сотрудник ИГС' shortname from dual
union all
select ' ', 'Нет данных' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_CLIENTJURIDICAL" ("CLIENTJURIDICAL", "SHORTNAME") AS 
  select 'Y' CLIENTJURIDICAL, 'Юр.лицо' shortname from dual
union all
select 'N' CLIENTJURIDICAL, 'Физ.лицо' shortname from dual
union all
select '0','Не указано' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_COMCLASS" ("COMCLASSISN", "SHORTNAME") AS 
  Select
isn comclassisn, shortname
From DICTI start with parentisn = 2336990303  connect by prior isn =parentisn
union
select 0, cognos_const.get_value('EMPTY_CHAR') from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_COMTYPE" ("COMTYPE", "SHORTNAME") AS 
  Select
isn comtype, shortname
From DICTI start with parentisn = 1304293803  connect by prior isn =parentisn
union
select 0, cognos_const.get_value('EMPTY_CHAR') from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_CONTACT_STATUS" ("CONTACT_TYPE", "STATUS", "STATUS_NAME") AS 
  SELECT CONTACT_TYPE,
        STATUS,
        STATUS_NAME
   FROM (--Обзвон по анкетам( кроме номера 1)
         SELECT 1 CONTACT_TYPE, 'F' STATUS, 'Всем доволен' STATUS_NAME from dual
          union all
         SELECT 1 CONTACT_TYPE, 'N' STATUS, 'Отказ респондента' STATUS_NAME from dual
          union all
         SELECT 1 CONTACT_TYPE, 'B' STATUS, 'Ошибочный телефон' STATUS_NAME from dual
          union all
         SELECT 1 CONTACT_TYPE, 'Y' STATUS, 'Анкета заполнена/Уведомлен' STATUS_NAME from dual
         )

 UNION ALL

 SELECT CONTACT_TYPE,
        STATUS,
        STATUS_NAME
   FROM (--Обзвон по анкете номер 1
         SELECT 2 CONTACT_TYPE, 'F' STATUS, 'Всем доволен' STATUS_NAME from dual
          union all
         SELECT 2 CONTACT_TYPE, 'N' STATUS, 'Отказ респондента' STATUS_NAME from dual
          union all
         SELECT 2 CONTACT_TYPE, 'B' STATUS, 'Ошибочный телефон' STATUS_NAME from dual
          union all
         SELECT 2 CONTACT_TYPE, 'Y' STATUS, 'Анкета заполнена' STATUS_NAME from dual
         )

  UNION ALL

 SELECT CONTACT_TYPE,
        STATUS,
        STATUS_NAME
   FROM (--Почтовые отпрвления + Почтовые отправления без информации о договоре
         SELECT 5 CONTACT_TYPE, 'W' STATUS, 'Ожидает обработки' STATUS_NAME from dual
          union all
         SELECT 5 CONTACT_TYPE, 'Y' STATUS, 'Обработан' STATUS_NAME from dual
          union all
         SELECT 5 CONTACT_TYPE, 'F' STATUS, 'Отослан' STATUS_NAME from dual
         )

   UNION ALL

 SELECT --СМС Клиенту
        7 CONTACT_TYPE,
        D.TableName STATUS,
        D.SHORTNAME STATUS_NAME
   FROM DICTI D
  WHERE parentisn = 3426296503 --c.get('SMS_STATUSES');

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_CREDIT" ("CREDITFLAG", "SHORTNAME") AS 
  select 'CR' CREDITFLAG, 'Кредитный' SHORTNAME
   from dual
union all
 select 'NCR', 'Некредитный'
   from dual
union all
 select '0', cognos_const.get_value('EMPTY_CHAR')
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_CURRENCY" ("CURRISN", "CURRCODE") AS 
  (
Select isn CurrIsn,code CurrCode from currency c
union
Select 0, cognos_const.get_value('EMPTY_CHAR') from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_DEBTTYPE" ("ISN", "NAME") AS 
  select 0 ISN, '<не задано>' NAME from dual
  union all
select 1, 'ПРОСРОЧЕННАЯ' from dual
  union all
select 2, 'ТЕКУЩАЯ' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_DELAY" ("DELAYISN", "SHORTNAME", "DELAY_SORT") AS 
  select "ISN","SHORTNAME", "DFROM" from storages.rep_delay
Union
select 0, 'Нет Данных', -1 from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_DEPOSITPROGR" ("ISN", "ДЕПОЗИТНАЯ ПРОГРАММА") AS 
  SELECT         d.isn,
               d.shortname as "ДЕПОЗИТНАЯ ПРОГРАММА"
FROM dicti d
start with d.isn in
--(1500758503, 2293897603, 2317815203)
 (1500758503, 2293897603, 2317815203, 2434777803, 3358966203)
connect by prior d.isn = d.parentisn;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_DEPT" ("ISN", "LOADISN", "DEPTISN", "DEPTACTIVE", "CLASSISN", "DEPTNAME", "DEPTBNAME", "DEPT0ISN", "DEPT0NAME", "RCISN", "RCNAME", "FILNAME", "FILISN", "DEPT1NAME", "DEPT1ISN", "DEPTCITYISN", "FILCITYISN", "OISN", "ONAME", "DEPTTYPE", "DOISN", "DONAME") AS 
  select RD.ISN,
       RD.LOADISN,
       RD.DEPTISN,
       RD.DEPTACTIVE,
       RD.CLASSISN,
       nvl(RD.DEPTNAME, '<не задано>') DEPTNAME,
       nvl(RD.DEPTBNAME, '<не задано>') DEPTBNAME,
       nvl(RD.DEPT0ISN, -1) DEPT0ISN,
       nvl(RD.DEPT0NAME, '<не задано>') DEPT0NAME,
       nvl(RD.RCISN, -1) RCISN,
       nvl(RD.RCNAME, '<не задано>') RCNAME,
       nvl(RD.FILNAME, nvl(RD.RCNAME, '<не задано>')) FILNAME,
       nvl(RD.FILISN, nvl(RD.RCISN, -1)) FILISN,
       nvl(RD.DEPT1NAME, nvl(RD.DEPT0NAME, '<не задано>')) DEPT1NAME,
       nvl(RD.DEPT1ISN, nvl(RD.DEPT0ISN,-1)) DEPT1ISN,
       RD.DEPTCITYISN,
       RD.FILCITYISN,
       nvl(RD.OISN, nvl(RD.DEPT1ISN, nvl(RD.DEPT0ISN,-1))) OISN,
       nvl(RD.ONAME, nvl(RD.DEPT1NAME, nvl(RD.DEPT0NAME, '<не задано>'))) ONAME,
       RD.DEPTTYPE,
       nvl(RD.DOISN, nvl(RD.FILISN, nvl(RD.RCISN, -1))) DOISN,
       nvl(RD.DONAME, nvl(RD.FILNAME, nvl(RD.RCNAME, '<не задано>'))) DONAME
  from STORAGE_SOURCE.REP_DEPT RD
 where RD.DEPTISN is not null

union all

 select -1, --ISN
        null, -- LOADISN
        0, -- DEPTISN
        null, --DEPTACTIVE
        null, --CLASSISN
        '<не задано>', --DEPTNAME
        '<не задано>', --DEPTBNAME
        -1, --DEPT0ISN
        '<не задано>', --DEPT0NAME
        -1, --RCISN
        '<не задано>', --RCNAME
        '<не задано>', --FILNAME
        -1, --FILISN
        '<не задано>', --DEPT1NAME
        -1, --DEPT1ISN
        -1, --DEPTCITYISN
        -1, --FILCITYISN
        -1, --OISN
        '<не задано>',  --ONAME
        null, --DEPTTYPE
        -1, --DOISN
        '<не задано>'  --DONAME
   from dual

union all

 select -2,--ISN
        null, -- LOADISN
        492, -- DEPTISN
        null, --DEPTACTIVE
        null,  --CLASSISN
        'ИНГОССТРАХ', --DEPTNAME
        'ОСАО "Ингосстрах"', --DEPTBNAME
        -1, --DEPT0ISN
        '<не задано>', --DEPT0NAME
        -1, --RCISN
        '<не задано>',  --RCNAME
        '<не задано>', --FILNAME
        -1, --FILISN
        '<не задано>', --DEPT1NAME
        -1, --DEPT1ISN
        -1, --DEPTCITYISN
        -1, --FILCITYISN
        -1, --OISN
        '<не задано>',  --ONAME
        null, --DEPTTYPE
        -1, --DOISN
        '<не задано>'  --DONAME
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_DEPTCITY" ("DEPTCITYISN", "CITYNAME", "REGIONISN", "REGIONNAME", "COUNTRYISN", "COUNTRYNAME") AS 
  (
Select
distinct c.Isn DEPTCITYIsn,c.SHORTNAME CITYNAME ,
c.regionisn,rg.shortname regionname, c.countryisn,cn.shortname countryname
from storages.rep_dept rd, city c,region rg,country cn
Where
rd.deptcityisn=c.isn
and c.regionisn=rg.isn(+) and c.countryisn=cn.isn(+)
union all
Select 0 ,'Нет Данных',0,'Нет Данных',0,'Нет Данных' from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_DEPT_1" ("ISN", "LOADISN", "DEPTISN", "DEPTACTIVE", "CLASSISN", "DEPTNAME", "DEPTBNAME", "DEPT0ISN", "DEPT0NAME", "RCISN", "RCNAME", "FILNAME", "FILISN", "DEPT1NAME", "DEPT1ISN", "DEPTCITYISN", "FILCITYISN", "OISN", "ONAME", "DEPTTYPE", "DOISN", "DONAME") AS 
  select RD.ISN,
       RD.LOADISN,
       RD.DEPTISN,
       RD.DEPTACTIVE,
       RD.CLASSISN,
       nvl(RD.DEPTNAME, '<не задано>') DEPTNAME,
       nvl(RD.DEPTBNAME, '<не задано>') DEPTBNAME,
       nvl(RD.DEPT0ISN, -1) DEPT0ISN,
       nvl(RD.DEPT0NAME, '<не задано>') DEPT0NAME,
       nvl(RD.RCISN, DEPTISN) RCISN,
       nvl(RD.RCNAME, DEPTNAME) RCNAME,
       nvl(RD.FILNAME, nvl(RD.RCNAME, DEPTNAME)) FILNAME,
       nvl(RD.FILISN, nvl(RD.RCISN, DEPTISN)) FILISN,
       nvl(RD.DEPT1NAME, nvl(RD.DEPT0NAME, '<не задано>')) DEPT1NAME,
       nvl(RD.DEPT1ISN, nvl(RD.DEPT0ISN,-1)) DEPT1ISN,
       RD.DEPTCITYISN,
       RD.FILCITYISN,
       nvl(RD.OISN, nvl(RD.DEPT1ISN, nvl(RD.DEPT0ISN,-1))) OISN,
       nvl(RD.ONAME, nvl(RD.DEPT1NAME, nvl(RD.DEPT0NAME, '<не задано>'))) ONAME,
       RD.DEPTTYPE,
       nvl(RD.DOISN, nvl(RD.FILISN, nvl(RD.RCISN, -1))) DOISN,
       nvl(RD.DONAME, nvl(RD.FILNAME, nvl(RD.RCNAME, '<не задано>'))) DONAME
  from STORAGE_SOURCE.REP_DEPT RD
 where RD.DEPTISN is not null

union all

 select -1, --ISN
        null, -- LOADISN
        0, -- DEPTISN
        null, --DEPTACTIVE
        null, --CLASSISN
        '<не задано>', --DEPTNAME
        '<не задано>', --DEPTBNAME
        -1, --DEPT0ISN
        '<не задано>', --DEPT0NAME
        -1, --RCISN
        '<не задано>', --RCNAME
        '<не задано>', --FILNAME
        -1, --FILISN
        '<не задано>', --DEPT1NAME
        -1, --DEPT1ISN
        -1, --DEPTCITYISN
        -1, --FILCITYISN
        -1, --OISN
        '<не задано>',  --ONAME
        null, --DEPTTYPE
        -1, --DOISN
        '<не задано>'  --DONAME
   from dual

union all

 select -2,--ISN
        null, -- LOADISN
        492, -- DEPTISN
        null, --DEPTACTIVE
        null,  --CLASSISN
        'ИНГОССТРАХ', --DEPTNAME
        'ОСАО "Ингосстрах"', --DEPTBNAME
        -1, --DEPT0ISN
        '<не задано>', --DEPT0NAME
        -1, --RCISN
        '<не задано>',  --RCNAME
        '<не задано>', --FILNAME
        -1, --FILISN
        '<не задано>', --DEPT1NAME
        -1, --DEPT1ISN
        -1, --DEPTCITYISN
        -1, --FILCITYISN
        -1, --OISN
        '<не задано>',  --ONAME
        null, --DEPTTYPE
        -1, --DOISN
        '<не задано>'  --DONAME
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_DOC_STATUS" ("STATUS", "STATUSNAME") AS 
  select 'ШБ' as STATUS, 'Шаблон' as STATUSNAME from dual
union all
select 'КЗ', 'Отказ банка' from dual
union all
select 'ОШ', 'Ошибка в реквизитах' from dual
union all
select '-1', 'Создан системой: новый, без номера' from dual
union all
select '00', 'Без визы: в работе, с номером' from dual
union all
select 'ГО', 'Подписан: руководителем подразделения (Готов)' from dual
union all
select 'ПН', 'Запланирован' from dual
union all
select 'РЦ', 'В бухгалтерии: готов к отправке' from dual
union all
select 'ВП', 'Квитуется: отправлен в бухгалтерию (Выпущен)' from dual
union all
select 'ОЛ', 'Отложен до лучших времен' from dual
union all
select 'БК', 'В банке' from dual
union all
select 'ВЯ', 'До выяснения после выписки' from dual
union all
select '99', 'Оплачен (скрыжен, полностью сквитован)' from dual
union all
select '98', 'Отказ банка получателя' from dual
union all
select 'АН', 'Аннулирован' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_DOC_TYPE" ("DOC_TYPE", "DOC_TYPENAME") AS 
  select '01' as DOC_TYPE, 'исходящее платежное поручение' as DOC_TYPENAME from dual
union all
select '02', 'входящее платежное поручение' as DOC_TYPENAME from dual
union all
select '05', 'расходный кассовый ордер' as DOC_TYPENAME from dual
union all
select '06', 'приходный кассовый ордер' as DOC_TYPENAME from dual
union all
select '07', 'расходный ордер внутренней кассы' as DOC_TYPENAME from dual
union all
select '08', 'приходный ордер внутренней кассы' as DOC_TYPENAME from dual
union all
select '21', 'исходящий счет' as DOC_TYPENAME from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_DOMESTIC" ("DOMESTIC", "SHORTNAME") AS 
  Select 'N' domestic,'Иномарка' shortname  from dual
 union all
Select 'Y' ,'Отечественная' shortname  from dual
 union all
Select '0', cognos_const.get_value('EMPTY_CHAR')  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_DSI_BGROUP" ("DSI_BGROUPISN", "SHORTNAME") AS 
  select isn dsi_bgroupisn, shortname  from dicti where isn in
(select nvl(dsinewisn, isn) from storages.rep_bg_with_newdsi)
union all
select 0, cognos_const.get_value('EMPTY_CHAR') from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_EMITENT" ("ISN", "LOADISN", "DEPTISN", "DEPTACTIVE", "CLASSISN", "DEPTNAME", "DEPTBNAME", "DEPT0ISN", "DEPT0NAME", "RCISN", "RCNAME", "FILNAME", "FILISN", "DEPT1NAME", "DEPT1ISN", "DEPTCITYISN", "FILCITYISN", "OISN", "ONAME", "DEPTTYPE", "DOISN", "DONAME") AS 
  select ISN,
       LOADISN,
       DEPTISN,
       DEPTACTIVE,
       CLASSISN,
       DEPTNAME,
       DEPTBNAME,
       DEPT0ISN,
       DEPT0NAME,
       RCISN,
       RCNAME,
       FILNAME,
       FILISN,
       DEPT1NAME,
       DEPT1ISN,
       DEPTCITYISN,
       FILCITYISN,
       OISN,
       ONAME,
       DEPTTYPE,
       DOISN,
       DONAME
  from STORAGE_SOURCE.REP_DEPT
 where DEPT0NAME = 'РЕГИОН'

union all

select 0,
       null,
       0,
       null,
       null,
       ' ',
       ' ',
       null,
       ' ',
       null,
       ' ',
       ' ',
       null,
       null,
       null,
       null,
       null,
       null,
       ' ',
       null,
       null,
       null
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_EMITENT_WITH_MSK" ("ISN", "LOADISN", "DEPTISN", "DEPTACTIVE", "CLASSISN", "DEPTNAME", "DEPTBNAME", "DEPT0ISN", "DEPT0NAME", "RCISN", "RCNAME", "FILNAME", "FILISN", "DEPT1NAME", "DEPT1ISN", "DEPTCITYISN", "FILCITYISN", "OISN", "ONAME", "DEPTTYPE", "DOISN", "DONAME", "DEPT2ISN", "DEPT2NAME", "UPRISN") AS 
  select RD.ISN ISN,
       RD.LOADISN,
       RD.DEPTISN,
       RD.DEPTACTIVE,
       RD.CLASSISN,
       RD.DEPTNAME,
       RD.DEPTBNAME,
       RD.DEPT0ISN,
       RD.DEPT0NAME,
       RD.RCISN,
       RD.RCNAME,
       RD.FILNAME,
       RD.FILISN,
       RD.DEPT1NAME,
       RD.DEPT1ISN,
       RD.DEPTCITYISN,
       RD.FILCITYISN,
       RD.OISN,
       RD.ONAME,
       RD.DEPTTYPE,
       RD.DOISN,
       RD.DONAME,
       RD.DEPT2ISN,
       RD.DEPT2NAME,
       RD.UPRISN
  from STORAGE_SOURCE.REP_DEPT RD
  
  union all
  
  select 
       0 ISN,
       0 LOADISN,
       0 DEPTISN,
       'Y' DEPTACTIVE,
       null CLASSISN,
       'Москва' DEPTNAME,
       'Москва' DEPTBNAME,
       0 DEPT0ISN,
       'Москва' DEPT0NAME,
       null RCISN,
       'Москва' RCNAME,
       'Москва' FILNAME,
       null FILISN,
       'Москва' DEPT1NAME,
       0 DEPT1ISN,
       3783 DEPTCITYISN,
       null FILCITYISN,
       0 OISN,
       'Москва' ONAME,
       'ЦО' DEPTTYPE,
       0 DOISN,
       'Москва' DONAME,
       0 DEPT2ISN,
       'Москва' DEPT2NAME,
       0 UPRISN
from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_EXT_DOC_STATUS" ("ISN", "PARENTISN", "CODE", "N_CHILDREN", "FILTERISN", "SHORTNAME", "FULLNAME", "TABLENAME", "CONSTNAME", "ACTIVE", "UPDATED", "UPDATEDBY", "SYNISN") AS 
  select d."ISN",d."PARENTISN",d."CODE",d."N_CHILDREN",d."FILTERISN",d."SHORTNAME",d."FULLNAME",d."TABLENAME",d."CONSTNAME",d."ACTIVE",d."UPDATED",d."UPDATEDBY",d."SYNISN" from dicti d where d.parentisn = 1372023203 --c.get('PrDocFileStatus');

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_FILCITY" ("FILCITYISN", "CITYNAME", "REGIONISN", "REGIONNAME", "COUNTRYISN", "COUNTRYNAME") AS 
  (
Select
distinct c.Isn DEPTCITYIsn,c.SHORTNAME CITYNAME ,
c.regionisn,rg.shortname regionname, c.countryisn,cn.shortname countryname
from storages.rep_dept rd, city c,region rg,country cn
Where
rd.deptcityisn=c.isn
and c.regionisn=rg.isn(+) and c.countryisn=cn.isn(+)
union all
Select 0 ,'Нет Данных',0,'Нет Данных',0,'Нет Данных' from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_FRANCHTYPE" ("FRANCHTYPE", "SHORTNAME") AS 
  (
Select 'Б' FRANCHTYPE,'Без условная' SHORTNAME from dual
union all
Select 'У' FRANCHTYPE,'Условная' SHORTNAME from dual
union all
Select ' ','Нет данных'  from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_FROM_STRAIGHT_SALE" ("ISN", "FROM_STRAIGHT_SALE") AS 
  select 1 ISN, 'Y' FROM_STRAIGHT_SALE from dual
  union all
select 2, 'N' from dual
  union all
select 0, '<не задано>' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_GM" ("GMISN", "PARENTISN", "SHORTNAME", "PARENTNAME") AS 
  select isn, 2255842303 parentisn, shortname, 'GM' parentname 
from dicti z start with isn = 2255842303 connect by prior isn = parentisn
union 
select 0, 0, 'Не определено', 'Не определено' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_GUILTY" ("GUILTY", "SHORTNAME") AS 
  Select 'N' GUILTY,'Невиновен' shortname  from dual
 union all
Select 'Y' ,'Виновен' shortname  from dual
 union all
Select '0', cognos_const.get_value('EMPTY_CHAR')  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_G_REGION" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_SHORTNAME") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM AIS.REGION
  START WITH PARENTISN IN (0)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM AIS.REGION D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (0)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_ADDTYPE" ("ISN", "SHORTNAME", "LEV1_ISN", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_SHORTNAME") AS 
  (-- расшифровка "тип аддендума"
SELECT
NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))AS ISN, -- PK
NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))AS SHORTNAME,
 NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,
NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') LEV1_SHORTNAME,
 NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,
NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) LEV2_SHORTNAME
FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 34710216)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (34710216) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_AGENT_CATEGORY" ("ISN", "SHORTNAME", "LEV1_ISN", "LEV1_SHORTNAME") AS 
  SELECT
NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )AS ISN, -- PK
NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')AS SHORTNAME,
 NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,
NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') LEV1_SHORTNAME
FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ CARDINALITY (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ CARDINALITY (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 2291580903)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (2291580903) THEN 0 ELSE PARENTISN END = ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_AGRCLASSISN" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "LEV1_CODE", "ТИП ДОГОВОРА  УРОВЕНЬ 1 ", "LEV2_ISN", "LEV2_CODE", "ТИП ДОГОВОРА  УРОВЕНЬ 2 ") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (34711216)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV2_CODE, ' НЕТ ДАННЫХ ') as LEV2_CODE,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_CODE, LEV1_CODE, ' НЕТ ДАННЫХ ') as LEV2_CODE,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,CODE)) as LEV1_CODE,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,CODE)) as LEV2_CODE,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        D.CODE,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (34711216)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_CODE,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_CODE,
  t.SHORTNAME as LEV2_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_AGRRULE" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 1", "LEV2_ISN", "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 2", "LEV3_ISN", "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 3", "LEV4_ISN", "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 4", "LEV5_ISN", "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 5", "LEV6_ISN", "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 6") AS 
  WITH ISNS AS (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF
  FROM DICTI
  START WITH PARENTISN = 24890816
  CONNECT BY PRIOR ISN = PARENTISN
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV6_IS_LEAF, LEV5_IS_LEAF, LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, 0) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 1",

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 2",

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 3",

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 4",

  coalesce(LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV5_ISN,
  coalesce(LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 5",

  coalesce(LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV6_ISN,
  coalesce(LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОДУКТ ДОГОВОРА  УРОВЕНЬ 6"
FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,SHORTNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF,

    MAX(DECODE(LEV,5,ISN)) as LEV5_ISN,
    MAX(DECODE(LEV,5,SHORTNAME)) as LEV5_SHORTNAME,
    MAX(DECODE(LEV,5,IS_LEAF)) as LEV5_IS_LEAF,

    MAX(DECODE(LEV,6,ISN)) as LEV6_ISN,
    MAX(DECODE(LEV,6,SHORTNAME)) as LEV6_SHORTNAME,
    MAX(DECODE(LEV,6,IS_LEAF)) as LEV6_IS_LEAF
  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR DECODE(D.PARENTISN, 24890816, 0, D.PARENTISN) = D.ISN
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL

select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_SHORTNAME,

  t.ISN as LEV3_ISN,
  t.SHORTNAME as LEV3_SHORTNAME,

  t.ISN as LEV4_ISN,
  t.SHORTNAME as LEV4_SHORTNAME,

  t.ISN as LEV5_ISN,
  t.SHORTNAME as LEV5_SHORTNAME,

  t.ISN as LEV6_ISN,
  t.SHORTNAME as LEV6_SHORTNAME
from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_AGRSTATUS" ("LEV1_ISN", "СТАТУС ДОГОВОРА  УРОВЕНЬ 1 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "СТАТУС ДОГОВОРА  УРОВЕНЬ 1 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 601)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (601) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_BUH_OPER" ("ISN", "SHORTNAME", "LEV1_ISN", "БУХ ОПЕРАЦИЯ УРОВЕНЬ 1", "LEV2_ISN", "БУХ ОПЕРАЦИЯ УРОВЕНЬ 2", "БУХ ОПЕРАЦИЯ КОД УРОВЕНЬ 2") AS 
  (

SELECT /*+ Cardinality (5000) */
NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))AS ISN, -- PK
NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))AS SHORTNAME,
 NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,
NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "БУХ ОПЕРАЦИЯ УРОВЕНЬ 1",
 NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,
NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "БУХ ОПЕРАЦИЯ УРОВЕНЬ 2",
NVL(MAX(DECODE(LEV,2,CODE,NULL)),NVL(MAX(DECODE(LEV,1,CODE,NULL)),' НЕТ ДАННЫХ ')) "БУХ ОПЕРАЦИЯ КОД УРОВЕНЬ 2"
FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
 ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT,CODE
FROM DICTI D
START WITH ISN IN

(
SELECT /*+ CARDINALITY (100)*/
 ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 9533816)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (9533816) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT

);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_CAR_TARIFF_GROUPKA2003" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "LEV1_CODE", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_CODE", "LEV2_SHORTNAME") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (797677700,797676000,797661200)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)
  
SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_CODE, ' НЕТ ДАННЫХ ') as LEV1_CODE,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME, 

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_CODE, LEV1_CODE, ' НЕТ ДАННЫХ ') as LEV2_CODE,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,
    
    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,CODE)) as LEV1_CODE,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF, 

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,CODE)) as LEV2_CODE,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        D.CODE,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (797677700,797676000,797661200)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,
  
  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_CODE,
  t.SHORTNAME as LEV1_SHORTNAME, 

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_CODE,
  t.SHORTNAME as LEV2_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_CITY" ("CITY_ISN", "CITY_NAME", "CITY_SOCR", "CITY_CLASSISN", "COUNTRY_ISN", "COUNTRY_NAME", "REGION_ISN", "REGION_NAME", "DISTRICT_ISN", "DISTRICT_NAME") AS 
  select
  C.ISN as CITY_ISN,
  C.SHORTNAME as CITY_NAME,
  C.SOCR as CITY_SOCR,
  C.CLASSISN as CITY_CLASSISN,

  CTR.ISN as COUNTRY_ISN,
  CTR.SHORTNAME as COUNTRY_NAME,

  nvl(RG.REGION_ISN, 0) as REGION_ISN,
  nvl(RG.REGION_NAME, '<регион не задан>') as REGION_NAME,

  nvl(RG.DISTRICT_ISN, 0) as DISTRICT_ISN,
  -- для городов, не привязанных к районам выводим название города
  nvl(decode(RG.REGION_ISN, RG.DISTRICT_ISN, C.SHORTNAME, RG.DISTRICT_NAME), '<район не задан>') as DISTRICT_NAME

from
  AIS.CITY C,
  AIS.COUNTRY CTR,
  COGNOS.V_DIC_H_DISTRICTS RG
where
  C.COUNTRYISN = CTR.ISN
  and nvl(C.ACTIVE, 'S') <> 'S'
  and C.REGIONISN = RG.District_ISN(+)

UNION ALL

select
  t.ISN as CITY_ISN,      t.SHORTNAME as CITY_NAME, null as CITY_SOCR, to_number(null) as CITY_CLASSISN,
  t.ISN as COUNTRY_ISN,   t.SHORTNAME as COUNTRY_NAME,
  t.ISN as REGION_ISN,    t.SHORTNAME as REGION_NAME,
  t.ISN as DISTRICT_ISN,  t.SHORTNAME as DISTRICT_NAME
from (
  select 0 as ISN, '<не задано>' as SHORTNAME from dual
  union all
  select -1 as ISN, '<все>' as SHORTNAME from dual
) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_CLAIMCLASSISN" ("LEV1_ISN", "ТИП УБЫТКОВ  УРОВЕНЬ 1 ", "LEV2_ISN", "ТИП УБЫТКОВ  УРОВЕНЬ 2 ", "LEV3_ISN", "ТИП УБЫТКОВ  УРОВЕНЬ 3 ", "LEV4_ISN", "ТИП УБЫТКОВ  УРОВЕНЬ 4 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "ТИП УБЫТКОВ  УРОВЕНЬ 1 ", NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "ТИП УБЫТКОВ  УРОВЕНЬ 2 ", NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,  NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))) "ТИП УБЫТКОВ  УРОВЕНЬ 3 ", NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )))) LEV4_ISN,  NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')))) "ТИП УБЫТКОВ  УРОВЕНЬ 4 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN = 2886479203
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR DECODE(PARENTISN,2886479203,0,PARENTISN)=ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_CLAIMINVOICELINE_CLASS" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_SHORTNAME", "LEV3_ISN", "LEV3_SHORTNAME") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (959954725)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)
  
SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME, 

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME, 

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,
    
    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF, 

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF, 

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (959954725)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,
  
  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME, 

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_SHORTNAME, 

  t.ISN as LEV3_ISN,
  t.SHORTNAME as LEV3_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_DEPTS_ALL" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_SHORTNAME", "LEV3_ISN", "LEV3_SHORTNAME", "LEV4_ISN", "LEV4_SHORTNAME", "LEV5_ISN", "LEV5_SHORTNAME", "LEV6_ISN", "LEV6_SHORTNAME", "LEV7_ISN", "LEV7_SHORTNAME", "LEV8_ISN", "LEV8_SHORTNAME") AS 
  WITH ISNS AS (
              SELECT
                     s.*,
                     CONNECT_BY_ISLEAF as IS_LEAF, LEVEL LV
              FROM AIS.SUBDEPT_T s
              START WITH s.PARENTISN=0
              CONNECT BY PRIOR ISN = ParentIsn
            )
            --SELECT COUNT(1) FROM isns; -- 7 775
SELECT
  S.ROOT as ISN,
  S.ROOT_FULLNAME as SHORTNAME,
  coalesce(LEV8_IS_LEAF, LEV7_IS_LEAF, LEV6_IS_LEAF, LEV5_IS_LEAF, LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME,

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV4_SHORTNAME,

  coalesce(LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV5_ISN,
  coalesce(LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV5_SHORTNAME,

  coalesce(LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV6_ISN,
  coalesce(LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV6_SHORTNAME,

  coalesce(LEV7_ISN, LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV7_ISN,
  coalesce(LEV7_SHORTNAME, LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV7_SHORTNAME,

  coalesce(LEV8_ISN, LEV7_ISN, LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV8_ISN,
  coalesce(LEV8_SHORTNAME, LEV7_SHORTNAME, LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV8_SHORTNAME


FROM (
  SELECT
    S.ROOT,
    S.ROOT_FULLNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,FULLNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,FULLNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,FULLNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,FULLNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF,

    MAX(DECODE(LEV,5,ISN)) as LEV5_ISN,
    MAX(DECODE(LEV,5,FULLNAME)) as LEV5_SHORTNAME,
    MAX(DECODE(LEV,5,IS_LEAF)) as LEV5_IS_LEAF,

    MAX(DECODE(LEV,6,ISN)) as LEV6_ISN,
    MAX(DECODE(LEV,6,FULLNAME)) as LEV6_SHORTNAME,
    MAX(DECODE(LEV,6,IS_LEAF)) as LEV6_IS_LEAF,

    MAX(DECODE(LEV,7,ISN)) as LEV7_ISN,
    MAX(DECODE(LEV,7,FULLNAME)) as LEV7_SHORTNAME,
    MAX(DECODE(LEV,7,IS_LEAF)) as LEV7_IS_LEAF,

    MAX(DECODE(LEV,8,ISN)) as LEV8_ISN,
    MAX(DECODE(LEV,8,FULLNAME)) as LEV8_SHORTNAME,
    MAX(DECODE(LEV,8,IS_LEAF)) as LEV8_IS_LEAF

  from (
    select
          S.*,
          ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
          ISNS.IS_LEAF
    from
         ( SELECT
              D.ISN,
              D.SHORTNAME,
              D.FULLNAME,
              LEVEL as levl,
              CONNECT_BY_ROOT (D.ISN) as ROOT,
              CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME,
              CONNECT_BY_ROOT (D.FULLNAME) as ROOT_FULLNAME
            FROM AIS.SUBDEPT_T D
            START WITH isn IN (SELECT ISN from ISNS)
            CONNECT BY PRIOR
              D.PARENTISN = D.ISN
         ) S, ISNS
    WHERE S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_FULLNAME
) S

UNION ALL

select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,
  t.ISN as LEV1_ISN, t.SHORTNAME as LEV1_SHORTNAME,
  t.ISN as LEV2_ISN, t.SHORTNAME as LEV2_SHORTNAME,
  t.ISN as LEV3_ISN, t.SHORTNAME as LEV3_SHORTNAME,
  t.ISN as LEV4_ISN, t.SHORTNAME as LEV4_SHORTNAME,
  t.ISN as LEV5_ISN, t.SHORTNAME as LEV5_SHORTNAME,
  t.ISN as LEV6_ISN, t.SHORTNAME as LEV6_SHORTNAME,
  t.ISN as LEV7_ISN, t.SHORTNAME as LEV7_SHORTNAME,
  t.ISN as LEV8_ISN, t.SHORTNAME as LEV8_SHORTNAME
FROM (select 0 as ISN, '<не задано>' as Shortname from dual) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_DISCOUNTDET" ("LEV1_ISN", "СКИДКИ НА ДЕТАЛИ УРОВЕНЬ 1") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "СКИДКИ НА ДЕТАЛИ УРОВЕНЬ 1"

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 1522535203)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (1522535203) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_DISTRICTS" ("DISTRICT_ISN", "DISTRICT_NAME", "REGION_ISN", "REGION_NAME") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM REGION
  START WITH PARENTISN IN ((nvl(ParentISN, 0)))
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)
  
SELECT   /*+ Cardinality (10000) */
  -- Район
  S.ROOT as District_ISN,
  S.ROOT_SHORTNAME as District_Name,
  -- Субъект РФ
  LEV1_ISN as Region_ISN,
  LEV1_SHORTNAME as Region_Name
FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,
    
    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF, 

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM REGION D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN ((nvl(ParentISN, 0)))
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_DOCSUMTYPE" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "ТИП СУММЫ  УРОВЕНЬ 1", "LEV2_ISN", "ТИП СУММЫ  УРОВЕНЬ 2", "LEV3_ISN", "ТИП СУММЫ  УРОВЕНЬ 3") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (610)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (610)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_SHORTNAME,

  t.ISN as LEV3_ISN,
  t.SHORTNAME as LEV3_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_FAMILYSTATE" ("LEV1_ISN", "СЕМЕЙНОЕ ПОЛОЖ.  УРОВЕНЬ 1 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "СЕМЕЙНОЕ ПОЛОЖ.  УРОВЕНЬ 1 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 11275519)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (11275519) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_GPHAGRCLASSISN" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "ТИП ДОГОВОРА ГПХ уровень 1", "LEV2_ISN", "ТИП ДОГОВОРА ГПХ уровень 2", "LEV3_ISN", "ТИП ДОГОВОРА ГПХ уровень 3") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (12415216)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ТИП ДОГОВОРА ГПХ уровень 1",

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ТИП ДОГОВОРА ГПХ уровень 2",

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ТИП ДОГОВОРА ГПХ уровень 3"

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (12415216)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_SHORTNAME,

  t.ISN as LEV3_ISN,
  t.SHORTNAME as LEV3_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_JURTYPE" ("LEV1_ISN", "ТИП  УРОВЕНЬ 1 ", "LEV2_ISN", "ТИП  УРОВЕНЬ 2 ", "LEV3_ISN", "ТИП  УРОВЕНЬ 3 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "ТИП  УРОВЕНЬ 1 ", NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "ТИП  УРОВЕНЬ 2 ", NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,  NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))) "ТИП  УРОВЕНЬ 3 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 407,410)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (407,410) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_MEDOBJCLASS" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "КЛАСС ОБЪЕКТА уровень 1", "LEV2_ISN", "КЛАСС ОБЪЕКТА уровень 2") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (1098082203)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "КЛАСС ОБЪЕКТА уровень 1",

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "КЛАСС ОБЪЕКТА уровень 2"

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (1098082203)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_MOTIVGROUP" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "ЛИНИИ БИЗНЕСА УРОВЕНЬ 1", "LEV2_ISN", "ЛИНИИ БИЗНЕСА УРОВЕНЬ 2") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (1342981303)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (1342981303)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_MOTORDETAIL" ("LEV1_ISN", "ДЕТАЛИ А/М  УРОВЕНЬ 1 ", "LEV2_ISN", "ДЕТАЛИ А/М  УРОВЕНЬ 2 ", "LEV3_ISN", "ДЕТАЛИ А/М  УРОВЕНЬ 3 ", "LEV4_ISN", "ДЕТАЛИ А/М  УРОВЕНЬ 4 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "ДЕТАЛИ А/М  УРОВЕНЬ 1 ", NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "ДЕТАЛИ А/М  УРОВЕНЬ 2 ", NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,  NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))) "ДЕТАЛИ А/М  УРОВЕНЬ 3 ", NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )))) LEV4_ISN,  NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')))) "ДЕТАЛИ А/М  УРОВЕНЬ 4 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 27763216)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (27763216) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_MOTOR_MAKE" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "МАРКА А/М  УРОВЕНЬ 1 ", "LEV2_ISN", "МАРКА А/М  УРОВЕНЬ 2 ", "LEV3_ISN", "МАРКА А/М  УРОВЕНЬ 3 ", "LEV4_ISN", "МАРКА А/М  УРОВЕНЬ 4 ") AS 
  WITH ISNS AS (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF
  FROM DICTI
  WHERE ACTIVE IS NOT NULL  -- sts 14.13.2012 - выдрал из АИС
  START WITH
    PARENTISN IN (604607816)
    and ACTIVE IS NOT NULL  -- sts 14.13.2012 - выдрал из АИС
  CONNECT BY PRIOR ISN = PARENTISN
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME,

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV4_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,SHORTNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      WHERE ACTIVE IS NOT NULL  -- sts 14.13.2012 - выдрал из АИС
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and D.ISN NOT IN (604607816)
        and ACTIVE IS NOT NULL  -- sts 14.13.2012 - выдрал из АИС
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL

select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,
  t.ISN as LEV1_ISN, t.SHORTNAME as LEV1_SHORTNAME,
  t.ISN as LEV2_ISN, t.SHORTNAME as LEV2_SHORTNAME,
  t.ISN as LEV3_ISN, t.SHORTNAME as LEV3_SHORTNAME,
  t.ISN as LEV4_ISN, t.SHORTNAME as LEV4_SHORTNAME
from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_MOTOR_MODEL" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "МОДЕЛЬ А/М  УРОВЕНЬ 1 ", "LEV2_ISN", "МОДЕЛЬ А/М  УРОВЕНЬ 2 ", "LEV3_ISN", "МОДЕЛЬ А/М  УРОВЕНЬ 3 ") AS 
  WITH ISNS AS (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF
  FROM DICTI
  WHERE ACTIVE IS NOT NULL  -- sts 14.13.2012 - выдрал из АИС
  START WITH
    PARENTISN IN (8240)
    and ACTIVE IS NOT NULL  -- sts 14.13.2012 - выдрал из АИС
  CONNECT BY PRIOR ISN = PARENTISN
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      WHERE ACTIVE IS NOT NULL  -- sts 14.13.2012 - выдрал из АИС
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and D.ISN NOT IN (8240)
        and ACTIVE IS NOT NULL  -- sts 14.13.2012 - выдрал из АИС
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL

select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,
  t.ISN as LEV1_ISN, t.SHORTNAME as LEV1_SHORTNAME,
  t.ISN as LEV2_ISN, t.SHORTNAME as LEV2_SHORTNAME,
  t.ISN as LEV3_ISN, t.SHORTNAME as LEV3_SHORTNAME
from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_MSFO_SUBACC" ("ISN", "SHORTNAME", "DATEBEG", "DATEEND", "LAST_ITEM_IS_LEAF", "LAST_ITEM_ID", "LEV1_ISN", "СУБСЧЕТ УРОВЕНЬ 1", "СУБСЧЕТ АНГ УРОВЕНЬ 1", "КОД СУБСЧЕТ УРОВЕНЬ 1", "LEV2_ISN", "СУБСЧЕТ УРОВЕНЬ 2", "СУБСЧЕТ АНГ УРОВЕНЬ 2", "КОД СУБСЧЕТ УРОВЕНЬ 2", "LEV3_ISN", "СУБСЧЕТ УРОВЕНЬ 3", "СУБСЧЕТ АНГ УРОВЕНЬ 3", "КОД СУБСЧЕТ УРОВЕНЬ 3", "LEV4_ISN", "СУБСЧЕТ УРОВЕНЬ 4", "СУБСЧЕТ АНГ УРОВЕНЬ 4", "КОД СУБСЧЕТ УРОВЕНЬ 4") AS 
  With BSACC AS

(
 Select
*
From
AIS.BUHSUBACC_T BA
WHERE
nvl(classisn,5)=5
And Nvl(Active,'S')<>'S'
and
(
 (DATEEND >TO_DATE('31/12/2001','DD/MM/YYYY')
OR
EXISTS (
SELECT ISN
FROM AIS.BUHSUBACC BS
WHERE BS.ID=BA.ID AND BS.DATEBEG=TO_DATE('01/01/2002','DD/MM/YYYY')
)
)
)

),

 ISNS AS (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF
  FROM BSACC
  START WITH PARENTISN = 3553325003
  CONNECT BY PRIOR ISN = PARENTISN
)

SELECT   /*+ Cardinality (10000) */
    
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  to_date(datebeg,'DD.MM.RRRR') datebeg,
  to_date(dateend,'DD.MM.RRRR') dateend,

  coalesce( LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, 0) as LAST_ITEM_IS_LEAF,
  coalesce( LEV4_ID, LEV3_ID, LEV2_ID, LEV1_ID, '') as LAST_ITEM_ID,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "СУБСЧЕТ УРОВЕНЬ 1",
  coalesce(LEV1_FULLNAME, ' НЕТ ДАННЫХ ') as "СУБСЧЕТ АНГ УРОВЕНЬ 1",
  coalesce(LEV1_ID, '') as "КОД СУБСЧЕТ УРОВЕНЬ 1",

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "СУБСЧЕТ УРОВЕНЬ 2",
  coalesce(LEV2_FULLNAME, LEV1_FULLNAME, ' НЕТ ДАННЫХ ') as "СУБСЧЕТ АНГ УРОВЕНЬ 2",
  coalesce(LEV2_ID, LEV1_ID, '') as "КОД СУБСЧЕТ УРОВЕНЬ 2",

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "СУБСЧЕТ УРОВЕНЬ 3",
  coalesce(LEV3_FULLNAME, LEV2_FULLNAME, LEV1_FULLNAME, ' НЕТ ДАННЫХ ') as "СУБСЧЕТ АНГ УРОВЕНЬ 3",
  coalesce(  LEV3_ID, LEV2_ID, LEV1_ID, '') as "КОД СУБСЧЕТ УРОВЕНЬ 3",

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "СУБСЧЕТ УРОВЕНЬ 4",
  coalesce(LEV4_FULLNAME, LEV3_FULLNAME, LEV2_FULLNAME, LEV1_FULLNAME, ' НЕТ ДАННЫХ ') as "СУБСЧЕТ АНГ УРОВЕНЬ 4",
  coalesce( LEV4_ID, LEV3_ID, LEV2_ID, LEV1_ID, '') as "КОД СУБСЧЕТ УРОВЕНЬ 4"
FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,
    datebeg,
    dateend,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,FULLNAME)) as LEV1_FULLNAME,
    MAX(DECODE(LEV,1,ID)) as LEV1_ID,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,FULLNAME)) as LEV2_FULLNAME,
    MAX(DECODE(LEV,2,ID)) as LEV2_ID,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,FULLNAME)) as LEV3_FULLNAME,
    MAX(DECODE(LEV,3,ID)) as LEV3_ID,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,SHORTNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,FULLNAME)) as LEV4_FULLNAME,
    MAX(DECODE(LEV,4,ID)) as LEV4_ID,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        D.FULLNAME,
        ID,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME,
        CONNECT_BY_ROOT (D.DATEBEG) as DATEBEG,
        CONNECT_BY_ROOT (D.DATEEND) as DATEEND

      FROM BSACC D
      START WITH isn IN (SELECT ISN from ISNS WHERE IS_LEAF=1)
      CONNECT BY PRIOR DECODE(D.PARENTISN, 3553325003, 0, D.PARENTISN) = D.ISN
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME, s.datebeg, s.dateend
) S
ORDER BY "КОД СУБСЧЕТ УРОВЕНЬ 4";

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_OBJECT_CLASS" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "ОБЪЕКТКЛАСС УРОВЕНЬ 1 ", "LEV2_ISN", "ОБЪЕКТКЛАСС УРОВЕНЬ 2 ", "LEV3_ISN", "ОБЪЕКТКЛАСС УРОВЕНЬ 3 ", "LEV4_ISN", "ОБЪЕКТКЛАСС УРОВЕНЬ 4 ", "LEV5_ISN", "ОБЪЕКТКЛАСС УРОВЕНЬ 5 ", "LEV6_ISN", "ОБЪЕКТКЛАСС УРОВЕНЬ 6 ") AS 
  WITH ISNS AS (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF
  FROM DICTI
  START WITH PARENTISN IN (2002)
  CONNECT BY PRIOR ISN = PARENTISN
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV6_IS_LEAF, LEV5_IS_LEAF, LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME,

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV4_SHORTNAME,

  coalesce(LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV5_ISN,
  coalesce(LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV5_SHORTNAME,

  coalesce(LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV6_ISN,
  coalesce(LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV6_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,SHORTNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF,

    MAX(DECODE(LEV,5,ISN)) as LEV5_ISN,
    MAX(DECODE(LEV,5,SHORTNAME)) as LEV5_SHORTNAME,
    MAX(DECODE(LEV,5,IS_LEAF)) as LEV5_IS_LEAF,

    MAX(DECODE(LEV,6,ISN)) as LEV6_ISN,
    MAX(DECODE(LEV,6,SHORTNAME)) as LEV6_SHORTNAME,
    MAX(DECODE(LEV,6,IS_LEAF)) as LEV6_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and D.ISN NOT IN (2002)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_SHORTNAME,

  t.ISN as LEV3_ISN,
  t.SHORTNAME as LEV3_SHORTNAME,

  t.ISN as LEV4_ISN,
  t.SHORTNAME as LEV4_SHORTNAME,

  t.ISN as LEV5_ISN,
  t.SHORTNAME as LEV5_SHORTNAME,

  t.ISN as LEV6_ISN,
  t.SHORTNAME as LEV6_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_OPER_DEPTS_ALL" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_SHORTNAME", "LEV3_ISN", "LEV3_SHORTNAME", "LEV4_ISN", "LEV4_SHORTNAME", "LEV5_ISN", "LEV5_SHORTNAME", "LEV6_ISN", "LEV6_SHORTNAME", "LEV7_ISN", "LEV7_SHORTNAME") AS 
  WITH ISNS AS (
SELECT /*+ Cardinality (100) */
     s.*,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM AIS.SUBDEPT_T s
  WHERE
    NVL(active, 'S') <> 'S'
    and (
      classisn is not null
      or
      connect_by_root(ISN) in (
        655904816,   -- АГЕНТСКАЯ СЕТЬ
        28763316     -- РЕГИОНАЛЬНЫЕ ОРГАНИЗАЦИИ
      )
      or connect_by_root(ClassISN) in (
        11296719,      -- ОПЕРАТИВНОЕ
        2758435703     -- ФУНКЦИОНАЛЬНО-ОПЕРАТИВНОЕ
      )
    )

  START WITH PARENTISN IN (0)
    AND NVL(active, 'S') <> 'S'
    -- для агентской сети подставляем ClassISN = РЕГИОНЫ (чтобы отобрать дополнительно агентскую сеть, а не все дочерние)
    and decode(s.isn, 655904816, 2757256203, s.classisn) in (
      11296719,      -- ОПЕРАТИВНОЕ
      2758435703,    -- ФУНКЦИОНАЛЬНО-ОПЕРАТИВНОЕ
      2757256203,    -- РЕГИОНЫ
      956868025      -- ДОП. ОФИС
    )
  CONNECT BY PRIOR ISN = ParentIsn
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_FULLNAME as SHORTNAME,
  coalesce(LEV7_IS_LEAF, LEV6_IS_LEAF, LEV5_IS_LEAF, LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME,

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV4_SHORTNAME,

  coalesce(LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV5_ISN,
  coalesce(LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV5_SHORTNAME,

  coalesce(LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV6_ISN,
  coalesce(LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV6_SHORTNAME,

  coalesce(LEV7_ISN, LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV7_ISN,
  coalesce(LEV7_SHORTNAME, LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV7_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_FULLNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,FULLNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,FULLNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,FULLNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,FULLNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF,

    MAX(DECODE(LEV,5,ISN)) as LEV5_ISN,
    MAX(DECODE(LEV,5,FULLNAME)) as LEV5_SHORTNAME,
    MAX(DECODE(LEV,5,IS_LEAF)) as LEV5_IS_LEAF,

    MAX(DECODE(LEV,6,ISN)) as LEV6_ISN,
    MAX(DECODE(LEV,6,FULLNAME)) as LEV6_SHORTNAME,
    MAX(DECODE(LEV,6,IS_LEAF)) as LEV6_IS_LEAF,

    MAX(DECODE(LEV,7,ISN)) as LEV7_ISN,
    MAX(DECODE(LEV,7,FULLNAME)) as LEV7_SHORTNAME,
    MAX(DECODE(LEV,7,IS_LEAF)) as LEV7_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        D.FULLNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME,
        CONNECT_BY_ROOT (D.FULLNAME) as ROOT_FULLNAME
      FROM AIS.SUBDEPT_T D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_FULLNAME
) S

UNION ALL

select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,
  t.ISN as LEV1_ISN, t.SHORTNAME as LEV1_SHORTNAME,
  t.ISN as LEV2_ISN, t.SHORTNAME as LEV2_SHORTNAME,
  t.ISN as LEV3_ISN, t.SHORTNAME as LEV3_SHORTNAME,
  t.ISN as LEV4_ISN, t.SHORTNAME as LEV4_SHORTNAME,
  t.ISN as LEV5_ISN, t.SHORTNAME as LEV5_SHORTNAME,
  t.ISN as LEV6_ISN, t.SHORTNAME as LEV6_SHORTNAME,
  t.ISN as LEV7_ISN, t.SHORTNAME as LEV7_SHORTNAME
from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_OPER_DEPTS_GO" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "ПОДРАЗДЕЛЕНИЕ УРОВЕНЬ 1", "LEV2_ISN", "ПОДРАЗДЕЛЕНИЕ УРОВЕНЬ 2", "LEV3_ISN", "ПОДРАЗДЕЛЕНИЕ УРОВЕНЬ 3", "LEV4_ISN", "ПОДРАЗДЕЛЕНИЕ УРОВЕНЬ 4", "LEV5_ISN", "ПОДРАЗДЕЛЕНИЕ УРОВЕНЬ 5") AS 
  WITH ISNS AS (
-- sts - 12.11.2012 13:45 - переписал, т.к. старый код давал замножение некоторых строк
  SELECT /*+ Cardinality (100) */
     s.*,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM AIS.SUBDEPT_T s
  WHERE NVL(active, 'S') <> 'S'
  START WITH PARENTISN IN (0)
    AND NVL (active, 'S') <> 'S'
    AND classisn In (11296719,2758435703)
  CONNECT BY PRIOR
          Case When shortname LIKE '%ОТДЕЛ%' Then null
          else isn  end = ParentIsn
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_FULLNAME as SHORTNAME,
  coalesce(LEV5_IS_LEAF, LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME,

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV4_SHORTNAME,

  coalesce(LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV5_ISN,
  coalesce(LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV5_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_FULLNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,FULLNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,FULLNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,FULLNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,FULLNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF,

    MAX(DECODE(LEV,5,ISN)) as LEV5_ISN,
    MAX(DECODE(LEV,5,FULLNAME)) as LEV5_SHORTNAME,
    MAX(DECODE(LEV,5,IS_LEAF)) as LEV5_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        D.FULLNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME,
        CONNECT_BY_ROOT (D.FULLNAME) as ROOT_FULLNAME
      FROM AIS.SUBDEPT_T D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and D.ISN NOT IN (0)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_FULLNAME
) S

UNION ALL

select
  0 ISN,
  '<не задано>' as SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,
  0 as LEV1_ISN, '<не задано>' as LEV1_SHORTNAME,
  0 as LEV2_ISN, '<не задано>' as LEV2_SHORTNAME,
  0 as LEV3_ISN, '<не задано>' as LEV3_SHORTNAME,
  0 as LEV4_ISN, '<не задано>' as LEV4_SHORTNAME,
  0 as LEV5_ISN, '<не задано>' as LEV5_SHORTNAME
from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_OPER_DEPTS_REG" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_SHORTNAME", "LEV3_ISN", "LEV3_SHORTNAME", "LEV4_ISN", "LEV4_SHORTNAME", "FILISN", "RCISN") AS 
  WITH ISNS AS (
-- sts - 12.11.2012 14:15 - переписал, т.к. старый код давал замножение некоторых строк + не все доп. офисы выгружались (S.ISN = 923092525)
-- Плюс добавил атрибут LAST_ITEM_IS_LEAF по аналогии с V_DIC_H_OPER_DEPTS_GO
 SELECT /*+ Cardinality (100) */
     s.*,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM SUBDEPT s
 WHERE
   CONNECT_BY_ISLEAF = 1
   and  NVL (active, 'S') <>'S'
  START WITH   parentisn = 28763316
               AND NVL (active, 'S') <> 'S'
  CONNECT BY Prior


     Case When  Upper(FULLNAME) Like '%ОТДЕЛ%'
               Or classisn in (3029816703, 956868025)     Then null
          else isn  end   = Parentisn and  NVL (active, 'S') <>'S'


)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_FULLNAME as SHORTNAME,
  coalesce(LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME,

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV4_SHORTNAME,
  
FILISN, RCISN
FROM (
  SELECT
    S.ROOT,

    S.ROOT_FULLNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,FULLNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,FULLNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,FULLNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,FULLNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF,
    MAX(FILISN) FILISN,
    MAX(RCISN) RCISN

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF,
      FILISN,
      RCISN
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        D.FULLNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME,
        CONNECT_BY_ROOT (D.FULLNAME) as ROOT_FULLNAME
      FROM SUBDEPT D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        decode(D.ORGPARENTISN, 0, 0, D.PARENTISN) = D.ISN
    ) S,
      ISNS,
      STORAGE_SOURCE.REP_DEPT RD
    where
      S.ROOT = ISNS.ISN
      and s.ROOT=rd.deptisn(+)
  ) S

  group by S.ROOT, S.ROOT_FULLNAME
) S

UNION ALL

select
  0 ISN,
  '<не задано>' as SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,
  0 as LEV1_ISN, '<не задано>' as LEV1_SHORTNAME,
  0 as LEV2_ISN, '<не задано>' as LEV2_SHORTNAME,
  0 as LEV3_ISN, '<не задано>' as LEV3_SHORTNAME,
  0 as LEV4_ISN, '<не задано>' as LEV4_SHORTNAME,
  0,0
from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_PAYMENTPROPERTY" ("LEV1_ISN", "ВЫПЛАТЫ ПО ИМУЩ.  УРОВЕНЬ 1 ", "LEV2_ISN", "ВЫПЛАТЫ ПО ИМУЩ.  УРОВЕНЬ 2 ", "LEV3_ISN", "ВЫПЛАТЫ ПО ИМУЩ.  УРОВЕНЬ 3 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "ВЫПЛАТЫ ПО ИМУЩ.  УРОВЕНЬ 1 ", NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "ВЫПЛАТЫ ПО ИМУЩ.  УРОВЕНЬ 2 ", NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,  NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))) "ВЫПЛАТЫ ПО ИМУЩ.  УРОВЕНЬ 3 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 959954725)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (959954725) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_PRIOR" ("LEV1_ISN", "ПРИОРИТЕТ СТО УРОВЕНЬ 1") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "ПРИОРИТЕТ СТО УРОВЕНЬ 1"

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 1705799303)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (1705799303) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_PROCESSCLASS" ("LEV1_ISN", "КЛАСС РЕМОНТ.ВОЗД.  УРОВЕНЬ 1 ", "LEV2_ISN", "КЛАСС РЕМОНТ.ВОЗД.  УРОВЕНЬ 2 ", "LEV3_ISN", "КЛАСС РЕМОНТ.ВОЗД.  УРОВЕНЬ 3 ") AS 
  (
SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN, 
 NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "КЛАСС РЕМОНТ.ВОЗД.  УРОВЕНЬ 1 ",
  NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN, 
 NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "КЛАСС РЕМОНТ.ВОЗД.  УРОВЕНЬ 2 ", 
 NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,  
 NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))) "КЛАСС РЕМОНТ.ВОЗД.  УРОВЕНЬ 3 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN = 27882116
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR DECODE(PARENTISN,27882116,0,PARENTISN)=ISN
) S
)
 GROUP BY ROOT
 );

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_PROGRAM_DMS" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "ПРОГРАММА ДМС уровень 1", "LEV2_ISN", "ПРОГРАММА ДМС уровень 2", "LEV3_ISN", "ПРОГРАММА ДМС уровень 3", "LEV4_ISN", "ПРОГРАММА ДМС уровень 4", "LEV5_ISN", "ПРОГРАММА ДМС уровень 5", "LEV6_ISN", "ПРОГРАММА ДМС уровень 6") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (961564825)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV6_IS_LEAF, LEV5_IS_LEAF, LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОГРАММА ДМС уровень 1",

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОГРАММА ДМС уровень 2",

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОГРАММА ДМС уровень 3",

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОГРАММА ДМС уровень 4",

  coalesce(LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV5_ISN,
  coalesce(LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОГРАММА ДМС уровень 5",

  coalesce(LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV6_ISN,
  coalesce(LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ПРОГРАММА ДМС уровень 6"

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,SHORTNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF,

    MAX(DECODE(LEV,5,ISN)) as LEV5_ISN,
    MAX(DECODE(LEV,5,SHORTNAME)) as LEV5_SHORTNAME,
    MAX(DECODE(LEV,5,IS_LEAF)) as LEV5_IS_LEAF,

    MAX(DECODE(LEV,6,ISN)) as LEV6_ISN,
    MAX(DECODE(LEV,6,SHORTNAME)) as LEV6_SHORTNAME,
    MAX(DECODE(LEV,6,IS_LEAF)) as LEV6_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (961564825)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_SHORTNAME,

  t.ISN as LEV3_ISN,
  t.SHORTNAME as LEV3_SHORTNAME,

  t.ISN as LEV4_ISN,
  t.SHORTNAME as LEV4_SHORTNAME,

  t.ISN as LEV5_ISN,
  t.SHORTNAME as LEV5_SHORTNAME,

  t.ISN as LEV6_ISN,
  t.SHORTNAME as LEV6_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_PSS" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "ПСС  УРОВЕНЬ 1 ", "LEV2_ISN", "ПСС  УРОВЕНЬ 2 ", "LEV3_ISN", "ПСС  УРОВЕНЬ 3 ", "LEV4_ISN", "ПСС  УРОВЕНЬ 4 ", "LEV5_ISN", "ПСС  УРОВЕНЬ 5 ") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (805711700)
  CONNECT BY PRIOR
    ISN = PARENTISN
    -- and NVL(ACTIVE, 'S') <> 'S'  -- sts 29.05.2013 - есть архивные ПСС-ки на договорах
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV5_IS_LEAF, LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME,

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV4_SHORTNAME,

  coalesce(LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV5_ISN,
  coalesce(LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV5_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,SHORTNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF,

    MAX(DECODE(LEV,5,ISN)) as LEV5_ISN,
    MAX(DECODE(LEV,5,SHORTNAME)) as LEV5_SHORTNAME,
    MAX(DECODE(LEV,5,IS_LEAF)) as LEV5_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        --and NVL(ACTIVE, 'S') <> 'S'  -- sts 29.05.2013 - есть архивные ПСС-ки на договорах
        and D.ISN NOT IN (805711700)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_SHORTNAME,

  t.ISN as LEV3_ISN,
  t.SHORTNAME as LEV3_SHORTNAME,

  t.ISN as LEV4_ISN,
  t.SHORTNAME as LEV4_SHORTNAME,

  t.ISN as LEV5_ISN,
  t.SHORTNAME as LEV5_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_RCLASSREAL" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "ПОДТВ.КЛАСС УБ.  УРОВЕНЬ 1 ", "LEV2_ISN", "ПОДТВ.КЛАСС УБ.  УРОВЕНЬ 2 ", "LEV3_ISN", "ПОДТВ.КЛАСС УБ.  УРОВЕНЬ 3 ", "LEV4_ISN", "ПОДТВ.КЛАСС УБ.  УРОВЕНЬ 4 ", "LEV5_ISN", "ПОДТВ.КЛАСС УБ.  УРОВЕНЬ 5 ", "LEV6_ISN", "ПОДТВ.КЛАСС УБ.  УРОВЕНЬ 6 ", "LEV7_ISN", "ПОДТВ.КЛАСС УБ.  УРОВЕНЬ 7 ", "LEV8_ISN", "ПОДТВ.КЛАСС УБ.  УРОВЕНЬ 8 ", "LEV9_ISN", "ПОДТВ.КЛАСС УБ.  УРОВЕНЬ 9 ") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (2001,2005,8258,959955425,2183,960203325)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV9_IS_LEAF, LEV8_IS_LEAF, LEV7_IS_LEAF, LEV6_IS_LEAF, LEV5_IS_LEAF, LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME,

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV4_SHORTNAME,

  coalesce(LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV5_ISN,
  coalesce(LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV5_SHORTNAME,

  coalesce(LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV6_ISN,
  coalesce(LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV6_SHORTNAME,

  coalesce(LEV7_ISN, LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV7_ISN,
  coalesce(LEV7_SHORTNAME, LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV7_SHORTNAME,

  coalesce(LEV8_ISN, LEV7_ISN, LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV8_ISN,
  coalesce(LEV8_SHORTNAME, LEV7_SHORTNAME, LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV8_SHORTNAME,

  coalesce(LEV9_ISN, LEV8_ISN, LEV7_ISN, LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV9_ISN,
  coalesce(LEV9_SHORTNAME, LEV8_SHORTNAME, LEV7_SHORTNAME, LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV9_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,SHORTNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF,

    MAX(DECODE(LEV,5,ISN)) as LEV5_ISN,
    MAX(DECODE(LEV,5,SHORTNAME)) as LEV5_SHORTNAME,
    MAX(DECODE(LEV,5,IS_LEAF)) as LEV5_IS_LEAF,

    MAX(DECODE(LEV,6,ISN)) as LEV6_ISN,
    MAX(DECODE(LEV,6,SHORTNAME)) as LEV6_SHORTNAME,
    MAX(DECODE(LEV,6,IS_LEAF)) as LEV6_IS_LEAF,

    MAX(DECODE(LEV,7,ISN)) as LEV7_ISN,
    MAX(DECODE(LEV,7,SHORTNAME)) as LEV7_SHORTNAME,
    MAX(DECODE(LEV,7,IS_LEAF)) as LEV7_IS_LEAF,

    MAX(DECODE(LEV,8,ISN)) as LEV8_ISN,
    MAX(DECODE(LEV,8,SHORTNAME)) as LEV8_SHORTNAME,
    MAX(DECODE(LEV,8,IS_LEAF)) as LEV8_IS_LEAF,

    MAX(DECODE(LEV,9,ISN)) as LEV9_ISN,
    MAX(DECODE(LEV,9,SHORTNAME)) as LEV9_SHORTNAME,
    MAX(DECODE(LEV,9,IS_LEAF)) as LEV9_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (2001,2005,8258,959955425,2183,960203325)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_SHORTNAME,

  t.ISN as LEV3_ISN,
  t.SHORTNAME as LEV3_SHORTNAME,

  t.ISN as LEV4_ISN,
  t.SHORTNAME as LEV4_SHORTNAME,

  t.ISN as LEV5_ISN,
  t.SHORTNAME as LEV5_SHORTNAME,

  t.ISN as LEV6_ISN,
  t.SHORTNAME as LEV6_SHORTNAME,

  t.ISN as LEV7_ISN,
  t.SHORTNAME as LEV7_SHORTNAME,

  t.ISN as LEV8_ISN,
  t.SHORTNAME as LEV8_SHORTNAME,

  t.ISN as LEV9_ISN,
  t.SHORTNAME as LEV9_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_REASON" ("LEV1_ISN", "ПРИЧИНЫ СКИДОК УРОВЕНЬ 1", "LEV2_ISN", "ПРИЧИНЫ СКИДОК УРОВЕНЬ 2", "LEV3_ISN", "ПРИЧИНЫ СКИДОК УРОВЕНЬ 3") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "ПРИЧИНЫ СКИДОК УРОВЕНЬ 1", NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "ПРИЧИНЫ СКИДОК УРОВЕНЬ 2", NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,  NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))) "ПРИЧИНЫ СКИДОК УРОВЕНЬ 3"

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 3095084403)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (3095084403) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_REFFORM" ("LEV1_ISN", "ФОРМА ВОЗМЕЩЕНИЯ  УРОВЕНЬ 1 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "ФОРМА ВОЗМЕЩЕНИЯ  УРОВЕНЬ 1 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 728191616)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (728191616) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_REPAIR_CONDISN" ("LEV1_ISN", "УСЛ.НАПР.НА РЕМОНТ  УРОВЕНЬ 1 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "УСЛ.НАПР.НА РЕМОНТ  УРОВЕНЬ 1 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 3192008903)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (3192008903) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_RISK" ("LEV1_ISN", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_SHORTNAME", "LEV3_ISN", "LEV3_SHORTNAME", "LEV4_ISN", "LEV4_SHORTNAME", "LEV5_ISN", "LEV5_SHORTNAME", "LEV6_ISN", "LEV6_SHORTNAME", "LEV7_ISN", "LEV7_SHORTNAME", "LEV8_ISN", "LEV8_SHORTNAME", "LEV9_ISN", "LEV9_SHORTNAME") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' ??? ?????? ') LEV1_SHORTNAME, NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' ??? ?????? ')) LEV2_SHORTNAME, NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,  NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' ??? ?????? '))) LEV3_SHORTNAME, NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )))) LEV4_ISN,  NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' ??? ?????? ')))) LEV4_SHORTNAME, NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))))) LEV5_ISN,  NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' ??? ?????? '))))) LEV5_SHORTNAME, NVL(MAX(DECODE(LEV,6,ISN,NULL)),NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )))))) LEV6_ISN,  NVL(MAX(DECODE(LEV,6,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' ??? ?????? ')))))) LEV6_SHORTNAME, NVL(MAX(DECODE(LEV,7,ISN,NULL)),NVL(MAX(DECODE(LEV,6,ISN,NULL)),NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))))))) LEV7_ISN,  NVL(MAX(DECODE(LEV,7,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,6,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' ??? ?????? '))))))) LEV7_SHORTNAME, NVL(MAX(DECODE(LEV,8,ISN,NULL)),NVL(MAX(DECODE(LEV,7,ISN,NULL)),NVL(MAX(DECODE(LEV,6,ISN,NULL)),NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )))))))) LEV8_ISN,  NVL(MAX(DECODE(LEV,8,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,7,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,6,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' ??? ?????? ')))))))) LEV8_SHORTNAME, NVL(MAX(DECODE(LEV,9,ISN,NULL)),NVL(MAX(DECODE(LEV,8,ISN,NULL)),NVL(MAX(DECODE(LEV,7,ISN,NULL)),NVL(MAX(DECODE(LEV,6,ISN,NULL)),NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))))))))) LEV9_ISN,  NVL(MAX(DECODE(LEV,9,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,8,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,7,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,6,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' ??? ?????? '))))))))) LEV9_SHORTNAME

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 2005)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (2005) THEN 0 ELSE PARENTISN END =ISN
) S
)
GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_RISK_RULE" ("LEV1_ISN", "ПРАВИЛА СТР  УРОВЕНЬ 1 ", "LEV2_ISN", "ПРАВИЛА СТР  УРОВЕНЬ 2 ", "LEV3_ISN", "ПРАВИЛА СТР  УРОВЕНЬ 3 ", "LEV4_ISN", "ПРАВИЛА СТР  УРОВЕНЬ 4 ", "LEV5_ISN", "ПРАВИЛА СТР  УРОВЕНЬ 5 ", "LEV6_ISN", "ПРАВИЛА СТР  УРОВЕНЬ 6 ", "LEV7_ISN", "ПРАВИЛА СТР  УРОВЕНЬ 7 ") AS 
  (
SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "ПРАВИЛА СТР  УРОВЕНЬ 1 ", NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "ПРАВИЛА СТР  УРОВЕНЬ 2 ", NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,  NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))) "ПРАВИЛА СТР  УРОВЕНЬ 3 ", NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )))) LEV4_ISN,  NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')))) "ПРАВИЛА СТР  УРОВЕНЬ 4 ", NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))))) LEV5_ISN,  NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))))) "ПРАВИЛА СТР  УРОВЕНЬ 5 ", NVL(MAX(DECODE(LEV,6,ISN,NULL)),NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )))))) LEV6_ISN,  NVL(MAX(DECODE(LEV,6,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')))))) "ПРАВИЛА СТР  УРОВЕНЬ 6 ", NVL(MAX(DECODE(LEV,7,ISN,NULL)),NVL(MAX(DECODE(LEV,6,ISN,NULL)),NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))))))) LEV7_ISN,  NVL(MAX(DECODE(LEV,7,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,6,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))))))) "ПРАВИЛА СТР  УРОВЕНЬ 7 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 2001)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (2001) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_RISK_RULE_ENGLCOLUMNS" ("LEV1_ISN", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_SHORTNAME", "LEV3_ISN", "LEV3_SHORTNAME", "LEV4_ISN", "LEV4_SHORTNAME", "LEV5_ISN", "LEV5_SHORTNAME", "LEV6_ISN", "LEV6_SHORTNAME", "LEV7_ISN", "LEV7_SHORTNAME") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') LEV1_SHORTNAME, NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) LEV2_SHORTNAME, NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,  NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))) LEV3_SHORTNAME, NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )))) LEV4_ISN,  NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')))) LEV4_SHORTNAME, NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))))) LEV5_ISN,  NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))))) LEV5_SHORTNAME, NVL(MAX(DECODE(LEV,6,ISN,NULL)),NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )))))) LEV6_ISN,  NVL(MAX(DECODE(LEV,6,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')))))) LEV6_SHORTNAME, NVL(MAX(DECODE(LEV,7,ISN,NULL)),NVL(MAX(DECODE(LEV,6,ISN,NULL)),NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))))))) LEV7_ISN,  NVL(MAX(DECODE(LEV,7,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,6,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))))))) LEV7_SHORTNAME

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 2001)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (2001) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_ROLEIGS" ("LEV1_ISN", "РОЛЬ ИГС  УРОВЕНЬ 1 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "РОЛЬ ИГС  УРОВЕНЬ 1 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 1577327703)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (1577327703) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_RPTGROUP" ("LEV1_ISN", "УЧЕТНАЯ ГРУППА  УРОВЕНЬ 1", "LEV2_ISN", "УЧЕТНАЯ ГРУППА  УРОВЕНЬ 2", "LEV3_ISN", "УЧЕТНАЯ ГРУППА  УРОВЕНЬ 3", "КОД УГ") AS 
  (
SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "УЧЕТНАЯ ГРУППА  УРОВЕНЬ 1 ", NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "УЧЕТНАЯ ГРУППА  УРОВЕНЬ 2 ", NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,  NVL(MAX(DECODE(LEV,3,Code||' '||SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,Code||' '||SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,Code||' '||SHORTNAME,NULL)),' НЕТ ДАННЫХ '))) "УЧЕТНАЯ ГРУППА  УРОВЕНЬ 3 ",MAX(CODE) "КОД УГ"

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME, LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT,CODE
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 747776200)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (747776200) THEN 0 ELSE PARENTISN END =ISN
) S
UNION ALL
SELECT 0 ISN, 'НЕ УКАЗАННА' SHORTNAME, 1 LEVL ,0 ROOT, '0' CODE, 1 LEV FROM DUAL
)

GROUP BY ROOT


);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_RPTGROUP4RZU" ("ISN", "SHORTNAME", "CODE", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "LEV1_SHORTNAME", "LEV1_CODE", "LEV1_CODE4SORT", "LEV2_ISN", "LEV2_SHORTNAME", "LEV2_CODE", "LEV2_CODE4SORT", "LEV3_ISN", "LEV3_SHORTNAME", "LEV3_CODE", "LEV3_CODE4SORT") AS 
  WITH ISNS AS (
              SELECT /*+ Cardinality (100) */
                 ISN,
                 CONNECT_BY_ISLEAF as IS_LEAF,
                 LEVEL LV,
                 (select sum(system.GetItem(code, '.', level)*power(0.1,LEVEL-1))
                  from dual
                  connect by level <= length(translate(code,'.-0123456789','.'))+1) AS code4sort
              FROM DICTI
              START WITH PARENTISN IN (747776200)
              CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
             )
SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  S.root_code AS code,
  coalesce(LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,
  coalesce(LEV1_code, ' НЕТ ДАННЫХ ') as LEV1_code,
  coalesce(LEV1_code4sort, 0) as LEV1_code4sort,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,
  coalesce(LEV2_code, LEV1_code, ' НЕТ ДАННЫХ ') as LEV2_code,
  coalesce(LEV2_code4sort, LEV1_code4sort, 0) as LEV2_code4sort,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME,
  coalesce(LEV3_code, LEV2_code, LEV1_code, ' НЕТ ДАННЫХ ') as LEV3_code,
  coalesce(LEV3_code4sort, LEV2_code4sort, LEV1_code4sort, 0) as LEV3_code4sort

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,
    S.root_code,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,code||' '||SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,code)) as LEV1_code,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,
    MAX(DECODE(LEV,1,code4sort)) as LEV1_code4sort,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,code||' '||SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,code)) as LEV2_code,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,
    MAX(DECODE(LEV,2,code4sort)) as LEV2_code4sort,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,code||' '||SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,code)) as LEV3_code,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,
    MAX(DECODE(LEV,3,code4sort)) as LEV3_code4sort

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF, isns.code4sort
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        D.code,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME,
        CONNECT_BY_ROOT (D.code) AS root_code/*,
        (select sum(system.GetItem(d.code, '.', level)*power(0.1,LEVEL-1))
                  from dual
                  connect by level <= length(translate(d.code,'.-0123456789','.'))+1) AS code4sort*/

      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (747776200)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME, S.root_code
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  t.code,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.code||' '||t.SHORTNAME as LEV1_SHORTNAME,
  t.code,
  t.code4sort,

  t.ISN as LEV2_ISN,
  t.code||' '||t.SHORTNAME as LEV2_SHORTNAME,
  t.code,
  t.code4sort,

  t.ISN as LEV3_ISN,
  t.code||' '||t.SHORTNAME as LEV3_SHORTNAME,
  t.code,
  t.code4sort

from
  (select 0 as ISN, '<НЕ ЗАДАНО>' as Shortname, '0' AS code, 0 AS code4sort from dual) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_RULEDETAIL" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "ДЕТАЛИЗ.ПРОДУКТА  УРОВЕНЬ 1 ", "LEV2_ISN", "ДЕТАЛИЗ.ПРОДУКТА  УРОВЕНЬ 2 ", "LEV3_ISN", "ДЕТАЛИЗ.ПРОДУКТА  УРОВЕНЬ 3 ") AS 
  WITH ISNS AS (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF
  FROM DICTI
  START WITH PARENTISN IN (1071774425)
  CONNECT BY PRIOR ISN = PARENTISN
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ДЕТАЛИЗ.ПРОДУКТА  УРОВЕНЬ 1 ",

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ДЕТАЛИЗ.ПРОДУКТА  УРОВЕНЬ 2 ",

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "ДЕТАЛИЗ.ПРОДУКТА  УРОВЕНЬ 3 "

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and D.ISN NOT IN (1071774425)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL

select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,
  t.ISN as LEV1_ISN, t.SHORTNAME as LEV1_SHORTNAME,
  t.ISN as LEV2_ISN, t.SHORTNAME as LEV2_SHORTNAME,
  t.ISN as LEV3_ISN, t.SHORTNAME as LEV3_SHORTNAME
from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_SALERCHANEL" ("LEV1_ISN", "КАНАЛ ПРОДАЖ  УРОВЕНЬ 1 ") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (1020048325)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as LEV1_ISN,
  S.ROOT_SHORTNAME as LEV1_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (1020048325)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_SALERCLASS" ("LEV1_ISN", "МОТИВАЦ.ГРУППА  УРОВЕНЬ 1 ", "LEV2_ISN", "МОТИВАЦ.ГРУППА  УРОВЕНЬ 2 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "МОТИВАЦ.ГРУППА  УРОВЕНЬ 1 ", NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "МОТИВАЦ.ГРУППА  УРОВЕНЬ 2 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 1428521003)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (1428521003) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_SALETYPE" ("LEV1_ISN", "СПОСОБ РЕАЛИЗ.ТС  УРОВЕНЬ 1 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "СПОСОБ РЕАЛИЗ.ТС  УРОВЕНЬ 1 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 2787967303)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (2787967303) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_SETTLEMENTDOCS" ("LEV1_ISN", "ДОКУМ.ПО УРЕГУЛИР.  УРОВЕНЬ 1 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "ДОКУМ.ПО УРЕГУЛИР.  УРОВЕНЬ 1 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 2149993403)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (2149993403) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_STOCAUSENAME" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "LEV1_SHORTNAME") AS 
  WITH ISNS AS (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF
  FROM DICTI
  START WITH PARENTISN IN (1798438403)
  CONNECT BY PRIOR ISN = PARENTISN
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and D.ISN NOT IN (1798438403)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_SUBDEPT" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "ROOT_CLASSISN", "LEV1_ISN", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_SHORTNAME", "LEV3_ISN", "LEV3_SHORTNAME", "LEV4_ISN", "LEV4_SHORTNAME", "LEV5_ISN", "LEV5_SHORTNAME", "LEV6_ISN", "LEV6_SHORTNAME", "LEV7_ISN", "LEV7_SHORTNAME", "LEV8_ISN", "LEV8_SHORTNAME") AS 
  WITH ISNS AS (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     CONNECT_BY_ROOT classisn as ROOT_CLASSISN
  FROM AIS.SUBDEPT_T
  START WITH
    PARENTISN IN (0)
    AND NVL (active, 'S') <> 'S'
    /* убрал, т.к. обрабатываю фильтром в отчете
    AND classisn In (
      11296719,    -- ОПЕРАТИВНОЕ
      2758435703,  -- ФУНКЦИОНАЛЬНО-ОПЕРАТИВНОЕ
      2757256203,  -- РЕГИОНЫ
      2757255803   -- ДОЧЕРНИЕ
    )
    */

  CONNECT BY PRIOR ISN = PARENTISN
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV8_IS_LEAF, LEV7_IS_LEAF, LEV6_IS_LEAF, LEV5_IS_LEAF, LEV4_IS_LEAF, LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,
  S.ROOT_CLASSISN,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME,

  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV3_ISN,
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV3_SHORTNAME,

  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV4_ISN,
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV4_SHORTNAME,

  coalesce(LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV5_ISN,
  coalesce(LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV5_SHORTNAME,

  coalesce(LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV6_ISN,
  coalesce(LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV6_SHORTNAME,

  coalesce(LEV7_ISN, LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV7_ISN,
  coalesce(LEV7_SHORTNAME, LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV7_SHORTNAME,

  coalesce(LEV8_ISN, LEV7_ISN, LEV6_ISN, LEV5_ISN, LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as LEV8_ISN,
  coalesce(LEV8_SHORTNAME, LEV7_SHORTNAME, LEV6_SHORTNAME, LEV5_SHORTNAME, LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV8_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,
    S.ROOT_CLASSISN,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF,

    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF,

    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,SHORTNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF,

    MAX(DECODE(LEV,5,ISN)) as LEV5_ISN,
    MAX(DECODE(LEV,5,SHORTNAME)) as LEV5_SHORTNAME,
    MAX(DECODE(LEV,5,IS_LEAF)) as LEV5_IS_LEAF,

    MAX(DECODE(LEV,6,ISN)) as LEV6_ISN,
    MAX(DECODE(LEV,6,SHORTNAME)) as LEV6_SHORTNAME,
    MAX(DECODE(LEV,6,IS_LEAF)) as LEV6_IS_LEAF,

    MAX(DECODE(LEV,7,ISN)) as LEV7_ISN,
    MAX(DECODE(LEV,7,SHORTNAME)) as LEV7_SHORTNAME,
    MAX(DECODE(LEV,7,IS_LEAF)) as LEV7_IS_LEAF,

    MAX(DECODE(LEV,8,ISN)) as LEV8_ISN,
    MAX(DECODE(LEV,8,SHORTNAME)) as LEV8_SHORTNAME,
    MAX(DECODE(LEV,8,IS_LEAF)) as LEV8_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF,
      ISNS.ROOT_CLASSISN
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM AIS.SUBDEPT_T D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME, S.ROOT_CLASSISN
) S;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_TARIFFGROUP" ("LEV1_ISN", "ТАРИФНЫЕ ГРУППЫ  УРОВЕНЬ 1 ", "LEV2_ISN", "ТАРИФНЫЕ ГРУППЫ  УРОВЕНЬ 2 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "ТАРИФНЫЕ ГРУППЫ  УРОВЕНЬ 1 ", NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "ТАРИФНЫЕ ГРУППЫ  УРОВЕНЬ 2 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN = 36775816
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR DECODE(PARENTISN,36775816,0,PARENTISN)=ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_VEHICLE_TARIFF_GROUP" ("ISN", "SHORTNAME", "LEV1_ISN", "LEV1_CODE", "ТАРИФНАЯ ГР А/М  УРОВЕНЬ 1", "LEV2_ISN", "LEV2_CODE", "ТАРИФНАЯ ГР А/М  УРОВЕНЬ 2") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (36775816)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_CODE, ' НЕТ ДАННЫХ ') as LEV1_CODE,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV1_SHORTNAME,

  coalesce(LEV2_ISN, LEV1_ISN, 0) as LEV2_ISN,
  coalesce(LEV2_CODE, LEV1_CODE, ' НЕТ ДАННЫХ ') as LEV2_CODE,
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as LEV2_SHORTNAME

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,CODE)) as LEV1_CODE,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF,

    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,CODE)) as LEV2_CODE,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        D.CODE,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (36775816)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_CODE,
  t.SHORTNAME as LEV1_SHORTNAME,

  t.ISN as LEV2_ISN,
  t.SHORTNAME as LEV2_CODE,
  t.SHORTNAME as LEV2_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_H_VEHICLE_TYPE" ("LEV1_ISN", "ТИП А/М  УРОВЕНЬ 1 ", "LEV2_ISN", "ТИП А/М  УРОВЕНЬ 2 ", "LEV3_ISN", "ТИП А/М  УРОВЕНЬ 3 ") AS 
  SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "ТИП А/М  УРОВЕНЬ 1 ", NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "ТИП А/М  УРОВЕНЬ 2 ", NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,  NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))) "ТИП А/М  УРОВЕНЬ 3 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN = 8240
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR DECODE(PARENTISN,8240,0,PARENTISN)=ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_INGO" ("INGOISN", "SHORTNAME") AS 
  select 0 ingoisn,'Не группа ИНГО' shortname from dual
union all
select isn ingoisn, shortname from v_rep_ingogrp;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_INSHUREDTYPE" ("INSHUREDTYPEISN", "SHORTNAME") AS 
  SELECT /*+ ordered use_nl(DP D) index(D X_DICTI_PARENT) Cardinality (10000) */
  D.ISN as INSHUREDTYPEISN, D.SHORTNAME
FROM
  (SELECT /*+ Cardinality (15) index(D X_DICTI_PARENT) */ ISN FROM DICTI D WHERE PARENTISN = 24890816 /*C.GET('AGRKIND')*/) DP,
  DICTI D
WHERE
  DP.ISN = D.PARENTISN

union all

select 0, cognos_const.GET_VALUE('EMPTY_CHAR')
from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_INVOICE_STATUS" ("ISN", "PARENTISN", "CODE", "N_CHILDREN", "FILTERISN", "SHORTNAME", "FULLNAME", "TABLENAME", "CONSTNAME", "ACTIVE", "UPDATED", "UPDATEDBY", "SYNISN") AS 
  select
d."ISN",
d."PARENTISN",
d."CODE",
d."N_CHILDREN",
d."FILTERISN",
d."SHORTNAME",
d."FULLNAME",
d."TABLENAME",
d."CONSTNAME",
d."ACTIVE",
d."UPDATED",
d."UPDATEDBY",
d."SYNISN"
from dicti d
where d.parentisn = 1982491503;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_ISBASEL" ("ISBAZEL", "SHORTNAME") AS 
  select 'Y' ISBAZEL,'Принадлежит к группе "БАЗЭЛ"' shortname from dual
union all
select 'N' ,'Не принадлежит к группе "БАЗЭЛ"' shortname from dual
union all
select '0', cognos_const.get_value('EMPTY_CHAR') from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_ISCREDIT" ("ISCREDITISN", "ISCREDIT") AS 
  select 1 ISCREDITISN, 'Y' ISCREDIT from dual
union all
 select 2, 'N' from dual
union all
 select 0, cognos_const.get_value('EMPTY_CHAR') from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_ISFRANCH" ("ISFRANCH", "SHORTNAME") AS 
  (
Select 'Y' ISFRANCH,'Есть франшиза' shortname  from dual
 union all
Select 'N' ,'Нет франшизы' shortname  from dual
 union all
Select '0',cognos_const.get_value('EMPTY_CHAR')  from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_ISLONGAGR" ("LONGAGRCLASSISN", "SHORTNAME") AS 
  select "ISN","SHORTNAME" from storages.rep_islongAgr;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_ISREINDEPT" ("ISREINDEPT", "SHORTNAME") AS 
  (
Select 'Y' isreindept,'Бизнес ДП' shortname  from dual
 union all
Select 'N' isreindept,'Не Бизнес ДП' shortname  from dual
 union all
Select ' ','Не указано'  from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_ISSALER" ("ISSALER", "SHORTNAME") AS 
  select 'Y' IsSaler, 'Присутствует' shortname from dual
union all
select 'N', 'Отсутствует' from dual
union all
select '0', cognos_const.GET_VALUE('EMPTY_CHAR') from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_ISSTEAL" ("ISSTEAL", "SHORTNAME") AS 
  (
Select 'Y' isSteal,'Угон' shortname  from dual
 union all
Select 'N' isSteal,'Не Угон' shortname  from dual
 union all
Select ' ','Нет данных'  from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_ISTOTAL" ("ISTOTAL", "SHORTNAME") AS 
  (
Select 'Y' ISTOTAL,'Тоталь' shortname  from dual
 union all
Select 'N' ,'Не тоталь' shortname  from dual
 union all
Select '0', cognos_const.get_value('EMPTY_CHAR') from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_IS_REINSURED" ("ISREINSURED", "SHORTNAME") AS 
  (
Select 'Y' IsReInsured, 'Перестрахование есть' shortname  from dual
 union all
Select 'N' ,'Перестрахования нет' shortname  from dual
 union all
Select '1' IsReInsured, 'Перестрахование есть' shortname  from dual
 union all
Select '0' ,'Перестрахования нет' shortname  from dual
 union all
 
Select ' ', 'Нет данных'  from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_JURTYPE" ("ISN", "JURTYPE", "JURCLASSISN", "JURCLASS") AS 
  select D.ISN,
       D.SHORTNAME JURTYPE,
       connect_by_root D.ISN as JURCLASSISN,
       connect_by_root D.SHORTNAME as JURCLASS
  from DICTI D
 start with D.ISN in ( 407,  -- типы юридических лиц
                       410 ) -- типы физических лиц
connect by prior isn = parentisn

union all

select 0,
       '<не задано>',
       0,
       '<не задано>'
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_KALENDAR" ("DAY", "WEEK", "MONTHS", "QUART", "YEAR", "DAY_OF_WEEK", "QUARTNO", "HALFYEARNO", "MONTHNO", "YEARNO", "HALFYEAR") AS 
  select
  A.DAY,
  -- sts т.к. NLS_TERRITORY=RUSSIA в to_char пропихнуть не получилось, оперирую полем DAY_OF_WEEK
  --A.WEEK,
  A.DAY - (
    case trim(lower(A.DAY_OF_WEEK))
      when 'понедельник' then 1
      when 'вторник' then 2
      when 'среда' then 3
      when 'четверг' then 4
      when 'пятница' then 5
      when 'суббота' then 6
      when 'воскресенье' then 7
    end) + 1
  as WEEK,

  A.MONTHS,
  A.QUART,
  A.YEAR,
  A.DAY_OF_WEEK,
  A.QUARTNO,
  A.HALFYEARNO,
  A.MONTHNO,
  A.YEARNO,
  add_months(trunc(A.DAY, 'yy'), 6*(A.HALFYEARNO - 1)) as HALFYEAR
  from ( select DAY,
                trunc(DAY, 'ww') WEEK ,
                trunc(DAY, 'mm') MONTHS,
                trunc(DAY, 'q') QUART,
                trunc(DAY, 'YYYY') YEAR,
                to_char(DAY, 'day', 'NLS_DATE_LANGUAGE=RUSSIAN') DAY_OF_WEEK,
                to_number(to_char(DAY, 'q')) QUARTNO,
                ceil(to_number(to_char(DAY, 'mm')) / 6) HALFYEARNO,
                to_number(to_char(DAY, 'mm')) MONTHNO,
                to_number(to_char(DAY, 'yyyy')) YEARNO
         from (
              /* sts 30.05.2012 - переделал на connect by по аналогии с V_DIC_KALENDAR_1
              select to_date('01-jan-1947') + rownum - 1 DAY
                  from table(ANY_SIZE_TABLE(365*150))
               union all
                select to_date('01-jan-1900') DAY from dual
              */

               select  to_date('01.01.1947','DD.MM.YYYY') + Level - 1 DAY
               from dual
               Connect By Level<=365*150
               union all
                select to_date('01.01.1900','DD.MM.YYYY') DAY from dual
              ) A
       ) A;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_KALENDAR_1" ("MONTHS", "QUART", "YEAR", "QUARTNO", "MONTHNO", "YEARNO", "YEAR_DATE", "QUART_DATE", "DB", "DE") AS 
  select --   + cardinality (10000)
  A.MONTHS,
  A.QUART,
  A.YEAR,
  A.QUARTNO,
  A.MONTHNO,
  A.YEARNO,
  A.YEAR_DATE,
  A.QUART_DATE,
  MIN(A.DAY) DB,
  MAX(A.DAY) DE
from ( select DAY ,
                TO_CHAR(DAY, 'ww.yyyy') WEEK ,
                TO_CHAR(DAY, 'mm.yyyy') MONTHS,
                TO_CHAR(DAY, 'q.yyyy') QUART,
                TO_CHAR(DAY, 'YYYY') YEAR,
                to_number(to_char(DAY, 'q')) QUARTNO,
                to_number(to_char(DAY, 'mm')) MONTHNO,
                to_number(to_char(DAY, 'yyyy')) YEARNO,
                TRUNC(DAY, 'YYYY') YEAR_DATE,
                TRUNC(DAY, 'Q') QUART_DATE
         from (

               select  to_date('01.01.1947','DD.MM.YYYY') + Level - 1 DAY
               from dual
               Connect By Level<=365*150
               union all
                select to_date('01.01.1900','DD.MM.YYYY') DAY from dual) A ) A

group by
  A.MONTHS,
  A.QUART,
  A.YEAR,
  A.QUARTNO,
  A.MONTHNO,
  A.YEARNO,
  A.YEAR_DATE,
  A.QUART_DATE;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_KALENDAR_DAY" ("MONTHS", "QUART", "YEAR", "QUARTNO", "MONTHNO", "YEARNO", "YEAR_DATE", "QUART_DATE", "MONTH_DATE", "DAY") AS 
  (

select
  A.MONTHS,
  A.QUART,
  A.YEAR,
  A.QUARTNO,
  A.MONTHNO,
  A.YEARNO,
  A.YEAR_DATE,
  A.QUART_DATE,
  A.MONTH_DATE,
  A.DAY
--       add_months(trunc(A.DAY, 'yy'), 6*(A.HALFYEARNO - 1)) HALFYEAR
  from ( select DAY ,
                TO_CHAR(DAY, 'ww.yyyy') WEEK ,
                TO_CHAR(DAY, 'mm.yyyy') MONTHS,
                TO_CHAR(DAY, 'q.yyyy') QUART,
                TO_CHAR(DAY, 'YYYY') YEAR,
                to_number(to_char(DAY, 'q')) QUARTNO,
--                ceil(to_number(to_char(DAY, 'mm')) / 6) HALFYEARNO,
                to_number(to_char(DAY, 'mm')) MONTHNO,
                to_number(to_char(DAY, 'yyyy')) YEARNO,
                
                TRUNC(DAY, 'YYYY') YEAR_DATE,
                TRUNC(DAY, 'Q') QUART_DATE,
                TRUNC(DAY, 'mm') MONTH_DATE
                
         from ( 
         
               select  to_date('01.01.1947','DD.MM.YYYY') + Level - 1 DAY 
               from dual
               Connect By Level<=365*150         
               union all
                select to_date('01.01.1900','DD.MM.YYYY') DAY from dual) 
                A )
                 A


);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_LINEDESC" ("LINEDESCCODE", "LINEDESC") AS 
  select to_char(STATCODE) LINEDESCCODE,
        DESCRIPTION LINEDESC
   from REP_STATCODE

 union all

 select STATCODE,
        DESCRIPTION
   from rep_budget_statcode;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_LIST005" ("ISN") AS 
  select (0+5*(Level-1)/100 ) isn
from dual
connect by level<42;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_LONGAGR" ("ISLONG", "SHORTNAME") AS 
  (
Select 'Y' ISLONG,'Длинный(более 13 мес)' shortname  from dual
 union all
Select 'N' ,'Короткий (менее 13 мес)' shortname  from dual
 union all
Select ' ','Нет данных'  from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_LOSSREJECTREASON" ("ISN", "SHORTNAME") AS 
  Select Isn, SHORTNAME 
from dicti 
where parentisn in (965603525, 3053116303)
union all
Select 0, '<не задано>' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_LOYALTYGRP" ("ISN", "PARENTISN", "CODE", "N_CHILDREN", "FILTERISN", "SHORTNAME", "FULLNAME", "TABLENAME", "CONSTNAME", "ACTIVE", "UPDATED", "UPDATEDBY", "SYNISN", "SHORTNAMECUT", "FULLNAMECUT") AS 
  select d."ISN",d."PARENTISN",d."CODE",d."N_CHILDREN",d."FILTERISN",d."SHORTNAME",d."FULLNAME",d."TABLENAME",d."CONSTNAME",d."ACTIVE",d."UPDATED",d."UPDATEDBY",d."SYNISN",
       substr(shortname, instr(shortname, '/') + 1, length(shortname)) as shortnamecut,
       substr(fullname, instr(fullname, '/') + 1, length(fullname)) as fullnamecut
  from ais.dicti d
 start with isn = 3864260203
connect by prior isn = parentisn;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_MEDSERVCATEGORY" ("LEV1_ISN", "LEV1_SHORTNAME") AS 
  SELECT ISN as LEV1_ISN, SHORTNAME as LEV1_SHORTNAME
   FROM DICTI
      WHERE PARENTISN = 3117611303;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_MOTIVATION" ("LEV1_ISN", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_SHORTNAME") AS 
  (--Вьюха, возвращающая мотивационные группы сабжей (не путать с V_DIC_MOTIVGROUP)
SELECT  NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,  NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') "МОТИВ-АЯ ГР СОТР  УРОВЕНЬ 1 ", NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,  NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) "МОТИВ-АЯ ГР СОТР  УРОВЕНЬ 2 "

FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 1428521003)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN (1428521003) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_MOTIVGROUP" ("MOTIVGROUPISN", "SHORTNAME") AS 
  (-- вьюха, возвращающая линии бизнеса (не путать с V_DIC_MOTIVATION)
Select Isn motivgroupisn,shortname  from dicti start with  parentisn=1342981303 connect by  prior isn=  parentisn
 union all
Select 0,' '  from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_MOTOR_MAKE" ("MAKEISN", "MAKENAME", "MAKEPARENTISN") AS 
  select ISN MAKEISN,
        SHORTNAME MAKENAME,
        PARENTISN MAKEPARENTISN
   from DICTI
  start with PARENTISN = 604607816
connect by prior ISN = PARENTISN

union all

select 0,
       cognos_const.get_value('EMPTY_CHAR'),
       0
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_MOTOR_MODEL" ("MODELISN", "MODELNAME") AS 
  select ISN MODELISN,
        SHORTNAME MODELNAME
   from DICTI
  start with PARENTISN = 8240
connect by prior ISN = PARENTISN

union all

select 0,
       'Нет Данных'
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_MOTOR_RPTCLASS" ("RPTCLASS", "SHORTNAME") AS 
  select '??' RPTCLASS,
       'Не определен' SHORTNAME
  from dual

union all

select 'ГО' RPTCLASS,
       'ГО' SHORTNAME
  from dual

union all

select 'ДО' RPTCLASS,
       'ДО' SHORTNAME
  from dual

union all

select 'КА-угон' RPTCLASS,
       'КА-угон' SHORTNAME
  from dual

union all

select 'КА-ущерб' RPTCLASS,
       'КА-ущерб' SHORTNAME
  from dual

union all

select 'НС' RPTCLASS,
       'НС' SHORTNAME
  from dual

union all

select ' ',
       'Не указан'
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_MOTOR_TYPETS" ("TYPEISN", "MAKENAME") AS 
  (
Select Isn TYPEISN,SHORTNAME makename  from dicti  start with Parentisn=1065483325 connect by prior isn=  parentisn
union all
Select 0 ,'Нет Данных' from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_MULTIDRIVE" ("ISN", "MULTIDRIVE") AS 
  select 1 ISN, 'Y' MULTIDRIVE from dual
  union all
select 2, 'N' from dual
  union all
select 0, '<не задано>' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_PAYFORM" ("PAYFORMISN", "PAYFORMCODE", "PAYFORMNAME", "PAYFORM") AS 
  select ISN payformISN,
       CODE payformcode,
       SHORTNAME payformname,
       FULLNAME  payform
   from DICTI
  start with PARENTISN = 114616
connect by prior ISN = PARENTISN

union all

select 0,
       ' ',
       ' ',
       ' '
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_PREMRATE" ("PREMRATE", "SHORTNAME") AS 
  (
Select 100 premrate,'1. ..<100' shortname  from dual
 union all
Select 1000 premrate,'2. 100<=..<1000' shortname  from dual
 union all
Select 5000 ,'3. 1000<=..<5 000' shortname  from dual
 union all
Select 10000 ,'4. 5000<=..<10 000' shortname  from dual
 union all
Select 50000 ,'5. 10 000<=..<50 000' shortname  from dual
 union all
Select 100000 ,'6. 50 000<=..<100 000' shortname  from dual
 union all
Select 10000000 ,'7. ..>=100 000' shortname  from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_PROLONG" ("PROLONG", "SHORTNAME") AS 
  select 'Y' Prolong, 'Продленный договор' Shortname from dual
union
select 'N' Prolong, 'Новый договор' Shortname from dual
union
select '0' Prolong, cognos_const.get_value('EMPTY_CHAR') Shortname from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_PSS" ("PSSISN", "PSS") AS 
  select Isn PSSISN,SHORTNAME PSS
from dicti
start with parentisn=805711700
connect by prior isn=parentisn
union all
select 0 PPCISN, cognos_const.get_value('EMPTY_CHAR') PPS
from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_REFCLAIMSTATUS" ("STATUSCODE", "SHORTNAME") AS 
  (-- sts 13.12.2012 - сделал как в АИС: ф-ия AIS.AGRN.DecodeRefundStatus
Select code STATUSCODE, shortname
From DICTI
where
  parentisn = 742093100 -- C.Get('PrRefundStatus')
  and Active is not null
union all
Select '0' ClaimStatus, cognos_const.get_value('EMPTY_CHAR')  From dual

/* sts 13.12.2012 - убрал и сделал как в АИС: ф-ия AIS.AGRN.DecodeRefundStatus
select 'Y' STATUSCODE, 'УРЕГУЛИРОВАН' SHORTNAME
   from dual
union all
select 'N', 'РАССМАТРИВАЕТСЯ'
   from dual
union all
select 'S', 'ОТКАЗ ЗАЯВИТЕЛЯ'
   from dual
union all
select 'R', 'ОТКЛОНЕН'
   from dual
union all
select 'D', 'ОФОРМЛЕНИЕ'
   from dual
union all
select 'F', 'АННУЛИРОВАН'
   from dual
union all
select 'A', 'АННУЛИРОВАН'
   from dual
union all
 select null, cognos_const.get_value('EMPTY_CHAR')
   from dual
*/
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_REFCLASS" ("REFCLASSISN", "REFCLASS") AS 
  select distinct REFCLASSISN, REFCLASS
from
(
select ISN REFCLASSISN,
        SHORTNAME REFCLASS
   from DICTI
  start with PARENTISN = 2005
connect by prior ISN = PARENTISN

union all

 select ISN REFCLASSISN,
        SHORTNAME REFCLASS
   from DICTI
  start with PARENTISN = 2001
connect by prior ISN = PARENTISN

union all

 select ISN REFCLASSISN,
        SHORTNAME REFCLASS
   from DICTI
  start with PARENTISN = 8258
connect by prior ISN = PARENTISN

union all

 select ISN REFCLASSISN,
        SHORTNAME REFCLASS
   from DICTI
  start with PARENTISN = 959955425
connect by prior ISN = PARENTISN

union all

 select ISN REFCLASSISN,
        SHORTNAME REFCLASS
   from DICTI
  where ISN = 2183

union all

 select ISN REFCLASSISN,
        SHORTNAME REFCLASS
   from DICTI
  where ISN = 960203325

union all

select 0,
       cognos_const.get_value('EMPTY_CHAR')
  from DUAL) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_REFFORM" ("REFFORMISN", "SHORTNAME") AS 
  select ISN REFFORMISN,
        SHORTNAME
   from DICTI
  start with PARENTISN = 728191616
connect by prior ISN = PARENTISN

union all

select 0 REFFORMISN,
       'Нет данных' SHORTNAME
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_REFSTATUS" ("REFSTATUS", "SHORTNAME") AS 
  Select CODE REFSTATUS, SHORTNAME  From DICTI where PARENTISN = 742093100
union all
Select '0', cognos_const.get_value('EMPTY_CHAR')  From DUAl;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_REFUNDSTATUS" ("REFUNDSTATUS", "SHORTNAME") AS 
  Select code refundstatus, shortname  From DICTI where parentisn = 742093100                             
union all 
Select ' ' refundstatus, 'Нет данных'  From dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_REGRESS_REASON" ("ISN", "SHORTNAME") AS 
  (
Select Isn, SHORTNAME 
from dicti 
where parentisn = 953636425
union all
Select 0, '<не задано>' from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_REGRESS_ROLE" ("ROLECODE", "ROLENAME") AS 
  (-- sts 26.04.2013 - Расшифровка роли регрессного иска (REGRESS.ROLE)
-- Выдрано из AIS.REGRESS_FUNC.DecodeRegrRole
select 'D' as ROLECODE, 'Истец' as ROLENAME from dual
union all
select 'R' as ROLECODE, 'Ответчик' as ROLENAME from dual
union all
select 'S' as ROLECODE, 'Соответчик' as ROLENAME from dual
union all
select '3' as ROLECODE, '3 лицо' as ROLENAME from dual
union all
select '0' as ROLECODE, '<не задано>' as ROLENAME from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_REJECT_REASON" ("ISN", "SHORTNAME") AS 
  (
Select Isn, SHORTNAME
from dicti
where parentisn in (965603525,3053116303)
union all
Select 0, '<не задано>' from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_REPLOAD" ("DATEREP", "LOADISN") AS 
  select DATEREP, ISN LOADISN
from STORAGES.REPLOAD;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_REPTYPE" ("REPTYPEISN", "SHORTNAME") AS 
  select ISN REPTYPEISN,
        SHORTNAME
   from DICTI
  start with PARENTISN = 2337004203
connect by prior ISN =PARENTISN

union

 select 0,
        'Не определено'
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_REP_DELAY_R" ("ISN", "NAME") AS 
  select 0 ISN, '<не задано>' NAME from dual
  union all
select 1, 'ДО 30 ДНЕЙ' from dual
  union all
select 2, 'ОТ 1 ГОДА ДО 2 ЛЕТ' from dual
  union all
select 3, 'ОТ 181 ДО 360 ДНЕЙ' from dual
  union all
select 4, 'ОТ 2 ЛЕТ ДО 3 ЛЕТ' from dual
  union all
select 5, 'ОТ 31 ДО 60 ДНЕЙ' from dual
  union all
select 6, 'ОТ 61 ДО 90 ДНЕЙ' from dual
  union all
select 7, 'ОТ 91 ДО 180 ДНЕЙ' from dual
  union all
select 8, 'СВЫШЕ 3 ЛЕТ' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_RESIDENT" ("RESIDENT", "RESIDENTNAME") AS 
  (
Select 'Y' RESIDENT,'Резидент' RESIDENTNAME  from dual
 union all
Select 'N' RESIDENT,'Не резидент' RESIDENTNAME  from dual
 union all
Select '0', cognos_const.get_value('EMPTY_CHAR')  from dual
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_RPTCLASS" ("SHORTSTATUS", "RPTCLASS") AS 
  select 'НС' SHORTSTATUS, 'НС' RPTCLASS
   from dual
union all
select 'Имущ-КА', 'Имущ-КА'
   from dual
union all
select 'Имущ-ГО', 'Имущ-ГО'
   from dual
union all
select '??', '??'
   from dual
union all
select 'ГО', 'ГО'
   from dual
union all
select 'КА-ущерб', 'КА-ущерб'
   from dual
union all
select 'КА-угон', 'КА-угон'
   from dual
union all
 select '0', cognos_const.get_value('EMPTY_CHAR')
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_RPTCLASSISN" ("ISN", "SHORTNAME", "LEV1_ISN", "LEV1_SHORTNAME", "LEV2_ISN", "LEV2_SHORTNAME", "LEV3_ISN", "LEV3_SHORTNAME", "LEV4_ISN", "LEV4_SHORTNAME", "LEV5_ISN", "LEV5_SHORTNAME") AS 
  SELECT
NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )))))AS ISN, -- PK
NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')))))AS SHORTNAME,
 NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ) LEV1_ISN,
NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ') LEV1_SHORTNAME,
 NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )) LEV2_ISN,
NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')) LEV2_SHORTNAME,
 NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))) LEV3_ISN,
NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))) LEV3_SHORTNAME,
 NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 )))) LEV4_ISN,
NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ ')))) LEV4_SHORTNAME,
 NVL(MAX(DECODE(LEV,5,ISN,NULL)),NVL(MAX(DECODE(LEV,4,ISN,NULL)),NVL(MAX(DECODE(LEV,3,ISN,NULL)),NVL(MAX(DECODE(LEV,2,ISN,NULL)),NVL(MAX(DECODE(LEV,1,ISN,NULL)), 0 ))))) LEV5_ISN,
NVL(MAX(DECODE(LEV,5,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,4,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,3,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,2,SHORTNAME,NULL)),NVL(MAX(DECODE(LEV,1,SHORTNAME,NULL)),' НЕТ ДАННЫХ '))))) LEV5_SHORTNAME
FROM
(
SELECT S.*,
ABS( LEVL-1-MAX(LEVL) OVER (PARTITION BY ROOT)) LEV

 FROM
(
SELECT /*+ Cardinality (10000) */
  ISN, SHORTNAME,LEVEL LEVL ,CONNECT_BY_ROOT (ISN) ROOT
FROM DICTI
START WITH ISN IN

(
SELECT /*+ Cardinality (100) */
  ISN
FROM DICTI
--WHERE CONNECT_BY_ISLEAF=1
START WITH PARENTISN IN ( 2004) OR ISN IN (818752900, 57687916)
CONNECT BY PRIOR ISN=PARENTISN
)
CONNECT BY PRIOR CASE WHEN PARENTISN IN ( 2004) OR ISN IN (818752900, 57687916) THEN 0 ELSE PARENTISN END =ISN
) S
)

GROUP BY ROOT;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_RPTGROUP" ("RPTGROUPISN", "CODE", "RPTGROUP") AS 
  select ISN RPTGROUPISN,
        CODE,
        SHORTNAME RPTGROUP
   from DICTI
  start with PARENTISN = 747776200 -- Учетные группы для резервов
connect by prior ISN = PARENTISN

union all

 select 0,
        'Нет данных',
        'Нет данных'
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_RULEDETAIL" ("RULEDETAIL", "SHORTNAME") AS 
  select ISN RULEDETAIL,
        SHORTNAME
   from DICTI
  start with PARENTISN = 1071774425
connect by prior ISN = PARENTISN

union all

select 0 RULEDETAIL,
       cognos_const.get_value('EMPTY_CHAR') shortname
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_RULETYPE" ("ISN", "NAME") AS 
  select 0 ISN, '<не задано>' NAME from dual
  union all
select 1, 'ГО НС' from dual
  union all
select 2, 'ДСАГО' from dual
  union all
select 3, 'Имущество физлиц' from dual
  union all
select 4, 'КАСКО' from dual
  union all
select 5, 'КИС' from dual
  union all
select 6, 'НС' from dual
  union all
select 7, 'ОСАГО' from dual
  union all
select 8, 'Страхование путешествующих' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_SAGROUP" ("SAGROUP", "GROUPNAME", "SHORTNAME") AS 
  select 1 as sagroup, 'Основной субсчет' as groupname, 'Начисления ИГС' as shortname from dual
union all
select 2, 'Субсчет "Ингосстрах-ЛМТ"', 'Субсчета ЛМТ' from dual
union all
select 3, 'Дополнительный субсчет', 'Несвоевременные начисления ИГС' from dual
union all
select 5, 'Прочие субсчета', 'Прочие субсчета (зеленая карта)' from dual
union all
Select 0, 'Не определено', 'Не определено'  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_SALECHANNELISN" ("SALECHANELISN", "SHORTNAME") AS 
  SELECT isn salechanelisn, shortname
   FROM dicti
  WHERE parentisn = 1366868203
 UNION ALL
 SELECT 0, 'Нет данных'
   FROM DUAL;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_SALER" ("SALERISN", "SHORTNAME", "FULLNAME") AS 
  select "ISN" SALERISN,
       "SHORTNAME",
       "FULLNAME"
from dic_saler

union all

select 0,
       cognos_const.GET_VALUE('EMPTY_CHAR'),
       cognos_const.GET_VALUE('EMPTY_CHAR')
from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_SALERCLASS" ("SALERCLASSISN", "PARENTNAME", "SHORTNAME", "PARENTSALERCLASSISN") AS 
  select ISN SALERCLASSISN,
       PARENT2 PARENTNAME,
       nvl(PARENT3, PARENT2) SHORTNAME,
       to_number(nvl(PISN, ISN)) PARENTSALERCLASSISN
  from ( select X.*,
                max(lv) over() MLV,
                system.getitem(PN, '#', 3) PARENT2,
                system.getitem(PN, '#', 4) PARENT3,
                system.getitem(PI, '#', 3) PISN
           from ( select level LV,
                         sys_connect_by_path(D.SHORTNAME, '#') PN,
                         sys_connect_by_path(D.ISN, '#') PI,
                         D.*
                    from DICTI D
                   start with ISN = 1428521003
                 connect by prior ISN = PARENTISN
                   order by level ) X )
          where LV >= 2

union all

select 0 SALERCLASSISN,
       cognos_const.get_value('EMPTY_CHAR'),
       cognos_const.get_value('EMPTY_CHAR'),
       0
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_SALERGO" ("ISGO", "SHORTNAME") AS 
  select 'Y' ISGO,
        'Продавец ГО' SHORTNAME
   from dual

 union all

 select 'N' ISGO,
        'Продавец филиала' SHORTNAME
   from dual

 union all

 select '0',
        cognos_const.GET_VALUE('EMPTY_CHAR')
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_SIGN" ("SGN", "SHORTNAME") AS 
  select -1 sgn, 'Отрицательное значение' shortname from dual
union all
select 0 sgn, 'Нуль' shortname from dual
union all
select 1 sgn, 'Положительное значение'  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_SIMPLE_FLAGS" ("ISN", "FLAG") AS 
  select 1 ISN, 'Y' FLAG from dual
  union all
select 2, 'N' from dual
  union all
select 0, '<не задано>' from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_STATCODE" ("ISN", "STATCODE", "DESCRIPTION", "GRP", "SHORTDESC") AS 
  select "ISN","STATCODE","DESCRIPTION","GRP","SHORTDESC"
    from STORAGES.REP_STATCODE

union all

select
      0,
      0,
      '<не задано>',
      '<не задано>',
      '<не задано>'
from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_SUBACC" ("SUBACC", "CODE") AS 
  select distinct
       CODE SUBACC,
       CODE CODE
  from SUBACC4DEPT

union all

select ' ',
       'Не определено'
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_SUBJCITY" ("SUBJCITYISN", "CITYNAME", "REGIONISN", "REGIONNAME", "REGION0ISN", "REGION0NAME", "COUNTRYISN", "COUNTRYNAME") AS 
  select C.ISN SUBJCITYISN,
       C.SHORTNAME CITYNAME ,
       C.REGIONISN,
       RG.SHORTNAME REGIONNAME,
       RG0.ISN REGION0ISN,
       RG0.SHORTNAME REGION0NAME,
       C.COUNTRYISN,
       CN.SHORTNAME COUNTRYNAME
  from CITY C,
       REGION RG,
       COUNTRY CN,
       ( select Z.ISN,
                Z.SHORTNAME
           from REGION Z
          where nvl(Z.PARENTISN, 0) = 0
          start with nvl(Z.ISN, 0) = 0
        connect by prior Z.PARENTISN = Z.ISN ) RG0
 where C.REGIONISN  = RG.ISN(+)
   and C.COUNTRYISN = CN.ISN(+)
   and RG.ISN       = RG0.ISN(+)

 union all

 select 0,
        'Нет Данных',
        0,
        'Нет Данных',
        0,
        'Нет Данных',
        0,
        'Нет Данных'
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_SUBJECT_CLASS" ("ISN", "PARENTISN", "SHORTNAME", "LEV", "IS_LEAF", "LEV1_SHORTNAME", "LEV2_SHORTNAME", "LEV3_SHORTNAME", "LEV4_SHORTNAME", "ISN1", "ISN2", "ISN3", "ISN4") AS 
  WITH ISNS AS (
  SELECT /*+ materialize Cardinality (500) */
     ISN, PARENTISN, SHORTNAME,
     LEVEL L,
     CONNECT_BY_ISLEAF IS_LEAF
  FROM ais.DICTI
  START WITH PARENTISN =41683316
  CONNECT BY PRIOR ISN = PARENTISN)
--------------------------------------------------------------------------------
SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_PARENTISN PARENTISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  S.ROOT_LEVEL LEV,
  ----------------------------------------------------------
  coalesce(LEV3_IS_LEAF, LEV2_IS_LEAF, LEV1_IS_LEAF, -1) as IS_LEAF,
  ----------------------------------------------------------
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as SHORTNAME1, 
  coalesce(LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as SHORTNAME2, 
  coalesce(LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as SHORTNAME3, 
  coalesce(LEV4_SHORTNAME, LEV3_SHORTNAME, LEV2_SHORTNAME, LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as SHORTNAME4,
  ----------------------------------------------------------
  coalesce(LEV1_ISN, 0) as ISN1,
  coalesce(LEV2_ISN, LEV1_ISN, 0) as ISN2,
  coalesce(LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as ISN3,
  coalesce(LEV4_ISN, LEV3_ISN, LEV2_ISN, LEV1_ISN, 0) as ISN4
  ----------------------------------------------------------
FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,
    S.ROOT_PARENTISN,
    S.ROOT_LEVEL,
    ----------------------------------------------------------
    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF, 
    ----------------------------------------------------------
    MAX(DECODE(LEV,2,ISN)) as LEV2_ISN,
    MAX(DECODE(LEV,2,SHORTNAME)) as LEV2_SHORTNAME,
    MAX(DECODE(LEV,2,IS_LEAF)) as LEV2_IS_LEAF, 
    ----------------------------------------------------------
    MAX(DECODE(LEV,3,ISN)) as LEV3_ISN,
    MAX(DECODE(LEV,3,SHORTNAME)) as LEV3_SHORTNAME,
    MAX(DECODE(LEV,3,IS_LEAF)) as LEV3_IS_LEAF, 
    ----------------------------------------------------------
    MAX(DECODE(LEV,4,ISN)) as LEV4_ISN,
    MAX(DECODE(LEV,4,SHORTNAME)) as LEV4_SHORTNAME,
    MAX(DECODE(LEV,4,IS_LEAF)) as LEV4_IS_LEAF,
    ----------------------------------------------------------
    MAX(DECODE(LEV,5,ISN)) as LEV5_ISN,
    MAX(DECODE(LEV,5,SHORTNAME)) as LEV5_SHORTNAME,
    MAX(DECODE(LEV,5,IS_LEAF)) as LEV5_IS_LEAF,
    ----------------------------------------------------------
    MAX(DECODE(LEV,6,ISN)) as LEV6_ISN,
    MAX(DECODE(LEV,6,SHORTNAME)) as LEV6_SHORTNAME,
    MAX(DECODE(LEV,6,IS_LEAF)) as LEV6_IS_LEAF
    ----------------------------------------------------------
  from (
     SELECT
        D.ISN, D.PARENTISN, D.SHORTNAME, D.L LEV, D.IS_LEAF,
        LEVEL as levl,        
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME,
        CONNECT_BY_ROOT (D.PARENTISN) as ROOT_PARENTISN,
        CONNECT_BY_ROOT (D.L) as ROOT_LEVEL
     FROM ISNS D
     START WITH isn IN (SELECT ISN from ISNS)
     CONNECT BY PRIOR D.PARENTISN = D.ISN
  ) S
  group by S.ROOT, S.ROOT_SHORTNAME, S.ROOT_PARENTISN, S.ROOT_LEVEL
) S;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_TARIFF" ("ISN", "SHORTNAME") AS 
  select D.ISN,
        D.SHORTNAME
   from DICTI D
  start with D.PARENTISN = 2322537603 -- Типы скидок
connect by prior D.ISN = D.PARENTISN

 union all

 select 0,
        ' '
   from dual

 union all

 select -1,
        'Ошибка'
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_TARIFFGROUP" ("TARIFFGROUPISN", "TARIFFGROUP", "SHORTNAME") AS 
  select ISN TARIFFGROUPISN,
        CODE TARIFFGROUP,
        SHORTNAME
   from DICTI
  start with PARENTISN = 36775816
connect by prior ISN = PARENTISN

union all

select 0,
       cognos_const.get_value('EMPTY_CHAR'),
       'Не указана'
  from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_TARIFFREASON" ("REASONISN", "REASONNAME") AS 
  select ISN REASONISN,
        SHORTNAME REASONNAME
   from DICTI
  start with PARENTISN = 2327767703
connect by prior ISN = parentisn

union all

 select 0,
        'Не указана'
   from dual

union all

 select -1,
        'Ошибка'
   from dual;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_TYPE_CALC_PREM" ("ISN", "SHORTNAME", "LAST_ITEM_IS_LEAF", "LEV1_ISN", "СПОСОБ РАСЧЕТА УРОВЕНЬ 1") AS 
  WITH ISNS AS

 (
  SELECT /*+ Cardinality (100) */
     ISN,
     CONNECT_BY_ISLEAF as IS_LEAF,
     LEVEL LV
  FROM DICTI
  START WITH PARENTISN IN (3224050003)
  CONNECT BY PRIOR ISN = PARENTISN and NVL(ACTIVE, 'S') <> 'S'
)

SELECT   /*+ Cardinality (10000) */
  S.ROOT as ISN,
  S.ROOT_SHORTNAME as SHORTNAME,
  coalesce(LEV1_IS_LEAF, -1) as LAST_ITEM_IS_LEAF,

  coalesce(LEV1_ISN, 0) as LEV1_ISN,
  coalesce(LEV1_SHORTNAME, ' НЕТ ДАННЫХ ') as "СПОСОБ РАСЧЕТА УРОВЕНЬ 1"

FROM (
  SELECT
    S.ROOT,
    S.ROOT_SHORTNAME,

    MAX(DECODE(LEV,1,ISN)) as LEV1_ISN,
    MAX(DECODE(LEV,1,SHORTNAME)) as LEV1_SHORTNAME,
    MAX(DECODE(LEV,1,IS_LEAF)) as LEV1_IS_LEAF

  from (
    select
      S.*,
      ABS (levl - 1 - MAX (levl) OVER (PARTITION BY root)) as lev,
      ISNS.IS_LEAF
    from
    ( SELECT
        D.ISN,
        D.SHORTNAME,
        LEVEL as levl,
        CONNECT_BY_ROOT (D.ISN) as ROOT,
        CONNECT_BY_ROOT (D.SHORTNAME) as ROOT_SHORTNAME
      FROM DICTI D
      START WITH isn IN (SELECT ISN from ISNS)
      CONNECT BY PRIOR
        D.PARENTISN = D.ISN
        and NVL(ACTIVE, 'S') <> 'S'
        and D.ISN NOT IN (3224050003)
    ) S,
      ISNS
    where
      S.ROOT = ISNS.ISN
  ) S

  group by S.ROOT, S.ROOT_SHORTNAME
) S

UNION ALL


select
  t.ISN,
  t.SHORTNAME,
  1 as LAST_ITEM_IS_LEAF,

  t.ISN as LEV1_ISN,
  t.SHORTNAME as LEV1_SHORTNAME

from
  (select 0 as ISN, '<не задано>' as Shortname from dual
   union all
   select -1 as ISN, '<все>' as Shortname from dual
  ) t;

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_VEHICLE" ("ISN1", "NAME1", "ISN2", "NAME2", "MAKEISN", "MAKENAME", "MODELISN", "MODELNAME") AS 
  (
SELECT
       T1.ISN1,
       T1.NAME1,
       T2.ISN2,
       T2.NAME2,
       S_MAKE.MAKEISN,
       S_MAKE.MAKENAME,
       S_MODEL.MODELISN,
       S_MAKE.MAKENAME || ' ' ||S_MODEL.MODELNAME MODELNAME
FROM
(
  SELECT
   ISN ISN1,
   SHORTNAME NAME1
  FROM DICTI
  WHERE LEVEL = 1
    START WITH PARENTISN=604607816 CONNECT BY PRIOR ISN=PARENTISN) T1,
(
  SELECT
   ISN ISN2,
   SHORTNAME NAME2,
   PARENTISN
  FROM DICTI
  WHERE LEVEL = 2
    START WITH PARENTISN=604607816 CONNECT BY PRIOR ISN=PARENTISN) T2,
(
  SELECT
   ISN MAKEISN,
   SHORTNAME MAKENAME,
   PARENTISN
  FROM DICTI
  WHERE LEVEL = 3
    START WITH PARENTISN=604607816 CONNECT BY PRIOR ISN=PARENTISN) S_MAKE,
(
  SELECT
   ISN MODELISN,
   SHORTNAME MODELNAME,
   PARENTISN
  FROM DICTI
  WHERE LEVEL = 4
    START WITH PARENTISN=604607816 CONNECT BY PRIOR ISN=PARENTISN) S_MODEL
WHERE
    T1.ISN1 = T2.PARENTISN AND
    T2.ISN2 = S_MAKE.PARENTISN AND
    S_MAKE.MAKEISN = S_MODEL.PARENTISN

UNION ALL -- добавляем на нижний уровень значения верхнего ур.

SELECT ISN ISN1,
       SHORTNAME NAME1,
       ISN ISN2,
       SHORTNAME NAME2,
       ISN MAKEISN,
       SHORTNAME MAKENAME,
       ISN MODELISN,
       SHORTNAME MODELNAME
FROM DICTI
WHERE LEVEL = 1
START WITH PARENTISN=604607816 CONNECT BY PRIOR ISN=PARENTISN

UNION ALL -- добавляем на нижний уровень второй уровень

SELECT
       T1.ISN1,
       T1.NAME1,
       T2.ISN2,
       T2.NAME2,
       T2.ISN2 MAKEISN,
       T2.NAME2 MAKENAME,
       T2.ISN2 MODELISN,
       T2.NAME2 MODELNAME
FROM
(
  SELECT
   ISN ISN1,
   SHORTNAME NAME1
  FROM DICTI
  WHERE LEVEL = 1
    START WITH PARENTISN=604607816 CONNECT BY PRIOR ISN=PARENTISN) T1,
(
  SELECT
   ISN ISN2,
   SHORTNAME NAME2,
   PARENTISN
  FROM DICTI
  WHERE LEVEL = 2
    START WITH PARENTISN=604607816 CONNECT BY PRIOR ISN=PARENTISN) T2
WHERE
    T1.ISN1 = T2.PARENTISN

UNION ALL

SELECT
       T1.ISN1,
       T1.NAME1,
       T2.ISN2,
       T2.NAME2,
       S_MAKE.MAKEISN,
       S_MAKE.MAKENAME,
       S_MAKE.MAKEISN MODELISN,
       S_MAKE.MAKENAME MODELNAME
FROM
(
  SELECT
   ISN ISN1,
   SHORTNAME NAME1
  FROM DICTI
  WHERE LEVEL = 1
    START WITH PARENTISN=604607816 CONNECT BY PRIOR ISN=PARENTISN) T1,
(
  SELECT
   ISN ISN2,
   SHORTNAME NAME2,
   PARENTISN
  FROM DICTI
  WHERE LEVEL = 2
    START WITH PARENTISN=604607816 CONNECT BY PRIOR ISN=PARENTISN) T2,
(
  SELECT
   ISN MAKEISN,
   SHORTNAME MAKENAME,
   PARENTISN
  FROM DICTI
  WHERE LEVEL = 3
    START WITH PARENTISN=604607816 CONNECT BY PRIOR ISN=PARENTISN) S_MAKE
WHERE
    T1.ISN1 = T2.PARENTISN AND
    T2.ISN2 = S_MAKE.PARENTISN

UNION ALL

SELECT 0 ISN1,
       COGNOS_CONST.GET_VALUE('EMPTY_CHAR') NAME1,
       0 ISN2,
       COGNOS_CONST.GET_VALUE('EMPTY_CHAR') NAME2,
       0 MAKEISN,
       COGNOS_CONST.GET_VALUE('EMPTY_CHAR') MAKENAME,
       0 MODELISN,
       COGNOS_CONST.GET_VALUE('EMPTY_CHAR') MODELNAME
FROM DUAL
);

CREATE OR REPLACE FORCE VIEW "COGNOS"."V_DIC_VEHICLE_TYPE" ("VEHICLE_TYPE", "SHORTNAME") AS 
  select ISN VEHICLE_TYPE,
       SHORTNAME
  from DICTI
 where PARENTISN  = 8240
union all
 select 0 VEHICLE_TYPE,
        'Нет данных' SHORTNAME
   from dual;