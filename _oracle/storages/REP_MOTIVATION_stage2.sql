CREATE OR REPLACE PACKAGE "STORAGES"."REP_MOTIVATION" 
IS



/* инфоструктуры по продавцам (tt_agr_salers)*/
procedure REP_AGR_SALERS_MAKE;
procedure REP_AGR_SALERS_INS(pMinIsn number, pMaxIsn number, pLoadisn Number := 0);


procedure LOAD_REPBUH2OBJ (
  pFromDate in date := null,    -- включительно (по умолчанию - 01.01.2012)
  pToDate in date := null       -- исключительно (по умолчанию - первый день текущего месяца. Но т.к. исключительно - загрузка будет по прошлый месяц)
);

END REP_MOTIVATION; -- Package spec

CREATE OR REPLACE PACKAGE BODY "STORAGES"."REP_MOTIVATION" 
IS

C_PACKAGE_NAME constant varchar2(100) := 'STORAGES.REP_MOTIVATION';
C_PACKAGE_NAME_DOTTED constant varchar2(100) := C_PACKAGE_NAME || '.';
C_MIN_DATE constant date := '01-jan-1900';
C_MAX_DATE constant date := '01-jan-3000';

C_REPBUH2OBJ_TABLE constant varchar2(100) := 'storages.repbuh2obj_sts';
C_TT_REPBUH2OBJ_TABLE constant varchar2(100) := 'storages.tt_repbuh2obj_sts';
C_TT_QUITPC_TABLE constant varchar2(100) := 'tt_quitpc_sts';

procedure LOAD_REPBUH2OBJ (
  pFromDate in date := null,    -- включительно (по умолчанию - 01.01.2012)
  pToDate in date := null       -- исключительно (по умолчанию - первый день текущего месяца. Но т.к. исключительно - загрузка будет по прошлый месяц)
)
is
 vFromDate date;  --:='01-jan-2012';
 vToDate date;
 vDestPartName Varchar2(32);
 vModuleName varchar2(32);
 vDateStr varchar2(15);
 vCaptionStr varchar2(100);
 vcnt Number:=0;
 vMV_Text varchar2(10000);

 -- Период загрузки материализованной таблицы (даты включительно) - далее MV
 vMV_DateBeg date := trunc(ADD_MONTHS(sysdate, -12), 'YYYY');  -- первый месяц года, отстоящего на 12 мес. от текущей даты
 vMV_DateEnd date := add_months(trunc(sysdate, 'mm'), 1) - 1;  -- последний день текущего месяца
 vIsNeed_MV_Refresh Boolean;
 vMVPartName Varchar2(32);
 vMVDatesText varchar2(1000);
 vStub boolean;

begin
  vFromDate := nvl(pFromDate, '01-jan-2012');
  vToDate := trunc(nvl(pToDate, sysdate), 'mm');

  /* убрано до исправления Oracle-ом 600 ошибки про мат. вью */
  vIsNeed_MV_Refresh := vMV_DateEnd >= vFromDate and vMV_DateBeg < vToDate;


  vModuleName := 'Load_repbuh2obj: ';

  /* test
  dbms_output.put_line('vFromDate = ' || vFromDate);
  dbms_output.put_line('vToDate = ' || vToDate);

  */

  if vIsNeed_MV_Refresh then
    -- удаляем представление
    vStub := system.F_DROP_MATERIALIZED_VIEW('STORAGES.MV_REPBUH2OBJ_SMALL');
  end if;


  for Cur In (
    select
      add_months(vFromDate, Level - 1) as Db,
      add_months(vFromDate, Level) as De
    from dual
    connect by level <= months_between(vToDate, vFromDate)
  )
  loop
    vDateStr := to_char(Cur.DB, 'mm.yy') || '/' || to_char(vToDate - 1, 'mm.yy');
    vCaptionStr := vModuleName || vDateStr;

    /* test
    dbms_output.put_line('Cur.DB = ' || Cur.DB);
    dbms_output.put_line('Cur.DE = ' || Cur.DE);
    if 1=0 then
    */


    DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'транкейт и откл. индексов');

    vDestPartName := system.GET_TABLE_VALUE_PARTITITON('storages.repbuh2obj', Cur.DB);

    if vDestPartName is not null then
      /* чистим буфер*/
      execute immediate 'truncate table tt_repbuh2obj_final_part';
      execute immediate 'truncate table tt_repbuh2obj';
      execute immediate 'truncate table tt_quitpc';
      execute immediate 'truncate table TT_BUH_LINECODE';
      execute immediate 'truncate table tt_rowid';

      -- выставляем отчетные даты (Cur.DE - исключительно, дальше во вьюхах в таблицу отсаживается как -1 день)
      pParam.SetParamD('pDB', Cur.DB);
      pParam.SetParamD('pDE', Cur.DE);

      -- грузим BodyISN с расшифровкой 48 статкода
      DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'расшифр. 48 статкода');
      insert into TT_BUH_LINECODE
        select * from V_BUH_LINECODE;

      /* грузим данные по начислениям сначала без исходящих*/
      DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'начисления прямые');
      insert into tt_repbuh2obj
        select * from V_REPBUH2OBJ_NACH_DIRECT;

      /* грузим данные по начислениям теперь исходящие*/
      DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'начисления исходящие');
      insert into tt_repbuh2obj
        select * from V_REPBUH2OBJ_NACH_RE;

      /* -------- грузим данные по квитовкам в темпоралку -------- */
      /* проводки, сквитованные или начисленные в периоде (для кассовых операций), с коэффициентом квитовки*/
      DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'квитовки');
      insert into tt_quitpc
        select * from V_REPBUH2OBJ_QUITPC_NORMAL;

      /* кассовый метод */
      DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'квитовки (кассовый метод)');
      -- Отсаживаем BodyISN
      insert into tt_rowid(ISN)
        select --+ Index_Combine (b)
        distinct
          b.bodyisn
        from
          storage_source.repbuhbody b
        where
          b.DateVal >= Cur.DB and b.DateVal < Cur.DE
          and b.statcode in (220, 60, 221);

      insert into tt_quitpc
        select * from V_REPBUH2OBJ_QUITPC_KASSA;   -- версия из мотивационного отчета

      /* -------- грузим данные по квитовкам из темпоралки -------- */
      /* по прямым договорам квитовки на аналитику договоров */
      DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'квитовки (аналитика прямые)');
      insert into tt_repbuh2obj
        select * from V_REPBUH2OBJ_QUIT_DIRECT;

      /* по исходящим договорам квитовки на аналитику договоров за два прохода
      (сначала строки по tt_quitpc в текущей партиции по DateVal, а потом все остальные), иначе план плохой
      Отличаются способом доступа к данным - первая часть - фулскан по текущей партиции,
      вторая - доступ по индексам в остальные партиции
      */
      DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'квитовки (аналитика исходящие)');
      insert into tt_repbuh2obj
        /*select * from V_REPBUH2OBJ_QUIT_RE;*/  -- текст вьюхи вынесен сюда. Почему - см. комментарий к вьюхе
select
  rb.DB,
  rb.DE - 1 as DE,
  rb.agrisn,
  0 as refundisn,
  rb.ReAgrisn,
  rb.statcode,
  rb.sagroup,
  rb.LineCode,
  rb.deptisn,
  rb.subaccisn,
  0 as BuhSubjIsn,
  0 as ruleisn,
  0 as ObjIsn,
  0 as objclassisn,
  rb.rptclass,
  rb.rptgroupisn,
  rb.motivgroupisn,
  null as carrptclass,
  0 as NAmountrub,
  0 as NAmountusd,
  rb.qamountrub,
  0 as qamountusd,
  rb.dateval,
  rb.datequit,
  rb.addisn,
  cast(null as char(1)) as IS_NACH,       -- признак наличия начислений
  'Y' as IS_QUIT,  -- признак наличия квитовок
  rb.Exclude_Medic, -- признак исключения строк из мотивационного отчета

  to_date(null) as first_datepay,
  to_date(null) as last_datepay,
  to_date(null) as datepaylast

from (
  select --+ ordered use_nl(Prm rb) use_hash(ST bq) no_parallel(bq) parallel(rb 28)
    Cur.DB,
    Cur.DE,
    rb.agrisn,
    rb.ReIsn as ReAgrisn,
    bq.statcode,
    rb.sagroup,
    bq.LineCode,
    rb.deptisn,
    rb.subaccisn,
    rb.rptclass,
    rb.rptgroupisn,
    rb.motivgroupisn,
    rb.amountrub * bq.quitpc as qamountrub,
    trunc(rb.dateval) as dateval,
    trunc(bq.datequit) as datequit,
    rb.agrisn as addisn,
    nvl(Bq.Exclude_Medic, 'N') as Exclude_Medic -- признак исключения строк из мотивационного отчета
  from
    repbuhre2directanalytics rb,
    (select statcode from rep_statcode Where grp = 'Исходящее перестрахование') ST,
    tt_quitpc Bq

  where
    rb.DateVal >= Cur.DB and rb.DateVal < Cur.DE
    and rb.statcode = ST.statcode
    and Bq.BodyIsn = rb.BodyIsn
) rb;


insert into tt_repbuh2obj
select
  rb.DB,
  rb.DE - 1 as DE,
  rb.agrisn,
  0 as refundisn,
  rb.ReAgrisn,
  rb.statcode,
  rb.sagroup,
  rb.LineCode,
  rb.deptisn,
  rb.subaccisn,
  0 as BuhSubjIsn,
  0 as ruleisn,
  0 as ObjIsn,
  0 as objclassisn,
  rb.rptclass,
  rb.rptgroupisn,
  rb.motivgroupisn,
  null as carrptclass,
  0 as NAmountrub,
  0 as NAmountusd,
  rb.qamountrub,
  0 as qamountusd,
  rb.dateval,
  rb.datequit,
  rb.addisn,
  cast(null as char(1)) as IS_NACH,       -- признак наличия начислений
  'Y' as IS_QUIT,  -- признак наличия квитовок
  rb.Exclude_Medic, -- признак исключения строк из мотивационного отчета
  to_date(null) as first_datepay,
  to_date(null) as last_datepay,
  to_date(null) as datepaylast

from (
  select --+ ordered use_nl(rb st)
    Cur.DB,
    Cur.DE,
    rb.agrisn,
    rb.ReIsn as ReAgrisn,
    bq.statcode,
    rb.sagroup,
    bq.LineCode,
    rb.deptisn,
    rb.subaccisn,
    rb.rptclass,
    rb.rptgroupisn,
    rb.motivgroupisn,
    rb.amountrub * bq.quitpc as qamountrub,
    trunc(rb.dateval) as dateval,
    trunc(bq.datequit) as datequit,
    rb.agrisn as addisn,
    nvl(Bq.Exclude_Medic, 'N') as Exclude_Medic -- признак исключения строк из мотивационного отчета
  from
   tt_quitpc Bq,
   repbuhre2directanalytics rb,
 (select statcode from rep_statcode Where grp = 'Исходящее перестрахование')  St


  where
    not (bq.DateVal >= Cur.DB and bq.DateVal < Cur.DE)
    and Bq.BodyIsn = rb.BodyIsn
    and rb.statcode = ST.statcode

) rb;

      commit; -- освобождаем Rollback сегмент перед группировкой

      select count(*) into vCnt from tt_repbuh2obj;
      DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'группировка: ' || vCnt);
      -- "схлопываем" строки начислений и квитовок во временную таблицу
      insert
        into tt_repbuh2obj_final_part b
          select * from V_REPBUH2OBJ_FINAL_PART;
      commit;

      -- транкейт нужно делать перед отключением индексов, т.к. после транкейта индексы опять включаются...
      execute immediate 'Alter TAble repbuh2obj truncate partition '||vDestPartName||' reuse storage';
      execute immediate 'ALTER TABLE storages.repbuh2obj MODIFY PARTITION '||vDestPartName||' UNUSABLE LOCAL INDEXES';
      -- "заливаем" данные в окончательную таблицу
      insert /*+ APPEND */ into repbuh2obj
        select * from tt_repbuh2obj_final_part;

      commit;
/* kgs 17.06.13 т.к. сверху инсер с Append то компресировать типа не надо */
--      DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'сжатие партиции');
--      execute immediate 'ALTER TABLE storages.repbuh2obj MOVE PARTITION '||vDestPartName||'  ';

      DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'перестройка индексов');
      --execute immediate 'ALTER TABLE storages.repbuh2obj MODIFY PARTITION '||vDestPartName||' REBUILD UNUSABLE LOCAL INDEXES';
      -- так резко быстрей
      system.rebuld_table_index(TABLENAME=>'storages.repbuh2obj', TBLSPACE=>'IDXDATANEW', PPARTITITON=>vDestPartName);

      -- сбор статистики
      DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'сбор статистики');
      system.PRC_COLLECT_STATS (
        OWNNAME => 'STORAGES',
        TABNAME => 'REPBUH2OBJ',
        PARTNAME => vDestPartName,
        DEGREE => 16
      );

      -- загрузка таблицы материализованного представления.
      -- даты vMV_DateBeg и vMV_DateEnd - включительно

      if vIsNeed_MV_Refresh and Cur.DB between vMV_DateBeg and vMV_DateEnd then
        DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'создание табл. мат. представления');

        vMVPartName := system.GET_TABLE_VALUE_PARTITITON('STORAGES.MV_REPBUH2OBJ_SMALL', Cur.DB);
        if vMVPartName is not null then
          execute immediate 'alter table STORAGES.MV_REPBUH2OBJ_SMALL truncate Partition ' || vMVPartName;
          execute immediate 'ALTER TABLE storages.MV_REPBUH2OBJ_SMALL MODIFY PARTITION '||vMVPartName||' UNUSABLE LOCAL INDEXES';
        end if;

        insert /*+ APPEND */ into STORAGES.MV_REPBUH2OBJ_SMALL
        select * from STORAGES.V_MV_REPBUH2OBJ_SMALL V
        where V.DB = Cur.DB;

        commit;

        if vMVPartName is null then
          vMVPartName := system.GET_TABLE_VALUE_PARTITITON('STORAGES.MV_REPBUH2OBJ_SMALL', Cur.DB);
        end if;

/* kgs 17.06.13 т.к. сверху инсер с Append то компресировать типа не надо */

--        DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'MV - сжатие партиции');
--        execute immediate 'ALTER TABLE STORAGES.MV_REPBUH2OBJ_SMALL MOVE PARTITION '||vMVPartName||'  ';

        DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'MV - перестройка индексов');
        system.rebuld_table_index(TABLENAME=>'STORAGES.MV_REPBUH2OBJ_SMALL', TBLSPACE=>'IDXDATANEW', PPARTITITON=>vMVPartName);

        -- сбор статистики
        DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'MV - сбор статистики');
        system.PRC_COLLECT_STATS (
          OWNNAME => 'STORAGES',
          TABNAME => 'MV_REPBUH2OBJ_SMALL',
          PARTNAME => vMVPartName,
          DEGREE => 16
        );
      end if;


    end if;  -- <vDestPartName is not null>

    /*
    end if;  --test (от if 1=0)
    */

  end loop;

  commit;

  -- создаем мат. вью
  if vIsNeed_MV_Refresh then
    DBMS_APPLICATION_INFO.set_module(vCaptionStr, 'создание мат. представления');
    -- определяем даты, на которые есть данные
    select
      nvl(min(V.DB), vMV_DateBeg), nvl(max(V.DE), vMV_DateEnd)
    into vMV_DateBeg, vMV_DateEnd
    from STORAGES.MV_REPBUH2OBJ_SMALL V;

    select V.Text
    into vMV_Text
    from all_views V
    where
      owner = 'STORAGES'
      and view_name = 'V_MV_REPBUH2OBJ_SMALL';

    -- заменяем секцию во вьюхе на условие по датам
    vMVDatesText := 'where DB between to_date(''' || to_char(vMV_DateBeg, 'dd.mm.yyyy') || ''', ''dd.mm.yyyy'')
                 and to_date(''' || to_char(vMV_DateEnd, 'dd.mm.yyyy') || ''', ''dd.mm.yyyy'')';
    vMV_Text := replace(vMV_Text, '/*$MAT_VIEW_DATE_SECTION$*/', vMVDatesText);

    -- убираем хинты
    vMV_Text := regexp_replace(vMV_Text, '\/\*\+.+\*\/', '');

    -- убираем комментарий
    vMV_Text := regexp_replace(vMV_Text, '\/\*\$(['||chr(10)||chr(13)||']|[^\$\*])+\$\*\/', '');

    -- создаем мат. вью
    execute immediate '
      CREATE MATERIALIZED VIEW STORAGES.MV_REPBUH2OBJ_SMALL
      ON PREBUILT TABLE
      NEVER REFRESH
      ENABLE QUERY REWRITE
      AS ' ||
      vMV_Text;
  end if;

  DBMS_APPLICATION_INFO.set_module('', '');
end;

END REP_MOTIVATION;
