CREATE OR REPLACE PROCEDURE cognos.MAKE_DIC_TABLES(
  pRaiseWhenError in char := 'N'  -- признак генерации исключений при ошибках. N = подавлять исключения
) AUTHID CURRENT_USER
as
  C_OWNER            constant varchar2(30) := 'COGNOS';
  C_VIEW_NAME_TEMPL  constant varchar2(30) := 'V_DIC%';

  vNum number := 1;
  vResDrop boolean;
  vResCreate boolean;
  vErrorText varchar2(1000);
  vRecCount number;
  vPKField varchar2(30);
  vSQL varchar2(1000);
  vFields varchar2(1000);

  function l_exec_SQL(pSQL in varchar2, pErrorText out varchar2) return Boolean
  is
  begin
    dbms_output.put_line(pSQL);

    execute immediate pSQL;
    return True;
  exception
    when others then
      pErrorText := SQLERRM;
      if pRaiseWhenError = 'Y' then
        raise;
      end if;
      return False;
  end;

  procedure l_write_log(
    pOwner varchar2,
    pViewName varchar2,
    pTableName varchar2,
    pAction varchar2,
    pErrorMsg varchar2 := null,
    pRecordCount number := null
  )
  is
    PRAGMA AUTONOMOUS_TRANSACTION;

    vIsError char(1) := 'N';
  begin
    if pErrorMsg is not null then
      vIsError := 'Y';
    end if;

    insert into COGNOS.CFG_TRANSFORM_VIEWS_LOGS(OWNER, VIEW_NAME, TABLE_NAME, ACTION, IS_ERROR, MSG, TABLE_RECORD_COUNT, UPDATED)
    values(pOwner, pViewName, pTableName, pAction, vIsError, pErrorMsg, pRecordCount, current_timestamp);

    commit;
  end;

  procedure l_Grant_Table(
    pOwner varchar2,
    pTableName varchar2,
    pViewName varchar2

  )
  is
    vErrTxt varchar2(1000);
  begin
    for x in (
      Select
        'GRANT ' || p.PRIVILEGE || ' on ' || pTableName || ' to ' || p.GRANTEE ||
        decode(p.GRANTABLE, 'YES', ' with grant option') ||
        decode(p.HIERARCHY, 'YES', ' with hierarchy option ') as grant_script
      from user_tab_privs p
      where
        grantor = pOwner and
        table_name = pViewName  -- грантуем созданные таблицы так же, как и исходные вьюхи
    )
    loop

    dbms_output.put_line(x.grant_script);
      if not l_exec_SQL(x.grant_script, vErrTxt) then
        l_write_log(pOwner, pViewName, pTableName, 'Grant', vErrTxt);
      end if;

    end loop;
  end;

begin
  for Cur in (
    select
      V.OWNER, V.VIEW_NAME, V.TABLE_NAME, nvl2(T.TABLE_NAME, 'Y', 'N') as TABLE_EXISTS,
      count(*) over() as CNT
    from (
      select
        V.OWNER, V.VIEW_NAME, nvl(T.TABLE_NAME, replace(V.VIEW_NAME, 'V_', 'T_')) as TABLE_NAME
      from
       (select V.OWNER, V.VIEW_NAME
        from ALL_VIEWS V
        where V.VIEW_NAME like 'V_DIC%'
        UNION
        select upper(V.OWNER), upper(V.VIEW_NAME)
        from COGNOS.CFG_INCLUDED_TRANSFORM_VIEWS V -- список вьюх, по которым нужно строить таблицы и которые не подходят под like 'V_DIC%'
        ) V,
        COGNOS.CFG_EXCLUDED_TRANSFORM_VIEWS EXCL,   -- список вьюх, по которым не нужно строить таблиц
        COGNOS.CFG_TRANSFORM_VIEWS_TABLENAME T      -- соответствие вьюхи и создаваемой таблицы (в случае несоответствия названий, создаваемых автоматически)
      where
        V.OWNER = 'COGNOS'
        and V.OWNER = EXCL.OWNER(+)
        and V.VIEW_NAME = EXCL.VIEW_NAME(+)
        and EXCL.VIEW_NAME is null
        and V.OWNER = T.OWNER(+)
        and V.VIEW_NAME = T.VIEW_NAME(+)

        --and V.VIEW_NAME in ('V_DIC_KALENDAR_1', 'V_DIC_H_AGRRULE')  --for test only
        --and V.VIEW_NAME = 'V_DIC_STATCODE'

    ) V,
      ALL_TABLES T
    where
      V.OWNER = T.OWNER(+)
      and V.TABLE_NAME = T.TABLE_NAME(+)
    order by V.VIEW_NAME
  )
  loop
    DBMS_APPLICATION_INFO.set_module('MAKE_DIC_TABLES ' || vNum || '/' || Cur.CNT, 'view: ' || CUR.VIEW_NAME);

    if Cur.TABLE_EXISTS = 'Y' then
      vResDrop := l_exec_SQL('DROP TABLE ' || Cur.OWNER || '.' || Cur.TABLE_NAME, vErrorText);
      if not vResDrop then
        l_write_log(Cur.OWNER, Cur.VIEW_NAME, Cur.TABLE_NAME, 'Drop', vErrorText);

        dbms_output.put_line('Drop ' || Cur.OWNER || '.' || Cur.TABLE_NAME || ' --- Error: ' || vErrorText);
      end if;
    end if;

    -- определяем первичный ключ
    select
      max(t.column_name)
        keep(dense_rank last
             order by decode(upper(t.column_name), 'ISN', 1, 0) asc,
                      REGEXP_SUBSTR(upper(t.column_name), '[[:digit:]]+') asc) as PKField
      into vPKField
          from all_tab_columns t
          Where Owner = Cur.OWNER and Table_Name = Cur.VIEW_NAME
          and (
            upper(t.column_name) = 'ISN'
            or REGEXP_LIKE(upper(t.column_name), 'LEV([[:digit:]])+_ISN')
          );

    vFields := '';
    if vPKField is not null then

      -- формируем строку для создания Index organized table
      for Col in (
        select
          decode(instr(t.column_name, ' '), '0', '', '"') || -- обрамляем в кавычки, если есть пробелы в названии поля
            t.column_name ||
              decode(instr(t.column_name, ' '), '0', '', '"') as column_name  -- обрамляем в кавычки, если есть пробелы в названии поля
        from all_tab_columns t
        where Owner = Cur.OWNER and Table_Name = Cur.VIEW_NAME
        order by column_id asc
      ) loop
        if upper(Col.column_name) = vPKField then
          vFields := vFields || Col.column_name || ' primary key, ';
        else
          vFields := vFields || Col.column_name || ', ';
        end if;
      end loop;
      vFields := ' (' || RTrim(vFields, ', ') || ') organization index ';
    end if;


    vResCreate := l_exec_SQL('CREATE TABLE ' || Cur.OWNER || '.' || Cur.TABLE_NAME || vFields || ' as SELECT * FROM ' || Cur.OWNER || '.' || Cur.VIEW_NAME, vErrorText);
    if not vResCreate then
      l_write_log(Cur.OWNER, Cur.VIEW_NAME, Cur.TABLE_NAME, 'Create', vErrorText);

      dbms_output.put_line('Create ' || Cur.OWNER || '.' || Cur.TABLE_NAME || ' --- Error: ' || vErrorText);
    end if;

    if vResCreate then
      -- собираем статистику
      SHARED_SYSTEM.PRC_COLLECT_STATS(Cur.OWNER, Cur.TABLE_NAME, null, 1);
      -- грантуем созданные таблицы так же, как и исходные вьюхи
      l_Grant_Table(Cur.OWNER, Cur.TABLE_NAME, Cur.VIEW_NAME);

      execute immediate 'select count(*) from ' || Cur.OWNER || '.' || Cur.TABLE_NAME
      into vRecCount;

      l_write_log(Cur.OWNER, Cur.VIEW_NAME, Cur.TABLE_NAME, 'Done', pErrorMsg => null, pRecordCount => vRecCount);
      dbms_output.put_line(Cur.OWNER || '.' || Cur.VIEW_NAME || ' -> ' || Cur.OWNER || '.' || Cur.TABLE_NAME || '   --- Done. Count records: ' || vRecCount);
    end if;

    vNum := vNum + 1;
  end loop;

  DBMS_APPLICATION_INFO.set_module('', '');
end;
