CREATE OR REPLACE FUNCTION "SHARED_SYSTEM"."CUT_TABLE" (
  pTableName in varchar2,
  pColumnName in varchar2,
  pMinIsn in Number,
  pIndexName in varchar2:=null,
  pRowCount in Number:=100000
) RETURN  Number
IS
  vSql            VARCHAR2(4000);
  vMaxIsn         NUMBER;
  vIndex          VARCHAR2(150):= pIndexName;
  vTableOwner     VARCHAR2(30) := UPPER(substr(pTableName,1,instr(pTableName,'.')-1));
  vTableName      VARCHAR2(30) := UPPER(substr(pTableName,instr(pTableName,'.')+1));
  vColumnName     VARCHAR2(30) := UPPER(pColumnName);
  vOwnCnt         NUMBER;
  vDbTableOwner   VARCHAR2(30);
  vTableOwnerNull VARCHAR2(30) := 'NO OWNER';
BEGIN
  IF pparam.GetParamV('TABLENAME')  = vTableName AND
     pparam.GetParamV('TABLEOWNER') = NVL(vTableOwner, vTableOwnerNull) AND
     pparam.GetParamV('COLUMNNAME') = vColumnName THEN
     vDbTableOwner :=  pparam.GetParamV('DBTABLEOWNER');
     vIndex := pparam.GetParamV('INDEXNAME');
  ELSE
    -- выбираем индекс с наименьшим кол-вом столбцов (MIN(index_name) KEEP...)
    -- условие, при котором будет выполнена нарезка - tblowner=1, поэтому столбцы tblowner и
    -- tblcnt определяются простым MIN. Т.е. когда таблиц будет больше 1, то будет не важно
    -- какого из ownerов вернет запрос, а когда таблица одна, то значение поля owner
    -- будет у всех записей одинаковое. Значение поля owncnt одинаковое для всех записей всегда и
    -- равно кол-ву ownerов из dba_tables
    -- внешнее соединение dba_tables t c dba_ind_columns cl нужно для того, чтобы
    -- различать ошибки входных параметров (нет таблицы, много таблиц или есть таблица, но нет индекса)
    SELECT MIN(index_name) KEEP (dense_rank FIRST ORDER BY colcnt) index_name,
           MIN(owner)  tblowner,
           NVL(MIN(owncnt), 0) tblcnt
    INTO vIndex, vDbTableOwner, vOwnCnt
    FROM (SELECT t.owner,
                 t.table_name,
                 cl.index_name,
                 COUNT(DISTINCT t.owner) over () AS owncnt, --кол-во владельцев указанной таблицы
                (SELECT COUNT(1)
                 FROM  dba_ind_columns x
                 WHERE x.table_owner=cl.table_owner
                   AND x.table_name=cl.table_name
                   AND x.index_name=cl.index_name
                ) AS colcnt -- кол-во столбцов в индексе
          FROM dba_tables t,
               dba_ind_columns cl
          WHERE t.owner=NVL(vTableOwner,t.owner)
            AND t.table_name=vTableName
            AND cl.table_owner(+)=t.owner
            AND cl.table_name(+)=t.table_name
            AND cl.column_name(+)=vColumnName
            AND cl.column_position(+)=1 -- указанная колонка должна быть ведущей в индексе
         );


    IF vOwnCnt = 0 THEN
      raise_application_error(-20001,'Invalid table name: pTableName='||pTableName);
    ELSIF vOwnCnt > 1 THEN
      raise_application_error(-20001,'Too many tables: pTableName='||pTableName);
    END IF;

    IF vIndex IS NULL THEN
      raise_application_error(-20001,'No index on column '||vColumnName||' of table '||UPPER(pTableName));
    END IF;

    -- Сохраняем в коллекции параметры запроса
    pparam.SetParamV('TABLEOWNER',NVL(vTableOwner,vTableOwnerNull));
    pparam.SetParamV('TABLENAME',vTableName);
    pparam.SetParamV('DBTABLEOWNER',vDbTableOwner);
    pparam.SetParamV('COLUMNNAME',vColumnName);
    pparam.SetParamV('INDEXNAME',vIndex);

  END IF;



  -- режем таблицу
  vSql:='SELECT MAX(cValue)
         FROM
         ( SELECT --+ Index_Asc (a '||vIndex||')
            '||vColumnName||' cValue
           FROM '||vDbTableOwner||'.'||vTableName||' a
           WHERE '||vColumnName||'>:pMinIsn and RowNum<=:pRowCount
           ORder by '||vColumnName||' Asc)';
--dbms_output.put_line(vSql);
  EXECUTE IMMEDIATE vSql INTO vMaxIsn USING pMinIsn,pRowCount;



  RETURN vMaxIsn;

END;
;

  CREATE OR REPLACE PROCEDURE "SHARED_SYSTEM"."RESTORE_TABLE_INDEX" 
   (pTableName Varchar2,
    pnoBitmap Number:=0, /* 0 - все индексы, 1 - кроме битовых*/
    piteration NUMBER := 20 -- EGAO 07/08/2008 кол-во итераций операций создания индексов
)
   IS
       vSql varchar2(32000);
       PSesId Number;
       viteration NUMBER := nvl(piteration,20);
Begin
--RESTORE_TABLE_INDEX_P(pTableName,pnoBitmap,piteration);

/* восстановление ранее сохраненных индексов таблицы (процедурой STORE_AND_DROP_TABLE_INDEX)
если имя таблицы не полное - ищется в текущей схеме

если произошла ошибка при построении индекса - будет записть в PT_LOG, процедура ошибки не вернет
*/



FOR indx IN 1..viteration LOOP -- EGAO 07/08/2008
  DBMS_APPLICATION_INFO.set_module('Restore Index On '||pTableName,'iteration: '||TO_CHAR(indx));

  pSesId:=Parallel_Tasks.createnewsession();

  For Cur In
   (/*Select Replace(Upper(ddl),'TRNDATA','IDXDATANEW') DDL,INDEXNAME,OWNER
    from SHARED_SYSTEM.SS_TABLE_INDEX
    Where
      TABLENAME=Upper(pTableName) and OWNER=USer
      Or
      owner||'.'||tablename=Upper(pTableName) */
    -- EGAO 07/08/2008 Выбираем только отсутствующие индексы (старый код см. выше в комментариях)
    SELECT
      REPLACE(Upper(a.ddl), 'TRNDATA', 'IDXDATANEW') as DDL,
      a.INDEXNAME,
      a.OWNER,
      Instr(upper(a.DDl), 'PARALLEL') as ParallelPos
    FROM SHARED_SYSTEM.SS_TABLE_INDEX a
    WHERE (a.TABLENAME=Upper(pTableName) AND a.OWNER=USER OR a.owner||'.'||a.tablename=Upper(pTableName))
      AND (owner, indexname) NOT IN (SELECT i.owner,i.index_name
                                     FROM all_indexes i
                                     WHERE i.owner=a.owner
                                       AND i.table_name=a.tablename)
    order by Instr(upper(a.DDl), 'PARALLEL') asc -- sts - сортируем сначала не параллельные индексы

  ) Loop




  IF Instr(Cur.DDl,'INDEX')>0 AND Instr(Cur.DDl,'BITMAP')<=0 And Instr(Cur.DDl,'LOCAL')<=0 And Instr(Cur.DDl,'COMPRESS')<=0
  AND Instr(Cur.DDl,'UNIQUE')<=0   THen
   vSql:=Cur.DDl||' COMPRESS';

  else
   vSql:=Cur.DDl;

  end if;
   --dbms_output.put_line(vSql);

IF Instr(Cur.DDl,'LOCAL')<=0 Then
vSql:=vSql||'   INITRANS    12';
--vSql:=vSql||' PARALLEL 12';
vSql:=vSQl||'   STORAGE ( INITIAL 51200K
                NEXT 5120K
                MINEXTENTS 1)';
end if;


  vSql:='Begin
           execute immediate  ''Alter session set db_file_multiblock_read_count=1028'';
           DBMS_APPLICATION_INFO.set_module(''Restore Index On '||pTableName||''','''||Cur.INDEXNAME||''');
           execute Immediate '''||vSql||''';
           exception when others then raise_application_error(PARALLEL_TASKS.cError,SQLERRM);
         end;';
   If (pnoBitmap=0 or (Upper(Cur.ddl) not Like '%BITMAP%')) THen
     -- sts для параллельных индексов усекаем гидру до одной головы
     -- Обратного "размножения" сессий гидры нет, т.к. по сортировке параллельные индексы идут последними
     -- и для них всех д.б. одна голова
     if Cur.ParallelPos >= 1 then
       Parallel_Tasks.SetSesJobCnt(1);
     end if;
     Parallel_Tasks.processtask(psesid,vsql,pUser=>Cur.Owner);

  -- Execute immediate vSql;
  end if;

   end loop;

  Parallel_Tasks.endsession(psesid);
END LOOP;

end;;

  CREATE OR REPLACE PROCEDURE "SHARED_SYSTEM"."STORE_AND_DROP_TABLE_INDEX" 
  ( pTableName Varchar2, /* таблица, или в текущей схеме или полное название*/
    pBitmap Number:=0, /* 0 - только битовые, 1 - все*/   --MSerp .б твою мать, наоборот!!!!!  0 - все!!!
    pStore Number:=1, /*1 - сохраняем, 0 - нет*/
    pDrop Number:=1,  /* 1 - удаляем, 0 - нет*/
    pDropPk Number:=0  /* 1 - удаляем первичные ключи 0 - нет. Восстанавливаемый индекс  уникален, но не PK */
    )
   IS
       pDdl Long;

BEGIN
/*сохранение и удаление индексов таблицы сохраняется имя таблицы, индекса и DDL
DDL генерится самостоятельно
Если индекса уже нет - его DDL не удаляется и он будет восстановлен RESTORE_TABLE_INDEX*/

IF pDropPk=1 then


  FOR Cur IN (SELECT *
              FROM  all_constraints
              WHERE  ( (OWNER||'.'||table_name=Upper(pTableName)) OR
                       (OWNER=USER AND table_name=Upper(pTableName))
                      )
               AND constraint_name NOT LIKE 'SYS%'
               AND  constraint_type='P'
             ) LOOP
     Execute immediate 'Alter Table '||Cur.Owner||'.'||Cur.Table_Name||' drop constraint '||Cur.constraint_name;

  end loop;

end if;

  FOR Cur IN (SELECT *
              FROM  all_Indexes
              WHERE (TABLESPACE_NAME IS NOT NULL OR Partitioned='YES')
                AND( Index_type IN ('BITMAP', 'NORMAL') -- EGAO 17.09.2010 AND (Index_Type='BITMAP' OR  Index_Type LIKE '%NORMAL%')
                OR  Index_type like  '%BITMAP')
                AND (pBitmap=0 OR Index_Type ='BITMAP')
                AND INDEX_NAME NOT LIKE 'SYS%'
                AND (TABLE_OWNER||'.'||table_name=Upper(pTableName) OR
                    (TABLE_OWNER=USER AND table_name=Upper(pTableName)))
                AND (pDropPk=1 OR  UNIQUENESS='NONUNIQUE')
             ) LOOP


    Select 'Create '||Decode(UNIQUENESS,'UNIQUE','UNIQUE',' ')
    ||Decode(INDEX_TYPE,'BITMAP','BITMAP','FUNCTION-BASED BITMAP','BITMAP','')||
    ' Index '||TABLE_OWNER||'.'||index_name||' on '||TABLE_OWNER||'.'||TABLE_NAME ||'('||
    (
    Select conc( Decode(rownum,1,'',',')||
    -- sts 15.08.2011 - если имя индексируемого столбца отлично от латинских букв в верх. регистре, то обрамляем в кавычки
    decode(c.column_name, CONVERT(Upper(c.column_name), 'US7ASCII', 'WE8ISO8859P1'), '', '"') ||
    nvl(system.getColExpression(c.index_owner, c.index_name, c.column_position), c.column_name) ||
    decode(c.column_name, CONVERT(Upper(c.column_name), 'US7ASCII', 'WE8ISO8859P1'), '', '"') || ' ' || c.descend || ' ')
    from  all_ind_columns  c
    where c.index_name=i.index_name
    and (c.TABLE_OWNER||'.'||c.table_name=Upper(pTableName) or c.TABLE_OWNER=User And c.table_name=Upper(pTableName))


    )||')'|| Decode(compression,' ENABLED ','COMPRESS','')||' TABLESPACE IDXDATANEW '||
    Decode(LOGGING,'NO',' NOLOGGING','')||
    Decode(PARTITIONED,'YES',' LOCAL','' /*EGAO 24.12.2010 Decode(GLOBAL_STATS,'NO',' LOCAL','GLOBAL'),''*/)||
    Decode(DEGREE,'1',' ','DEFAULT',' ',' PARALLEL '||DEGREE||' ' /*KGS 28.11.2011 */)
    ||' COMPUTE STATISTICS' /* KGS 15.02.2011*/

    INTO pDDl
    FROM all_Indexes i
    WHERE INDEX_NAME=Cur.INDEX_NAME
      AND Owner=Cur.Owner;

    IF  pStore=1 THEN
      /*UPDATE  SHARED_SYSTEM.SS_TABLE_INDEX
      SET DDl=pDdl
       WHERE TableName=Cur.TABLE_NAME
         AND INDEXNAME=Cur.INDEX_NAME
         AND Owner=Cur.Owner;

     IF Sql%rowcount=0 THEN
      INSERT INTO SHARED_SYSTEM.SS_TABLE_INDEX
      VALUES (Cur.TABLE_NAME,Cur.INDEX_NAME,pDdl,Cur.Owner);
     END IF;*/
     -- EGAO 09.11.2009 Предыдущий вариант в комментариях выше
     MERGE INTO SHARED_SYSTEM.SS_TABLE_INDEX a
     USING (SELECT Cur.Owner AS owner, Cur.TABLE_NAME AS tablename, Cur.INDEX_NAME AS indexname, pDdl AS ddl  FROM dual) b
     ON (a.owner=b.owner AND a.tablename=b.tablename AND a.indexname=b.indexname)
     WHEN MATCHED THEN UPDATE SET a.ddl=b.ddl
     WHEN NOT MATCHED THEN INSERT (a.owner, a.tablename, a.indexname, a.ddl)
     VALUES(b.owner, b.tablename, b.indexname, b.ddl);

      COMMIT;
    END IF;

    IF pDrop=1 THEN
      system.DROP_INDEX(Cur.Owner||'.'||Cur.INDEX_NAME); -- EGAO 11.04.2009 system.DROP_INDEX(Cur.TABLE_OWNER||'.'||Cur.INDEX_NAME);
    END IF;

  END LOOP;
end;;

PROCEDURE PRC_COLLECT_STATS (
    OWNNAME varchar2,
    TABNAME varchar2,
    PARTNAME varchar2 := null,
    DEGREE number := DBMS_STATS.to_degree_type(DBMS_STATS.get_param('DEGREE'))
)
is
  GRANULARITY varchar2(32);
  METHOD_OPT varchar2(32);
begin
  DBMS_STATS.DELETE_TABLE_STATS (
    OWNNAME => OWNNAME,
    TABNAME => TABNAME,
    PARTNAME => PARTNAME
  );
  commit;


  IF PARTNAME IS NULL THEN
    GRANULARITY:='GLOBAL AND PARTITION';
    METHOD_OPT:='FOR ALL COLUMNS SIZE AUTO';
  ELSE
    GRANULARITY:='PARTITION';
    METHOD_OPT:='FOR ALL COLUMNS';
  end if;

  DBMS_STATS.GATHER_TABLE_STATS (
    OWNNAME => OWNNAME,
    TABNAME => TABNAME,
    PARTNAME => PARTNAME,
    ESTIMATE_PERCENT => DBMS_STATS.AUTO_SAMPLE_SIZE,
    BLOCK_SAMPLE => false,
    METHOD_OPT => METHOD_OPT,
    DEGREE => DEGREE,
    GRANULARITY => GRANULARITY,
    CASCADE => true,
    FORCE => true
  );

  commit;
exception
  when others then
    case SQLCODE
      when -00909 then null; -- ORA-00909: invalid number of arguments
      else raise;
    end case;
end;