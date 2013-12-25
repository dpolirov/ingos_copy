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