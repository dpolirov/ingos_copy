------------------------------------------------------------
-- Sequences
------------------------------------------------------------
CREATE SEQUENCE storage_adm.histlog_chunks_seq;

--loadisn
CREATE SEQUENCE storage_adm.repload_seq;

--isn for ss_histlog, ss_buf_log
CREATE SEQUENCE storage_adm.ss_seq;

--isn for tt_rowid 
create sequence storage_adm.tt_seq;

--isn for log records
create sequence storage_adm.replog_seq;

create sequence storage_adm.SEQ_TASK_START;
------------------------------------------------------------
-- Log-based ETL
------------------------------------------------------------
CREATE TABLE storage_adm.mx_histlog_etl(
   LOCKED smallint
) DISTRIBUTED BY (LOCKED);

--created
CREATE TABLE storage_adm.histlog_chunks (
    isn                              INTEGER,
    max_completed_dttm               TIMESTAMP,
    added_dttm                       TIMESTAMP DEFAULT current_timestamp,
    chunk_rows                       BIGINT
)
distributed by (isn);

--migrated
CREATE TABLE storage_adm.repload (
    isn                              NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    terminal                         VARCHAR(255),
    buhdate                          TIMESTAMP,
    tablename                        VARCHAR(40),
    updatedby                        NUMERIC,
    updated                          TIMESTAMP,
    classisn                         NUMERIC,
    description                      VARCHAR(4000),
    loadtype                         NUMERIC,
    lastisnloaded                    NUMERIC,
    lastrundate                      TIMESTAMP,
    lastenddate                      TIMESTAMP,
    procisn                          NUMERIC,
    daterep                          TIMESTAMP
)
distributed by (isn);

COMMENT ON COLUMN storage_adm.repload.isn IS 'Системный идентификатор';
COMMENT ON COLUMN storage_adm.repload.datebeg IS 'Дата начала загрузки';
COMMENT ON COLUMN storage_adm.repload.dateend IS 'Дата окончания';
COMMENT ON COLUMN storage_adm.repload.buhdate IS 'Отчетная дата';
COMMENT ON COLUMN storage_adm.repload.updatedby IS 'Автор изменения';
COMMENT ON COLUMN storage_adm.repload.updated IS 'Дата изменения';
COMMENT ON COLUMN storage_adm.repload.description IS 'Описание';
COMMENT ON COLUMN storage_adm.repload.lastisnloaded IS 'НЕ ISN!!!! Просто номер "шага" для составных задач';
COMMENT ON COLUMN storage_adm.repload.procisn IS 'FK SA_PROCESS.ISN Процесс экземпляром которого является данная загрузка';

--created
CREATE TABLE storage_adm.tt_histlog (
    isn                              NUMERIC,
    table_name                       VARCHAR(32),
    recisn                           NUMERIC
)
distributed by (recisn);

CREATE TABLE storage_adm.tt_histlog_chunk (
    like hist.histlog
)
distributed by (isn);

--created
CREATE TABLE storage_adm.tt_input (
    findisn                          NUMERIC,
    table_name                       VARCHAR(32),
    recisn                           NUMERIC
)
distributed by (table_name, findisn);

--migrated
CREATE TABLE storage_adm.ss_histlog (
    isn                              NUMERIC,
    findisn                          NUMERIC,
    procisn                          integer,
    indate                           TIMESTAMP DEFAULT current_timestamp,
    table_name                       VARCHAR(32),
    loadisn                          NUMERIC DEFAULT NULL,
    recisn                           NUMERIC
)
distributed by (isn);
--WARNING: No primary key defined for storage_adm.ss_histlog

--created
--used in Load_Proc_By_tt_RowId
CREATE TABLE storage_adm.tt_rowid (
    isn                              NUMERIC
)
distributed by (isn);

COMMENT ON TABLE storage_adm.ss_histlog IS 'сюда отсаживаются логи по схемам загрузки для дальнейшей обработки. пишет сюда джоб, пакет забора -  SS_GET_LOG.LOAD_HISTLOG_PROCESS_BUFFER';
COMMENT ON COLUMN storage_adm.ss_histlog.findisn IS 'ISN, уже преобразованный';
COMMENT ON COLUMN storage_adm.ss_histlog.procisn IS 'ISN процесса, к которому относится';
COMMENT ON COLUMN storage_adm.ss_histlog.indate IS 'дата фактич попадания записи в лог';
COMMENT ON COLUMN storage_adm.ss_histlog.table_name IS 'таблица, обработанные ISN-ны которой лежат в FINDISN';
COMMENT ON COLUMN storage_adm.ss_histlog.loadisn IS 'загрузка, которая забрала данные';
COMMENT ON COLUMN storage_adm.ss_histlog.recisn IS 'Isn исходной таблицы, преобразованный в FindIsn';

--migrated
CREATE TABLE storage_adm.ss_buf_log (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    recisn                           NUMERIC,
    procisn                          integer
)
distributed by (recisn);
COMMENT ON TABLE storage_adm.ss_buf_log IS 'Буффер для отсадки записей из логов для схемы REPAGR. ';


CREATE TABLE storage_adm.ss_process_dest_tables (
    procisn                          integer,
    table_name                       VARCHAR(150),
    view_name                        VARCHAR(150),
    tt_function_name                 VARCHAR(150),
    tt_table_name                    VARCHAR(150),
    keyfield                         VARCHAR(250),
    hist_keyfield                    VARCHAR(150),
    priority                         integer,
    dest_table_index                 VARCHAR(150),
    is_histtable                     smallint DEFAULT 0::smallint,
    end_date_fld                     VARCHAR(150),
    beg_date_fld                     VARCHAR(150),
    after_script                     TEXT
)
distributed by (procisn);
--WARNING: No primary key defined for storage_adm.ss_process_dest_tables

COMMENT ON TABLE storage_adm.ss_process_dest_tables IS 'список таблиц, куда "заливает" процесс и вьюх источников. все вьюхи должны брать первоисточником tt_rowid, по Isn или Rowid - не важно. Список полей вьюхи должен точно соответсвовать списку полей таблицы (по именам тоже),
кроме первых 2-х поле - Isn,Loadisn  -  их заполнит загрузчик.';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.procisn IS 'ссылка на ISN из ss_process';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.table_name IS 'таблица получатель';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.view_name IS 'вьюха источник, должна содержать все поля получателя кроме Isn и Loadisn.';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.tt_table_name IS 'tt для промежуточных операций, должен полностью совпадать по полям с view_name и быть темпоралкой .  по полям с совпадающими названиями будет проводится поиск записи в получателе на предмет изменения';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.keyfield IS 'поле наката (связи) таблицы , в общем логический PK для таблицы получателя, список полей, ОБЯЗАТЕЛЬНО должны быть PK таблицы';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.hist_keyfield IS 'поле, по которому значения в буффере хиста - для удаления удаленных в первоисточнике записей';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.priority IS 'Очередность загрузки';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.dest_table_index IS 'Индекс для обращения к таблице получателю';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.is_histtable IS 'Признак, является ли таблица "исторической", с полями Datebg Dateend для записи, если да, то из нее не удаляем, а "останавливаем" записи датой загрузки';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.end_date_fld IS 'Поле для Update в HISTTABLE';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.beg_date_fld IS 'Поля даты начала действия записи';
COMMENT ON COLUMN storage_adm.ss_process_dest_tables.after_script IS 'Скрипт, запускается после наполнения TT_TABLE';


CREATE TABLE storage_adm.ss_process_source_tables (
    isn                              integer,
    procisn                          integer,
    log_table_owner                  VARCHAR(150),
    log_table_name                   VARCHAR(32),
    log_table_isnfld                 VARCHAR(1000),
    last_get_log                     TIMESTAMP,
    get_view                         VARCHAR(4000),
    fullloadview                     VARCHAR(4000),
    forse_hist                       NUMERIC(38)
)
distributed by (procisn);
--WARNING: No primary key defined for storage_adm.ss_process_source_tables



CREATE TABLE storage_adm.ss_processes (
    isn                              integer,
    name                             VARCHAR(150),
    description                      VARCHAR(150),
    active                           VARCHAR(1) DEFAULT 'Y',
    priority                         NUMERIC DEFAULT 0,
    blockcnt                         NUMERIC,
    wormactive                       VARCHAR(1) DEFAULT 'N',
    after_script                     TEXT
)
distributed by (isn);
--WARNING: No primary key defined for storage_adm.ss_proceses

COMMENT ON TABLE storage_adm.ss_processes IS 'список  процессов загрузки хранилища (запускаемых storage_adm.load_storage.LoadStorage(';
COMMENT ON COLUMN storage_adm.ss_processes.isn IS 'Isn процесаа';
COMMENT ON COLUMN storage_adm.ss_processes.name IS 'Название';
COMMENT ON COLUMN storage_adm.ss_processes.description IS 'Описание';
COMMENT ON COLUMN storage_adm.ss_processes.active IS 'признак активности Y|N';
COMMENT ON COLUMN storage_adm.ss_processes.priority IS 'Приоритет выполнения (чем меньше, тем раньше)';
COMMENT ON COLUMN storage_adm.ss_processes.blockcnt IS 'Размер блока логов для обработки (по сколько измененных Isn берет за раз)';
COMMENT ON COLUMN storage_adm.ss_processes.wormactive IS 'Признак обработки процесса "червяком"';


create view storage_adm.ac_process_source_tables as
select st.*
    from storage_adm.ss_process_source_tables st
    inner join storage_adm.ss_processes p
        on p.isn=st.procisn
    where p.active = 'Y';


    
CREATE TABLE storage_adm.replog (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    module                           VARCHAR(4000),
    operation                        VARCHAR(4000),
    action                           VARCHAR(4000),
    objcount                         NUMERIC,
    sqltext                          TEXT,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    errmsg                           VARCHAR(4000),
    isn1                             NUMERIC,
    isn2                             NUMERIC,
    n1                               NUMERIC,
    n2                               NUMERIC,
    date1                            TIMESTAMP,
    date2                            TIMESTAMP,
    updated                          TIMESTAMP default current_timestamp, 
    updatedby                        NUMERIC,
    terminal                         VARCHAR(255),
    previsn                          NUMERIC,
    no                               NUMERIC,
    action                           VARCHAR(4000),
    blockisn                         NUMERIC
)
DISTRIBUTED BY (isn);








------------------------------------------------------------
-- PL/SQL based ETL 
------------------------------------------------------------
CREATE TABLE storage_adm.sa_processes (
    isn                              NUMERIC,
    shortname                        VARCHAR(100),
    usefreq                          NUMERIC DEFAULT 0,
    lastrun                          TIMESTAMP,
    nextrun                          TIMESTAMP,
    errrepcnt                        NUMERIC(38),
    errtime                          NUMERIC(38),
    errcnt                           NUMERIC(38),
    isruning                         NUMERIC(38) DEFAULT 0,
    sqltext                          TEXT,
    stoprep                          NUMERIC,
    shemaname                        VARCHAR(16),
    shemapass                        VARCHAR(255),
    rerunsqltext                     VARCHAR(4000),
    lastrunisn                       NUMERIC,
    askbeforerun                     VARCHAR(1),
    nextrun1                         TIMESTAMP,
    runisn                           NUMERIC,
    runjobisn                        NUMERIC,
    isdispecher                      NUMERIC,
    topbanner                        VARCHAR(4000),
    bottombanner                     VARCHAR(4000),
    procname                         VARCHAR(3200),
    typetasks                        VARCHAR(1),
    sqlproc                          TEXT,
    typetask                         VARCHAR(4),
    flagiscomplex                    NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.sa_processes

COMMENT ON COLUMN storage_adm.sa_processes.isn IS 'Машинный номер задачи';
COMMENT ON COLUMN storage_adm.sa_processes.shortname IS 'Название задачи';
COMMENT ON COLUMN storage_adm.sa_processes.usefreq IS 'Ссылка на режим запуска(Sa_freq)';
COMMENT ON COLUMN storage_adm.sa_processes.lastrun IS 'дата последнего запуска';
COMMENT ON COLUMN storage_adm.sa_processes.nextrun IS 'дата следующего запуска';
COMMENT ON COLUMN storage_adm.sa_processes.errrepcnt IS 'кол-во повторений при ошибке';
COMMENT ON COLUMN storage_adm.sa_processes.errtime IS 'повторять через минут';
COMMENT ON COLUMN storage_adm.sa_processes.errcnt IS 'кол-во ошибок подряд';
COMMENT ON COLUMN storage_adm.sa_processes.isruning IS '0 - не запущен';
COMMENT ON COLUMN storage_adm.sa_processes.sqltext IS 'Текст запроса (чего выполняем)';
COMMENT ON COLUMN storage_adm.sa_processes.stoprep IS 'признак остановки задачи 1 - остановили (ананлог broken)';
COMMENT ON COLUMN storage_adm.sa_processes.shemaname IS 'запускать под пользователем (из менеджера)';
COMMENT ON COLUMN storage_adm.sa_processes.rerunsqltext IS 'Чего выполняем для "доделать"';
COMMENT ON COLUMN storage_adm.sa_processes.lastrunisn IS '"внутренний" Идентификатор последнего запуска Loadisn задачи, в общем, метка из выполняемиго кода';
COMMENT ON COLUMN storage_adm.sa_processes.askbeforerun IS '"задавать вопрос перед запуском" для запуска из утилиты';
COMMENT ON COLUMN storage_adm.sa_processes.nextrun1 IS 'дата следующего запуска 1';
COMMENT ON COLUMN storage_adm.sa_processes.runisn IS 'технический идентификатор запуска для логов в SA_TASKLOG';
COMMENT ON COLUMN storage_adm.sa_processes.runjobisn IS 'JobId для задач - диспечеров';
COMMENT ON COLUMN storage_adm.sa_processes.isdispecher IS 'признак диспечер или нет';
COMMENT ON COLUMN storage_adm.sa_processes.topbanner IS 'вверхняя шапка обёртки кода';
COMMENT ON COLUMN storage_adm.sa_processes.bottombanner IS 'нижняя  шапка обёртки кода';
COMMENT ON COLUMN storage_adm.sa_processes.procname IS 'имя процедуры в которой код задачи';
COMMENT ON COLUMN storage_adm.sa_processes.typetasks IS 'С - состовная задача, S - односложная';
COMMENT ON COLUMN storage_adm.sa_processes.sqlproc IS 'Текс процедуры';


CREATE TABLE storage_adm.sa_freq (
    freq                             NUMERIC,
    dateadd                          VARCHAR(1000),
    shortname                        VARCHAR(100),
    rconst                           VARCHAR(100)
)
;
--WARNING: No primary key defined for storage_adm.sa_freq

COMMENT ON TABLE storage_adm.sa_freq IS 'Справочник интервалов для расписаний выполнения отчетов';


CREATE TABLE storage_adm.sa_sole_ref (
    taskisn                          NUMERIC,
    atomrownum                       NUMERIC,
    atomsource                       VARCHAR(3200),
    atomname                         VARCHAR(100)
)
DISTRIBUTED BY (taskisn,atomrownum);
COMMENT ON TABLE storage_adm.sa_freq IS 'Список шагов для каждого из процессов';


CREATE TABLE storage_adm.sa_tasklog (
    isn                              NUMERIC,
    repisn                           NUMERIC,
    repname                          VARCHAR(100),
    event                            VARCHAR(1000),
    pdate                            TIMESTAMP,
    pdate2                           TIMESTAMP,
    p1                               NUMERIC,
    p2                               NUMERIC,
    premark                          VARCHAR(4000),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    terminal                         VARCHAR(255),
    errsql                           TEXT,
    job_isn                          NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.sa_tasklog

--data migration for pl/sql etl configuration tables
--select shared_system.load_from_ora('storage_adm.sa_processes', 'ext_temp_sa_processes', 1, 'select * from storage_adm.sa_processes');
--select shared_system.load_from_ora('storage_adm.sa_freq', 'ext_temp_sa_freq', 1, 'select * from storage_adm.sa_freq');
--select shared_system.load_from_ora('storage_adm.sa_sole_ref', 'ext_temp_sa_freq', 1, 'select * from storage_adm.sa_sole_ref');
