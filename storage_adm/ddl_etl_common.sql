CREATE SEQUENCE storage_adm.histlog_chunks_seq;

--loadisn
CREATE SEQUENCE storage_adm.repload_seq;

--isn for ss_histlog, ss_buf_log
CREATE SEQUENCE storage_adm.ss_seq;

--isn for tt_rowid 
create sequence storage_adm.tt_seq;

--isn for log records
create sequence storage_adm.replog_seq;

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
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    operation                        VARCHAR(4000),
    objcount                         NUMERIC,
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
    module                           VARCHAR(4000),
    no                               NUMERIC,
    action                           VARCHAR(4000),
    sqltext                          TEXT,
    blockisn                         NUMERIC
)
DISTRIBUTED BY (isn);

    
