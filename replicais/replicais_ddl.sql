DROP SCHEMA IF EXISTS REPLICAIS     CASCADE;

/* =========== create schema REPLICAIS ========== */ 

CREATE SCHEMA REPLICAIS;

/* =========== create sequence's REPLICAIS.REPLICATION_ITERATION_SEQ, REPLICAIS.REPLICATION_FULL_SEQ and REPLICAIS.REPLICATION_TABLE_SEQ ========== */

CREATE SEQUENCE REPLICAIS.REPLICATION_ITERATION_SEQ;
CREATE SEQUENCE REPLICAIS.REPLICATION_FULL_SEQ;
CREATE SEQUENCE REPLICAIS.REPLICATION_TABLE_SEQ;

/* ========== create REPLICAIS.REPLICATION_FLAG ========== */

CREATE TABLE REPLICAIS.REPLICATION_FLAG (
    flag                   INTEGER
)
DISTRIBUTED BY (flag);

-- insert FLAG into table --

INSERT INTO REPLICAIS.REPLICATION_FLAG VALUES(256);

/* ========== create REPLICAIS.REPLICATION_CONNECTION_ID ========== */

CREATE TABLE REPLICAIS.REPLICATION_CONNECTION_ID (
    id                   INTEGER
)
DISTRIBUTED BY (id);

-- insert ID into table --

INSERT INTO REPLICAIS.REPLICATION_CONNECTION_ID VALUES(1);

/* =========== create empty tables REPLICAIS.MX_REPLICATION_WORKER_0/N and MX_REPLICATION_ITERATOR ========== */

CREATE TABLE REPLICAIS.MX_REPLICATION_WORKER_0 (
    locked                 SMALLINT DEFAULT 0
)
DISTRIBUTED BY (locked);
------------------------------------

CREATE TABLE REPLICAIS.MX_REPLICATION_WORKER_1 (
    locked                 SMALLINT DEFAULT 0
)
DISTRIBUTED BY (locked);
------------------------------------

CREATE TABLE REPLICAIS.MX_REPLICATION_WORKER_2 (
    locked                 SMALLINT DEFAULT 0
)
DISTRIBUTED BY (locked);
------------------------------------

CREATE TABLE REPLICAIS.MX_REPLICATION_WORKER_3 (
    locked                 SMALLINT DEFAULT 0
)
DISTRIBUTED BY (locked);
------------------------------------

CREATE TABLE REPLICAIS.MX_REPLICATION_WORKER_4 (
    locked                 SMALLINT DEFAULT 0
)
DISTRIBUTED BY (locked);
------------------------------------

CREATE TABLE REPLICAIS.MX_REPLICATION_WORKER_5 (
    locked                 SMALLINT DEFAULT 0
)
DISTRIBUTED BY (locked);
------------------------------------

CREATE TABLE REPLICAIS.MX_REPLICATION_WORKER_6 (
    locked                 SMALLINT DEFAULT 0
)
DISTRIBUTED BY (locked);
------------------------------------

CREATE TABLE REPLICAIS.MX_REPLICATION_WORKER_7 (
    locked                 SMALLINT DEFAULT 0
)
DISTRIBUTED BY (locked);
------------------------------------

CREATE TABLE REPLICAIS.MX_REPLICATION_WORKER_8 (
    locked                 SMALLINT DEFAULT 0
)
DISTRIBUTED BY (locked);
------------------------------------

CREATE TABLE REPLICAIS.MX_REPLICATION_WORKER_9 (
    locked                 SMALLINT DEFAULT 0
)
DISTRIBUTED BY (locked);
------------------------------------

CREATE TABLE REPLICAIS.MX_REPLICATION_ITERATOR (
    locked                 SMALLINT DEFAULT 0
)
DISTRIBUTED BY (locked);

/* ========== create REPLICAIS.REPLICATION_TASKS_INCR ========== */

CREATE TABLE REPLICAIS.REPLICATION_TASKS_INCR (
    replication_iteration        INTEGER,
    replication_table            VARCHAR(32),
    replication_table_rows       INTEGER,
    replication_start_isn        INTEGER,
    replication_end_isn          INTEGER,
    replication_worker           SMALLINT,
    replication_status           VARCHAR(5) DEFAULT 'NEW',
    added_dttm                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_dttm                 TIMESTAMP,
    completed_dttm               TIMESTAMP
)
DISTRIBUTED BY (replication_iteration);

/* ========== create REPLICAIS.REPLICATION_TASKS_FULL ========== */

CREATE TABLE REPLICAIS.REPLICATION_TASKS_FULL (
    replication_task_isn        INTEGER,
    replication_table           VARCHAR(32),
    replication_status          VARCHAR(5) DEFAULT 'NEW',
    replication_job_isn         INTEGER,
    added_dttm                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_dttm                TIMESTAMP,
    completed_dttm              TIMESTAMP
)
DISTRIBUTED BY (replication_task_isn);

/* ========== create REPLICAIS.REPLICATION_TABLES ========== */

CREATE TABLE REPLICAIS.REPLICATION_TABLES (
    replication_table_isn       INTEGER,
    replication_table_schema    VARCHAR(32),
    replication_table_name      VARCHAR(32),
    replication_active          SMALLINT DEFAULT 1,
    added_dttm                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_dttm                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    primary_keys                VARCHAR[],
    blob_fields                 VARCHAR[],
    renamed_fields              VARCHAR[],
    SendBlob                    VARCHAR(1),
    SkipHist                    VARCHAR(1),
    SkipHistlog                 VARCHAR(1),
    FullReloadPriority          smallint
)
DISTRIBUTED BY (replication_table_isn);

/* ========== create REPLICAIS.REPLICATION_ERRORS ========== */

CREATE TABLE REPLICAIS.REPLICATION_ERRORS (    
    replication_iteration       INTEGER,
    replication_table_name      VARCHAR(32),
    error_dttm                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message               VARCHAR(4000),
    notification_flag           SMALLINT DEFAULT 0
)
DISTRIBUTED BY (replication_iteration);

/* ========== create REPLICAIS.REPLICATION_NEW_TABLES ========== */

CREATE TABLE REPLICAIS.REPLICATION_NEW_TABLES (
    replication_table_name      VARCHAR(32),
    added_dttm                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_dttm                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notification_dttm           TIMESTAMP,
    notification_disabled       SMALLINT DEFAULT 0
)
DISTRIBUTED BY (replication_table_name);

