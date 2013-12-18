--DROP SCHEMA IF EXISTS AIS 		CASCADE;
/* ========== create schema AIS ========== */

CREATE SCHEMA AIS;

/* ========== create table AIS.REGRESS ========== */

CREATE TABLE AIS.regress (
    isn                            NUMERIC,
    agrisn                         NUMERIC,
    currisn                        NUMERIC,
    emplisn                        NUMERIC,
    deptisn                        NUMERIC,
    agentisn                       NUMERIC,
    faultisn                       NUMERIC,
    id                             VARCHAR(20),
    role                           VARCHAR(1),
    dateopen                       DATE,
    dateclose                      DATE,
    demandsum                      NUMERIC(20,2),
    acceptdate                     DATE,
    acceptsum                      NUMERIC(20,2),
    actiondate                     DATE,
    actionsum                      NUMERIC(20,2),
    remark                         VARCHAR(1000),
    updated                        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updatedby                      NUMERIC,
    defendantisn                   NUMERIC,
    sharepc                        NUMERIC(9,6),
    claimisn                       NUMERIC,
    stateisn                       NUMERIC,
    reasonisn                      NUMERIC,
    classisn                       NUMERIC,
    ruleisn                        NUMERIC,
    refundisn                      NUMERIC,
    closereasonisn                 NUMERIC
)
DISTRIBUTED BY (isn);

/* ========== create table AIS.DOCLINE ========== */

 CREATE TABLE AIS.docline
    (isn                           NUMERIC,
    listisn                        NUMERIC,
    docisn                         NUMERIC,
    docid                          NUMERIC,
    datedoc                        DATE,
    lineno                         NUMERIC ,
    doctype                        VARCHAR(2),
    corruch                        VARCHAR(9),
    corracc                        VARCHAR(20),
    amount                         NUMERIC,
    dbcr                           VARCHAR(1),
    status                         VARCHAR(1),
    remark                         VARCHAR(1000),
    updated                        TIMESTAMP,
    updatedby                      NUMERIC,
    headisn                        NUMERIC,
    transactno                     NUMERIC,
    sndinn                         VARCHAR(15),
    sndname                        VARCHAR(1000),
    sndacc                         VARCHAR(40),
    sndbic                         NUMERIC(9,0),
    sndbank                        VARCHAR(1000),
    rcvinn                         VARCHAR(15),
    rcvname                        VARCHAR(1000),
    rcvacc                         VARCHAR(40),
    rcvbic                         NUMERIC(9,0),
    rcvbank                        VARCHAR(1000),
    flagauto                       VARCHAR(1),
    created                        DATE,
    docidin                        VARCHAR(50),
    docidoriginal                  VARCHAR(50)
)
DISTRIBUTED BY (isn);
 
/* ========== create tables AIS.MCSTATEDATA ========== */

CREATE TABLE AIS.mcstatedata (
    isn                            NUMERIC,
    carisn                         NUMERIC,
    vin                            VARCHAR(20),
    regnum                         VARCHAR(20),
    actiondate                     TIMESTAMP,
    actiontype                     NUMERIC,
    actionreason                   NUMERIC,
    created                        TIMESTAMP,
    createdby                      NUMERIC,
    updated                        TIMESTAMP,
    updatedby                      NUMERIC,
    opsname                        VARCHAR(100),
    aisvin                         VARCHAR(20)
)
DISTRIBUTED BY (isn); 