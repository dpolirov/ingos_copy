--DROP SCHEMA IF EXISTS HIST 		CASCADE;

/* ========== create schema HIST ========== */

CREATE SCHEMA HIST;

/* =========== create HIST.HISTLOG =========== */

CREATE TABLE HIST.HISTLOG (
    isn                           		NUMERIC,
	node                           		NUMERIC(38,0),
	tablename                      		VARCHAR(32),
	recisn                         		NUMERIC,
	agrisn                         		NUMERIC,
	isn3                           		NUMERIC,
	sessionid                      		NUMERIC(38,0),
	transid                        		VARCHAR(30),
	status                         		VARCHAR(1) DEFAULT CHR(255),
	operation                      		VARCHAR(1),
	updated                        		TIMESTAMP,
	updatedby                      		NUMERIC
)
DISTRIBUTED BY (isn);

/* ========== create table HIST.DOCLINE ========== */

CREATE TABLE HIST.DOCLINE(LIKE AIS.DOCLINE, HISTISN NUMERIC) DISTRIBUTED BY (isn);

/* ========== create table HIST.REGRESS ========== */

CREATE TABLE HIST.REGRESS(LIKE AIS.REGRESS, HISTISN NUMERIC) DISTRIBUTED BY (isn);

/* ========== create table HIST.MCSTATEDATA ========== */

CREATE TABLE HIST.MCSTATEDATA(LIKE AIS.MCSTATEDATA, HISTISN NUMERIC) DISTRIBUTED BY (isn);
