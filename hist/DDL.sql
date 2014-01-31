CREATE TABLE hist.histlog (
    isn numeric,
    node numeric(38,0),
    tablename character varying(32),
    recisn numeric,
    agrisn numeric,
    isn3 numeric,
    sessionid numeric(38,0),
    transid character varying(30),
    status character varying(1) DEFAULT chr(255),
    operation character varying(1),
    updated timestamp without time zone DEFAULT now(),
    updatedby numeric,
    unloadisn numeric
)
DISTRIBUTED BY (isn);

CREATE TABLE hist.temp_histlog (
    isn numeric,
    node numeric(38,0),
    tablename character varying(32),
    recisn numeric,
    agrisn numeric,
    isn3 numeric,
    sessionid numeric(38,0),
    transid character varying(30),
    status character varying(1),
    operation character varying(1),
    updated timestamp without time zone,
    updatedby numeric,
    unloadisn numeric
)
DISTRIBUTED BY (isn);

CREATE TABLE hist.histlog_arch (
    LIKE hist.histlog
) with (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5);