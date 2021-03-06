CREATE TABLE "GP_USER"."HISTLOG" (
    "ISN" NUMBER NOT NULL ENABLE, 
    "NODE" NUMBER(38,0) NOT NULL ENABLE, 
    "TABLENAME" VARCHAR2(32) NOT NULL ENABLE, 
    "RECISN" NUMBER NOT NULL ENABLE, 
    "AGRISN" NUMBER, 
    "ISN3" NUMBER, 
    "SESSIONID" NUMBER(38,0) NOT NULL ENABLE, 
    "TRANSID" VARCHAR2(30), 
    "STATUS" CHAR(1) DEFAULT CHR( 255), 
    "OPERATION" CHAR(1) NOT NULL ENABLE, 
    "UPDATED" DATE DEFAULT SYSDATE, 
    "UPDATEDBY" NUMBER, 
    CONSTRAINT "PK_HISTLOG" PRIMARY KEY ("ISN")
        USING INDEX PCTFREE 10 INITRANS 4 MAXTRANS 255 COMPUTE STATISTICS 
        STORAGE(INITIAL 1048576 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
                PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
                BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
        TABLESPACE "IDXDATA"  ENABLE
) SEGMENT CREATION IMMEDIATE 
    PCTFREE 0 PCTUSED 95 INITRANS 4 MAXTRANS 255 
    NOCOMPRESS LOGGING
    STORAGE(INITIAL 20971520 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
            PCTINCREASE 0 FREELISTS 4 FREELIST GROUPS 1
            BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
    TABLESPACE "HSTDATA" 
    PARALLEL 2;
    
GRANT INSERT ON "GP_USER"."HISTLOG" to HIST;
    
CREATE TABLE "GP_USER"."HISTUNLOADED" (
    "HISTISN" NUMBER NOT NULL ENABLE, 
	"NODE" NUMBER, 
	"TABLENAME" VARCHAR2(32) NOT NULL ENABLE, 
	"RECISN" NUMBER NOT NULL ENABLE, 
	"AGRISN" NUMBER, 
	"ISN3" NUMBER, 
	"SESSIONID" NUMBER NOT NULL ENABLE, 
	"TRANSID" VARCHAR2(30) NOT NULL ENABLE, 
	"OPERATION" CHAR(1), 
	"UNLOADISN" NUMBER NOT NULL ENABLE, 
	"FLAGFAST" NUMBER(1,0), 
	"FLAGSLOW" NUMBER(3,0), 
	"UPDATED" DATE, 
	CONSTRAINT "X_HISTUNLOADED" PRIMARY KEY ("HISTISN")
        USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
        STORAGE(INITIAL 1048576 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
                PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
                BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
        TABLESPACE "IDXDATANEW"  ENABLE
) SEGMENT CREATION IMMEDIATE
    PCTFREE 5 PCTUSED 50 INITRANS 3 MAXTRANS 255 
    NOCOMPRESS LOGGING
    STORAGE(INITIAL 20971520 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
            PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
            BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
    TABLESPACE "AISDATA";

CREATE SEQUENCE "GP_USER"."SEQ_HISTUNLOADED"
    MINVALUE 1
    MAXVALUE 999999999999999999999999999
    INCREMENT BY 100
    START WITH 326644702
    CACHE 20 NOORDER  NOCYCLE;