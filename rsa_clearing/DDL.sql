CREATE TABLE rsa_clearing.bufmsgxml (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    msg_id                           VARCHAR(19),
    msg_parentid                     VARCHAR(19),
    msg_date                         TIMESTAMP,
    msg_type                         VARCHAR(10),
    status                           VARCHAR(2),
    xmlblock                         TEXT,
    parentisn                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    sender_id                        VARCHAR(12),
    taskisn                          NUMERIC,
    imgisn                           NUMERIC,
    sysref                           VARCHAR(60),
    refisn1                          NUMERIC,
    refisn2                          NUMERIC,
    refisn3                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN rsa_clearing.bufmsgxml.refisn1 IS $COMM$ссылка на предклиринговый отчет$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.refisn2 IS $COMM$ссылка на постклиринговый отчет$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.refisn3 IS $COMM$зарезервировано (на будущее)$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.isn IS $COMM$PK$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.classisn IS $COMM$типы сообщений (dicti)$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.msg_id IS $COMM$Id (sysref)$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.msg_parentid IS $COMM$ParentId$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.msg_date IS $COMM$Дата создания сообщение (sysref)$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.msg_type IS $COMM$in out$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.status IS $COMM$статус$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.xmlblock IS $COMM$XML сообщение$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.parentisn IS $COMM$М.б. не понадобится$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.sender_id IS $COMM$код отправителя$COMM$;
COMMENT ON COLUMN rsa_clearing.bufmsgxml.imgisn IS $COMM$ссылка на docimage$COMM$;


CREATE TABLE rsa_clearing.pvu_clam_t (
    isn                              NUMERIC,
    daterequest                      TIMESTAMP,
    requestid                        VARCHAR(50),
    prevclmref                       VARCHAR(60),
    sendercode                       VARCHAR(50),
    agrid                            VARCHAR(20),
    agrisn                           NUMERIC,
    dateloss                         TIMESTAMP,
    gibddstn                         VARCHAR(1),
    acdnschm                         VARCHAR(20),
    tronschm                         VARCHAR(20),
    incplactype                      VARCHAR(20),
    lossaddrcode                     VARCHAR(13),
    lossregion                       VARCHAR(200),
    lossdistrict                     VARCHAR(200),
    losscity                         VARCHAR(200),
    lossterritory                    VARCHAR(200),
    initamt                          VARCHAR(200),
    hldng                            VARCHAR(2),
    dirbsoser                        VARCHAR(20),
    dirbsono                         VARCHAR(20),
    drvlim                           VARCHAR(1),
    diragrdatebeg                    TIMESTAMP,
    diragrdateend                    TIMESTAMP,
    dirdateusebeg1                   TIMESTAMP,
    dirdateuseend1                   TIMESTAMP,
    dirdateusebeg2                   TIMESTAMP,
    dirdateuseend2                   TIMESTAMP,
    dirdateusebeg3                   TIMESTAMP,
    dirdateuseend3                   TIMESTAMP,
    dircarregno                      VARCHAR(20),
    dircarvin                        VARCHAR(40),
    dircarbodyid                     VARCHAR(20),
    dircarmake                       VARCHAR(40),
    dircarmodel                      VARCHAR(40),
    dircarenginepowerhp              NUMERIC,
    dircarconstructed                TIMESTAMP,
    dircartype                       VARCHAR(40),
    dircardoctype                    VARCHAR(40),
    dircardocser                     VARCHAR(40),
    dircardocno                      VARCHAR(40),
    dirsubjjuridical                 VARCHAR(1),
    dirsubjname                      VARCHAR(200),
    dirsubjbirthdate                 TIMESTAMP,
    dirsubjaddrcode                  VARCHAR(13),
    dirsubjdoctype                   VARCHAR(40),
    dirsubjdocser                    VARCHAR(40),
    dirsubjdocno                     VARCHAR(40),
    dirownerjuridical                VARCHAR(1),
    dirownername                     VARCHAR(200),
    dirownerbirthdate                TIMESTAMP,
    dirowneraddrcode                 VARCHAR(13),
    dirownerdoctype                  VARCHAR(40),
    dirownerdocser                   VARCHAR(40),
    dirownerdocno                    VARCHAR(40),
    dirownerinn                      VARCHAR(15),
    dirdrivername                    VARCHAR(200),
    dirdriverbirthdate               TIMESTAMP,
    dirdriveraddrcode                VARCHAR(13),
    dirdriverdoctype                 VARCHAR(40),
    dirdriverdocser                  VARCHAR(40),
    dirdriverdocno                   VARCHAR(40),
    bsoser                           VARCHAR(20),
    bsono                            VARCHAR(20),
    regno                            VARCHAR(20),
    carmark                          VARCHAR(40),
    carmodel                         VARCHAR(40),
    subjname                         VARCHAR(200),
    subjdoctype                      VARCHAR(40),
    subjdocser                       VARCHAR(40),
    subjdocno                        VARCHAR(40),
    taskisn                          NUMERIC,
    refisn1                          NUMERIC,
    refisn2                          NUMERIC,
    refisn3                          NUMERIC,
    reftxt                           VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN rsa_clearing.pvu_clam_t.lossaddrcode IS $COMM$код КЛАДР метса ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.lossregion IS $COMM$регион места ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.lossdistrict IS $COMM$район места ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.losscity IS $COMM$город места ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.lossterritory IS $COMM$населённый пункт места ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.initamt IS $COMM$заявленная сумма убытка$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.hldng IS $COMM$признак наличия судебного решения$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirbsoser IS $COMM$серия полиса заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirbsono IS $COMM$номер полиса заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.drvlim IS $COMM$мультидрайв Y/N$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.diragrdatebeg IS $COMM$дата начала действия полиса заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.diragrdateend IS $COMM$дата окончания действия полиса заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirdateusebeg1 IS $COMM$периоды использования ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirdateusebeg2 IS $COMM$периоды использования ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirdateusebeg3 IS $COMM$периоды использования ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dircarregno IS $COMM$гос. номер ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dircarvin IS $COMM$VIN ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dircarbodyid IS $COMM$номер кузова ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dircarmake IS $COMM$марка ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dircarmodel IS $COMM$модель ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dircarenginepowerhp IS $COMM$Мощность в л.с$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dircarconstructed IS $COMM$год выпуска ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dircartype IS $COMM$тип ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dircardoctype IS $COMM$вид документа на ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dircardocser IS $COMM$серия документа на ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dircardocno IS $COMM$номер документа на ТС заявителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirsubjjuridical IS $COMM$юр/физ лицо$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirsubjname IS $COMM$ФИО страхователя по договору прямого страховщика$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirsubjbirthdate IS $COMM$дата рождения страхователя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirsubjaddrcode IS $COMM$код КЛАДР для адреса страхователя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirsubjdoctype IS $COMM$вид документа, удост. личность страхователя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirsubjdocser IS $COMM$серия документа страхователя ПС$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirsubjdocno IS $COMM$номер документа страхователя ПС$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirownerjuridical IS $COMM$юр/физ лицо$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirownername IS $COMM$ФИО собственника ТС по договору прямого страховщика$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirownerbirthdate IS $COMM$дата рождения собственника$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirowneraddrcode IS $COMM$код КЛАДР для адреса собственника$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirownerdoctype IS $COMM$вид документа, удост. личность собственника ПС$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirownerdocser IS $COMM$серия документа собственника ПС$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirownerdocno IS $COMM$номер документа собственника ПС$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirownerinn IS $COMM$ИНН Maksimov V.A. 27.11.2009 task 11804001403$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirdrivername IS $COMM$ФИО водителя - потерпевшего участника ДТП водителя ПС$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirdriverbirthdate IS $COMM$дата рождения водителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirdriveraddrcode IS $COMM$код КЛАДР для адреса водителя$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirdriverdoctype IS $COMM$вид документа, удост. личность водителя ПС$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirdriverdocser IS $COMM$серия документа водителя ПС$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dirdriverdocno IS $COMM$номер документа водителя ПС$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.bsoser IS $COMM$серия полиса отв. страховщика$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.bsono IS $COMM$номер полиса отв. страховщик$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.regno IS $COMM$гос. номер ТС виновника ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.carmark IS $COMM$марка ТС виновника ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.carmodel IS $COMM$модель ТС виновника ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.subjname IS $COMM$ФИО виновника ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.subjdoctype IS $COMM$вид документа, удост. личность виновника ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.subjdocser IS $COMM$серия документа виновника ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.subjdocno IS $COMM$номер документа виновника ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.taskisn IS $COMM$ISN Задачи заявки$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.refisn1 IS $COMM$зарезервировано на будущее$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.refisn2 IS $COMM$зарезервировано на будущее$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.refisn3 IS $COMM$зарезервировано на будущее$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.reftxt IS $COMM$зарезервировано на будущее$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.isn IS $COMM$ссылка для заявок bufmsgxml.isn$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.daterequest IS $COMM$дата заявки$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.requestid IS $COMM$номер заявки$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.prevclmref IS $COMM$номер претдыдущей заявки$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.sendercode IS $COMM$код прямого страховщика по заявке$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.agrid IS $COMM$номер договора страхователя из ИГС$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.agrisn IS $COMM$ISN договора страхователя из ИГС$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.dateloss IS $COMM$дата и время ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.gibddstn IS $COMM$признак оформления сотрудником ГИБДД$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.acdnschm IS $COMM$номер схемы ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.tronschm IS $COMM$ТС заявителя на схеме ДТП$COMM$;
COMMENT ON COLUMN rsa_clearing.pvu_clam_t.incplactype IS $COMM$тип места ДТП (код)$COMM$;


CREATE TABLE rsa_clearing.pvu_pacs_t (
    isn                              NUMERIC,
    taskisn                          NUMERIC,
    sendercode                       VARCHAR(50),
    sysref                           VARCHAR(60),
    id                               VARCHAR(50),
    credttm                          TIMESTAMP,
    fixsumrub                        NUMERIC,
    factsumrub                       NUMERIC,
    datecalc                         TIMESTAMP,
    regress                          VARCHAR(1),
    nboftxs                          NUMERIC,
    endtoendid                       VARCHAR(50),
    txid                             NUMERIC,
    prtry                            VARCHAR(10),
    intrbksttlmamt                   NUMERIC,
    chrgbr                           VARCHAR(10),
    cdtrnm                           VARCHAR(200),
    cdtrctrysubdvsn                  VARCHAR(13),
    cdtradrline1                     VARCHAR(200),
    cdtradrline2                     VARCHAR(200),
    cdtradrline3                     VARCHAR(200),
    cdtrtwnnm                        VARCHAR(200),
    cdtrid                           VARCHAR(40),
    cdtridtp                         VARCHAR(40),
    cdtrissr                         TIMESTAMP,
    cdtrctryofres                    VARCHAR(10),
    cdtragtbic                       VARCHAR(50),
    dbtrnm                           VARCHAR(200),
    dbtragtbic                       VARCHAR(50),
    purpprtry                        VARCHAR(200),
    rmtinfustrd                      VARCHAR(200),
    refisn1                          NUMERIC,
    refisn2                          NUMERIC,
    refisn3                          NUMERIC,
    reftxt                           VARCHAR(255),
    recisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN rsa_clearing.pvu_pacs_t.recisn IS $COMM$ссылка на bufmsgxml$COMM$;


