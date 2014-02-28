CREATE TABLE storages.rep_agent_ranks (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    addisn                           NUMERIC,
    is_move_obj_addisn               NUMERIC,
    addid                            VARCHAR(20),
    orderno                          NUMERIC,
    agr_id                           VARCHAR(20),
    agr_datebeg                      TIMESTAMP,
    agr_dateend                      TIMESTAMP,
    agr_datesign                     TIMESTAMP,
    agr_ruleisn                      NUMERIC,
    add_datebeg                      TIMESTAMP,
    add_dateend                      TIMESTAMP,
    role_datebeg                     TIMESTAMP,
    role_dateend                     TIMESTAMP,
    agentisn                         NUMERIC,
    agentclassisn                    NUMERIC,
    addruleisn                       NUMERIC,
    is_move_obj                      NUMERIC,
    sharepc_agent_by_add             NUMERIC(9,6),
    agent_sumclassisn                NUMERIC,
    agent_sumclassisn2               NUMERIC,
    agent_calcflg                    VARCHAR(1),
    agent_base                       NUMERIC(17,12),
    agent_baseloss                   NUMERIC,
    agent_planfact                   VARCHAR(1),
    agent_deptisn                    NUMERIC,
    rnk_move_obj                     NUMERIC,
    sharepc_by_add                   NUMERIC,
    cnt_agent_by_agr                 NUMERIC,
    cnt_agent_by_add                 NUMERIC,
    is_move_obj_id                   VARCHAR(20),
    is_move_obj_datebeg              TIMESTAMP,
    is_move_obj_dateend              TIMESTAMP,
    sharepc_by_is_move_obj           NUMERIC,
    cnt_agent_by_is_move_obj         NUMERIC,
    is_add_cancel                    NUMERIC,
    is_add_cancel_addisn             NUMERIC,
    subjclassisn                     NUMERIC
)
;
--WARNING: No primary key defined for storages.rep_agent_ranks

COMMENT ON COLUMN storages.rep_agent_ranks.is_add_cancel IS 'Признак аддендума "Прекращение договора"';
COMMENT ON COLUMN storages.rep_agent_ranks.is_add_cancel_addisn IS 'Ссылка на первый ненулевой аддендум/договор от аддендума "Прекращение договора"';
COMMENT ON COLUMN storages.rep_agent_ranks.agrisn IS 'ISN договора';
COMMENT ON COLUMN storages.rep_agent_ranks.addisn IS 'ISN аддендума, к которому относится агент';
COMMENT ON COLUMN storages.rep_agent_ranks.is_move_obj_addisn IS 'аддендум "Перенос ТС", к которому относится текущий аддендум (Для первоначального состояния = AgrISN)';
COMMENT ON COLUMN storages.rep_agent_ranks.addid IS 'номер аддендума (номер договора для договора)';
COMMENT ON COLUMN storages.rep_agent_ranks.orderno IS 'порядковый номер агента по критериям отбора отчета "Сборы агентов" - task(ДИТ-12-2-166347)';
COMMENT ON COLUMN storages.rep_agent_ranks.agr_id IS 'номер договора';
COMMENT ON COLUMN storages.rep_agent_ranks.agr_datebeg IS 'Дата начала действия договора';
COMMENT ON COLUMN storages.rep_agent_ranks.agr_dateend IS 'Дата окончания действия договора';
COMMENT ON COLUMN storages.rep_agent_ranks.agr_datesign IS 'Дата подписания действия договора';
COMMENT ON COLUMN storages.rep_agent_ranks.agr_ruleisn IS 'ISN страхового продукта договора';
COMMENT ON COLUMN storages.rep_agent_ranks.add_datebeg IS 'Дата начала действия аддендума';
COMMENT ON COLUMN storages.rep_agent_ranks.add_dateend IS 'Дата окончания действия аддендума';
COMMENT ON COLUMN storages.rep_agent_ranks.role_datebeg IS 'Дата начала действия роли';
COMMENT ON COLUMN storages.rep_agent_ranks.role_dateend IS 'Дата окончания действия роли';
COMMENT ON COLUMN storages.rep_agent_ranks.agentisn IS 'ISN агента';
COMMENT ON COLUMN storages.rep_agent_ranks.agentclassisn IS 'ISN класса агента';
COMMENT ON COLUMN storages.rep_agent_ranks.addruleisn IS 'ISN страхового продукта аддендума (тип аддендума)';
COMMENT ON COLUMN storages.rep_agent_ranks.is_move_obj IS 'Признак аддендума, относящегося к переносу ТС';
COMMENT ON COLUMN storages.rep_agent_ranks.sharepc_agent_by_add IS '% комиссии агента';
COMMENT ON COLUMN storages.rep_agent_ranks.agent_sumclassisn IS 'сумма % комиссии агентов по аддендуму';
COMMENT ON COLUMN storages.rep_agent_ranks.agent_sumclassisn2 IS 'Указатель класса суммы из роли';
COMMENT ON COLUMN storages.rep_agent_ranks.agent_calcflg IS 'Указатель подкласса суммы из роли';
COMMENT ON COLUMN storages.rep_agent_ranks.agent_base IS 'Признак "Участвует в начислениях" из роли';
COMMENT ON COLUMN storages.rep_agent_ranks.agent_baseloss IS 'Коэффициент относительно 100-процентно брутто-премии';
COMMENT ON COLUMN storages.rep_agent_ranks.agent_planfact IS 'Коэффициент относительно 100-процентного убытка';
COMMENT ON COLUMN storages.rep_agent_ranks.agent_deptisn IS 'Индикатор: P-план, F-факт';
COMMENT ON COLUMN storages.rep_agent_ranks.rnk_move_obj IS 'Подразделение агента из роли';
COMMENT ON COLUMN storages.rep_agent_ranks.sharepc_by_add IS 'порядковый номер агента по критериям отбора отчета "Сборы агентов" - task(ДИТ-12-2-166347)';
COMMENT ON COLUMN storages.rep_agent_ranks.cnt_agent_by_agr IS 'Кол-во агентов по договору';
COMMENT ON COLUMN storages.rep_agent_ranks.cnt_agent_by_add IS 'Кол-во агентов по аддендуму';
COMMENT ON COLUMN storages.rep_agent_ranks.is_move_obj_id IS '№ аддендума "Перенос ТС", к которому относится текущий аддендум (Для первоначального состояния = AgrISN)';
COMMENT ON COLUMN storages.rep_agent_ranks.is_move_obj_datebeg IS 'Дата начала аддендума "Перенос ТС", к которому относится текущий аддендум (Для первоначального состояния = AgrISN)';
COMMENT ON COLUMN storages.rep_agent_ranks.is_move_obj_dateend IS 'Дата окончания аддендума "Перенос ТС", к которому относится текущий аддендум (Для первоначального состояния = AgrISN)';
COMMENT ON COLUMN storages.rep_agent_ranks.sharepc_by_is_move_obj IS 'сумма % комиссии по дочерним аддендумам к аддендуму "Перенос ТС"';
COMMENT ON COLUMN storages.rep_agent_ranks.cnt_agent_by_is_move_obj IS 'кол-во агентов по дочерним аддендумам к аддендуму "Перенос ТС"';


CREATE TABLE storages.rep_agr_salers (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    salerisn                         NUMERIC,
    agrsalerclassisn                 NUMERIC,
    deptisn                          NUMERIC,
    dept0isn                         NUMERIC,
    dept1isn                         NUMERIC,
    doisn                            NUMERIC,
    oisn                             NUMERIC,
    is_salergo                       VARCHAR(1),
    is_salerf                        VARCHAR(1),
    salerclassisn                    NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP
)
;
--WARNING: No primary key defined for storages.rep_agr_salers

COMMENT ON COLUMN storages.rep_agr_salers.agrsalerclassisn IS 'Класс роли (нпр, продавец филиала)';
COMMENT ON COLUMN storages.rep_agr_salers.doisn IS 'Идентиф.  доп. офиса';
COMMENT ON COLUMN storages.rep_agr_salers.oisn IS 'Идентиф. отдела';
COMMENT ON COLUMN storages.rep_agr_salers.salerclassisn IS 'Мотивационная группа';


CREATE TABLE storages.rep_agr_salers_line (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    salergoisn                       NUMERIC,
    salergoclassisn                  NUMERIC,
    salergodept                      NUMERIC,
    salergodept0isn                  NUMERIC,
    salercrgoisn                     NUMERIC,
    salercrgoclassisn                NUMERIC,
    salercrgodept                    NUMERIC,
    salercrgodept0isn                NUMERIC,
    salerfisn                        NUMERIC,
    salerfclassisn                   NUMERIC,
    salerfdept                       NUMERIC,
    salerfdept0isn                   NUMERIC
)
;
--WARNING: No primary key defined for storages.rep_agr_salers_line



CREATE TABLE storages.st_bodydebcre (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    baseisn                          NUMERIC,
    db                               TIMESTAMP,
    de                               TIMESTAMP,
    basesaldo                        NUMERIC,
    subaccisn                        NUMERIC,
    code                             VARCHAR(10),
    baseamountrub                    NUMERIC,
    basedamountrub                   NUMERIC,
    basecamountrub                   NUMERIC,
    basedateval                      TIMESTAMP,
    fid                              NUMERIC,
    subjisn                          NUMERIC,
    currisn                          NUMERIC,
    agrisn                           NUMERIC,
    baseamount                       NUMERIC,
    basedamount                      NUMERIC,
    basecamount                      NUMERIC,
    basesaldoval                     NUMERIC
)
;
--WARNING: No primary key defined for storages.st_bodydebcre

COMMENT ON TABLE storages.st_bodydebcre IS $$Дебиторка по срокам. для каждой проводки (Baseisn) хранится значение дебитока в промежутке дат DB и DE

Логи для изменеия таблицы пишет тригер на ST_BUHBODY$$;
COMMENT ON COLUMN storages.st_bodydebcre.baseamount IS 'Сальдо проводки в валюте проводки';
COMMENT ON COLUMN storages.st_bodydebcre.basedamount IS 'Сумма дебета в валюте проводки';
COMMENT ON COLUMN storages.st_bodydebcre.basecamount IS 'Сумма кредита в валюте проводки';
COMMENT ON COLUMN storages.st_bodydebcre.basesaldoval IS 'Сальдо проводки в валюте (не сквитованный остаток)';
COMMENT ON COLUMN storages.st_bodydebcre.baseisn IS 'Isn проводки из Buhbody (базовой)';
COMMENT ON COLUMN storages.st_bodydebcre.db IS 'Дата начала действия записи';
COMMENT ON COLUMN storages.st_bodydebcre.de IS 'Дата окончания действия записи';
COMMENT ON COLUMN storages.st_bodydebcre.basesaldo IS 'Сальдо проводки (не сквитованный остаток)';
COMMENT ON COLUMN storages.st_bodydebcre.subaccisn IS 'Субсчет';
COMMENT ON COLUMN storages.st_bodydebcre.code IS 'Код субсчета';
COMMENT ON COLUMN storages.st_bodydebcre.baseamountrub IS 'Сальдо проводки в руб';
COMMENT ON COLUMN storages.st_bodydebcre.basedamountrub IS 'Сумма дебета в руб';
COMMENT ON COLUMN storages.st_bodydebcre.basecamountrub IS 'Сумма кредита в руб';
COMMENT ON COLUMN storages.st_bodydebcre.basedateval IS 'Дата начисления "базовой" проводки';
COMMENT ON COLUMN storages.st_bodydebcre.fid IS '№ проводки';
COMMENT ON COLUMN storages.st_bodydebcre.subjisn IS 'Контрагент из проводки';
COMMENT ON COLUMN storages.st_bodydebcre.currisn IS 'Валюта проводки';
COMMENT ON COLUMN storages.st_bodydebcre.agrisn IS 'Договор из проводки';


CREATE TABLE storages.st_buh_turn (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    subaccisn                        NUMERIC,
    code                             VARCHAR(10),
    subkindisn                       NUMERIC,
    oprisn                           NUMERIC,
    currisn                          NUMERIC,
    db                               TIMESTAMP,
    de                               TIMESTAMP,
    damount                          NUMERIC,
    damountrub                       NUMERIC,
    damountusd                       NUMERIC,
    camount                          NUMERIC,
    camountrub                       NUMERIC,
    camountusd                       NUMERIC,
    prm_key                          NUMERIC,
    deb                              TIMESTAMP,
    dee                              TIMESTAMP
)
;
--WARNING: No primary key defined for storages.st_buh_turn

COMMENT ON TABLE storages.st_buh_turn IS 'Бух. оборот';
COMMENT ON COLUMN storages.st_buh_turn.code IS 'Код субсчета';
COMMENT ON COLUMN storages.st_buh_turn.oprisn IS 'Код операции';
COMMENT ON COLUMN storages.st_buh_turn.db IS 'Начало периода (месяц)';
COMMENT ON COLUMN storages.st_buh_turn.de IS 'Конец периода (месяц)';
COMMENT ON COLUMN storages.st_buh_turn.damount IS 'Дебетовый оборот в валюте';
COMMENT ON COLUMN storages.st_buh_turn.damountrub IS 'Дебетовый оборот в руб';
COMMENT ON COLUMN storages.st_buh_turn.damountusd IS 'Дебетовый оборот в USD';
COMMENT ON COLUMN storages.st_buh_turn.camount IS 'Кредитовый оборот в валюте';
COMMENT ON COLUMN storages.st_buh_turn.camountrub IS 'Кредитовый оборот в руб';
COMMENT ON COLUMN storages.st_buh_turn.camountusd IS 'Кредитовый оборот в USD';
COMMENT ON COLUMN storages.st_buh_turn.deb IS 'Начало периода события (месяц)';
COMMENT ON COLUMN storages.st_buh_turn.dee IS 'Конец периода события (месяц)';


CREATE TABLE storages.st_buh_turn_contr (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    subaccisn                        NUMERIC,
    code                             VARCHAR(10),
    oprisn                           NUMERIC,
    db                               TIMESTAMP,
    de                               TIMESTAMP,
    damountrub                       NUMERIC,
    camountrub                       NUMERIC,
    resident                         VARCHAR(1),
    branchisn                        NUMERIC,
    prm_key                          NUMERIC,
    currisn                          NUMERIC,
    juridical                        VARCHAR(1)
)
;
--WARNING: No primary key defined for storages.st_buh_turn_contr



CREATE TABLE storages.st_buh_turn_corr (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    subaccisn                        NUMERIC,
    code                             VARCHAR(10),
    corcode                          VARCHAR(10),
    oprisn                           NUMERIC,
    db                               TIMESTAMP,
    de                               TIMESTAMP,
    damountrub                       NUMERIC,
    camountrub                       NUMERIC,
    prm_key                          NUMERIC,
    subkindisn                       NUMERIC
)
;
--WARNING: No primary key defined for storages.st_buh_turn_corr

COMMENT ON TABLE storages.st_buh_turn_corr IS 'Бух. оборот с корреспонденциями. В целях экономии не все счета';
COMMENT ON COLUMN storages.st_buh_turn_corr.subkindisn IS 'Ссылка на аналитику';
COMMENT ON COLUMN storages.st_buh_turn_corr.code IS 'Код субсчета';
COMMENT ON COLUMN storages.st_buh_turn_corr.corcode IS 'Код корреспондирующего субсчета';
COMMENT ON COLUMN storages.st_buh_turn_corr.oprisn IS 'Код операции';
COMMENT ON COLUMN storages.st_buh_turn_corr.db IS 'Начало периода (месяц)';
COMMENT ON COLUMN storages.st_buh_turn_corr.de IS 'Конец периода (месяц)';
COMMENT ON COLUMN storages.st_buh_turn_corr.damountrub IS 'Дебетовый оборот в руб';
COMMENT ON COLUMN storages.st_buh_turn_corr.camountrub IS 'Кредитовыйоборот в руб';


CREATE TABLE storages.st_buhbody (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    baseisn                          NUMERIC,
    parentisn                        NUMERIC,
    bodyisn                          NUMERIC,
    code                             VARCHAR(10),
    damountrub                       NUMERIC,
    camountrub                       NUMERIC,
    dateval                          TIMESTAMP,
    datequit                         TIMESTAMP,
    quitstatus                       VARCHAR(1),
    oprisn                           NUMERIC,
    subaccisn                        NUMERIC,
    balance                          NUMERIC,
    basedateval                      TIMESTAMP,
    fid                              NUMERIC,
    basedamountrub                   NUMERIC,
    basecamountrub                   NUMERIC,
    subjisn                          NUMERIC,
    currisn                          NUMERIC,
    agrisn                           NUMERIC,
    basedamount                      NUMERIC,
    basecamount                      NUMERIC,
    damount                          NUMERIC,
    camount                          NUMERIC
)
;
--WARNING: No primary key defined for storages.st_buhbody



CREATE TABLE storages.st_buhbody_nms (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    bodyisn                          NUMERIC,
    headisn                          NUMERIC,
    currisn                          NUMERIC,
    subaccisn                        NUMERIC,
    deptisn                          NUMERIC,
    code                             VARCHAR(10),
    dateval                          TIMESTAMP,
    damount                          NUMERIC(20,2),
    damountrub                       NUMERIC(20,2),
    damountusd                       NUMERIC(20,2),
    camount                          NUMERIC(20,2),
    camountrub                       NUMERIC(20,2),
    camountusd                       NUMERIC(20,2),
    oprisn                           NUMERIC,
    subkindisn                       NUMERIC,
    agrisn                           NUMERIC,
    docitemisn                       NUMERIC,
    fobjisn                          NUMERIC
)
;
--WARNING: No primary key defined for storages.st_buhbody_nms



CREATE TABLE storages.st_buhbody_reins (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    parentisn                        NUMERIC,
    bodyisn                          NUMERIC,
    code                             VARCHAR(10),
    damountrub                       NUMERIC,
    camountrub                       NUMERIC,
    dateval                          TIMESTAMP,
    oprisn                           NUMERIC,
    subjisn                          NUMERIC,
    agrisn                           NUMERIC,
    dssubjisn                        NUMERIC,
    amountsum                        NUMERIC,
    fullamountsum                    NUMERIC,
    dskoef                           NUMERIC,
    docsumcnt                        NUMERIC,
    dsisn                            NUMERIC,
    discr                            VARCHAR(1),
    status                           VARCHAR(1),
    classisn                         NUMERIC,
    classisn2                        NUMERIC,
    datepay                          TIMESTAMP,
    docisn                           NUMERIC,
    docisn2                          NUMERIC,
    splitisn                         NUMERIC,
    amount                           NUMERIC,
    dscurrisn                        NUMERIC,
    groupisn                         NUMERIC,
    statcode                         NUMERIC,
    dsagrisn                         NUMERIC,
    reaccisn                         NUMERIC,
    subaccisn                        NUMERIC,
    fid                              VARCHAR(150)
)
;
--WARNING: No primary key defined for storages.st_buhbody_reins



CREATE TABLE storages.st_docsumbody (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    bodyisn                          NUMERIC,
    bamount                          NUMERIC,
    agrisn                           NUMERIC,
    subjisn                          NUMERIC,
    amountrub                        NUMERIC,
    datepaylast                      TIMESTAMP,
    dsclassisn                       NUMERIC,
    dsisn                            NUMERIC,
    discr                            VARCHAR(1),
    subaccisn                        NUMERIC,
    splitisn                         NUMERIC,
    c_agr                            NUMERIC,
    agrkoef                          NUMERIC,
    dskoef                           NUMERIC,
    agrdskoef                        NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    remainder                        NUMERIC,
    reaccisn                         NUMERIC,
    agentisn                         NUMERIC,
    agrdatebeg                       TIMESTAMP
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN storages.st_docsumbody.agrkoef IS 'Коэф. суммы внутри договора';
COMMENT ON COLUMN storages.st_docsumbody.dskoef IS 'Коэффициент суммы в проводке';
COMMENT ON COLUMN storages.st_docsumbody.agrdskoef IS 'Коэффициент суммы';
COMMENT ON COLUMN storages.st_docsumbody.datebeg IS 'Дата начала записи';
COMMENT ON COLUMN storages.st_docsumbody.dateend IS 'Дата окончания записи';
COMMENT ON COLUMN storages.st_docsumbody.reaccisn IS '100% счет';
COMMENT ON COLUMN storages.st_docsumbody.agrdatebeg IS 'Дата начала договора, чтобы отчеты не ехали, т..к. правят';
COMMENT ON COLUMN storages.st_docsumbody.agentisn IS 'Isn агента из договора (участников)';
COMMENT ON COLUMN storages.st_docsumbody.bodyisn IS 'Isn проводки, к которому относится сумма';
COMMENT ON COLUMN storages.st_docsumbody.bamount IS 'Сумма проводки';
COMMENT ON COLUMN storages.st_docsumbody.agrisn IS 'Договор из суммы';
COMMENT ON COLUMN storages.st_docsumbody.subjisn IS 'Клиет из суммы';
COMMENT ON COLUMN storages.st_docsumbody.amountrub IS 'Сумма в руб "суммы"';
COMMENT ON COLUMN storages.st_docsumbody.datepaylast IS 'Дата последнего платежа';
COMMENT ON COLUMN storages.st_docsumbody.dsclassisn IS 'Класс суммы';
COMMENT ON COLUMN storages.st_docsumbody.dsisn IS 'Isn из Docsum';


CREATE TABLE storages.st_rep_subject (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    subjisn                          NUMERIC,
    classisn                         NUMERIC,
    roleclassisn                     NUMERIC,
    countryisn                       NUMERIC,
    branchisn                        NUMERIC,
    juridical                        VARCHAR(1),
    resident                         VARCHAR(1),
    vip                              VARCHAR(1),
    inn                              VARCHAR(15),
    id                               NUMERIC(38),
    fid                              NUMERIC(38),
    code                             VARCHAR(10),
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(255),
    active                           VARCHAR(1),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    licenseno                        VARCHAR(32),
    licensedate                      TIMESTAMP,
    okpo                             NUMERIC(38),
    okohx                            VARCHAR(255),
    synisn                           NUMERIC,
    createdby                        NUMERIC,
    created                          TIMESTAMP,
    profittaxflag                    VARCHAR(1),
    parentisn                        NUMERIC,
    namelat                          VARCHAR(255),
    orgformisn                       NUMERIC,
    remark                           VARCHAR(1000),
    kpp                              VARCHAR(15),
    searchname                       VARCHAR(40),
    securitylevel                    NUMERIC,
    ogrn                             BIGINT,
    okved                            VARCHAR(255),
    securitystr                      VARCHAR(255),
    regnm                            VARCHAR(4000),
    sbclass                          VARCHAR(40),
    relicend                         TIMESTAMP,
    licend                           TIMESTAMP,
    nrezname                         VARCHAR(255),
    likvidstatus                     VARCHAR(4000),
    r_best                           VARCHAR(4000),
    r_fitch                          VARCHAR(4000),
    r_moodys                         VARCHAR(4000),
    r_sp                             VARCHAR(4000),
    r_weiss                          VARCHAR(4000),
    addrcode                         VARCHAR(10),
    addrtype                         VARCHAR(40),
    cityisn                          NUMERIC,
    postcode                         VARCHAR(10),
    address                          VARCHAR(255),
    parentsubj                       VARCHAR(40),
    parentfullname                   VARCHAR(255),
    parentclass                      VARCHAR(255),
    parentinn                        VARCHAR(255),
    updatedby_name                   VARCHAR(255),
    bnk_vkey                         VARCHAR(255),
    bnk_vkeydel                      VARCHAR(255),
    bnk_active                       VARCHAR(255),
    valaam_name                      VARCHAR(4000),
    regnmdtend                       TIMESTAMP,
    likvidstatusdtend                TIMESTAMP,
    curator                          VARCHAR(4000),
    curatordeptisn                   NUMERIC,
    dealer                           NUMERIC,
    repsynkisn                       NUMERIC
)
;
--WARNING: No primary key defined for storages.st_rep_subject

COMMENT ON COLUMN storages.st_rep_subject.repsynkisn IS '"дедупликация для отчета"';


CREATE TABLE storages.rep_currate (
    cin                              NUMERIC,
    cout                             NUMERIC,
    cdate                            TIMESTAMP,
    crate                            NUMERIC,
    dateval                          TIMESTAMP
)
distributed by (cdate);


CREATE TABLE storages.repload (
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
    daterep                          TIMESTAMP,
    schema                           VARCHAR(30)
)
distributed by (isn);
--WARNING: No primary key defined for storages.repload

COMMENT ON COLUMN storages.repload.daterep IS 'Дата отчета';
COMMENT ON COLUMN storages.repload.datebeg IS 'Дата начала для логовой сессии';
COMMENT ON COLUMN storages.repload.dateend IS 'Дата окончания для логовой сессии';
COMMENT ON COLUMN storages.repload.classisn IS '1- активная загрузка';
COMMENT ON COLUMN storages.repload.loadtype IS 'Тип сессии - полная загрузка, по логам';
COMMENT ON COLUMN storages.repload.lastisnloaded IS 'Последний удачно загруж , для полной сессии';
COMMENT ON COLUMN storages.repload.lastrundate IS 'Дата последнего запуска';
COMMENT ON COLUMN storages.repload.lastenddate IS 'Дата последнего завершения';
COMMENT ON COLUMN storages.repload.procisn IS 'Isn задачи';


CREATE TABLE storages.tt_rule_rpngrp (
    ruleisn                          NUMERIC,
    groupisn                         NUMERIC,
    typerule                         VARCHAR(30)
) distributed by (ruleisn)
;