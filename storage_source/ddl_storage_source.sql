CREATE TABLE storage_source.rep_agrcargo (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    sea                              NUMERIC(38),
    more1                            NUMERIC(38)
)
;
--WARNING: No primary key defined for storage_source.rep_agrcargo



CREATE TABLE storage_source.rep_agrext (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    classisn                         NUMERIC,
    x1                               NUMERIC,
    x2                               NUMERIC,
    x3                               NUMERIC,
    x4                               NUMERIC,
    x5                               NUMERIC
)
;
--WARNING: No primary key defined for storage_source.rep_agrext

COMMENT ON TABLE storage_source.rep_agrext IS 'снял с поддержки - непонятно кто и зачем использует KGS 10.07.2012';


CREATE TABLE storage_source.rep_agrtur (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    isrussia                         NUMERIC,
    isshengen                        NUMERIC
)
;
--WARNING: No primary key defined for storage_source.rep_agrtur



CREATE TABLE storage_source.rep_longagraddendum (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    addisn                           NUMERIC,
    discr                            VARCHAR(1),
    datebeg                          TIMESTAMP,
    datesign                         TIMESTAMP,
    premiumsum                       NUMERIC,
    currisn                          NUMERIC
)
;
--WARNING: No primary key defined for storage_source.rep_longagraddendum

COMMENT ON TABLE storage_source.rep_longagraddendum IS 'Аддендумы длинных договоров';
COMMENT ON COLUMN storage_source.rep_longagraddendum.currisn IS 'Валюта аддендума';
COMMENT ON COLUMN storage_source.rep_longagraddendum.agrisn IS 'ISN договора';
COMMENT ON COLUMN storage_source.rep_longagraddendum.addisn IS 'ISN аддендума';
COMMENT ON COLUMN storage_source.rep_longagraddendum.datebeg IS 'Дата начала действия аддендума';
COMMENT ON COLUMN storage_source.rep_longagraddendum.datesign IS 'Дата подписания аддендума';
COMMENT ON COLUMN storage_source.rep_longagraddendum.premiumsum IS 'Сумма доплаты/возврата премии по аддендуму';


CREATE TABLE storage_source.rep_objclass_domestic (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    objclassisn                      NUMERIC,
    domestic                         VARCHAR(1),
    parentobjclassisn                NUMERIC
)
;
--WARNING: No primary key defined for storage_source.rep_objclass_domestic



CREATE TABLE storage_source.repagr (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    id                               VARCHAR(20),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    datesign                         TIMESTAMP,
    classisn                         NUMERIC,
    ruleisn                          NUMERIC,
    deptisn                          NUMERIC,
    dept0isn                         NUMERIC,
    filisn                           NUMERIC,
    ruledept                         NUMERIC,
    emplisn                          NUMERIC,
    clientisn                        NUMERIC,
    currisn                          NUMERIC,
    premiumsum                       NUMERIC(20,2),
    premusd                          NUMERIC,
    premrub                          NUMERIC,
    premeur                          NUMERIC,
    incomerate                       NUMERIC,
    status                           VARCHAR(1),
    discr                            VARCHAR(1),
    applisn                          NUMERIC,
    sharepc                          NUMERIC(9,6),
    reinspc                          NUMERIC(9,6),
    groupisn                         NUMERIC,
    bizflg                           VARCHAR(1),
    parentisn                        NUMERIC,
    insurantisn#                     NUMERIC,
    insurantcount#                   NUMERIC,
    agentisn                         NUMERIC,
    agentcount                       NUMERIC,
    emitisn                          NUMERIC,
    emitcount                        NUMERIC,
    comission                        NUMERIC,
    datebuh                          TIMESTAMP,
    limitsum                         NUMERIC,
    limitsumusd                      NUMERIC,
    insuredsum                       NUMERIC,
    insuredsumusd                    NUMERIC,
    agrcreated                       TIMESTAMP,
    agentjuridical                   VARCHAR(1),
    firmisn                          NUMERIC,
    agentclassisn                    NUMERIC,
    salergoisn                       NUMERIC,
    salerfisn                        NUMERIC,
    ownerdeptisn                     NUMERIC,
    clientjuridical                  VARCHAR(1),
    filcomission                     NUMERIC,
    bemitisn                         NUMERIC,
    bfilisn                          NUMERIC,
    calcbizflg                       VARCHAR(1),
    previsn                          NUMERIC,
    crossalerisn                     NUMERIC,
    transfercomission                NUMERIC,
    beneficiaryisn                   NUMERIC,
    partnerisn                       NUMERIC,
    limitsumrub                      NUMERIC,
    insuredsumrub                    NUMERIC,
    olddateend                       TIMESTAMP,
    calcemitisn                      NUMERIC,
    calcfilisn                       NUMERIC,
    gmisn                            NUMERIC,
    addrisn                          NUMERIC,
    agentdeptisn                     NUMERIC,
    datecalc                         TIMESTAMP,
    brokerisn                        NUMERIC,
    leaseficiary                     NUMERIC,
    agentcollectflg                  VARCHAR(1),
    agrdetailisn                     NUMERIC,
    pawnbrokerisn                    NUMERIC,
    saleschannelisn                  NUMERIC,
    datebase                         VARCHAR(1),
    reccomenderisn                   NUMERIC,
    formisn                          NUMERIC,
    createdby                        NUMERIC,
    uprisn                           NUMERIC,
    incomesum                        NUMERIC,
    incomesumusd                     NUMERIC,
    incomesumrub                     NUMERIC,
    discount                         NUMERIC,
    dateissue                        TIMESTAMP,
    createdate                       TIMESTAMP,
    businesslineisn                  NUMERIC
)
DISTRIBUTED BY (agrisn);

COMMENT ON TABLE storage_source.repagr IS 'Договоры';
COMMENT ON COLUMN storage_source.repagr.businesslineisn IS 'EGAO 22.01.2014 Направление корпоративного бизнеса';
COMMENT ON COLUMN storage_source.repagr.insurantisn# IS 'Добавлена решетка, чтобы выявить, где поле используется';
COMMENT ON COLUMN storage_source.repagr.insurantcount# IS 'Добавлена решетка, чтобы выявить, где поле используется';
COMMENT ON COLUMN storage_source.repagr.limitsum IS 'Лимит в валюте договора';
COMMENT ON COLUMN storage_source.repagr.limitsumusd IS 'Лимит в долларах';
COMMENT ON COLUMN storage_source.repagr.insuredsum IS 'Страховая сумма в валюте договора';
COMMENT ON COLUMN storage_source.repagr.isn IS 'PK SEQ_REPORTS.NextVal';
COMMENT ON COLUMN storage_source.repagr.loadisn IS 'FK (REPLOAD) Сеанс загрузки';
COMMENT ON COLUMN storage_source.repagr.agrisn IS 'PK FK (AIS.AGREEMENT)';
COMMENT ON COLUMN storage_source.repagr.id IS 'Номер договора';
COMMENT ON COLUMN storage_source.repagr.datebeg IS 'Дата начала договора';
COMMENT ON COLUMN storage_source.repagr.dateend IS 'Дата окончания договора';
COMMENT ON COLUMN storage_source.repagr.datesign IS 'Дата подписания договора';
COMMENT ON COLUMN storage_source.repagr.classisn IS 'FK(DICTI) Класс договора';
COMMENT ON COLUMN storage_source.repagr.ruleisn IS 'FK(DICTI) Страховой продукт';
COMMENT ON COLUMN storage_source.repagr.deptisn IS 'FK(SUBDEPT) Подразделение, выпустившее договор';
COMMENT ON COLUMN storage_source.repagr.dept0isn IS 'FK(SUBDEPT) Подразделение верхнего уровня (parentisn= 0)';
COMMENT ON COLUMN storage_source.repagr.filisn IS 'FK(SUBDEPT) Филиал, выпустивший договор';
COMMENT ON COLUMN storage_source.repagr.ruledept IS 'FK(SUBDEPT) Подразделение - владелец страхового продукта';
COMMENT ON COLUMN storage_source.repagr.emplisn IS 'FK(SUBJECT) Куратор договора';
COMMENT ON COLUMN storage_source.repagr.clientisn IS 'FK(SUBJECT) Страхователь (из формуляра)';
COMMENT ON COLUMN storage_source.repagr.currisn IS 'FK(CURRENCY) Валюта договора';
COMMENT ON COLUMN storage_source.repagr.premiumsum IS 'Премия в валюте догвора';
COMMENT ON COLUMN storage_source.repagr.premusd IS 'Пермия в долларах';
COMMENT ON COLUMN storage_source.repagr.premrub IS 'Премия в рублях';
COMMENT ON COLUMN storage_source.repagr.premeur IS 'Премия в евро';
COMMENT ON COLUMN storage_source.repagr.addrisn IS 'FK(SUBADDR). Указатель места выдачи полиса';
COMMENT ON COLUMN storage_source.repagr.agentdeptisn IS 'EGAO 27.07.2009 подразделение агента';
COMMENT ON COLUMN storage_source.repagr.datecalc IS 'Дата расчета премии EGAO 30.07.2009';
COMMENT ON COLUMN storage_source.repagr.brokerisn IS 'EGAO 03.09.2009 для использования в motor.v_carcond';
COMMENT ON COLUMN storage_source.repagr.leaseficiary IS 'EGAO 03.09.2009 для использования в motor.v_carcond';
COMMENT ON COLUMN storage_source.repagr.agentcollectflg IS 'EGAO 14.04.2009 флаг инкассации премии агентом';
COMMENT ON COLUMN storage_source.repagr.agrdetailisn IS 'OD 11.11.2009 детализация договора FK(AGR_DETAIL_DETAILS)';
COMMENT ON COLUMN storage_source.repagr.pawnbrokerisn IS 'Залогодержатель';
COMMENT ON COLUMN storage_source.repagr.saleschannelisn IS 'Канал продаж OD 6.05.2010 ДИТ-10-2-098743';
COMMENT ON COLUMN storage_source.repagr.datebase IS 'EGAO 05.07.2010 База перестрахования: I-страховой год, C-календарная';
COMMENT ON COLUMN storage_source.repagr.reccomenderisn IS 'рекомендатель';
COMMENT ON COLUMN storage_source.repagr.formisn IS 'EGAO 04.03.2011';
COMMENT ON COLUMN storage_source.repagr.createdby IS 'OD 22.03.2011';
COMMENT ON COLUMN storage_source.repagr.uprisn IS 'EGAO 17.03.2012 Вид аналитики "Подразделения Ингосстрах"';
COMMENT ON COLUMN storage_source.repagr.incomesum IS 'Общая полученная сумма премии по договору. Считается автоматически.';
COMMENT ON COLUMN storage_source.repagr.incomesumusd IS 'Общая полученная сумма премии по договору, USD';
COMMENT ON COLUMN storage_source.repagr.incomesumrub IS 'Общая полученная сумма премии по договору, RUB';
COMMENT ON COLUMN storage_source.repagr.discount IS 'Значение скидки для туристов';
COMMENT ON COLUMN storage_source.repagr.dateissue IS 'Дата выдачи полиса';


CREATE TABLE storage_source.repagr_economic (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    id                               VARCHAR(20),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    datesign                         TIMESTAMP,
    classisn                         NUMERIC,
    ruleisn                          NUMERIC,
    deptisn                          NUMERIC,
    dept0isn                         NUMERIC,
    filisn                           NUMERIC,
    ruledept                         NUMERIC,
    emplisn                          NUMERIC,
    clientisn                        NUMERIC,
    currisn                          NUMERIC,
    premiumsum                       NUMERIC(20,2),
    premusd                          NUMERIC,
    premrub                          NUMERIC,
    premeur                          NUMERIC,
    incomerate                       NUMERIC,
    status                           VARCHAR(1),
    discr                            VARCHAR(1),
    applisn                          NUMERIC,
    sharepc                          NUMERIC(9,6),
    reinspc                          NUMERIC(9,6),
    groupisn                         NUMERIC,
    bizflg                           VARCHAR(1),
    parentisn                        NUMERIC,
    insurantisn                      NUMERIC,
    insurantcount                    NUMERIC,
    agentisn                         NUMERIC,
    agentcount                       NUMERIC,
    emitisn                          NUMERIC,
    emitcount                        NUMERIC,
    comission                        NUMERIC,
    datebuh                          TIMESTAMP,
    limitsum                         NUMERIC,
    limitsumusd                      NUMERIC,
    insuredsum                       NUMERIC,
    insuredsumusd                    NUMERIC,
    agrcreated                       TIMESTAMP,
    agentjuridical                   VARCHAR(3),
    firmisn                          NUMERIC
)
;
--WARNING: No primary key defined for storage_source.repagr_economic



CREATE TABLE storage_source.repagrroleagr (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    agentisn                         NUMERIC,
    salergoisn                       NUMERIC,
    salerfisn                        NUMERIC,
    crossalerisn                     NUMERIC,
    cardealerisn                     NUMERIC,
    brokerisn                        NUMERIC,
    partnerisn                       NUMERIC,
    leaseficiaryisn                  NUMERIC,
    pawnbrokerisn                    NUMERIC,
    recommenderisn                   NUMERIC,
    irecommenderisn                  NUMERIC,
    emitisn                          NUMERIC,
    bemitisn                         NUMERIC,
    agentdeptisn                     NUMERIC,
    agentjuridical                   VARCHAR(1),
    agentcollectflg                  VARCHAR(1),
    agentcount                       NUMERIC,
    emitcount                        NUMERIC,
    filcommision                     NUMERIC,
    transfercomission                NUMERIC,
    beneficiaryisn                   NUMERIC,
    brokerdeptisn                    NUMERIC,
    brokerjuridical                  VARCHAR(1),
    brokercollectflg                 VARCHAR(1),
    brokercount                      NUMERIC,
    headclient                       NUMERIC,
    agentsharepc                     NUMERIC,
    brokersharepc                    NUMERIC,
    reinclientisn                    NUMERIC,
    salergodeptisn                   NUMERIC,
    salerfdeptisn                    NUMERIC,
    managerkkisn                     NUMERIC,
    empopgoisn                       NUMERIC,
    empopgodeptisn                   NUMERIC,
    uprisn                           NUMERIC,
    empoperu                         NUMERIC,
    salergoclassisn                  NUMERIC,
    salerfclassisn                   NUMERIC,
    crossalerdeptisn                 NUMERIC,
    avtodillerisn                    NUMERIC,
    admcuratorisn                    NUMERIC,
    doctorcuratorisn                 NUMERIC,
    underwriterisn                   NUMERIC,
    underwriteroldisn                NUMERIC,
    representativeisn                NUMERIC,
    crossalerfisn                    NUMERIC,
    crossalerfdeptisn                NUMERIC,
    agent_maxcomission_isn           NUMERIC,
    contractorisn                    NUMERIC,
    contrcomission                   NUMERIC,
    contrcount                       NUMERIC,
    agent_maxcomission_sharepc       INT
)
DISTRIBUTED BY (agrisn);

COMMENT ON COLUMN storage_source.repagrroleagr.agent_maxcomission_sharepc IS 'Доля в % агента с максимальной комиссией';
COMMENT ON COLUMN storage_source.repagrroleagr.cardealerisn IS 'Автосалон (sic!)';
COMMENT ON COLUMN storage_source.repagrroleagr.agent_maxcomission_isn IS 'Агент с максимальной комиссией';
COMMENT ON COLUMN storage_source.repagrroleagr.contractorisn IS 'Подрядчик';
COMMENT ON COLUMN storage_source.repagrroleagr.contrcomission IS 'Процент подрядчика';
COMMENT ON COLUMN storage_source.repagrroleagr.salergoclassisn IS 'класс продавца ГО (роль в договоре)';
COMMENT ON COLUMN storage_source.repagrroleagr.salerfclassisn IS 'класс продавца Ф (роль в договоре)';
COMMENT ON COLUMN storage_source.repagrroleagr.avtodillerisn IS 'Автодиллер';
COMMENT ON COLUMN storage_source.repagrroleagr.admcuratorisn IS 'Административный куратор';
COMMENT ON COLUMN storage_source.repagrroleagr.doctorcuratorisn IS 'Врач-куратор';
COMMENT ON COLUMN storage_source.repagrroleagr.underwriterisn IS 'Андеррайтер';
COMMENT ON COLUMN storage_source.repagrroleagr.underwriteroldisn IS 'Андеррайтер(старый)';
COMMENT ON COLUMN storage_source.repagrroleagr.representativeisn IS 'Представитель';
COMMENT ON COLUMN storage_source.repagrroleagr.crossalerfisn IS 'кросс-продавец филиала';
COMMENT ON COLUMN storage_source.repagrroleagr.crossalerfdeptisn IS 'подразделение кросс-продавца филиала';
COMMENT ON COLUMN storage_source.repagrroleagr.contrcount IS 'Количество подрядчиков';
COMMENT ON COLUMN storage_source.repagrroleagr.empopgoisn IS 'EGAO 13.04.2012 СОТРУДНИК ОП ГО';
COMMENT ON COLUMN storage_source.repagrroleagr.uprisn IS 'EGAO 17.04.2012 Виды аналитики "Подразделения Ингосстрах"';
COMMENT ON COLUMN storage_source.repagrroleagr.empoperu IS 'EGAO 21.05.2012 СОТРУДНИК ОПЕРУ: ВВОД';


CREATE TABLE storage_source.repbuhbody (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    deptisn                          NUMERIC,
    statcode                         NUMERIC,
    classisn                         NUMERIC,
    bodyisn                          NUMERIC,
    dateval                          TIMESTAMP,
    currisn                          NUMERIC,
    deptisnbuh                       NUMERIC,
    subjisn                          NUMERIC,
    subaccisn                        NUMERIC,
    buhamount                        NUMERIC,
    buhamountrub                     NUMERIC,
    docsumisn                        NUMERIC,
    datepaylast                      TIMESTAMP,
    agrisn                           NUMERIC,
    deptisnan                        NUMERIC,
    reprdeptisn                      NUMERIC,
    bizflg                           VARCHAR(1),
    addisn                           NUMERIC,
    refundisn                        NUMERIC,
    docsumpc                         NUMERIC,
    buhquitbodyisn                   NUMERIC,
    buhquitbodycnt                   NUMERIC,
    acccurrisn                       NUMERIC,
    quitdebetisn                     NUMERIC,
    quitcreditisn                    NUMERIC,
    quitdateval                      TIMESTAMP,
    datequit                         TIMESTAMP,
    quitcurrisn                      NUMERIC,
    quitdebetsubaccisn               NUMERIC,
    quitcreditsubaccisn              NUMERIC,
    quitdebetbuhamount               NUMERIC,
    quitcreditbuhamount              NUMERIC,
    amountclosedquit                 NUMERIC(23,5),
    buhquitamount                    NUMERIC,
    buhquitpartamount                NUMERIC,
    buhquitdate                      TIMESTAMP,
    parentisn                        NUMERIC,
    amountclosingquit                NUMERIC,
    fullamountclosingquit            NUMERIC,
    agrbuhdate                       TIMESTAMP,
    factisn                          NUMERIC,
    buhquitisn                       NUMERIC,
    buhheadfid                       NUMERIC,
    buhamountusd                     NUMERIC,
    oprisn                           NUMERIC,
    oprdeptisn                       NUMERIC,
    docisn                           NUMERIC,
    datepay                          TIMESTAMP,
    headisn                          NUMERIC,
    docsumsubj                       NUMERIC,
    docisn2                          NUMERIC,
    sagroup                          NUMERIC,
    corsubaccisn                     NUMERIC,
    dsdatebeg                        TIMESTAMP,
    dsdateend                        TIMESTAMP,
    adeptisn                         NUMERIC,
    dsclassisn                       NUMERIC,
    dsclassisn2                      NUMERIC,
    factpc                           NUMERIC,
    buhpc                            NUMERIC,
    amount                           NUMERIC,
    amountrub                        NUMERIC,
    amountusd                        NUMERIC,
    remark                           VARCHAR(255),
    dsstatus                         VARCHAR(5)
)
;
--WARNING: No primary key defined for storage_source.repbuhbody

COMMENT ON COLUMN storage_source.repbuhbody.adeptisn IS 'Значение аналитики "Подразделения сао "ингосстрах"" аналитического счета';
COMMENT ON COLUMN storage_source.repbuhbody.dsclassisn IS 'EGAO 02.02.2010 Тип суммы';
COMMENT ON COLUMN storage_source.repbuhbody.dsclassisn2 IS 'EGAO 02.02.2010 Подтип суммы ';
COMMENT ON COLUMN storage_source.repbuhbody.factpc IS 'EGAO 02.02.2010';
COMMENT ON COLUMN storage_source.repbuhbody.buhpc IS 'EGAO 02.02.2010';
COMMENT ON COLUMN storage_source.repbuhbody.amount IS 'EGAO 02.02.2010';
COMMENT ON COLUMN storage_source.repbuhbody.amountrub IS 'EGAO 02.02.2010';
COMMENT ON COLUMN storage_source.repbuhbody.amountusd IS 'EGAO 02.02.2010';
COMMENT ON COLUMN storage_source.repbuhbody.dsstatus IS 'Статус доксуммы.';


CREATE TABLE storage_source.repbuhquit (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    deptisn                          NUMERIC,
    statcode                         NUMERIC,
    classisn                         NUMERIC,
    bodyisn                          NUMERIC,
    dateval                          TIMESTAMP,
    currisn                          NUMERIC,
    buhheadfid                       NUMERIC,
    deptisnbuh                       NUMERIC,
    subjisn                          NUMERIC,
    subaccisn                        NUMERIC,
    parentisn                        NUMERIC,
    headisn                          NUMERIC,
    buhamount                        NUMERIC,
    buhamountrub                     NUMERIC,
    buhamountusd                     NUMERIC,
    oprisn                           NUMERIC,
    buhquitbodyisn                   NUMERIC,
    buhquitbodycnt                   NUMERIC,
    sagroup                          NUMERIC,
    buhpc                            NUMERIC,
    buhquitpc                        NUMERIC,
    buhquitdate                      TIMESTAMP,
    groupisn                         NUMERIC,
    buhquitisn                       NUMERIC,
    queisn                           NUMERIC,
    corsubaccisn                     NUMERIC,
    quitsum                          NUMERIC,
    quitpc                           NUMERIC,
    fact                             VARCHAR(1),
    quitbodyisn                      NUMERIC,
    quitdateval                      TIMESTAMP,
    repcursdiff                      NUMERIC
)
;
--WARNING: No primary key defined for storage_source.repbuhquit

COMMENT ON COLUMN storage_source.repbuhquit.repcursdiff IS 'Курсовая разница про фактической оплате в валюте';


CREATE TABLE storage_source.repcond (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    condisn                          NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    parentisn                        NUMERIC,
    agrisn                           NUMERIC,
    addisn                           NUMERIC,
    addstatus                        VARCHAR(1),
    addno                            NUMERIC,
    addbeg                           TIMESTAMP,
    addsign                          TIMESTAMP,
    parentaddisn                     NUMERIC,
    newaddisn                        NUMERIC,
    objisn                           NUMERIC,
    parentobjisn                     NUMERIC,
    riskisn                          NUMERIC,
    parentriskisn                    NUMERIC,
    limitisn                         NUMERIC,
    rptclassisn                      NUMERIC,
    limclassisn                      NUMERIC,
    currisn                          NUMERIC,
    premcurrisn                      NUMERIC,
    franchcurrisn                    NUMERIC,
    franchtype                       VARCHAR(1),
    premiumsum                       NUMERIC(20,2),
    premusd                          NUMERIC,
    premrub                          NUMERIC,
    premeur                          NUMERIC,
    limitsum                         NUMERIC(20,2),
    limitusd                         NUMERIC,
    limitrub                         NUMERIC,
    limiteur                         NUMERIC,
    franchsum                        NUMERIC(20,2),
    franchusd                        NUMERIC,
    franchrub                        NUMERIC,
    francheur                        NUMERIC,
    objclassisn                      NUMERIC,
    objrptclassisn                   NUMERIC,
    descisn                          NUMERIC,
    objprnclassisn                   NUMERIC,
    objprnrptclassisn                NUMERIC,
    riskclassisn                     NUMERIC,
    riskprnclassisn                  NUMERIC,
    riskrptclassisn                  NUMERIC,
    riskprnrptclassisn               NUMERIC,
    riskruleisn                      NUMERIC,
    riskprnruleisn                   NUMERIC,
    limitclassisn                    NUMERIC,
    agrdatebeg                       TIMESTAMP,
    agrdateend                       TIMESTAMP,
    agrruleisn                       NUMERIC,
    agrclassisn                      NUMERIC,
    agrcomission                     NUMERIC,
    agrdiscr                         VARCHAR(1),
    newaddsign                       TIMESTAMP,
    quantity                         NUMERIC,
    franchtariff                     NUMERIC,
    objregion                        NUMERIC,
    objcountry                       NUMERIC,
    clientisn                        NUMERIC,
    agrolddateend                    TIMESTAMP,
    addpremiumsum                    NUMERIC,
    agrcurrisn                       NUMERIC,
    agrdetailisn                     NUMERIC,
    premagr                          NUMERIC,
    carrptclass                      VARCHAR(32),
    discount                         NUMERIC,
    discount2                        NUMERIC,
    agrsharepc                       NUMERIC,
    cost                             NUMERIC,
    tariff                           NUMERIC(12,9),
    yeartariff                       NUMERIC(12,9)
)
;
--WARNING: No primary key defined for storage_source.repcond

COMMENT ON TABLE storage_source.repcond IS 'условия договоров';
COMMENT ON COLUMN storage_source.repcond.cost IS 'sts 14.03.2013 - Страховая стоимость';
COMMENT ON COLUMN storage_source.repcond.agrcurrisn IS 'EGAO 14.07.2010 Валюта договора';
COMMENT ON COLUMN storage_source.repcond.agrdetailisn IS 'OD 25.10.2010 детализация договора FK(AGR_DETAILS)';
COMMENT ON COLUMN storage_source.repcond.premagr IS 'EGAO 01.09.2011 Плановая премия условия в валюте договора';
COMMENT ON COLUMN storage_source.repcond.carrptclass IS 'KGS 19.11.2011';
COMMENT ON COLUMN storage_source.repcond.agrsharepc IS 'EGAO 19.03.2012 Доля ИГС от сострахования в процентах';


CREATE TABLE storage_source.repcrgdoc (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    agrisn                           NUMERIC,
    classisn                         NUMERIC,
    objisn                           NUMERIC,
    subjisn                          NUMERIC,
    subjjuridical                    VARCHAR(1)
)
;
--WARNING: No primary key defined for storage_source.repcrgdoc



CREATE TABLE storage_source.reprefund (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    refundisn                        NUMERIC,
    agrisn                           NUMERIC,
    condisn                          NUMERIC,
    currisn                          NUMERIC,
    claimsum                         NUMERIC,
    dateloss                         TIMESTAMP,
    dateclaim                        TIMESTAMP,
    subjisn                          NUMERIC,
    status                           VARCHAR(1),
    datesolution                     TIMESTAMP,
    claimstatus                      VARCHAR(1),
    dateevent                        TIMESTAMP,
    deptisn                          NUMERIC,
    franchtype                       VARCHAR(1),
    franchtariff                     NUMERIC,
    franchsum                        NUMERIC,
    agrdatebeg                       TIMESTAMP,
    rptclassisn                      NUMERIC,
    lossshare                        NUMERIC,
    claimisn                         NUMERIC,
    datereg                          TIMESTAMP,
    emplisn                          NUMERIC,
    objisn                           NUMERIC,
    parentobjisn                     NUMERIC,
    rptgroupisn                      NUMERIC DEFAULT 0,
    conddeptisn                      NUMERIC,
    isrevaluation                    NUMERIC,
    franchcurrisn                    NUMERIC,
    franchdeducted                   NUMERIC,
    classisn                         NUMERIC,
    refundsum                        NUMERIC,
    refundsumusd                     NUMERIC,
    claimsumusd                      NUMERIC,
    claimid                          VARCHAR(40),
    firmisn                          NUMERIC,
    daterefund                       TIMESTAMP,
    limitsum                         NUMERIC,
    limitcurrisn                     NUMERIC,
    ruleisnagr                       NUMERIC,
    ruleisnclaim                     NUMERIC,
    nrzu                             VARCHAR(1),
    budgetgroupisn                   NUMERIC,
    objclassisn                      NUMERIC,
    agrextisn                        NUMERIC,
    condpc                           NUMERIC,
    parentobjclassisn                NUMERIC,
    ragrisn                          NUMERIC,
    extdateevent                     TIMESTAMP,
    totalloss                        VARCHAR(3),
    rfranchcurrisn                   NUMERIC,
    rfranchsum                       NUMERIC,
    saleremplisn                     NUMERIC,
    salerdeptisn                     NUMERIC,
    motivgroupisn                    NUMERIC,
    riskruleisn                      NUMERIC,
    riskclassisn                     NUMERIC,
    rdateval                         TIMESTAMP,
    repdateloss                      TIMESTAMP,
    claimdatetotalloss               TIMESTAMP,
    claimcurrisn                     NUMERIC,
    refundsumrub                     NUMERIC,
    claimsumrub                      NUMERIC,
    regress                          VARCHAR(1),
    claimclassisn                    NUMERIC,
    refcreated                       TIMESTAMP,
    parentisn                        NUMERIC,
    aggrievednumber                  NUMERIC,
    refundid                         VARCHAR(20),
    agrclassisn                      NUMERIC
)
;
--WARNING: No primary key defined for storage_source.reprefund

COMMENT ON COLUMN storage_source.reprefund.aggrievednumber IS 'EGAO 20.03.2013 Кол-во потерпевших';
COMMENT ON COLUMN storage_source.reprefund.refundid IS 'EGAO 20.03.2013 Номер претензии';
COMMENT ON COLUMN storage_source.reprefund.agrclassisn IS 'EGAO 06.05.2013';
COMMENT ON COLUMN storage_source.reprefund.refcreated IS 'Дата создания записи о претензии OD 01.07.2011';
COMMENT ON COLUMN storage_source.reprefund.refundsumrub IS 'EGAO 07.04.2009';
COMMENT ON COLUMN storage_source.reprefund.claimsumrub IS 'EGAO 07.04.2009';
COMMENT ON COLUMN storage_source.reprefund.claimdatetotalloss IS 'Дата передачи убытка на тоталь';
COMMENT ON COLUMN storage_source.reprefund.nrzu IS 'не в РЗУ';
COMMENT ON COLUMN storage_source.reprefund.ragrisn IS 'договор из agrrefund (для тех,где есть extisn)';
COMMENT ON COLUMN storage_source.reprefund.claimcurrisn IS 'Валюта убытка';
COMMENT ON COLUMN storage_source.reprefund.claimclassisn IS $$Тип убытка EGAO 27.10.2010 ДИТ-10-4-121049
$$;
COMMENT ON COLUMN storage_source.reprefund.repdateloss IS 'Дата убытка для отчетности';


CREATE TABLE storage_source.subj_best_addr (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    subjisn                          NUMERIC,
    addrisn                          NUMERIC,
    subaddr                          VARCHAR(4000)
)
;
--WARNING: No primary key defined for storage_source.subj_best_addr

COMMENT ON TABLE storage_source.subj_best_addr IS 'Витрина с оптимальным адресом, выгружаемой функцией Крылова';


CREATE TABLE storage_source.subject_attrib (
    isn                              NUMERIC,
    loadisn                          NUMERIC,
    subjisn                          NUMERIC,
    home_addrisn                     NUMERIC,
    home_cityisn                     NUMERIC,
    home_regionisn                   NUMERIC,
    home_zip                         VARCHAR(10),
    post_addrisn                     NUMERIC,
    post_cityisn                     NUMERIC,
    post_regionisn                   NUMERIC,
    temporary_addrisn                NUMERIC,
    temporary_cityisn                NUMERIC,
    temporary_regionisn              NUMERIC,
    passport_addrisn                 NUMERIC,
    passport_cityisn                 NUMERIC,
    passport_regionisn               NUMERIC,
    passport_zip                     VARCHAR(10),
    passport_address                 VARCHAR(255),
    fact_addrisn                     NUMERIC,
    fact_cityisn                     NUMERIC,
    fact_regionisn                   NUMERIC,
    jur_addrisn                      NUMERIC,
    jur_cityisn                      NUMERIC,
    jur_regionisn                    NUMERIC,
    vipclassisn                      NUMERIC,
    subjsecuritystr                  VARCHAR(255),
    addresssecuritystr               VARCHAR(4000),
    phonesecuritystr                 VARCHAR(4000),
    drivingdatebeg                   TIMESTAMP,
    agegroup                         VARCHAR(4000),
    citizenship                      VARCHAR(4000),
    n_kids                           NUMERIC,
    marriagestateisn                 NUMERIC,
    familystateisn                   NUMERIC,
    stoadayspay                      NUMERIC,
    driverst                         VARCHAR(4000),
    motivation                       NUMERIC,
    no_mail                          VARCHAR(1),
    email                            VARCHAR(60),
    serv_phone                       VARCHAR(60),
    mobilephone                      VARCHAR(60),
    phone                            VARCHAR(60),
    home_phone                       VARCHAR(60),
    birthday                         TIMESTAMP,
    addrisn                          NUMERIC,
    cityisn                          NUMERIC,
    regionisn                        NUMERIC,
    sto_priority                     NUMERIC,
    juridical                        VARCHAR(1),
    subj_classisn                    NUMERIC,
    sms_phone                        VARCHAR(4000),
    deny_info_sms                    VARCHAR(1),
    deny_promo_sms                   VARCHAR(1),
    deny_info_email                  VARCHAR(1),
    deny_promo_email                 VARCHAR(1),
    agentcategoryisn                 NUMERIC,
    bestaddrisn                      NUMERIC,
    bestaddr                         VARCHAR(4000),
    mainokvedisn                     NUMERIC,
    clientisarrested                 VARCHAR(1),
    deny_info_post                   VARCHAR(1),
    deny_info_call                   VARCHAR(1),
    monitoringisn                    NUMERIC,
    monitoringbeg                    TIMESTAMP,
    monitoringend                    TIMESTAMP,
    monitoringupd                    NUMERIC
)
;
--WARNING: No primary key defined for storage_source.subject_attrib

COMMENT ON TABLE storage_source.subject_attrib IS 'Адреса и атрибуты клиента (выборка из Obj_Attrib)';
COMMENT ON COLUMN storage_source.subject_attrib.clientisarrested IS 'Признак клиента под арестом';
COMMENT ON COLUMN storage_source.subject_attrib.deny_info_post IS 'Запрет на информационное оповещение в виде бумажной почтовой рассылки. Y - запрет есть';
COMMENT ON COLUMN storage_source.subject_attrib.deny_info_call IS 'Запрет на информационное оповещение в виде звонка. Y - запрет есть';
COMMENT ON COLUMN storage_source.subject_attrib.monitoringisn IS 'Значение мониторинга';
COMMENT ON COLUMN storage_source.subject_attrib.monitoringbeg IS 'Дата начала мониторинга';
COMMENT ON COLUMN storage_source.subject_attrib.monitoringend IS 'Дата окончания мониторинга';
COMMENT ON COLUMN storage_source.subject_attrib.monitoringupd IS 'Автор изменения значения мониторинга';
COMMENT ON COLUMN storage_source.subject_attrib.mainokvedisn IS 'FK(Dicti) ОСНОВНОЙ ОКВЭД';
COMMENT ON COLUMN storage_source.subject_attrib.bestaddrisn IS 'FK(SubAddr) Оптимальный адрес по версии Крылова';
COMMENT ON COLUMN storage_source.subject_attrib.bestaddr IS 'Оптимальный адрес по версии Крылова';
COMMENT ON COLUMN storage_source.subject_attrib.subjisn IS 'FK(Subject) Клиент, ISN';
COMMENT ON COLUMN storage_source.subject_attrib.home_cityisn IS 'FK(City) Домашний адрес, город';
COMMENT ON COLUMN storage_source.subject_attrib.home_regionisn IS 'FK(City) Домашний адрес, субъект РФ';
COMMENT ON COLUMN storage_source.subject_attrib.post_cityisn IS 'FK(City) Почтовый адрес, город';
COMMENT ON COLUMN storage_source.subject_attrib.post_regionisn IS 'FK(Region) Почтовый адрес, субъект РФ';
COMMENT ON COLUMN storage_source.subject_attrib.temporary_cityisn IS 'FK(City) Адрес временной регистрации, город';
COMMENT ON COLUMN storage_source.subject_attrib.temporary_regionisn IS 'FK(Region) Адрес временной регистрации, субъект РФ';
COMMENT ON COLUMN storage_source.subject_attrib.passport_cityisn IS 'FK(City) Адрес регистрации по паспорту, город';
COMMENT ON COLUMN storage_source.subject_attrib.passport_regionisn IS 'FK(Region) Адрес регистрации по паспорту, субъект РФ';
COMMENT ON COLUMN storage_source.subject_attrib.passport_zip IS 'Адрес регистрации по паспорту, почтовый индекс';
COMMENT ON COLUMN storage_source.subject_attrib.passport_address IS 'Адрес регистрации по паспорту, строка адреса';
COMMENT ON COLUMN storage_source.subject_attrib.fact_cityisn IS 'FK(City) Фактический адрес, город';
COMMENT ON COLUMN storage_source.subject_attrib.fact_regionisn IS 'FK(Region) Фактический адрес, субъект РФ';
COMMENT ON COLUMN storage_source.subject_attrib.jur_cityisn IS 'FK(City) Юридический адрес, город';
COMMENT ON COLUMN storage_source.subject_attrib.jur_regionisn IS 'FK(Region) Юридический адрес, субъект РФ';
COMMENT ON COLUMN storage_source.subject_attrib.vipclassisn IS 'FK(Dicti) VIP-класс';
COMMENT ON COLUMN storage_source.subject_attrib.subjsecuritystr IS 'строка Security по SubHuman';
COMMENT ON COLUMN storage_source.subject_attrib.addresssecuritystr IS 'строка Security по SubAddress';
COMMENT ON COLUMN storage_source.subject_attrib.phonesecuritystr IS 'строка Security по SubPhone';
COMMENT ON COLUMN storage_source.subject_attrib.drivingdatebeg IS 'Дата начала водительского стажа';
COMMENT ON COLUMN storage_source.subject_attrib.agegroup IS 'Возрастная группа';
COMMENT ON COLUMN storage_source.subject_attrib.citizenship IS 'Гражданство';
COMMENT ON COLUMN storage_source.subject_attrib.n_kids IS 'Кол-во детей';
COMMENT ON COLUMN storage_source.subject_attrib.marriagestateisn IS 'FK(Dicti) Нахождение в браке, ISN';
COMMENT ON COLUMN storage_source.subject_attrib.familystateisn IS 'FK(Dicti) Семейное положение, ISN';
COMMENT ON COLUMN storage_source.subject_attrib.stoadayspay IS 'Срок оплаты СТОА (дни)';
COMMENT ON COLUMN storage_source.subject_attrib.driverst IS 'Стаж (категории)';
COMMENT ON COLUMN storage_source.subject_attrib.motivation IS 'FK(Dicti) Мотивационная группа';
COMMENT ON COLUMN storage_source.subject_attrib.cityisn IS 'FK(City) Город для V_BM';
COMMENT ON COLUMN storage_source.subject_attrib.regionisn IS 'FK(Region) Регион для V_BM';
COMMENT ON COLUMN storage_source.subject_attrib.sto_priority IS 'FK(Dicti) Приоритет при направлении для СТО';
COMMENT ON COLUMN storage_source.subject_attrib.juridical IS 'Признак юридичности';


CREATE TABLE storage_source.rep_city (
    cityisn                          NUMERIC,
    regionisn                        NUMERIC,
    countryisn                       NUMERIC,
    parentcity                       NUMERIC,
    parentregionisn                  NUMERIC,
    parentcountryisn                 NUMERIC,
    loadisn                          NUMERIC
)
DISTRIBUTED BY (cityisn);

COMMENT ON TABLE storage_source.rep_city IS 'Справочник городов с некотрой расшиыкой. Используется в том числе при загрузке Repcond для определения региона нахождения объекта';


CREATE TABLE storage_source.agr_detail_agrhash (
    agrisn                           NUMERIC,
    agrdetailisn                     NUMERIC
)
DISTRIBUTED BY (agrisn);

COMMENT ON COLUMN storage_source.agr_detail_agrhash.agrdetailisn IS 'FK(AGR_DETAILS)';


