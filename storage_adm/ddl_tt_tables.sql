---------------------------------------------
-- TT temporary tables with portion of data updating target tables
-- ddl is equivalent to target tables except isn and loadisn columns that are missing here
---------------------------------------------
CREATE TABLE storage_adm.tt_repagr_economic (
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
    buhdate                          TIMESTAMP,
    limitsum                         NUMERIC(20,2),
    limitsumusd                      NUMERIC,
    insuredsum                       NUMERIC(20,2),
    insuredsumusd                    NUMERIC,
    agrcreated                       TIMESTAMP,
    agentjuridical                   VARCHAR(1),
    firmisn                          NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.tt_repagr_economic



CREATE TABLE storage_adm.tt_rep_agrcargo (
    agrisn                           NUMERIC,
    sea                              NUMERIC,
    more1                            NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.tt_rep_agrcargo



CREATE TABLE storage_adm.tt_rep_agrtur (
    agrisn                           NUMERIC,
    isrussia                         NUMERIC,
    isshengen                        NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.tt_rep_agrtur



CREATE TABLE storage_adm.tt_rep_objclass_domestic (
    agrisn                           NUMERIC,
    objclassisn                      NUMERIC,
    domestic                         VARCHAR(1),
    parentobjclassisn                NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.tt_rep_objclass_domestic



CREATE TABLE storage_adm.tt_rep_agrext (
    agrisn                           NUMERIC,
    classisn                         NUMERIC,
    x1                               NUMERIC,
    x2                               NUMERIC,
    x3                               NUMERIC,
    x4                               NUMERIC,
    x5                               NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.tt_rep_agrext



CREATE TABLE storage_adm.tt_repcrgdoc (
    agrisn                           NUMERIC,
    classisn                         NUMERIC,
    objisn                           NUMERIC,
    subjisn                          NUMERIC,
    juridical                        VARCHAR(1)
)
;
--WARNING: No primary key defined for storage_adm.tt_repcrgdoc



CREATE TABLE storage_adm.tt_repagr (
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
    insurantisn                     NUMERIC,
    insurantcount                   NUMERIC,
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
    createdate                       TIMESTAMP
)
;
--WARNING: No primary key defined for storage_adm.tt_repagr



CREATE TABLE storage_adm.tt_repcond (
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
    agrcomission                     NUMERIC(9,6),
    agrdiscr                         VARCHAR(1),
    newaddsign                       TIMESTAMP,
    quantity                         NUMERIC(38),
    franchtariff                     NUMERIC(7,4),
    objregionisn                     NUMERIC,
    objcountryisn                    NUMERIC,
    clientisn                        NUMERIC,
    agrolddateend                    TIMESTAMP,
    addpremiumsum                    NUMERIC(20,2),
    agrcurrisn                       NUMERIC,
    agrdetailisn                     NUMERIC,
    premagr                          NUMERIC,
    carrptclass                      VARCHAR(4000),
    discount                         NUMERIC,
    discount2                        NUMERIC,
    agrsharepc                       NUMERIC,
    cost                             NUMERIC,
    tariff                           NUMERIC(12,9),
    yeartariff                       NUMERIC(12,9)
)
;
--WARNING: No primary key defined for storage_adm.tt_repcond



CREATE TABLE storage_adm.tt_longagraddendum (
    agrisn                           NUMERIC,
    addisn                           NUMERIC,
    discr                            VARCHAR(1),
    datebeg                          TIMESTAMP,
    datesign                         TIMESTAMP,
    premiumsum                       NUMERIC,
    currisn                          NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.tt_longagraddendum



CREATE TABLE storage_adm.tt_subj_best_addr (
    subjisn                          NUMERIC,
    addrisn                          NUMERIC,
    subaddr                          VARCHAR(4000)
)
;
--WARNING: No primary key defined for storage_adm.tt_subj_best_addr



CREATE TABLE storage_adm.tt_subject_attrib (
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
--WARNING: No primary key defined for storage_adm.tt_subject_attrib

COMMENT ON COLUMN storage_adm.tt_subject_attrib.monitoringisn IS 'Значение мониторинга';
COMMENT ON COLUMN storage_adm.tt_subject_attrib.monitoringbeg IS 'Дата начала мониторинга';
COMMENT ON COLUMN storage_adm.tt_subject_attrib.monitoringend IS 'Дата окончания мониторинга';
COMMENT ON COLUMN storage_adm.tt_subject_attrib.monitoringupd IS 'Автор изменения значения мониторинга';


CREATE TABLE storage_adm.tt_buhbody (
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
    dateval_prnt                     TIMESTAMP,
    dfid                             NUMERIC,
    damountrub_prnt                  NUMERIC,
    camountrub_prnt                  NUMERIC,
    subjisn                          NUMERIC,
    currisn                          NUMERIC,
    agrisn                           NUMERIC,
    basedamount                      NUMERIC,
    basecamount                      NUMERIC,
    damount                          NUMERIC,
    camount                          NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.tt_buhbody



CREATE TABLE storage_adm.tt_bodydebcre (
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
    basecamount                      NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.tt_bodydebcre



CREATE TABLE storage_adm.tt_buh_turn (
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
--WARNING: No primary key defined for storage_adm.tt_buh_turn



CREATE TABLE storage_adm.tt_rep_subject (
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
    parentclass                      VARCHAR(40),
    parentinn                        VARCHAR(15),
    updatedby_name                   VARCHAR(255),
    bnk_vkey                         VARCHAR(8),
    bnk_vkeydel                      VARCHAR(8),
    bnk_active                       VARCHAR(1),
    valaam_name                      VARCHAR(4000),
    regnmdtend                       TIMESTAMP,
    likvidstatusdtend                TIMESTAMP,
    curator                          VARCHAR(4000),
    curatordeptisn                   NUMERIC,
    dealer                           NUMERIC,
    repsynkisn                       NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.tt_rep_subject



CREATE TABLE storage_adm.tt_buh_turn_contr (
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
--WARNING: No primary key defined for storage_adm.tt_buh_turn_contr



CREATE TABLE storage_adm.tt_docsumbody (
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
    c_agr_1                          NUMERIC,
    agrkoef                          NUMERIC,
    dskoef                           NUMERIC,
    agrdskoef                        NUMERIC,
    db                               TIMESTAMP,
    de                               TIMESTAMP,
    remainder_1                      NUMERIC,
    reaccisn                         NUMERIC,
    agentisn                         NUMERIC,
    agrdatebeg                       TIMESTAMP
)
;
--WARNING: No primary key defined for storage_adm.tt_docsumbody



CREATE TABLE storage_adm.tt_buhbody_reins (
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
--WARNING: No primary key defined for storage_adm.tt_buhbody_reins



CREATE TABLE storage_adm.tt_buh_turn_corr (
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
--WARNING: No primary key defined for storage_adm.tt_buh_turn_corr



CREATE TABLE storage_adm.tt_reprefund (
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
    rptgroupisn_                     NUMERIC DEFAULT 0,
    conddeptisn_                     NUMERIC,
    isrevaluation_                   NUMERIC,
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
    budgetgroupisn_                  NUMERIC,
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
    motivgroupisn_                   NUMERIC,
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
--WARNING: No primary key defined for storage_adm.tt_reprefund



CREATE TABLE storage_adm.tt_rep_agr_salers (
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
--WARNING: No primary key defined for storage_adm.tt_rep_agr_salers



CREATE TABLE storage_adm.tt_buhbody_nms (
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
--WARNING: No primary key defined for storage_adm.tt_buhbody_nms



CREATE TABLE storage_adm.tt_repbuhquit (
    deptisn                          NUMERIC,
    statcode                         NUMERIC,
    classisn                         NUMERIC,
    bodyisn                          NUMERIC,
    dateval                          TIMESTAMP,
    currisn                          NUMERIC,
    buhheadfid                       NUMERIC(38),
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
--WARNING: No primary key defined for storage_adm.tt_repbuhquit



CREATE TABLE storage_adm.tt_repbuhbody (
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
--WARNING: No primary key defined for storage_adm.tt_repbuhbody



CREATE TABLE storage_adm.tt_rep_agr_salers_line (
    agrisn                           NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    salergoisn                       NUMERIC,
    salergoclassisn                  NUMERIC,
    salergodept                      NUMERIC,
    salergodept0isn                  NUMERIC,
    salercrgoisn                     NUMERIC,
    salercrclassisn                  NUMERIC,
    salercrgodept                    NUMERIC,
    salercrgodept0isn                NUMERIC,
    salerfisn                        NUMERIC,
    salerfclassisn                   NUMERIC,
    salerfdept                       NUMERIC,
    salerfdept0isn                   NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.tt_rep_agr_salers_line



CREATE TABLE storage_adm.tt_repagrroleagr (
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
    contrcount                       NUMERIC
)
;
--WARNING: No primary key defined for storage_adm.tt_repagrroleagr

COMMENT ON COLUMN storage_adm.tt_repagrroleagr.agent_maxcomission_isn IS 'Агент с максимальной комиссией';
COMMENT ON COLUMN storage_adm.tt_repagrroleagr.contrcount IS 'Количетво подрядчиков';
COMMENT ON COLUMN storage_adm.tt_repagrroleagr.contractorisn IS 'Подрядчик';
COMMENT ON COLUMN storage_adm.tt_repagrroleagr.contrcomission IS '% Подрядчика';


CREATE TABLE storage_adm.tt_rep_agent_ranks (
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
--WARNING: No primary key defined for storage_adm.tt_rep_agent_ranks