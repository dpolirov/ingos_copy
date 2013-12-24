CREATE TABLE aisfin.famounts (
    isn                              NUMERIC,
    fobjisn                          NUMERIC,
    classisn                         NUMERIC,
    currisn                          NUMERIC,
    parentisn                        NUMERIC,
    amount_cur                       NUMERIC,
    amount_rur                       NUMERIC,
    amount_usd                       NUMERIC,
    dateval                          TIMESTAMP,
    expenseisn                       NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    firmisn                          NUMERIC,
    datedoc                          TIMESTAMP,
    fid                              NUMERIC,
    itemisn                          NUMERIC,
    entryisn                         NUMERIC,
    inobject                         NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE aisfin.famounts IS $COMM$таблица хранения сумм$COMM$;
COMMENT ON COLUMN aisfin.famounts.itemisn IS $COMM$Ссылка на строку документа, ее породившую$COMM$;
COMMENT ON COLUMN aisfin.famounts.entryisn IS $COMM$Ссылка на проводку$COMM$;
COMMENT ON COLUMN aisfin.famounts.inobject IS $COMM$Писать в объект$COMM$;
COMMENT ON COLUMN aisfin.famounts.fobjisn IS $COMM$ISN сущности$COMM$;
COMMENT ON COLUMN aisfin.famounts.classisn IS $COMM$класс суммы$COMM$;
COMMENT ON COLUMN aisfin.famounts.currisn IS $COMM$валюта$COMM$;
COMMENT ON COLUMN aisfin.famounts.dateval IS $COMM$дата валютирования$COMM$;
COMMENT ON COLUMN aisfin.famounts.expenseisn IS $COMM$статья расхода$COMM$;


CREATE TABLE aisfin.fbuhobject (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    parentisn                        NUMERIC,
    code                             VARCHAR(20),
    shortname                        VARCHAR(150),
    objisn                           NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    branchisn                        NUMERIC,
    remark                           VARCHAR(1000),
    firmisn                          NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    fid                              NUMERIC,
    statusisn                        NUMERIC,
    dictiisn                         NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE aisfin.fbuhobject IS $COMM$Учетный объект : таблица хранит в себе указатели на объекты.Они упорядоченны в иерархию первым в иерархии стоят типы учетных объектов. Учетный объект то что может порождать аналитику$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.shortname IS $COMM$наименование : для инвентарных номеров - наименование$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.objisn IS $COMM$ISN учетного объекта в спец таблице$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.datebeg IS $COMM$дата начала действия объекта: для договоров - Дата начала действия, для инвентарников дата создания$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.dateend IS $COMM$дата окончания действия объекта поле мб не заполнено если объект еще действует например инвентарник еще не списан$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.branchisn IS $COMM$FK (SUBDEPT) Представительство$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.remark IS $COMM$Описание$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.firmisn IS $COMM$Организация$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.created IS $COMM$Время создания$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.createdby IS $COMM$Автор создания$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.updated IS $COMM$Время изменения$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.updatedby IS $COMM$Автор изменений$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.fid IS $COMM$Branchisn филиала(признак конвертации)$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.statusisn IS $COMM$Состояние$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.dictiisn IS $COMM$Указатель на справочник (DICTI)$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.isn IS $COMM$PK (seq_allfin.nextval)$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.classisn IS $COMM$FK (DICTI) тип учетного объекта$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.parentisn IS $COMM$родительский объект: для описания комплектов$COMM$;
COMMENT ON COLUMN aisfin.fbuhobject.code IS $COMM$код : для договоров № договора для объектов мат стола - инвентарный номер, для типов учетных объектов - наименование таблицы$COMM$;


CREATE TABLE aisfin.fcloseperiod (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    deptisn                          NUMERIC,
    calc_amort                       NUMERIC,
    stateisn                         NUMERIC,
    createdby                        NUMERIC,
    created                          TIMESTAMP,
    state_updated                    TIMESTAMP,
    state_updatedby                  NUMERIC,
    closed                           NUMERIC,
    statetypeisn                     NUMERIC,
    fobjclassisn                     NUMERIC,
    flags                            NUMERIC,
    remark                           VARCHAR(255),
    stateid                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN aisfin.fcloseperiod.remark IS $COMM$Комментарий$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.stateid IS $COMM$seq_state.nextval изменяется при возврате в статус В работе$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.fobjclassisn IS $COMM$Класс учетного объекта$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.classisn IS $COMM$Тип закрываемого периода$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.datebeg IS $COMM$Начало периода$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.dateend IS $COMM$Окончание периода$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.deptisn IS $COMM$Подразделение$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.calc_amort IS $COMM$Ссылка на документ амортизация$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.stateisn IS $COMM$текущий статус периода$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.createdby IS $COMM$Пользователь, создавший период$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.created IS $COMM$Дата создания$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.state_updated IS $COMM$Дата изменения статуса$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.state_updatedby IS $COMM$Пользователь изменивший статус$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.closed IS $COMM$Признак того, что период был уже закрыт$COMM$;
COMMENT ON COLUMN aisfin.fcloseperiod.statetypeisn IS $COMM$Способ закрытия периода (вручную или автоматически)$COMM$;


CREATE TABLE aisfin.fdochead (
    isn                              NUMERIC,
    operisn                          NUMERIC,
    in_id                            VARCHAR(50),
    out_id                           VARCHAR(50),
    parentisn                        NUMERIC,
    statusisn                        NUMERIC,
    datecur                          TIMESTAMP,
    dateval                          TIMESTAMP,
    datedoc                          TIMESTAMP,
    fobjisn                          NUMERIC,
    remark                           VARCHAR(1000),
    currisn                          NUMERIC,
    correct_flag                     VARCHAR(1),
    firmisn                          NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    molisn                           NUMERIC,
    terrisn                          NUMERIC,
    userisn                          NUMERIC,
    updversion                       NUMERIC,
    fid                              NUMERIC,
    deptown                          NUMERIC,
    dateoutdoc                       TIMESTAMP,
    agrisn                           NUMERIC,
    addisn                           NUMERIC,
    accessmode                       NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN aisfin.fdochead.agrisn IS $COMM$ссылка на договор$COMM$;
COMMENT ON COLUMN aisfin.fdochead.addisn IS $COMM$ссылка на адендум$COMM$;
COMMENT ON COLUMN aisfin.fdochead.accessmode IS $COMM$null-обычный документ, 0-автоматический документ, 1-документ перехода из старого МС.$COMM$;


CREATE TABLE aisfin.fdocitems (
    isn                              NUMERIC,
    docisn                           NUMERIC,
    suboperisn                       NUMERIC,
    no                               NUMERIC,
    partyisn                         NUMERIC,
    parentisn                        NUMERIC,
    nomisn                           NUMERIC,
    quantity                         NUMERIC,
    amount_cur                       NUMERIC,
    amount_rur                       NUMERIC,
    amount_nds                       NUMERIC,
    dbcr                             VARCHAR(1),
    fobjisn                          NUMERIC,
    discr                            VARCHAR(1),
    kindisn                          NUMERIC,
    fobjclassisn                     NUMERIC,
    molisn                           NUMERIC,
    terrisn                          NUMERIC,
    userisn                          NUMERIC,
    firmisn                          NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    fid                              NUMERIC,
    relisn                           NUMERIC,
    condisn                          NUMERIC,
    party_statusisn                  NUMERIC,
    deptisn                          NUMERIC,
    rowdate                          TIMESTAMP,
    invoiceisn                       NUMERIC,
    dateval                          TIMESTAMP,
    currisn                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN aisfin.fdocitems.rowdate IS $COMM$Дата учета$COMM$;
COMMENT ON COLUMN aisfin.fdocitems.relisn IS $COMM$FK (fdocitems) связь со строкой другого документа$COMM$;
COMMENT ON COLUMN aisfin.fdocitems.condisn IS $COMM$FK(AgrCond) ссылка на условие договора$COMM$;
COMMENT ON COLUMN aisfin.fdocitems.party_statusisn IS $COMM$FK(Dicti) статус партии$COMM$;
COMMENT ON COLUMN aisfin.fdocitems.deptisn IS $COMM$Подразделение, для ГО = 0$COMM$;


CREATE TABLE aisfin.feventlog (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    fobjisn                          NUMERIC,
    itemisn                          NUMERIC,
    docisn                           NUMERIC,
    dateevent                        TIMESTAMP,
    msg                              VARCHAR(1000),
    firmisn                          NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    fid                              NUMERIC,
    result_docisn                    NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE aisfin.fitemrels (
    isn                              NUMERIC,
    item1                            NUMERIC,
    item2                            NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE aisfin.fitemrels IS $COMM$связи строк документа$COMM$;
COMMENT ON COLUMN aisfin.fitemrels.isn IS $COMM$SEQ_ALLFIN$COMM$;
COMMENT ON COLUMN aisfin.fitemrels.item1 IS $COMM$(FK DocItem)$COMM$;
COMMENT ON COLUMN aisfin.fitemrels.item2 IS $COMM$(FK DocItem)$COMM$;


CREATE TABLE aisfin.fmc_obj (
    isn                              NUMERIC,
    invno                            VARCHAR(20),
    name                             VARCHAR(150),
    nomisn                           NUMERIC,
    firmisn                          NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    terrisn                          NUMERIC,
    userisn                          NUMERIC,
    molisn                           NUMERIC,
    fid                              NUMERIC,
    classisn                         NUMERIC,
    serialno                         VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE aisfin.fmc_obj IS $COMM$Инвертарный объект$COMM$;
COMMENT ON COLUMN aisfin.fmc_obj.isn IS $COMM$PK (seq_allfin.nextval)$COMM$;
COMMENT ON COLUMN aisfin.fmc_obj.invno IS $COMM$Инвентарный номер копируется в FBUHOBJECT.CODE$COMM$;
COMMENT ON COLUMN aisfin.fmc_obj.name IS $COMM$Название копирется FBUHOBJECT.SHORTNAME$COMM$;
COMMENT ON COLUMN aisfin.fmc_obj.nomisn IS $COMM$FK (DICTI) номенклатура$COMM$;
COMMENT ON COLUMN aisfin.fmc_obj.firmisn IS $COMM$Организация$COMM$;
COMMENT ON COLUMN aisfin.fmc_obj.created IS $COMM$Время создания$COMM$;
COMMENT ON COLUMN aisfin.fmc_obj.createdby IS $COMM$Автор создания$COMM$;
COMMENT ON COLUMN aisfin.fmc_obj.updated IS $COMM$Время изменения$COMM$;
COMMENT ON COLUMN aisfin.fmc_obj.updatedby IS $COMM$Автор изменений$COMM$;
COMMENT ON COLUMN aisfin.fmc_obj.classisn IS $COMM$FK (DICTI) класс объекта (ОС, Малооценка)$COMM$;


CREATE TABLE aisfin.fredolog (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    fdocitemisn                      NUMERIC,
    fobjisn                          NUMERIC,
    fobjclassisn                     NUMERIC,
    discr                            VARCHAR(1),
    logdate                          TIMESTAMP,
    oldvalue                         VARCHAR(500),
    newvalue                         VARCHAR(500),
    fdocheadisn                      NUMERIC,
    dateval                          TIMESTAMP,
    firmisn                          NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    obj_name                         VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN aisfin.fredolog.classisn IS $COMM$тип события$COMM$;
COMMENT ON COLUMN aisfin.fredolog.fdocitemisn IS $COMM$ссылка на строку обрабатываемого документа$COMM$;
COMMENT ON COLUMN aisfin.fredolog.fobjisn IS $COMM$объект, по которому произошло событие$COMM$;
COMMENT ON COLUMN aisfin.fredolog.fobjclassisn IS $COMM$класс объекта, по которому произошло событие$COMM$;
COMMENT ON COLUMN aisfin.fredolog.discr IS $COMM$тип события (вставка, изменение, удаление)$COMM$;
COMMENT ON COLUMN aisfin.fredolog.logdate IS $COMM$дата события$COMM$;
COMMENT ON COLUMN aisfin.fredolog.oldvalue IS $COMM$старое значение$COMM$;
COMMENT ON COLUMN aisfin.fredolog.newvalue IS $COMM$новое значение$COMM$;
COMMENT ON COLUMN aisfin.fredolog.fdocheadisn IS $COMM$ссылка на обрабатываемый документ$COMM$;
COMMENT ON COLUMN aisfin.fredolog.dateval IS $COMM$дата, на которую изменяется значение$COMM$;
COMMENT ON COLUMN aisfin.fredolog.obj_name IS $COMM$наименование поля, атрибута$COMM$;


CREATE TABLE aisfin.invjournal (
    isn                              NUMERIC,
    id                               VARCHAR(20),
    datedoc                          TIMESTAMP,
    amount                           NUMERIC,
    customerisn                      NUMERIC,
    customername                     VARCHAR(255),
    deptisn                          NUMERIC,
    basedocisn                       NUMERIC,
    currisn                          NUMERIC,
    cst_inn                          VARCHAR(15),
    cst_kpp                          VARCHAR(15),
    percentisn                       NUMERIC,
    amountnv                         NUMERIC,
    vatamount                        NUMERIC,
    staxfree                         NUMERIC,
    totalsales                       NUMERIC,
    remark                           VARCHAR(1000),
    dtype                            NUMERIC,
    pdatesaler                       TIMESTAMP,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    status                           VARCHAR(1),
    taxdate                          TIMESTAMP,
    vatanalytic                      NUMERIC,
    advdate                          TIMESTAMP,
    taxless                          NUMERIC,
    amountrub                        NUMERIC,
    countryisn                       NUMERIC,
    nds2                             NUMERIC,
    decltaxid                        VARCHAR(255),
    docgetdate                       TIMESTAMP,
    acckeepdate                      TIMESTAMP,
    parentisn                        NUMERIC,
    extid                            VARCHAR(255),
    basedocid                        VARCHAR(255),
    basedocdate                      TIMESTAMP,
    emplisn                          NUMERIC,
    classisn                         NUMERIC,
    dateact                          TIMESTAMP,
    operisn                          NUMERIC,
    opercode                         VARCHAR(255),
    actcode                          VARCHAR(8),
    idcorr                           VARCHAR(255),
    idcorr2                          VARCHAR(255),
    ndscurr                          NUMERIC,
    datecorr                         TIMESTAMP,
    ktotalplus                       NUMERIC,
    ktotalminus                      NUMERIC,
    knovatplus                       NUMERIC,
    knovatminus                      NUMERIC,
    kvatplus                         NUMERIC,
    kvatminus                        NUMERIC,
    ktaxfreeplus                     NUMERIC,
    ktaxfreeminus                    NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN aisfin.invjournal.ktotalplus IS $COMM$Покупки, освобождаемые от налога (корректировка)$COMM$;
COMMENT ON COLUMN aisfin.invjournal.id IS $COMM$номер документа$COMM$;
COMMENT ON COLUMN aisfin.invjournal.datedoc IS $COMM$дата счета-фактуры$COMM$;
COMMENT ON COLUMN aisfin.invjournal.amount IS $COMM$сумма выставленной СФ$COMM$;
COMMENT ON COLUMN aisfin.invjournal.customerisn IS $COMM$покупатель\продавец$COMM$;
COMMENT ON COLUMN aisfin.invjournal.customername IS $COMM$наименование покупателя$COMM$;
COMMENT ON COLUMN aisfin.invjournal.deptisn IS $COMM$isn подразделения$COMM$;
COMMENT ON COLUMN aisfin.invjournal.basedocisn IS $COMM$документ-основание$COMM$;
COMMENT ON COLUMN aisfin.invjournal.currisn IS $COMM$валюта СФ$COMM$;
COMMENT ON COLUMN aisfin.invjournal.cst_inn IS $COMM$ИНН покупателя\продавца$COMM$;
COMMENT ON COLUMN aisfin.invjournal.cst_kpp IS $COMM$КПП покупателя$COMM$;
COMMENT ON COLUMN aisfin.invjournal.percentisn IS $COMM$процентная ставка (dicti)$COMM$;
COMMENT ON COLUMN aisfin.invjournal.amountnv IS $COMM$стоимость продаж\покупок без НДС$COMM$;
COMMENT ON COLUMN aisfin.invjournal.vatamount IS $COMM$сумма НДС$COMM$;
COMMENT ON COLUMN aisfin.invjournal.staxfree IS $COMM$продажи\покупки, освобождаемые от налога$COMM$;
COMMENT ON COLUMN aisfin.invjournal.ktotalminus IS $COMM$Покупки, освобождаемые от налога (корректировка)$COMM$;
COMMENT ON COLUMN aisfin.invjournal.datecorr IS $COMM$Дата корректировки$COMM$;
COMMENT ON COLUMN aisfin.invjournal.knovatplus IS $COMM$Стоимость без НДС (корректировка)$COMM$;
COMMENT ON COLUMN aisfin.invjournal.knovatminus IS $COMM$Стоимость без НДС (корректировка)$COMM$;
COMMENT ON COLUMN aisfin.invjournal.kvatplus IS $COMM$Сумма НДС (корректировка)$COMM$;
COMMENT ON COLUMN aisfin.invjournal.kvatminus IS $COMM$Сумма НДС (корректировка)$COMM$;
COMMENT ON COLUMN aisfin.invjournal.totalsales IS $COMM$всего продаж\покупок, включая НДС$COMM$;
COMMENT ON COLUMN aisfin.invjournal.remark IS $COMM$примечания$COMM$;
COMMENT ON COLUMN aisfin.invjournal.dtype IS $COMM$тип счета-фактуры$COMM$;
COMMENT ON COLUMN aisfin.invjournal.pdatesaler IS $COMM$дата оплаты счета-фактуры продавца$COMM$;
COMMENT ON COLUMN aisfin.invjournal.status IS $COMM$статус: Y - активный, A - аннулирован$COMM$;
COMMENT ON COLUMN aisfin.invjournal.taxdate IS $COMM$дата начала налогового периода$COMM$;
COMMENT ON COLUMN aisfin.invjournal.vatanalytic IS $COMM$Аналитика для декларации по НДС$COMM$;
COMMENT ON COLUMN aisfin.invjournal.advdate IS $COMM$Дата зачета аванса$COMM$;
COMMENT ON COLUMN aisfin.invjournal.taxless IS $COMM$Налоговый вычет$COMM$;
COMMENT ON COLUMN aisfin.invjournal.amountrub IS $COMM$сумма СФ в рублях$COMM$;
COMMENT ON COLUMN aisfin.invjournal.countryisn IS $COMM$страна происхождения$COMM$;
COMMENT ON COLUMN aisfin.invjournal.decltaxid IS $COMM$номер таможенной декларации$COMM$;
COMMENT ON COLUMN aisfin.invjournal.docgetdate IS $COMM$дата получения СФ$COMM$;
COMMENT ON COLUMN aisfin.invjournal.acckeepdate IS $COMM$Дата принятия на учет товаров (работ, услуг)$COMM$;
COMMENT ON COLUMN aisfin.invjournal.parentisn IS $COMM$ссылка на базовую счет-фактуру$COMM$;
COMMENT ON COLUMN aisfin.invjournal.extid IS $COMM$внешний номер СФ$COMM$;
COMMENT ON COLUMN aisfin.invjournal.basedocid IS $COMM$ID документа-основания$COMM$;
COMMENT ON COLUMN aisfin.invjournal.basedocdate IS $COMM$Дата документа-основания$COMM$;
COMMENT ON COLUMN aisfin.invjournal.emplisn IS $COMM$Ответственный сотрудник$COMM$;
COMMENT ON COLUMN aisfin.invjournal.classisn IS $COMM$класс СФ (первоначальный, исправительный, корректировочный)$COMM$;
COMMENT ON COLUMN aisfin.invjournal.dateact IS $COMM$Дата выставления$COMM$;
COMMENT ON COLUMN aisfin.invjournal.operisn IS $COMM$Вид операции$COMM$;
COMMENT ON COLUMN aisfin.invjournal.opercode IS $COMM$Код вида операции$COMM$;
COMMENT ON COLUMN aisfin.invjournal.actcode IS $COMM$Код способа выставления$COMM$;
COMMENT ON COLUMN aisfin.invjournal.idcorr IS $COMM$Сумма НДС в валюте СФ$COMM$;
COMMENT ON COLUMN aisfin.invjournal.idcorr2 IS $COMM$Номер корректировки$COMM$;


CREATE TABLE aisfin.kpp_t (
    isn                              NUMERIC,
    code                             VARCHAR(10),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    kindisn                          NUMERIC,
    inspectisn                       NUMERIC,
    rate                             NUMERIC,
    deptisn                          NUMERIC,
    eno                              VARCHAR(7),
    enodatebeg                       TIMESTAMP,
    enodateend                       TIMESTAMP,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    discr                            VARCHAR(1),
    purposeisn                       NUMERIC,
    budgetisn                        NUMERIC,
    typeisn                          NUMERIC,
    valbeg                           NUMERIC,
    valend                           NUMERIC,
    transreason                      NUMERIC,
    nextkpp                          NUMERIC,
    agebeg                           NUMERIC,
    ageend                           NUMERIC,
    payisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE aisfin.kpp_t IS $COMM$Справочник КПП$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.agebeg IS $COMM$Возраст ТС с$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.ageend IS $COMM$Возраст ТС по$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.payisn IS $COMM$(FK dicti) Порядок оплаты, c.get('AGRPAYMENT')$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.isn IS $COMM$PK(seq_allfin.nextval)$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.code IS $COMM$Номер КПП, ветка dicti TAX_KPP$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.datebeg IS $COMM$Дата постановки на учет КПП$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.dateend IS $COMM$Дата снятия с учета по заданому КПП$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.kindisn IS $COMM$FK(dicti) Тип налога, ветка dicti TAX_KIND$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.inspectisn IS $COMM$FK(dicti) ИФНС, ветка dicti TAX_INSPECTION$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.rate IS $COMM$Ставка налога$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.deptisn IS $COMM$FK(subdept) Подразделение$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.eno IS $COMM$ЕНО, Формат значения 000.000$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.enodatebeg IS $COMM$Дата действия ЕНО С$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.enodateend IS $COMM$Дата действия ЕНО По$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.purposeisn IS $COMM$FK(dicti) Назначение налоговой ставки$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.budgetisn IS $COMM$FK(dicti) Тип бюджета$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.typeisn IS $COMM$FK(dicti) Тип транспортного средства$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.valbeg IS $COMM$Л/С с$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.valend IS $COMM$Л/С по$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.discr IS $COMM$=K - КПП, =R - налоговая ставка$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.transreason IS $COMM$FK(dicti) Куда переданы остатки$COMM$;
COMMENT ON COLUMN aisfin.kpp_t.nextkpp IS $COMM$FK(kpp_t) Следующий КПП$COMM$;


CREATE TABLE aisfin.rdochead (
    isn                              NUMERIC,
    operisn                          NUMERIC,
    in_id                            VARCHAR(50),
    out_id                           VARCHAR(50),
    parentisn                        NUMERIC,
    statusisn                        NUMERIC,
    datedoc                          TIMESTAMP,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    deptown                          NUMERIC,
    fobjisn                          NUMERIC,
    clientisn                        NUMERIC,
    remark                           VARCHAR(1000),
    firmisn                          NUMERIC,
    docimageisn                      NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE aisfin.rdochead IS $COMM$заголовок регламентного документа$COMM$;
COMMENT ON COLUMN aisfin.rdochead.isn IS $COMM$SEQ_ALLFIN$COMM$;
COMMENT ON COLUMN aisfin.rdochead.operisn IS $COMM$FK(DICTI) тип регламентного документа$COMM$;
COMMENT ON COLUMN aisfin.rdochead.in_id IS $COMM$внутренний номер$COMM$;
COMMENT ON COLUMN aisfin.rdochead.out_id IS $COMM$внешний номер$COMM$;
COMMENT ON COLUMN aisfin.rdochead.parentisn IS $COMM$FK(RDOCHEAD) ссылка на головной документ$COMM$;
COMMENT ON COLUMN aisfin.rdochead.statusisn IS $COMM$FK(DICTI) статус документа$COMM$;
COMMENT ON COLUMN aisfin.rdochead.datedoc IS $COMM$дата документа$COMM$;
COMMENT ON COLUMN aisfin.rdochead.datebeg IS $COMM$дата начала $COMM$;
COMMENT ON COLUMN aisfin.rdochead.dateend IS $COMM$дата окончания $COMM$;
COMMENT ON COLUMN aisfin.rdochead.deptown IS $COMM$подраделение 0 - ГО, иначе isn - филиала$COMM$;
COMMENT ON COLUMN aisfin.rdochead.fobjisn IS $COMM$FK(FBUHOBJECT) ссылка на объект$COMM$;
COMMENT ON COLUMN aisfin.rdochead.clientisn IS $COMM$FK(SUBJISN) ссылка на клиента$COMM$;
COMMENT ON COLUMN aisfin.rdochead.remark IS $COMM$комментарий$COMM$;
COMMENT ON COLUMN aisfin.rdochead.firmisn IS $COMM$FK(DICTI) организация$COMM$;
COMMENT ON COLUMN aisfin.rdochead.docimageisn IS $COMM$FK(DOCIMAGE) ссылка на комплект документов$COMM$;
COMMENT ON COLUMN aisfin.rdochead.created IS $COMM$время создания$COMM$;
COMMENT ON COLUMN aisfin.rdochead.createdby IS $COMM$автор создания$COMM$;
COMMENT ON COLUMN aisfin.rdochead.updated IS $COMM$время изменения$COMM$;
COMMENT ON COLUMN aisfin.rdochead.updatedby IS $COMM$автор изменения$COMM$;


CREATE TABLE aisfin.rdocitems (
    isn                              NUMERIC,
    docisn                           NUMERIC,
    parentisn                        NUMERIC,
    fobjisn                          NUMERIC,
    subjisn                          NUMERIC,
    val                              VARCHAR(1000),
    valn                             NUMERIC,
    classisn                         NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE aisfin.rdocitems IS $COMM$заголовок регламентного документа$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.fobjisn IS $COMM$FK(FBUHOBJECT) ссылка на объект$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.subjisn IS $COMM$FK(SUBJISN) ссылка на клиента$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.val IS $COMM$строковое значение$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.valn IS $COMM$числовое значение$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.classisn IS $COMM$FK(DICTI) ссылка на справочник$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.created IS $COMM$время создания$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.createdby IS $COMM$автор создания$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.updated IS $COMM$время изменения$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.updatedby IS $COMM$автор изменения$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.isn IS $COMM$SEQ_ALLFIN$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.docisn IS $COMM$FK(FDOCHEAD) шапка регламентного документа$COMM$;
COMMENT ON COLUMN aisfin.rdocitems.parentisn IS $COMM$родительская строка$COMM$;


CREATE TABLE aisfin.store_buhs (
    isn                              NUMERIC,
    code                             VARCHAR(10),
    fobjisn                          NUMERIC,
    dateval                          TIMESTAMP,
    classisn                         NUMERIC,
    branchisn                        NUMERIC,
    molisn                           NUMERIC,
    nomisn                           NUMERIC,
    damount                          NUMERIC,
    camount                          NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE aisfin.store_buhs IS $COMM$Таблица хранимых остатков (документы)$COMM$;
COMMENT ON COLUMN aisfin.store_buhs.isn IS $COMM$PK(seq_allfin.nextval)$COMM$;
COMMENT ON COLUMN aisfin.store_buhs.code IS $COMM$Субсчет$COMM$;
COMMENT ON COLUMN aisfin.store_buhs.fobjisn IS $COMM$(FK Fdocitems/Fbuhobject) ОС/партия (если есть на проводке)$COMM$;
COMMENT ON COLUMN aisfin.store_buhs.dateval IS $COMM$дата остатков$COMM$;
COMMENT ON COLUMN aisfin.store_buhs.classisn IS $COMM$(FK dicti) класс УО$COMM$;
COMMENT ON COLUMN aisfin.store_buhs.branchisn IS $COMM$(FK subdept) Подразделение (ГО =0)$COMM$;
COMMENT ON COLUMN aisfin.store_buhs.molisn IS $COMM$(FK fbuhobject) МОЛ$COMM$;
COMMENT ON COLUMN aisfin.store_buhs.nomisn IS $COMM$(FK dicti) номенклатура$COMM$;
COMMENT ON COLUMN aisfin.store_buhs.damount IS $COMM$исходящее сальдо (дебет)$COMM$;
COMMENT ON COLUMN aisfin.store_buhs.camount IS $COMM$исходящее сальдо (кредит)$COMM$;


CREATE TABLE aisfin.store_docs (
    isn                              NUMERIC,
    code                             VARCHAR(10),
    fobjisn                          NUMERIC,
    docitemisn                       NUMERIC,
    dateval                          TIMESTAMP,
    classisn                         NUMERIC,
    branchisn                        NUMERIC,
    molisn                           NUMERIC,
    nomisn                           NUMERIC,
    amount                           NUMERIC,
    quantity                         NUMERIC,
    stbuhisn                         NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE aisfin.store_docs IS $COMM$Таблица хранимых остатков (документы)$COMM$;
COMMENT ON COLUMN aisfin.store_docs.isn IS $COMM$PK(seq_allfin.nextval)$COMM$;
COMMENT ON COLUMN aisfin.store_docs.code IS $COMM$Субсчет$COMM$;
COMMENT ON COLUMN aisfin.store_docs.fobjisn IS $COMM$(FK Fdocitems/Fbuhobject) ОС/партия$COMM$;
COMMENT ON COLUMN aisfin.store_docs.docitemisn IS $COMM$(FK Fdocitems) строка документа создавшая партию$COMM$;
COMMENT ON COLUMN aisfin.store_docs.dateval IS $COMM$дата остатков$COMM$;
COMMENT ON COLUMN aisfin.store_docs.classisn IS $COMM$FK dicti) класс УО$COMM$;
COMMENT ON COLUMN aisfin.store_docs.branchisn IS $COMM$(FK subdept) Подразделение (ГО =0)$COMM$;
COMMENT ON COLUMN aisfin.store_docs.molisn IS $COMM$(FK fbuhobject) МОЛ$COMM$;
COMMENT ON COLUMN aisfin.store_docs.nomisn IS $COMM$(FK dicti) номенклатура$COMM$;
COMMENT ON COLUMN aisfin.store_docs.amount IS $COMM$исходящее сальдо (сумма)$COMM$;
COMMENT ON COLUMN aisfin.store_docs.quantity IS $COMM$исходящее сальдо (кол-во)$COMM$;
COMMENT ON COLUMN aisfin.store_docs.stbuhisn IS $COMM$(FK store_buhs) - связь с проводками $COMM$;


