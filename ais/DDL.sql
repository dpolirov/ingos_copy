CREATE TABLE ais.addrbase (
    isn                              NUMERIC,
    regionisn                        NUMERIC,
    countryisn                       NUMERIC,
    cityisn                          NUMERIC,
    codeiso                          VARCHAR(3),
    areaisn                          NUMERIC,
    streetisn                        NUMERIC,
    code                             VARCHAR(17),
    codeext                          VARCHAR(24),
    postcode                         VARCHAR(10),
    address                          VARCHAR(255),
    house                            VARCHAR(20),
    build                            VARCHAR(20),
    block                            VARCHAR(20),
    flat                             VARCHAR(20),
    addresslat                       VARCHAR(255),
    okato                            VARCHAR(11),
    oktmo                            VARCHAR(8),
    active                           VARCHAR(1),
    discr                            VARCHAR(1),
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    srcisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.addrbase IS $COMM$Таблица уникальных. Крылов В.А. 06.03.2013г$COMM$;
COMMENT ON COLUMN ais.addrbase.isn IS $COMM$PK$COMM$;
COMMENT ON COLUMN ais.addrbase.regionisn IS $COMM$ссылка на регион$COMM$;
COMMENT ON COLUMN ais.addrbase.countryisn IS $COMM$страна (FK DICTI)$COMM$;
COMMENT ON COLUMN ais.addrbase.cityisn IS $COMM$ссылка на н/п низкого уровня (FK DICTI)$COMM$;
COMMENT ON COLUMN ais.addrbase.codeiso IS $COMM$ИСО код страны$COMM$;
COMMENT ON COLUMN ais.addrbase.areaisn IS $COMM$ссылка на рaйон (FK DICTI)$COMM$;
COMMENT ON COLUMN ais.addrbase.streetisn IS $COMM$ссылка на улицу (FK STREET)$COMM$;
COMMENT ON COLUMN ais.addrbase.code IS $COMM$поисковый ключ (модиф.код КЛАДР,ФИАС)$COMM$;
COMMENT ON COLUMN ais.addrbase.codeext IS $COMM$расширение Code норм: дом,корп,стр,кв.$COMM$;
COMMENT ON COLUMN ais.addrbase.postcode IS $COMM$почтовый индекс$COMM$;
COMMENT ON COLUMN ais.addrbase.address IS $COMM$строковый адрес первичный или синтезированный$COMM$;
COMMENT ON COLUMN ais.addrbase.addresslat IS $COMM$латиница Address  или своё$COMM$;
COMMENT ON COLUMN ais.addrbase.oktmo IS $COMM$код муниц.или внутригородского округа$COMM$;
COMMENT ON COLUMN ais.addrbase.discr IS $COMM$код источника: S - Subaddr, A - Agraddr и т.п.$COMM$;
COMMENT ON COLUMN ais.addrbase.created IS $COMM$Дата создания$COMM$;
COMMENT ON COLUMN ais.addrbase.createdby IS $COMM$FK(Subject) Создатель$COMM$;
COMMENT ON COLUMN ais.addrbase.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.addrbase.updatedby IS $COMM$FK(Subject) Автор изменения$COMM$;
COMMENT ON COLUMN ais.addrbase.srcisn IS $COMM$Isn (FK) источника данной записи$COMM$;


CREATE TABLE ais.agent_cond (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    classisn                         NUMERIC,
    remark                           VARCHAR(1000),
    comission                        NUMERIC(9,6),
    comissionsum                     NUMERIC(20,2),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    currisn                          NUMERIC,
    comissionmax                     NUMERIC(9,6),
    deptisn                          NUMERIC,
    addisn                           NUMERIC,
    parentisn                        NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    newaddisn                        NUMERIC,
    limitsum                         NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.agent_cond.isn IS $COMM$Машинный номер: SEQ_AGENTCOND$COMM$;
COMMENT ON COLUMN ais.agent_cond.agrisn IS $COMM$Машинный номер объекта. Устанавливается по умолчанию равным SEQ_AGENTAGR.nextval$COMM$;
COMMENT ON COLUMN ais.agent_cond.comission IS $COMM$Комиссия с премии в процентах от премии$COMM$;
COMMENT ON COLUMN ais.agent_cond.comissionsum IS $COMM$Сумма комиссии, если задана, используется для расчета доли комиссии$COMM$;
COMMENT ON COLUMN ais.agent_cond.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agent_cond.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agent_cond.currisn IS $COMM$FK(CURRENCY). Валюта комиссии агента$COMM$;
COMMENT ON COLUMN ais.agent_cond.deptisn IS $COMM$Ссылка на отдел-куратор агента$COMM$;
COMMENT ON COLUMN ais.agent_cond.addisn IS $COMM$Машинный номер аддендума$COMM$;
COMMENT ON COLUMN ais.agent_cond.parentisn IS $COMM$Машинный номер старого условия, на осное которого построено новое$COMM$;
COMMENT ON COLUMN ais.agent_cond.datebeg IS $COMM$Начало действия условия$COMM$;
COMMENT ON COLUMN ais.agent_cond.dateend IS $COMM$Окончание действия условия$COMM$;
COMMENT ON COLUMN ais.agent_cond.newaddisn IS $COMM$Машинный номер нового аддендума, сделавшего запись архивной$COMM$;
COMMENT ON COLUMN ais.agent_cond.limitsum IS $COMM$Лимит на заключение договоров$COMM$;


CREATE TABLE ais.agentagr (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    deptisn                          NUMERIC,
    subjisn                          NUMERIC,
    id                               VARCHAR(20),
    datesign                         TIMESTAMP,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    name                             VARCHAR(255),
    status                           VARCHAR(1) DEFAULT 'Р',
    remark                           VARCHAR(1000),
    created                          TIMESTAMP DEFAULT current_timestamp,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agentagr IS $COMM$Формуляр агентского соглашения. Ожидается около 1300 записей, из них 670-хозяйственных
договоров.$COMM$;
COMMENT ON COLUMN ais.agentagr.isn IS $COMM$Машинный номер объекта. Устанавливается по умолчанию равным SEQ_AGENTAGR.nextval$COMM$;
COMMENT ON COLUMN ais.agentagr.classisn IS $COMM$FK(DICTI). Тип агентского соглашения$COMM$;
COMMENT ON COLUMN ais.agentagr.id IS $COMM$Учетный номер договора$COMM$;
COMMENT ON COLUMN ais.agentagr.datesign IS $COMM$Дата подписания договора$COMM$;
COMMENT ON COLUMN ais.agentagr.datebeg IS $COMM$Дата начала<= Дата конца$COMM$;
COMMENT ON COLUMN ais.agentagr.dateend IS $COMM$Дата окончания  >= Дата начала.$COMM$;
COMMENT ON COLUMN ais.agentagr.name IS $COMM$Название договора$COMM$;
COMMENT ON COLUMN ais.agentagr.status IS $COMM$Статус договора, характеризующий прохождение фаз технологического процесса:
М - макет договора: для создания на его основе договоров определенного вида
С - согласование
В - выпущен
null - истек срок давности
А - аннулирован$COMM$;
COMMENT ON COLUMN ais.agentagr.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.agentagr.created IS $COMM$Дата создания. Регистрируется автоматически$COMM$;
COMMENT ON COLUMN ais.agentagr.updated IS $COMM$Датаизменения$COMM$;
COMMENT ON COLUMN ais.agentagr.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.agraddr (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    placeisn                         NUMERIC,
    perisn                           NUMERIC,
    agrisn                           NUMERIC,
    discr                            VARCHAR(1),
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    countryisn                       NUMERIC,
    cityisn                          NUMERIC,
    city                             VARCHAR(40),
    zip                              VARCHAR(20),
    objisn                           NUMERIC,
    insuredsum                       NUMERIC(20,2),
    limitsum                         NUMERIC(20,2),
    currisn                          NUMERIC,
    streetisn                        NUMERIC,
    house                            VARCHAR(20),
    agrdateend                       TIMESTAMP,
    riskpc                           NUMERIC,
    stroenie                         VARCHAR(40),
    korpus                           VARCHAR(40),
    pml                              NUMERIC,
    currpmlisn                       NUMERIC,
    limitcomb                        NUMERIC,
    currcombisn                      NUMERIC,
    regionisn                        NUMERIC,
    vilageisn                        NUMERIC,
    republic                         NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agraddr IS $COMM$Местонахождение объекта страхования в течение указанного периода. Один объект может иметь
несколько местонахождений.$COMM$;
COMMENT ON COLUMN ais.agraddr.pml IS $COMM$PML$COMM$;
COMMENT ON COLUMN ais.agraddr.currpmlisn IS $COMM$валюта PML$COMM$;
COMMENT ON COLUMN ais.agraddr.limitcomb IS $COMM$Общий комбинированный лимит$COMM$;
COMMENT ON COLUMN ais.agraddr.currcombisn IS $COMM$Валюта общего комбинированного лимита$COMM$;
COMMENT ON COLUMN ais.agraddr.regionisn IS $COMM$Район$COMM$;
COMMENT ON COLUMN ais.agraddr.vilageisn IS $COMM$Поселок$COMM$;
COMMENT ON COLUMN ais.agraddr.republic IS $COMM$Республика /край /область$COMM$;
COMMENT ON COLUMN ais.agraddr.isn IS $COMM$PK. Уникальный машинный номер записи: SEQ_AGRADDR.nextval$COMM$;
COMMENT ON COLUMN ais.agraddr.classisn IS $COMM$FK(DICTI). Ссылка на класс местонахождения (родительский узел)$COMM$;
COMMENT ON COLUMN ais.agraddr.placeisn IS $COMM$FK(DICTI). Ссылка на местонахождение$COMM$;
COMMENT ON COLUMN ais.agraddr.perisn IS $COMM$FK(AGRPEROD). Ссылка на период или договор, если местонахождение всех объектов одно.
См [Дискриманатор]$COMM$;
COMMENT ON COLUMN ais.agraddr.agrisn IS $COMM$FK(AGREEMENT). Ссылка на договор. Поддерживается автоматически по периоду.$COMM$;
COMMENT ON COLUMN ais.agraddr.discr IS $COMM$Дискриминатор элемента договора, к которому относится территория  страхования$COMM$;
COMMENT ON COLUMN ais.agraddr.remark IS $COMM$Полный почтовый адрес территории страхования$COMM$;
COMMENT ON COLUMN ais.agraddr.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agraddr.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agraddr.countryisn IS $COMM$FK(COUNTRY). Указатель страны$COMM$;
COMMENT ON COLUMN ais.agraddr.cityisn IS $COMM$FK(CITY). Указатель населенного пункта в пределах страны$COMM$;
COMMENT ON COLUMN ais.agraddr.city IS $COMM$Наименование населенного пункта$COMM$;
COMMENT ON COLUMN ais.agraddr.zip IS $COMM$ZIP-код или почтовый индекс$COMM$;
COMMENT ON COLUMN ais.agraddr.objisn IS $COMM$FK(AGROBJECT). Указатель объекта страхования, наследуется из периода$COMM$;
COMMENT ON COLUMN ais.agraddr.insuredsum IS $COMM$Страховая сумма объектов, находящихся на данной территории$COMM$;
COMMENT ON COLUMN ais.agraddr.limitsum IS $COMM$Суммарный лимит ответственности объектов, находящихся на данной территории$COMM$;
COMMENT ON COLUMN ais.agraddr.currisn IS $COMM$FK(CURRENCY).Указатель валюты сумм территории$COMM$;


CREATE TABLE ais.agrcalcelement (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    condisn                          NUMERIC,
    classisn                         NUMERIC,
    etype                            VARCHAR(1),
    datatype                         VARCHAR(1),
    evals                            VARCHAR(100),
    evaln                            NUMERIC(20,6),
    evald                            TIMESTAMP,
    labelisn                         NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrcalcelement IS $COMM$@purpose=Таблица для хранения параметров СРТ расчета @created=06.06.2008 @createdby=Бородин А.Ю. @seq=AIS.SEQ_AGRCALCELEMENT @noaiuds$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.isn IS $COMM$@purpose=Машинный номер @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.agrisn IS $COMM$@fk=AGREEMENT(restrict) @purpose=Указатель договора @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.condisn IS $COMM$@fk=AGRCOND(restrict) @purpose=Ссылка на условие договора @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.classisn IS $COMM$@fk=DICTI(restrict) @purpose=Ссылка на элемент расчета @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.etype IS $COMM$@purpose=Тип элемента (P-параметр, C- коэффициент, R-фактор риска) @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.datatype IS $COMM$@purpose=Физический тип данных, определяет имя поля для хранения значения @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.evals IS $COMM$@purpose=Значение элемента в строком представлении (заполняется всегда) @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.evaln IS $COMM$@purpose=Значение элемента если тип – число @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.evald IS $COMM$@purpose=Значение элемента если тип – дата @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.labelisn IS $COMM$@fk=DICTI(restrict) @purpose=Метка узла тарифного дерева из СРТ, для коэффициента расчета @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.updated IS $COMM$@purpose=Дата изменения @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;
COMMENT ON COLUMN ais.agrcalcelement.updatedby IS $COMM$@purpose=Автор изменения @createdby=Бородин А.Ю. @created=06.06.2008$COMM$;


CREATE TABLE ais.agrclaim (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    deptisn                          NUMERIC,
    emplisn                          NUMERIC,
    subjisn                          NUMERIC,
    currisn                          NUMERIC,
    id                               VARCHAR(20),
    dateclaim                        TIMESTAMP,
    datereg                          TIMESTAMP,
    dateloss                         TIMESTAMP,
    datechk                          TIMESTAMP,
    datesolution                     TIMESTAMP,
    description                      VARCHAR(255),
    losssum                          NUMERIC(20,2),
    claimsum                         NUMERIC(20,2),
    refundsum                        NUMERIC(20,2),
    rejectsum                        NUMERIC(20,2),
    status                           VARCHAR(1),
    reinsstatus                      VARCHAR(1),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    subacc                           NUMERIC(38),
    no                               NUMERIC(38),
    extid                            VARCHAR(40),
    placeisn                         NUMERIC,
    agentisn                         NUMERIC,
    refagrid                         VARCHAR(20),
    formisn                          NUMERIC,
    agryear                          SMALLINT,
    agrdatebeg                       TIMESTAMP,
    ruleisn                          NUMERIC,
    reasonisn                        NUMERIC,
    siteisn                          NUMERIC,
    createdby                        NUMERIC,
    created                          TIMESTAMP,
    applisn                          NUMERIC,
    classisn                         NUMERIC,
    mediaisn                         NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrclaim IS $COMM$Паспорт убытка содержит интегральную информацию по убытку из заявления страхователя на
возмещение, акта осмотра и других источников.
Убыток относится к одному полису и подразделению.
В процессе согласования убытка страховщик принимает решение о выплате$COMM$;
COMMENT ON COLUMN ais.agrclaim.mediaisn IS $COMM$FK(DICTI) Для грузов - Вид перевозки (c.get('MSTRANSPMEDIA')), для других - отдельная трактовка (Smirnov 16/10/12)$COMM$;
COMMENT ON COLUMN ais.agrclaim.classisn IS $COMM$FK(DICTI). Ссылка на справочник типов убытков (Rusov)$COMM$;
COMMENT ON COLUMN ais.agrclaim.applisn IS $COMM$FK(DICTI). Приложение (отличное от АИС) - источник убытка (Smirnov)$COMM$;
COMMENT ON COLUMN ais.agrclaim.subjisn IS $COMM$FK(SUBJECT). Клиент-заявитель (страхователь).$COMM$;
COMMENT ON COLUMN ais.agrclaim.currisn IS $COMM$FK(CURRENCY). Валюта убытка. Предполагается, что все суммы заданы в одной валюте.$COMM$;
COMMENT ON COLUMN ais.agrclaim.id IS $COMM$Регистрационный номер заявления$COMM$;
COMMENT ON COLUMN ais.agrclaim.dateclaim IS $COMM$Дата заявления об убытке$COMM$;
COMMENT ON COLUMN ais.agrclaim.datereg IS $COMM$Дата регистрации заявления об убытке.$COMM$;
COMMENT ON COLUMN ais.agrclaim.dateloss IS $COMM$Дата убытка (возникновения страхового случая)$COMM$;
COMMENT ON COLUMN ais.agrclaim.datechk IS $COMM$Дата осмотра объекта страхования$COMM$;
COMMENT ON COLUMN ais.agrclaim.datesolution IS $COMM$Дата урегулирования - принятия решения по убытку. Определяет, в какой период и
какую категорию резерва (РНУ, РПНУ) попадет убыток.$COMM$;
COMMENT ON COLUMN ais.agrclaim.description IS $COMM$Описание убытка$COMM$;
COMMENT ON COLUMN ais.agrclaim.losssum IS $COMM$Общая сумма убытка по оценке страхователя, может быть больше суммы, заявленной на
возмещение.$COMM$;
COMMENT ON COLUMN ais.agrclaim.claimsum IS $COMM$Сумма, заявленная страхователем на возмещение.$COMM$;
COMMENT ON COLUMN ais.agrclaim.refundsum IS $COMM$Сумма, которую собирается возмещать страховщик.$COMM$;
COMMENT ON COLUMN ais.agrclaim.rejectsum IS $COMM$Сумма, которую не будет возмещать страховщик. В процессе урегулирования разницей
между заявленной суммой и суммами возмещения и отказа является неурегулированной. После урегулирования убытка сумма возмещения и
отказа дают заявленную сумму.$COMM$;
COMMENT ON COLUMN ais.agrclaim.status IS $COMM$Статус убытка: D-неоформлен, N-в рассмотрении, Y-урегулирован, R-отклонен,S-отказ заявителя,null-аннулирован.$COMM$;
COMMENT ON COLUMN ais.agrclaim.reinsstatus IS $COMM$Статус перестраховаия: W-сообщено в упр.перестрахования, P-учтен,
Y-перестрахован, null$COMM$;
COMMENT ON COLUMN ais.agrclaim.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.agrclaim.updated IS $COMM$Автор изменения. Поддерживается автоматически.$COMM$;
COMMENT ON COLUMN ais.agrclaim.updatedby IS $COMM$Дата изменения. Поддерживается автоматически.$COMM$;
COMMENT ON COLUMN ais.agrclaim.subacc IS $COMM$Субсчет плана счетов, соответсвующий виду страхования$COMM$;
COMMENT ON COLUMN ais.agrclaim.no IS $COMM$Порядковый номер документа в рамках фирмы, подразделения, сотрудника$COMM$;
COMMENT ON COLUMN ais.agrclaim.extid IS $COMM$Внешний референс$COMM$;
COMMENT ON COLUMN ais.agrclaim.placeisn IS $COMM$FK(DICTI). Указатель места возниконовения убытка. Ограничивается территорией
страхования.$COMM$;
COMMENT ON COLUMN ais.agrclaim.agentisn IS $COMM$FK(SUBJECT). Указатель агента убытка$COMM$;
COMMENT ON COLUMN ais.agrclaim.formisn IS $COMM$Форма просмотра$COMM$;
COMMENT ON COLUMN ais.agrclaim.agrdatebeg IS $COMM$Дата начала договора$COMM$;
COMMENT ON COLUMN ais.agrclaim.ruleisn IS $COMM$FK(DICTI) Вид страхования для договора перестрахования из DICTI: Договоры страхования/Классификация$COMM$;
COMMENT ON COLUMN ais.agrclaim.reasonisn IS $COMM$FK(DICTI) Причина убытка$COMM$;
COMMENT ON COLUMN ais.agrclaim.siteisn IS $COMM$FK(DICTI) Тип места происшествия$COMM$;


CREATE TABLE ais.agrclause (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    agrisn                           NUMERIC,
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    orderno                          NUMERIC(38)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrclause IS $COMM$Ограничительные условия договора: территория, использование и др.$COMM$;
COMMENT ON COLUMN ais.agrclause.isn IS $COMM$Машинный номер записи: SEQ_AGRCLAUSE.nextval$COMM$;
COMMENT ON COLUMN ais.agrclause.classisn IS $COMM$FK(DICTI). Класс оговорки$COMM$;
COMMENT ON COLUMN ais.agrclause.agrisn IS $COMM$FK(AGREEMENT). Указатель договора$COMM$;
COMMENT ON COLUMN ais.agrclause.remark IS $COMM$Текст ограничения$COMM$;
COMMENT ON COLUMN ais.agrclause.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrclause.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agrclause.orderno IS $COMM$Порядковый номер оговорки для вывода на печать$COMM$;


CREATE TABLE ais.agrcond (
    isn                              NUMERIC,
    perisn                           NUMERIC,
    objisn                           NUMERIC,
    agrisn                           NUMERIC,
    addisn                           NUMERIC,
    riskisn                          NUMERIC,
    rptclassisn                      NUMERIC,
    currisn                          NUMERIC,
    limclassisn                      NUMERIC,
    limitsum                         NUMERIC(20,2),
    yeartariff                       NUMERIC(12,9),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    tariff                           NUMERIC(12,9),
    discount                         NUMERIC,
    premiumsum                       NUMERIC(20,2),
    limiteverysum                    NUMERIC(20,2),
    franchtype                       VARCHAR(1),
    franchtariff                     NUMERIC(7,4),
    franchsum                        NUMERIC(20,2),
    franchmaxsum                     NUMERIC(20,2),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    premcurrisn                      NUMERIC,
    roundm                           NUMERIC(38),
    limitisn                         NUMERIC,
    franchcurrisn                    NUMERIC,
    incomesum                        NUMERIC(20,2),
    newaddisn                        NUMERIC,
    olddateend                       TIMESTAMP,
    oldtariff                        NUMERIC(12,9),
    oldpremiumsum                    NUMERIC(20,2),
    duration                         NUMERIC,
    cost                             NUMERIC(20,2),
    costshare                        NUMERIC(9,6),
    quantity                         NUMERIC(38),
    parentisn                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    discount2                        NUMERIC,
    retentionperc                    NUMERIC(12,9),
    origlimitsum                     NUMERIC(20,2),
    origpremiumsum                   NUMERIC(20,2),
    reprioritysum                    NUMERIC(20,2),
    pml                              NUMERIC(20,2),
    fdocisn                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrcond IS $COMM$Определяет условия страхования объекта по данному типу риска в течение некоторого периода:
лимит ответственности, франшизу, суммы, тариф и премию.
При изменении условий страхования по данному объекту в старой записи отмечается дата окончания действия условий и формируется
новая запись  с новым сроком.
При прекращении страхования объекта для соответсвующего периода не должно быть условий.$COMM$;
COMMENT ON COLUMN ais.agrcond.reprioritysum IS $COMM$Приоритет в перестраховании [ДИТ-09-1-077816] (Smirnov 23/06/09)$COMM$;
COMMENT ON COLUMN ais.agrcond.pml IS $COMM$Максимально возможный убыток [ДИТ-09-1-077816] (Smirnov 23/06/09)$COMM$;
COMMENT ON COLUMN ais.agrcond.fdocisn IS $COMM$FK (AISFIN.FDOCHEAD)$COMM$;
COMMENT ON COLUMN ais.agrcond.discount2 IS $COMM$Согласованная скидка. Yunin V.A. 22/12/04$COMM$;
COMMENT ON COLUMN ais.agrcond.isn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrcond.perisn IS $COMM$FK(AGRPERIOD). Ссылка на период условия.$COMM$;
COMMENT ON COLUMN ais.agrcond.objisn IS $COMM$FK(AgrObject). Указатель объекта данного условия, принудительно наследуется из
периода.$COMM$;
COMMENT ON COLUMN ais.agrcond.agrisn IS $COMM$FK(AGREEMENT). Указатель договора, принудительно наследуется из периода.$COMM$;
COMMENT ON COLUMN ais.agrcond.addisn IS $COMM$FK(AGREEMENT). Указатель аддендума, принудительно наследуется из периода.$COMM$;
COMMENT ON COLUMN ais.agrcond.riskisn IS $COMM$FK(AGRRISK). Ссылка на риск условия.$COMM$;
COMMENT ON COLUMN ais.agrcond.rptclassisn IS $COMM$FK(DICTI). Указатель класса вида страхования для отчетности. Поддерживается
автоматически как функция от классов риска и объекта
Обязателен !$COMM$;
COMMENT ON COLUMN ais.agrcond.currisn IS $COMM$FK(CURRENCY). Указатель валюты условия. Если не задана, наследуется из периода.$COMM$;
COMMENT ON COLUMN ais.agrcond.limclassisn IS $COMM$FK(DICTI). Указатель типа суммы договора для LimitSum$COMM$;
COMMENT ON COLUMN ais.agrcond.limitsum IS $COMM$Лимит ответственности или страховая сумма одного объекта в валюте условия. При
задании стоимости и доли считается автоматически.$COMM$;
COMMENT ON COLUMN ais.agrcond.yeartariff IS $COMM$Годовой тариф премии в %$COMM$;
COMMENT ON COLUMN ais.agrcond.datebeg IS $COMM$Дата начала, наследуется из периода$COMM$;
COMMENT ON COLUMN ais.agrcond.dateend IS $COMM$Дата окончания, наследуется из периода$COMM$;
COMMENT ON COLUMN ais.agrcond.tariff IS $COMM$Тариф премии в %$COMM$;
COMMENT ON COLUMN ais.agrcond.discount IS $COMM$Скидка/надбавка (-/+): в %$COMM$;
COMMENT ON COLUMN ais.agrcond.premiumsum IS $COMM$Сумма премии в валюте премии. Поддерживается по AGRSUM и договору автоматически.
PremiumSum = LimitSum * Tariff * (1+Discount/100)$COMM$;
COMMENT ON COLUMN ais.agrcond.limiteverysum IS $COMM$Сумма лимита ответственности по каждому страховому случаю в валюте условия. Не
поддерживается договору.$COMM$;
COMMENT ON COLUMN ais.agrcond.franchtype IS $COMM$Тип франшизы: У-условная, Б-безусловная$COMM$;
COMMENT ON COLUMN ais.agrcond.franchtariff IS $COMM$% франшизы: сумма франшизы = страховая сумма * % франшизы / 100$COMM$;
COMMENT ON COLUMN ais.agrcond.franchsum IS $COMM$Сумма франшизы для авсолютной франшизы или минимальная сумма франшизы в валюте
условия$COMM$;
COMMENT ON COLUMN ais.agrcond.franchmaxsum IS $COMM$Максимальная сумма франшизы в валюте условия$COMM$;
COMMENT ON COLUMN ais.agrcond.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.agrcond.updated IS $COMM$Дата изменения, устанавливается автоматически$COMM$;
COMMENT ON COLUMN ais.agrcond.updatedby IS $COMM$Автор изменения, устанавливается автоматически$COMM$;
COMMENT ON COLUMN ais.agrcond.premcurrisn IS $COMM$FK(CURRENCY). Указатель валюты премии. Если не задана, наследуется из валюты
периода.$COMM$;
COMMENT ON COLUMN ais.agrcond.roundm IS $COMM$Масштаб округления, наследуется из договора, соответствует параметру m ф-ии
ROUND(n,m)$COMM$;
COMMENT ON COLUMN ais.agrcond.limitisn IS $COMM$FK(AGRLIMIT). Указатель ограничения для условия договора$COMM$;
COMMENT ON COLUMN ais.agrcond.franchcurrisn IS $COMM$FK(CURRENCY). Указатель валюты франшизы. Если не задана, наследуется из валюты
периода.
Обязателен !$COMM$;
COMMENT ON COLUMN ais.agrcond.incomesum IS $COMM$Фактически полученная сумма премии в валюте премии. Поддерживается по DOCSUM
автоматически.$COMM$;
COMMENT ON COLUMN ais.agrcond.newaddisn IS $COMM$FK(AGREEMENT). Указатель аддендума, изменившего условия.$COMM$;
COMMENT ON COLUMN ais.agrcond.olddateend IS $COMM$Старая дата окончания, до регистрации аддендума. Исходно устанавливается равной
DateEnd$COMM$;
COMMENT ON COLUMN ais.agrcond.oldtariff IS $COMM$Старый тариф, до регистрации аддендума. Исходно устанавливается равным Tariff$COMM$;
COMMENT ON COLUMN ais.agrcond.oldpremiumsum IS $COMM$Старая премия, до регистрации аддендума. Исходно устанавливается равной
PremiumSum$COMM$;
COMMENT ON COLUMN ais.agrcond.duration IS $COMM$Продолжительность действия страхового покрытия в днях. По умолчанию = DateEnd -
DateBeg + 1, но может быть короче. Используется при расчете по тарифному справочнику.$COMM$;
COMMENT ON COLUMN ais.agrcond.cost IS $COMM$Стоимость объекта. Может отличаться от страховой суммы, если доля участия страховщика
отлична от 100.$COMM$;
COMMENT ON COLUMN ais.agrcond.costshare IS $COMM$Доля участия страховщика = Страховая сумма / Стоимость. NULL трактуется как 100
процентов.$COMM$;
COMMENT ON COLUMN ais.agrcond.quantity IS $COMM$Количество одинаковых объектов, перевозок$COMM$;
COMMENT ON COLUMN ais.agrcond.parentisn IS $COMM$FK(AGRCOND) Указатель предыдущего условия при изменении условий договора$COMM$;
COMMENT ON COLUMN ais.agrcond.created IS $COMM$Дата создания условия$COMM$;
COMMENT ON COLUMN ais.agrcond.createdby IS $COMM$Создатель условия$COMM$;


CREATE TABLE ais.agreement (
    isn                              NUMERIC,
    id                               VARCHAR(20),
    parentisn                        NUMERIC,
    datesign                         TIMESTAMP,
    previsn                          NUMERIC,
    dateissue                        TIMESTAMP,
    classisn                         NUMERIC,
    datebeg                          TIMESTAMP,
    ruleisn                          NUMERIC,
    dateend                          TIMESTAMP,
    deptisn                          NUMERIC,
    emplisn                          NUMERIC,
    clientisn                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    currisn                          NUMERIC,
    name                             VARCHAR(255),
    policyyear                       TIMESTAMP,
    reinsyear                        TIMESTAMP,
    insuredsum                       NUMERIC(20,2) DEFAULT 0,
    limitsum                         NUMERIC(20,2),
    refundsum                        NUMERIC(20,2) DEFAULT 0,
    returnsum                        NUMERIC(20,2) DEFAULT 0,
    premiumsum                       NUMERIC(20,2) DEFAULT 0,
    incomesum                        NUMERIC(20,2) DEFAULT 0,
    incomissionsum                   NUMERIC(20,2),
    basepremiumsum                   NUMERIC(20,2),
    nettopremiumsum                  NUMERIC(20,2),
    quantity                         NUMERIC,
    status                           VARCHAR(1),
    reinsstatus                      VARCHAR(1),
    discr                            VARCHAR(1),
    remark                           VARCHAR(1000),
    comission                        NUMERIC(9,6),
    applisn                          NUMERIC,
    applno                           NUMERIC,
    appldate                         TIMESTAMP,
    sharepc                          NUMERIC(9,6) DEFAULT 100,
    created                          TIMESTAMP DEFAULT current_timestamp,
    roundm                           NUMERIC,
    applid                           VARCHAR(20),
    datebase                         VARCHAR(1),
    groupisn                         NUMERIC,
    reinspc                          NUMERIC(9,6) DEFAULT 0,
    sectisn                          NUMERIC,
    formisn                          NUMERIC,
    base                             NUMERIC(17,12) DEFAULT 1.0,
    sharesum                         NUMERIC(20,2),
    formula                          VARCHAR(20) DEFAULT 's0',
    outcomission                     NUMERIC(15,12) DEFAULT 0,
    discount                         NUMERIC(9,6),
    duration                         NUMERIC,
    addrisn                          NUMERIC,
    olddateend                       TIMESTAMP,
    oldduration                      NUMERIC,
    prolongation                     VARCHAR(1),
    firmisn                          NUMERIC,
    createdby                        NUMERIC,
    no                               NUMERIC,
    limiteverysum                    NUMERIC(20,2),
    fid                              NUMERIC,
    datecalc                         TIMESTAMP,
    bizflg                           VARCHAR(1),
    liabeverysum                     NUMERIC,
    ownerdeptisn                     NUMERIC,
    presaleisn                       NUMERIC,
    saleschannelisn                  NUMERIC,
    pmlsum                           NUMERIC,
    pmlcurrisn                       NUMERIC,
    combsum                          NUMERIC,
    combcurrisn                      NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agreement IS $COMM$Формуляр договора страхования (полиса). Характеризует договор в целом.
В зависимости от стадии технологического процесса договор представляет следующие объекты: макет, запрос, заявление на страхование, полис и т.д.
Прохождение договором перечисленных фаз отмечается в статусе договора.
Договору соответствует правило страхования, перечень объектов и рисков (покрытие).
Если есть несколько покрытий с разными правилами, то каждое отражается в отдельном разделе договора.
Таким образом, договор может иметь один и более разделов.
Изменения и добавления новых условий оформляются в виде аддендумов, во многом повторяющих договор по составу реквизитов.
Договор может заключаться в соответствии с генеральными соглашением и иметь предыдущий договор при пролонгации.
Тип записи (генеральный договор, договор страхования, аддендум, сегмент) определяется дискриминатором [Discr].$COMM$;
COMMENT ON COLUMN ais.agreement.pmlsum IS $COMM$PML сумма$COMM$;
COMMENT ON COLUMN ais.agreement.pmlcurrisn IS $COMM$pml валюта$COMM$;
COMMENT ON COLUMN ais.agreement.combsum IS $COMM$Общий комбинированный лимит$COMM$;
COMMENT ON COLUMN ais.agreement.combcurrisn IS $COMM$Валюта общего комбинированного лимита$COMM$;
COMMENT ON COLUMN ais.agreement.saleschannelisn IS $COMM$FK(DICTI) Ссылка на канал продажи 08.06.09 Лавров С.В.$COMM$;
COMMENT ON COLUMN ais.agreement.presaleisn IS $COMM$FK(PRESALE)ссылка на предпродажу 15.01.09 Угринович А.Н.$COMM$;
COMMENT ON COLUMN ais.agreement.nettopremiumsum IS $COMM$Нетто-премия: базовая премия за вычетом исходящей комиссии (перестраховочной). Считается автоматически.$COMM$;
COMMENT ON COLUMN ais.agreement.quantity IS $COMM$Количество застрахованных объектов договора, аддендума для статотчетности.$COMM$;
COMMENT ON COLUMN ais.agreement.status IS $COMM$Статус договора, характеризующий прохождение фаз технологического процесса:
М - макет договора: для создания на его основе договоров определенного вида
Р - зарезервирован для агента, представительства
З - запрос или заявка (устно выраженное намерение, не имеющее юридической силы)
С - заявление на страхование (имеющее юридическую силу)
В - выпущен
Д - досрочно прекращен страхователем
Щ - досрочно прекращен страховщиком
null - истек срок давности по рекламациям (чтобы индекс сократить)
А - аннулирован не входя в силу, заявки:  О - оформление, Б - рассмотрение, П - проект, У - подтверждение$COMM$;
COMMENT ON COLUMN ais.agreement.reinsstatus IS $COMM$Статус перестраховаия: W-сообщено в упр.перестрахования, P-учтен, Y-перестрахован, null$COMM$;
COMMENT ON COLUMN ais.agreement.discr IS $COMM$Дискриминатор записи: Д-договор, Г-генеральное соглашение, А-аддендум$COMM$;
COMMENT ON COLUMN ais.agreement.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.agreement.comission IS $COMM$Процент комиссии посредников, поддерживается по AGRROLE автоматически$COMM$;
COMMENT ON COLUMN ais.agreement.applisn IS $COMM$FK(DICTI). Приложение (отличное от АИС) - источник договора$COMM$;
COMMENT ON COLUMN ais.agreement.applno IS $COMM$Перестраховочная дробь$COMM$;
COMMENT ON COLUMN ais.agreement.appldate IS $COMM$Дата заявления на страхование$COMM$;
COMMENT ON COLUMN ais.agreement.sharepc IS $COMM$Доля ИГС от сострахования в процентах$COMM$;
COMMENT ON COLUMN ais.agreement.created IS $COMM$Дата создания. Регистрируется автоматически$COMM$;
COMMENT ON COLUMN ais.agreement.roundm IS $COMM$Масштаб округления, аргумент m ф-ции ROUND(n,m), задающий количество цифр правее запятой: 0 - до целых, -3 - до тысяч$COMM$;
COMMENT ON COLUMN ais.agreement.applid IS $COMM$Номер заявления на страхование$COMM$;
COMMENT ON COLUMN ais.agreement.datebase IS $COMM$Временная база перестрахования для перестрах.договора: I-страховой год, C-календарная$COMM$;
COMMENT ON COLUMN ais.agreement.groupisn IS $COMM$FK(DICTI). Указатель учетной группы договора для РНП (1,1К,2,3). По умолчанию - 1-я$COMM$;
COMMENT ON COLUMN ais.agreement.reinspc IS $COMM$Доля перестраховщиков по QS факультативам в процентах: sum(AGRROLE.SharePC). Поддерживается автоматически.$COMM$;
COMMENT ON COLUMN ais.agreement.sectisn IS $COMM$FK(RESECTION). Указатель секции перестрахования входящего договора перестрахования для регистрации слипов прямых договоров$COMM$;
COMMENT ON COLUMN ais.agreement.formisn IS $COMM$FK(DICTI). Тип экранной формы для работы с договором$COMM$;
COMMENT ON COLUMN ais.agreement.base IS $COMM$Базовый коэффициент ИГС (Умноженный на брутто-премию даёт долю ИГС)$COMM$;
COMMENT ON COLUMN ais.agreement.sharesum IS $COMM$Доля ИГС (считается начислениями по договору)$COMM$;
COMMENT ON COLUMN ais.agreement.formula IS $COMM$Строка вида S0 - S1. Даёт сумму, с которой считается доля ИГС$COMM$;
COMMENT ON COLUMN ais.agreement.outcomission IS $COMM$Суммарный базовый коэффициент комиссии по исходящим делам$COMM$;
COMMENT ON COLUMN ais.agreement.discount IS $COMM$Скидка/надбавка (-/+): в процентах$COMM$;
COMMENT ON COLUMN ais.agreement.duration IS $COMM$Продолжительность действия страхового покрытия в днях. По умолчанию = DateEnd - DateBeg + 1, но может быть короче. Используется при расчете по тарифному справочнику.$COMM$;
COMMENT ON COLUMN ais.agreement.addrisn IS $COMM$FK(SUBADDR) - указатель места выдачи полиса, FK(DICTI) - населенный пункт КЦ для гарантийного письма ДМС$COMM$;
COMMENT ON COLUMN ais.agreement.olddateend IS $COMM$Заполняется датой конца срока действия при прекращении договора$COMM$;
COMMENT ON COLUMN ais.agreement.oldduration IS $COMM$Заполняется значением срока действия при прекращении договора$COMM$;
COMMENT ON COLUMN ais.agreement.prolongation IS $COMM$Флаг, определяющий деиствия по окончании договора: Y-пролонгация, N-прекращение, null-никаких действий$COMM$;
COMMENT ON COLUMN ais.agreement.firmisn IS $COMM$FK(SUBJECT). Указатель эмитента полиса, заполняется для чужих полисов (ЗК, полисы по вх.облигатору). null-ИГС$COMM$;
COMMENT ON COLUMN ais.agreement.createdby IS $COMM$Создатель$COMM$;
COMMENT ON COLUMN ais.agreement.no IS $COMM$Порядковый номер договора$COMM$;
COMMENT ON COLUMN ais.agreement.limiteverysum IS $COMM$Сумма лимита ответственности по каждому страховому случаю в валюте договора$COMM$;
COMMENT ON COLUMN ais.agreement.fid IS $COMM$Внешний машинный номер$COMM$;
COMMENT ON COLUMN ais.agreement.datecalc IS $COMM$Дата расчета премии (Литвин, 6/01/04)$COMM$;
COMMENT ON COLUMN ais.agreement.bizflg IS $COMM$Принадлежность бизнеса (филиал, центр.офис)$COMM$;
COMMENT ON COLUMN ais.agreement.ownerdeptisn IS $COMM$Ссылка на подразделения владельц$COMM$;
COMMENT ON COLUMN ais.agreement.isn IS $COMM$Машинный номер объекта. Устанавливается по умолчанию равным SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agreement.id IS $COMM$Учетный номер запроса, заявки, проекта договора, уникаленые для подразделения.
Номер полиса, договора , уникальный для страховщика.
Номер аддендума, уникальный в рамках договора.$COMM$;
COMMENT ON COLUMN ais.agreement.parentisn IS $COMM$FK(AGREEMENT). Для договора - генеральный договор (полис), на основании которого заключен данный договор.
Для аддендума и раздела - ссылка на соответствующий договор (см.Discr).$COMM$;
COMMENT ON COLUMN ais.agreement.datesign IS $COMM$Дата подписания договора (Status -> 'ДП').$COMM$;
COMMENT ON COLUMN ais.agreement.previsn IS $COMM$FK(AGREEMENT). Ссылка на предыдущий договор при пролонгации, предыдущий аддендум$COMM$;
COMMENT ON COLUMN ais.agreement.dateissue IS $COMM$Дата выдачи полиса.$COMM$;
COMMENT ON COLUMN ais.agreement.classisn IS $COMM$FK(DICTI,ДОГКЛАСС). Указатель класса договора: прямой, сострахование, перестрахование...$COMM$;
COMMENT ON COLUMN ais.agreement.datebeg IS $COMM$Дата начала периода страхования, <= Дата конца$COMM$;
COMMENT ON COLUMN ais.agreement.ruleisn IS $COMM$FK(DICTI). Вид страхования для договора перестрахования из DICTI: Договоры страхования/Классификация$COMM$;
COMMENT ON COLUMN ais.agreement.dateend IS $COMM$Дата окончания периода страхования >= Дата начала.$COMM$;
COMMENT ON COLUMN ais.agreement.deptisn IS $COMM$FK(DICTI,ОТДЕЛ). Подразделение - владелец договора или раздела.$COMM$;
COMMENT ON COLUMN ais.agreement.emplisn IS $COMM$FK(DICTI,ФИЗЛИЦО). Сотрудник, ведущий договор или раздел. Должен работать в подразделении - владельце.$COMM$;
COMMENT ON COLUMN ais.agreement.clientisn IS $COMM$FK(SUBJECT). Указатель основного страхователя. Поле избыточно по отношению к AGRROLE, введено для удобства просмотра формуляра.$COMM$;
COMMENT ON COLUMN ais.agreement.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agreement.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agreement.currisn IS $COMM$FK(DICTI,ВАЛЮТА). Валюта договора, раздела, на основании которой задается валюта всех сумм по умолчанию и в которую пересчитываются все суммы договора.$COMM$;
COMMENT ON COLUMN ais.agreement.name IS $COMM$Полное название договора, аддендума: "ПОЛИС по страхованию имущества (игровых автоматов)".$COMM$;
COMMENT ON COLUMN ais.agreement.policyyear IS $COMM$Полисный год для статистики, обычно соответствует дате начала договора$COMM$;
COMMENT ON COLUMN ais.agreement.reinsyear IS $COMM$Перестраховочный год для статистики, обычно соответствует дате начала договора$COMM$;
COMMENT ON COLUMN ais.agreement.insuredsum IS $COMM$Общая страховая сумма по договору, аддендуму. Считается автоматически.$COMM$;
COMMENT ON COLUMN ais.agreement.limitsum IS $COMM$Общий лимит ответственности по договору, аддендуму. Считается автоматически.$COMM$;
COMMENT ON COLUMN ais.agreement.refundsum IS $COMM$Общая сумма выплаченного возмещения по договору. Считается автоматически.$COMM$;
COMMENT ON COLUMN ais.agreement.returnsum IS $COMM$Общая сумма возврата премии по договору при досрочном прекращении или изменении условий. Считается автоматически.$COMM$;
COMMENT ON COLUMN ais.agreement.premiumsum IS $COMM$Общая начисленная сумма премии по договору, аддендуму. Считается автоматически.$COMM$;
COMMENT ON COLUMN ais.agreement.incomesum IS $COMM$Общая полученная сумма премии по договору. Считается автоматически.$COMM$;
COMMENT ON COLUMN ais.agreement.incomissionsum IS $COMM$Сумма входящей комиссии (посредников). Считается автоматически.$COMM$;
COMMENT ON COLUMN ais.agreement.basepremiumsum IS $COMM$Базовая премия: начисленная премия за вычетом входящей комиссии (посредников). Считается автоматически.$COMM$;


CREATE TABLE ais.agrext (
    isn                              NUMERIC,
    currisn                          NUMERIC,
    classisn                         NUMERIC,
    agrisn                           NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    x1                               NUMERIC,
    x2                               NUMERIC,
    x3                               NUMERIC,
    x4                               NUMERIC,
    x5                               NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    subjisn                          NUMERIC,
    asubjisn                         NUMERIC,
    remark                           VARCHAR(1000),
    addisn                           NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrext IS $COMM$Всякие тарифные таблицы, например, по агентскому договору$COMM$;
COMMENT ON COLUMN ais.agrext.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.agrext.addisn IS $COMM$FK(AGREEMENT). Указатель аддендума.$COMM$;
COMMENT ON COLUMN ais.agrext.created IS $COMM$Дата создания. Регистрируется автоматически$COMM$;
COMMENT ON COLUMN ais.agrext.createdby IS $COMM$Создатель$COMM$;
COMMENT ON COLUMN ais.agrext.subjisn IS $COMM$FK(SUBJECT)$COMM$;
COMMENT ON COLUMN ais.agrext.asubjisn IS $COMM$FK(SUBJECT)$COMM$;
COMMENT ON COLUMN ais.agrext.isn IS $COMM$Машинный номер записи: SEQ_AGREXT.nextval$COMM$;
COMMENT ON COLUMN ais.agrext.currisn IS $COMM$FK(CURRENCY). Указатель валюты$COMM$;
COMMENT ON COLUMN ais.agrext.classisn IS $COMM$FK(DICTI). Класс таблицы$COMM$;
COMMENT ON COLUMN ais.agrext.agrisn IS $COMM$FK(AGREEMENT). Ссылка на договор$COMM$;
COMMENT ON COLUMN ais.agrext.datebeg IS $COMM$Дата начала действия$COMM$;
COMMENT ON COLUMN ais.agrext.dateend IS $COMM$Дата окончания действия$COMM$;
COMMENT ON COLUMN ais.agrext.x1 IS $COMM$Параметры тарифов. Интерпретация зависит от типа тарифа$COMM$;
COMMENT ON COLUMN ais.agrext.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrext.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.agrlimit (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    no                               NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    remark                           VARCHAR(255),
    updatedby                        NUMERIC,
    classisn                         NUMERIC,
    refagrisn                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrlimit IS $COMM$Ограничительные условия договора: территория, использование и др.$COMM$;
COMMENT ON COLUMN ais.agrlimit.isn IS $COMM$Машинный номер: SEQ_AGRADDRX.nextval$COMM$;
COMMENT ON COLUMN ais.agrlimit.agrisn IS $COMM$FK(AGREEMENT). Указатель договора страхования$COMM$;
COMMENT ON COLUMN ais.agrlimit.no IS $COMM$Номер ограничения$COMM$;
COMMENT ON COLUMN ais.agrlimit.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrlimit.remark IS $COMM$Текст ограничения$COMM$;
COMMENT ON COLUMN ais.agrlimit.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agrlimit.classisn IS $COMM$FK(DICTI). Указатель класса ограничения$COMM$;
COMMENT ON COLUMN ais.agrlimit.refagrisn IS $COMM$FK(AGREEMENT). Указатель договора с поставщиком услуг$COMM$;


CREATE TABLE ais.agrlimitem (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    subclassisn                      NUMERIC,
    limisn                           NUMERIC,
    excl                             VARCHAR(1),
    no                               NUMERIC(38),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrlimitem IS $COMM$Элемент ограничительного условия. Класс элемента задает вид ограничения (корень справочника
DICTI): регион, страна, способ эксплуатации etc. Подкласс - ссылается на элемент справочника: Европа, Германия, чартер.
Ограничения различных видов объединяются по условию "И".$COMM$;
COMMENT ON COLUMN ais.agrlimitem.isn IS $COMM$Машинный номер: SEQ_LIMITITEM.next$COMM$;
COMMENT ON COLUMN ais.agrlimitem.classisn IS $COMM$FK(DICTI). Указатель класса элемента, ограничивающий подкласс$COMM$;
COMMENT ON COLUMN ais.agrlimitem.subclassisn IS $COMM$FK(DICTI). Указатель подкласса элемента в рамках класса$COMM$;
COMMENT ON COLUMN ais.agrlimitem.limisn IS $COMM$FK(AGRLIMIT). Указатель ограничительного условия, к которому относится элемент$COMM$;
COMMENT ON COLUMN ais.agrlimitem.excl IS $COMM$Индикатор исключения: Y-за исключением$COMM$;
COMMENT ON COLUMN ais.agrlimitem.no IS $COMM$Порядковый номер элемента, например, при задании маршрута$COMM$;
COMMENT ON COLUMN ais.agrlimitem.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrlimitem.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.agrlink (
    isn                              NUMERIC,
    agrisn1                          NUMERIC,
    agrisn2                          NUMERIC,
    classisn                         NUMERIC,
    objisn                           NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    chgflg                           VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrlink IS $COMM$Связь двух произвольных договоров$COMM$;
COMMENT ON COLUMN ais.agrlink.chgflg IS $COMM$Режим ввода: Автоматически - A;
Ручной ввод - M;$COMM$;
COMMENT ON COLUMN ais.agrlink.agrisn1 IS $COMM$FK(AGREEMENT) ISN договора$COMM$;
COMMENT ON COLUMN ais.agrlink.agrisn2 IS $COMM$FK(AGREEMENT) ISN договора$COMM$;


CREATE TABLE ais.agrobject (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    classisn                         NUMERIC,
    rptclassisn                      NUMERIC,
    name                             VARCHAR(255),
    agrisn                           NUMERIC,
    currisn                          NUMERIC,
    descisn                          NUMERIC,
    id                               VARCHAR(20),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    remark                           VARCHAR(1000),
    updatedby                        NUMERIC,
    constructed                      TIMESTAMP,
    sex                              VARCHAR(1),
    cityisn                          NUMERIC,
    cost                             NUMERIC(20,2),
    groupisn                         NUMERIC,
    previsn                          NUMERIC,
    addisn                           NUMERIC,
    groupid                          VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrobject IS $COMM$Определяет объекты страхования и подобъекты (1.оборудование: 1.1.станки ...) страхования.$COMM$;
COMMENT ON COLUMN ais.agrobject.isn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrobject.parentisn IS $COMM$FK(AGROBJECT). Ссылка на групповой объект. Объединение объектов в группы
позволяет указывать одинаковые условия однократно.$COMM$;
COMMENT ON COLUMN ais.agrobject.classisn IS $COMM$FK(DICTI,ОБЪЕКТ). Класс объекта, наследуется из физического объекта по DescISN$COMM$;
COMMENT ON COLUMN ais.agrobject.rptclassisn IS $COMM$FK(DICTI). Класс отчетности, устанавливается автоматически по классу объекта$COMM$;
COMMENT ON COLUMN ais.agrobject.name IS $COMM$Полное название объекта как в тексте договора, наследуется из физического объекта по
DescISN$COMM$;
COMMENT ON COLUMN ais.agrobject.agrisn IS $COMM$FK(AGREEMENT). Ссылка на договор$COMM$;
COMMENT ON COLUMN ais.agrobject.currisn IS $COMM$FK(CURRENCY). Указатель валюты объекта. Наследуется из договора по умолчанию или
очередного периода$COMM$;
COMMENT ON COLUMN ais.agrobject.descisn IS $COMM$FK(OBJAGR). Ссылка на физический объект$COMM$;
COMMENT ON COLUMN ais.agrobject.id IS $COMM$Номер пункта в договоре. Если задан, то уникален в рамках договора.$COMM$;
COMMENT ON COLUMN ais.agrobject.updated IS $COMM$Дата создания или последнего изменения объекта.$COMM$;
COMMENT ON COLUMN ais.agrobject.remark IS $COMM$Дополнительная информация, которая появляется в соответсвующем объекту параграфе
договора.$COMM$;
COMMENT ON COLUMN ais.agrobject.updatedby IS $COMM$Автор создания или последнего изменения объекта.$COMM$;
COMMENT ON COLUMN ais.agrobject.constructed IS $COMM$Дата постройки$COMM$;
COMMENT ON COLUMN ais.agrobject.sex IS $COMM$Пол объекта личного страхования$COMM$;
COMMENT ON COLUMN ais.agrobject.cityisn IS $COMM$FK(CITY). Указатель города места жительства застрахованного. Используется для
оценки необходимости заключения договора с местным ЛПУ, если он обслуживается в другом городе.$COMM$;
COMMENT ON COLUMN ais.agrobject.cost IS $COMM$Начальная полисная стоимость физ. объекта ( не страховая сумма! не амортиз.!)$COMM$;
COMMENT ON COLUMN ais.agrobject.groupisn IS $COMM$FK(AGROBJGROUP). Ссылка на группу объекта для обеспечения групповых операций$COMM$;
COMMENT ON COLUMN ais.agrobject.previsn IS $COMM$FK(AGROBJECT). Ссылка на предыдущий логический объект при пролонгации, переносе страхования или бонуса-малуса$COMM$;
COMMENT ON COLUMN ais.agrobject.addisn IS $COMM$FK(AGREEMENT) Указатель аддендума, добавившего этот объект$COMM$;
COMMENT ON COLUMN ais.agrobject.groupid IS $COMM$Идентификатор группы для задания одинаковых условий страхования$COMM$;


CREATE TABLE ais.agrobjectext (
    isn                              NUMERIC,
    objisn                           NUMERIC,
    subjisn                          NUMERIC,
    refid                            VARCHAR(20),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    classisn                         NUMERIC,
    templisn                         NUMERIC,
    detclassisn                      NUMERIC,
    paramclassisn                    NUMERIC,
    paramtype                        VARCHAR(1),
    paramvalue                       NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    refagrisn                        NUMERIC,
    quephoneisn                      NUMERIC,
    paramdate                        TIMESTAMP,
    paramstr                         VARCHAR(255),
    addisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrobjectext IS $COMM$Связь m:m между застрахованным и медицинским учреждением: мед.карта$COMM$;
COMMENT ON COLUMN ais.agrobjectext.paramdate IS $COMM$Значение параметра - дата (Yunin V.A. 30/01/06)$COMM$;
COMMENT ON COLUMN ais.agrobjectext.paramstr IS $COMM$Значение параметра - текст (Lavrov S.V. 15/03/2010)$COMM$;
COMMENT ON COLUMN ais.agrobjectext.objisn IS $COMM$FK(AGROBJECT). Указатель объекта страхования$COMM$;
COMMENT ON COLUMN ais.agrobjectext.subjisn IS $COMM$FK(SUBJECT). Указатель субъекта - участника договора$COMM$;
COMMENT ON COLUMN ais.agrobjectext.refid IS $COMM$Внешний референс: номер медицинской карты$COMM$;
COMMENT ON COLUMN ais.agrobjectext.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrobjectext.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agrobjectext.refagrisn IS $COMM$FK(Agreement). Yunin V.A. 07/07/04$COMM$;
COMMENT ON COLUMN ais.agrobjectext.quephoneisn IS $COMM$FK(QUEPHONE). Yunin V.A. 07/07/04$COMM$;


CREATE TABLE ais.agrobjext (
    isn                              NUMERIC,
    groupno                          VARCHAR(255),
    extid                            VARCHAR(255),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    groupisn                         NUMERIC,
    typeisn                          NUMERIC,
    typeisn2                         NUMERIC,
    x1                               NUMERIC,
    linkobjisn                       NUMERIC,
    x2                               NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrobjext IS $COMM$Дополнительные реквизиты объекта страхования$COMM$;
COMMENT ON COLUMN ais.agrobjext.linkobjisn IS $COMM$застрахованный из другого договора (туристы), Попов В.О. 2012.10.17, задача 38319416403$COMM$;
COMMENT ON COLUMN ais.agrobjext.x2 IS $COMM$в ДМС для связанных застрахованных признак "В отчеты"(0-нет, 1-да), Попов В.О. 2012.03.28, задача 45570516203$COMM$;
COMMENT ON COLUMN ais.agrobjext.isn IS $COMM$FK(AGROBJECT). Указатель объекта страхования$COMM$;
COMMENT ON COLUMN ais.agrobjext.groupno IS $COMM$Номер группы (абстрактной), подгруппы, аддендума, наследуется из AGROBJGROUP.ID$COMM$;
COMMENT ON COLUMN ais.agrobjext.extid IS $COMM$Внешний референс, например номер полиса ОМС$COMM$;
COMMENT ON COLUMN ais.agrobjext.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.agrobjext.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrobjext.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.agrobjgroup (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    id                               VARCHAR(255),
    description                      VARCHAR(255),
    createdby                        NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    classisn                         NUMERIC,
    returnclass                      NUMERIC,
    returnparam                      NUMERIC,
    nextisn                          NUMERIC,
    x1                               NUMERIC,
    parentisn                        NUMERIC,
    previsn                          NUMERIC,
    sexisn                           NUMERIC,
    ageisn                           NUMERIC,
    inscount                         NUMERIC,
    status                           VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrobjgroup IS $COMM$Группа объектов страхования

Описывает дополнительные параметры, единые для группы объектов страхования. Существует независимо от кондов (пока, во всяком
случае).$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.parentisn IS $COMM$ДМС - в случае варианта программы-ссылка на программу ЛПУ по прикрпелению, Попов В.О., 2009.09.10, задача 11293796903$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.previsn IS $COMM$ЛПУ по прикреплению, Ссылка на предыдущую программу ЛПУ по прикреплению, предыдущий вариант в предыдущем хоз. договоре; Попов В.О., задача 12241034603$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.isn IS $COMM$Машинный номер seq_AgrObjGroup.NextVal$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.agrisn IS $COMM$FK(AGREEMENT). Ссылка на договор$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.id IS $COMM$Номер группы$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.description IS $COMM$Описание группы$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.createdby IS $COMM$Создатель группы$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.created IS $COMM$Время создания$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.updated IS $COMM$Время изменения$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.classisn IS $COMM$Класс группы объектов$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.returnclass IS $COMM$??? Правило возврата премии при снятии объекта группы со страхования$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.returnparam IS $COMM$??? Параметр возрата$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.nextisn IS $COMM$В ДМС - указатель на следующую версию плана. NextISN введен для простоты получения набора последних версий планов.$COMM$;
COMMENT ON COLUMN ais.agrobjgroup.x1 IS $COMM$В ДМС - номер версии плана$COMM$;


CREATE TABLE ais.agrobjgroupcity (
    isn                              NUMERIC,
    groupisn                         NUMERIC,
    cityisn                          NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE ais.agrobjgroupitem (
    isn                              NUMERIC,
    groupisn                         NUMERIC,
    classisn                         NUMERIC,
    riskclassisn                     NUMERIC,
    limitisn                         NUMERIC,
    description                      VARCHAR(255),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    createdby                        NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    id                               VARCHAR(20),
    riskisn                          NUMERIC,
    cost                             NUMERIC,
    yeartariff                       NUMERIC,
    limitsum                         NUMERIC,
    quantity                         NUMERIC(38),
    discount                         NUMERIC,
    premiumsum                       NUMERIC,
    franchtariff                     NUMERIC,
    franchsum                        NUMERIC,
    currisn                          NUMERIC,
    premcurrisn                      NUMERIC,
    remark                           VARCHAR(1000),
    ctgriskisn                       NUMERIC,
    bonusisn                         NUMERIC,
    classisn2                        NUMERIC,
    x1                               NUMERIC,
    x2                               NUMERIC,
    props                            VARCHAR(255),
    addisn                           NUMERIC,
    newaddisn                        NUMERIC,
    refagrgroupisn                   NUMERIC,
    programmtype                     VARCHAR(10),
    plannedloss                      NUMERIC,
    franchtype                       VARCHAR(1),
    franchcurrisn                    NUMERIC,
    inccoeff                         NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrobjgroupitem IS $COMM$Элемент группы объектов страхования.

В ДМС задаёт набор услуг (ClassISN), оказываемых по программе страхования (ClassRiskISN) в ЛПУ (LimitISN). Может иметь отличное
от Dicti.ShortName
наименование Description.$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.franchtype IS $COMM$Тип франшизы: У-условная, Б-безусловная, Попов В.О. 2008.12.26, задача 7962933803$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.inccoeff IS $COMM$Повышающий коэффициент для "черного" списка застрахованных, Попов В.О. 2013.10.24 задача 53958560603$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.franchcurrisn IS $COMM$FK(CURRENCY). Указатель валюты франшизы. Попов В.О. 2008.12.26, задача 7962933803$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.props IS $COMM$Попов В.О. 29.10.2004 Признаки риска: L-в списки ЛПУ, G-гар.письмо, S-услуга по согласованию$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.programmtype IS $COMM$Тип программы - рисковая(R) или с фиксированным покрытием(F)$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.plannedloss IS $COMM$запланированный процент убыточности для программ с фиксированным покрытием в договорах с комбинированным покрытием$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.isn IS $COMM$Элемент группы.

Машинный номер seq_AgrObjGroupItem.NextVal$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.groupisn IS $COMM$FK(AGROBJGROUP).Группа элемента.$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.classisn IS $COMM$FK(DICTI).Класс элемента группы.

В ДМС - мед. услуга$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.riskclassisn IS $COMM$FK(DICTI).Класс риска элемента группы.

В ДМС - программа страхования, по которой оказывается услуга ClassISN.$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.limitisn IS $COMM$FK(AGRLIMIT).Ограничение элемента группы.

В ДМС - ЛПУ, в которой оказывается услуга ClassISN.$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.description IS $COMM$Описание элемента услуги.

В ДМС - одна и та же услуга может быть описана разными словами
(УДАЛЯТЬ зубы, ДЁРГАТЬ зубы и пр.)$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.datebeg IS $COMM$Дата начала существования элемента в группе$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.dateend IS $COMM$Дата окончания существования элемента в группе$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.createdby IS $COMM$Создатель элемента в группе$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.created IS $COMM$Время создания элемента в группе$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.updatedby IS $COMM$Автор изменения элемента в группе$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.updated IS $COMM$Время изменения элемента в группе$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.id IS $COMM$Номер$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.riskisn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.cost IS $COMM$Стоимость$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.yeartariff IS $COMM$Годовой тариф$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.limitsum IS $COMM$Лимит ответственности или страховая сумма одного объекта в валюте условия.$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.quantity IS $COMM$Количество одинаковых объектов, перевозок$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.discount IS $COMM$Скидка/надбавка (-/+): в $COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.premiumsum IS $COMM$Сумма премии в валюте премии. PremiumSum = LimitSum * Tariff * (1+Discount/100)$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.franchtariff IS $COMM$ франшизы: сумма франшизы = страховая сумма *  франшизы / 100$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.franchsum IS $COMM$Сумма франшизы в валюте условия$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.currisn IS $COMM$Валюта LimitSum$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.premcurrisn IS $COMM$Валюта PremiumSum$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.ctgriskisn IS $COMM$FK(DICTI) категория риска$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.bonusisn IS $COMM$FK(DICTI) бонус$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.addisn IS $COMM$Попов В.О. 13.11.2004 Исп. для ведения детальных рисков$COMM$;
COMMENT ON COLUMN ais.agrobjgroupitem.newaddisn IS $COMM$Попов В.О. 13.11.2004 Исп. для ведения детальных рисков$COMM$;


CREATE TABLE ais.agrobjgrouplimitaddr (
    isn                              NUMERIC,
    itemisn                          NUMERIC,
    addrisn                          NUMERIC,
    discr                            VARCHAR(1),
    description                      VARCHAR(255),
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrobjgrouplimitaddr IS $COMM$Хранить и по заданному условию в рамках плана (по ЛПУ или детальному риску) иметь возможность отобрать адреса ЛПУ, по которым может обслуживаться застрахованный.$COMM$;
COMMENT ON COLUMN ais.agrobjgrouplimitaddr.itemisn IS $COMM$Ссылка на условия по ЛПУ для программы в плане (AgrObjGroupItem.ISN, где GroupISN, RiskISN, LimitISN Is Not Null и RiskClassISN Is Null );$COMM$;
COMMENT ON COLUMN ais.agrobjgrouplimitaddr.addrisn IS $COMM$строка из таблицы "Виды медпомощи" х/д с ЛПУ (AgrExt.ISN);$COMM$;
COMMENT ON COLUMN ais.agrobjgrouplimitaddr.discr IS $COMM$Дискриминатор условий по плану: L-условия по ЛПУ ; R-условия по детальному риску$COMM$;
COMMENT ON COLUMN ais.agrobjgrouplimitaddr.description IS $COMM$текстовая информация, включающая адрес, вид медпомощи, возраст, взятая из строки таблицы «Виды медпомощи (новые)» в х/д. Заполняется при сохранении новой записи в текущей таблице.$COMM$;


CREATE TABLE ais.agrperiod (
    isn                              NUMERIC,
    objisn                           NUMERIC,
    addisn                           NUMERIC,
    currisn                          NUMERIC,
    agrisn                           NUMERIC,
    addrisn                          NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    quantity                         NUMERIC(38),
    insuredsum                       NUMERIC(20,2),
    limitsum                         NUMERIC(20,2),
    yeartariff                       NUMERIC(7,4),
    premiumsum                       NUMERIC(20,2),
    status                           VARCHAR(1),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    newaddisn                        NUMERIC,
    olddateend                       TIMESTAMP,
    oldpremiumsum                    NUMERIC(20,2)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrperiod IS $COMM$Период действия одинаковых условий страхования для данного объекта.
Изменения условий оформляются аддендумом, поэтому период соответствует одному объекту и аддендуму.
При прекращении страхования объекта в таблицу AGRPERIOD помещается запись с соответствующими датами, не имеющая подчиненных ей
условий страхования.$COMM$;
COMMENT ON COLUMN ais.agrperiod.updated IS $COMM$Дата создания или последнего изменения объекта$COMM$;
COMMENT ON COLUMN ais.agrperiod.updatedby IS $COMM$Автор создания или последнего изменения объекта$COMM$;
COMMENT ON COLUMN ais.agrperiod.newaddisn IS $COMM$FK(AGREEMENT). Указатель аддендума, изменившего условия.$COMM$;
COMMENT ON COLUMN ais.agrperiod.olddateend IS $COMM$Старая дата окончания, до регистрации аддендума. Исходно устанавливается равной
DateEnd$COMM$;
COMMENT ON COLUMN ais.agrperiod.oldpremiumsum IS $COMM$Старая премия, до регистрации аддендума. Исходно устанавливается равной
PremiumSum$COMM$;
COMMENT ON COLUMN ais.agrperiod.isn IS $COMM$Машинный номер, уникальный в рамках договора. SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrperiod.objisn IS $COMM$FK(AGROBJECT или AGREEMENT). Ссылка на элемент договора, соответствующий периоду.
Тип элемента определяется дискриминатором.$COMM$;
COMMENT ON COLUMN ais.agrperiod.addisn IS $COMM$FK(AGREEMENT). Ссылка на аддендум.$COMM$;
COMMENT ON COLUMN ais.agrperiod.currisn IS $COMM$FK(CURENCY). Указатель валюты периода. Наследуется из объекта по умолчанию.
Продвигается в объект.$COMM$;
COMMENT ON COLUMN ais.agrperiod.agrisn IS $COMM$FK(AGREEMENT). Ссылка на договор,  наследуется из объекта.$COMM$;
COMMENT ON COLUMN ais.agrperiod.addrisn IS $COMM$FK(SUBADDR). Указатель места страхования, введен ВРЕМЕННО !$COMM$;
COMMENT ON COLUMN ais.agrperiod.datebeg IS $COMM$Дата начала действия данных условий для пары объект-риск.$COMM$;
COMMENT ON COLUMN ais.agrperiod.dateend IS $COMM$Дата окончания действия данных условий для пары объект-риск.$COMM$;
COMMENT ON COLUMN ais.agrperiod.quantity IS $COMM$Количество одинаковых объектов.$COMM$;
COMMENT ON COLUMN ais.agrperiod.insuredsum IS $COMM$Cтраховая сумма по объекту в течение данного периода. Поддерживается
автоматически по AGRSUM.$COMM$;
COMMENT ON COLUMN ais.agrperiod.limitsum IS $COMM$Лимит ответственности по объекту в течение данного периода, не зависящий от риска.
Поддерживается автоматически по AGRSUM.$COMM$;
COMMENT ON COLUMN ais.agrperiod.yeartariff IS $COMM$Годовой тариф премии в %$COMM$;
COMMENT ON COLUMN ais.agrperiod.premiumsum IS $COMM$Общая начисленная сумма премии по объекту в данном периоде. Считается
автоматически как сумма премий по детальным условиям.$COMM$;
COMMENT ON COLUMN ais.agrperiod.status IS $COMM$Состояние периода: N-не действует (соответствующий аддендум не утвержден, период не
попадает в статистику), Y-действует (попадает в статистику), Z-страхование прекращено в соответсии с аддендумом, null-архив
(задел на будущее для сохранения старых периодов в их первоначальном виде)
Q-new tech$COMM$;
COMMENT ON COLUMN ais.agrperiod.remark IS $COMM$Дополнительная информация, которая появляется в соответсвующем параграфе
аддендума.$COMM$;


CREATE TABLE ais.agrrefund (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    condisn                          NUMERIC,
    objisn                           NUMERIC,
    riskisn                          NUMERIC,
    claimisn                         NUMERIC,
    agrisn                           NUMERIC,
    currisn                          NUMERIC,
    losssum                          NUMERIC(20,2),
    claimsum                         NUMERIC(20,2),
    refundsum                        NUMERIC(20,2),
    rejectsum                        NUMERIC(20,2),
    status                           VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    remark                           VARCHAR(1000),
    respondisn                       NUMERIC,
    lossshare                        NUMERIC(9,6),
    regress                          VARCHAR(1) DEFAULT 'N',
    regressisn                       NUMERIC,
    dateevent                        TIMESTAMP,
    qnt                              NUMERIC,
    daterefund                       TIMESTAMP,
    rptclassisn                      NUMERIC,
    objclassisn                      NUMERIC,
    lossreins                        NUMERIC(9,6),
    refundid                         VARCHAR(20),
    aqu                              VARCHAR(1) DEFAULT 'N',
    deptisn                          NUMERIC,
    emplisn                          NUMERIC,
    dateend                          TIMESTAMP,
    period                           NUMERIC,
    tariff                           NUMERIC(12,9),
    dateval                          TIMESTAMP,
    descisn                          NUMERIC,
    rejectisn                        NUMERIC,
    descisn2                         NUMERIC,
    qnt2                             NUMERIC,
    costshare                        NUMERIC,
    parentisn                        NUMERIC,
    franchsum                        NUMERIC(20,2),
    franchcurrisn                    NUMERIC,
    nrzu                             VARCHAR(1) DEFAULT 'N',
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    origclaimsum                     NUMERIC(20,2),
    claimclassisn                    NUMERIC,
    fizikobjisn                      NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.agrrefund.fizikobjisn IS $COMM$FK(AgrObjGRoupItem) ссылка на объект(авто,судно, самолёт)$COMM$;
COMMENT ON COLUMN ais.agrrefund.createdby IS $COMM$Создатель -- Rusov 05.06.06$COMM$;
COMMENT ON COLUMN ais.agrrefund.franchcurrisn IS $COMM$FK(CURRENCY).
    Указатель валюты франшизы. Если не задана, наследуется из валюты периода.
Обязателен ! /* 30.06.05 SR */$COMM$;
COMMENT ON COLUMN ais.agrrefund.franchsum IS $COMM$Сумма франшизы 
    для авсолютной франшизы или минимальная сумма франшизы в 
    валюте
условия /* 30.06.05 SR */$COMM$;
COMMENT ON COLUMN ais.agrrefund.created IS $COMM$Дата создания. Регистрируется автоматически -- Rusov 05.06.06$COMM$;
COMMENT ON COLUMN ais.agrrefund.claimclassisn IS $COMM$Наследуемое поле AgrClaim.ClassISN (Rusov)$COMM$;
COMMENT ON COLUMN ais.agrrefund.nrzu IS $COMM$Признак "Не учитывается в РЗУ"/*17.05.05*/$COMM$;
COMMENT ON COLUMN ais.agrrefund.isn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrrefund.classisn IS $COMM$FK(DICTI,РИСК). Класс убытка. Должен быть подклассом риска условия, к которому
относится возмещение. Если не задан, наследуется из условия (AGRCOND.RiskISN.ClassISN)$COMM$;
COMMENT ON COLUMN ais.agrrefund.condisn IS $COMM$FK(AGRCOND). Ссылка на условие договора, соответсвующее убытку.$COMM$;
COMMENT ON COLUMN ais.agrrefund.objisn IS $COMM$FK(AGROBJECT). Указатель объекта, наследуется из условия$COMM$;
COMMENT ON COLUMN ais.agrrefund.riskisn IS $COMM$FK(AGRRISK). Указатель риска, наследуется из условия$COMM$;
COMMENT ON COLUMN ais.agrrefund.claimisn IS $COMM$FK(AGRCLAIM). Ссылка на паспорт убытка.$COMM$;
COMMENT ON COLUMN ais.agrrefund.agrisn IS $COMM$FK(AGREEMENT). Ссылка на договор,  наследуется из условия.$COMM$;
COMMENT ON COLUMN ais.agrrefund.currisn IS $COMM$FK(CURRENCY). Ссылка на валюту возмещения.$COMM$;
COMMENT ON COLUMN ais.agrrefund.losssum IS $COMM$Сумма убытка в валюте возмещения, суммируется в заявление об убытке (AGRCLAIM)$COMM$;
COMMENT ON COLUMN ais.agrrefund.claimsum IS $COMM$Заявленная сумма убытка в валюте возмещения, суммируется в заявление об убытке
AGRCLAIM$COMM$;
COMMENT ON COLUMN ais.agrrefund.refundsum IS $COMM$Сумма к оплате с учетом франшизы в валюте возмещения, суммируется в заявление об
убытке (AGRCLAIM)$COMM$;
COMMENT ON COLUMN ais.agrrefund.rejectsum IS $COMM$Сумма, которую не будет возмещать страховщик. В процессе урегулирования разницей
между заявленной суммой и суммами возмещения и отказа является неурегулированной. После урегулирования убытка сумма возмещения и
отказа дают заявленную сумму. Суммируется в заявление об убытке (AGRCLAIM)$COMM$;
COMMENT ON COLUMN ais.agrrefund.status IS $COMM$Статус претензии: D-неоформлена, N-в рассмотрении, Y-урегулирована, R-отклонена,
S-отказ заявителя, F-включена в другую (по ParentISN) при одновременном урегудировании, null-аннулирована.$COMM$;
COMMENT ON COLUMN ais.agrrefund.updated IS $COMM$Автор изменения. Устанавливается автоматически.$COMM$;
COMMENT ON COLUMN ais.agrrefund.updatedby IS $COMM$Дата изменения. Устанавливается автоматически.$COMM$;
COMMENT ON COLUMN ais.agrrefund.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.agrrefund.respondisn IS $COMM$FK(SUBJECT). Указатель виновного лица$COMM$;
COMMENT ON COLUMN ais.agrrefund.lossshare IS $COMM$Доля ИГС в процентах$COMM$;
COMMENT ON COLUMN ais.agrrefund.regress IS $COMM$Признак регресса: N-нет, Y-требуется регресс$COMM$;
COMMENT ON COLUMN ais.agrrefund.regressisn IS $COMM$FK(REGRESS). Указатель регресса$COMM$;
COMMENT ON COLUMN ais.agrrefund.dateevent IS $COMM$Дата страхового случая$COMM$;
COMMENT ON COLUMN ais.agrrefund.qnt IS $COMM$Количество услуг для ДМС$COMM$;
COMMENT ON COLUMN ais.agrrefund.daterefund IS $COMM$Дата урегулирования - принятия решения по убытку.$COMM$;
COMMENT ON COLUMN ais.agrrefund.aqu IS $COMM$Признак передачи в аквизицию$COMM$;
COMMENT ON COLUMN ais.agrrefund.deptisn IS $COMM$Подразделение-куратор$COMM$;
COMMENT ON COLUMN ais.agrrefund.emplisn IS $COMM$Сотрудник, курирующий претензию$COMM$;
COMMENT ON COLUMN ais.agrrefund.dateend IS $COMM$Дата окончания выплат по регулярным убыткам (аннуитеты, несч. случаи и т.п.)$COMM$;
COMMENT ON COLUMN ais.agrrefund.period IS $COMM$Периодичность выплат - раз в 1/x года$COMM$;
COMMENT ON COLUMN ais.agrrefund.tariff IS $COMM$Годовой тариф для регулярных выплат. Если задается, то пересчитывается REFUNDSUM$COMM$;
COMMENT ON COLUMN ais.agrrefund.dateval IS $COMM$Дата калькуляции - валютирования по претензии$COMM$;
COMMENT ON COLUMN ais.agrrefund.descisn IS $COMM$FK(OBJAGR). Указатель физического объекта, наследуется из AGROBJECT(AGRCOND.ObjISN).DescISN$COMM$;
COMMENT ON COLUMN ais.agrrefund.rejectisn IS $COMM$FK(Dicti) Причина отказа в выплате$COMM$;
COMMENT ON COLUMN ais.agrrefund.descisn2 IS $COMM$FK(ObjAgr) Ссылка на объект - имущество потерпевшего/заявителя$COMM$;
COMMENT ON COLUMN ais.agrrefund.parentisn IS $COMM$FK(AgrRefund) Ссылка на претензию в которой произошло возмещение (23.08.04 SR)$COMM$;


CREATE TABLE ais.agrrefundexam (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    refundextisn                     NUMERIC,
    riskclassisn                     NUMERIC,
    ctgriskisn                       NUMERIC,
    ctgriskvalue                     NUMERIC,
    description                      VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    folderisn                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrrefundexam IS $COMM$Результат технической экспертизы - медицина$COMM$;
COMMENT ON COLUMN ais.agrrefundexam.isn IS $COMM$Машинный номер SEQ_AGRREFUNDEXT.nextval$COMM$;
COMMENT ON COLUMN ais.agrrefundexam.classisn IS $COMM$Ссылка на ошибку dicti.isn$COMM$;
COMMENT ON COLUMN ais.agrrefundexam.refundextisn IS $COMM$Ссылка на убыток по застрахованному AgrRefundExt.Isn$COMM$;
COMMENT ON COLUMN ais.agrrefundexam.riskclassisn IS $COMM$Ссылка на детальный риск Dicti.isn, по которому выявлена ошибка$COMM$;
COMMENT ON COLUMN ais.agrrefundexam.ctgriskisn IS $COMM$Ограничения детального риска$COMM$;
COMMENT ON COLUMN ais.agrrefundexam.description IS $COMM$Дополнительное описание ошибки по риску$COMM$;
COMMENT ON COLUMN ais.agrrefundexam.folderisn IS $COMM$Ссылка на личную папку Folder формы MedServExamSrch, Попов В.О., 2008.11.17, задача 7368749203$COMM$;


CREATE TABLE ais.agrrefundext (
    isn                              NUMERIC,
    id                               VARCHAR(20),
    extid                            VARCHAR(20),
    code                             VARCHAR(20),
    name                             VARCHAR(255),
    description                      VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    currisn                          NUMERIC,
    insuredsum                       NUMERIC(20,2),
    refundisn                        NUMERIC,
    claimisn                         NUMERIC,
    agrisn                           NUMERIC,
    objisn                           NUMERIC,
    riskisn                          NUMERIC,
    classisn                         NUMERIC,
    condisn                          NUMERIC,
    claimsum                         NUMERIC(20,2),
    refundsum                        NUMERIC(20,2),
    dateevent                        TIMESTAMP,
    qnt                              NUMERIC(8,4),
    status                           VARCHAR(1),
    remark                           VARCHAR(1000),
    classisn2                        NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    remark2                          VARCHAR(1000),
    subjisn                          NUMERIC,
    flag                             VARCHAR(10),
    garletterisn                     NUMERIC,
    examstatus                       VARCHAR(1),
    examdate                         TIMESTAMP,
    numvalue                         NUMERIC,
    payssetisn                       NUMERIC,
    claimoldisn                      NUMERIC,
    detriskclassisn                  NUMERIC,
    groupisn                         NUMERIC,
    groupverno                       NUMERIC,
    refextisn                        NUMERIC,
    calcdatebeg                      TIMESTAMP,
    calcdateend                      TIMESTAMP,
    paysdatebeg                      TIMESTAMP,
    paysdateend                      TIMESTAMP,
    pharm_prescrid                   VARCHAR(40),
    pharm_code                       VARCHAR(40),
    pharm_prescrdate                 TIMESTAMP,
    pharm_drugcode                   NUMERIC,
    pharm_id                         NUMERIC,
    pharm_subjisn                    NUMERIC,
    pharm_price                      NUMERIC(20,2),
    mkbisn                           NUMERIC,
    mkbname                          VARCHAR(500),
    mkbcode                          VARCHAR(10),
    cardname                         VARCHAR(20),
    mkbmain                          VARCHAR(6),
    surfacetooth                     VARCHAR(255),
    branch                           VARCHAR(500),
    addisn                           NUMERIC,
    typeadd                          VARCHAR(1),
    divextisn                        NUMERIC,
    claimcondisn                     NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrrefundext IS $COMM$Временное хранилище убытков$COMM$;
COMMENT ON COLUMN ais.agrrefundext.typeadd IS $COMM$ЛПУ по прикреплению, Тип аддендума условия страхования, =D-принятия, =A-снятия; Попов В.О., задача 12241034603$COMM$;
COMMENT ON COLUMN ais.agrrefundext.paysdatebeg IS $COMM$Попов В.О. 30.05.07 Дата начала периода рассрочки(ЛПУ по прикреплению)$COMM$;
COMMENT ON COLUMN ais.agrrefundext.paysdateend IS $COMM$Попов В.О. 30.05.07 Дата окончания периода рассрочки(ЛПУ по прикреплению)$COMM$;
COMMENT ON COLUMN ais.agrrefundext.pharm_prescrid IS $COMM$N рецепта, Попов В.О. 2009.04.06, задача 8085856303$COMM$;
COMMENT ON COLUMN ais.agrrefundext.pharm_code IS $COMM$Уникальный код рецепта, Попов В.О. 2009.04.06, задача 8085856303$COMM$;
COMMENT ON COLUMN ais.agrrefundext.calcdatebeg IS $COMM$Попов В.О. 04.05.07 Дата начала периода расчета(ЛПУ по прикреплению)$COMM$;
COMMENT ON COLUMN ais.agrrefundext.calcdateend IS $COMM$Попов В.О. 04.05.07 Дата окончания периода расчета(ЛПУ по прикреплению)$COMM$;
COMMENT ON COLUMN ais.agrrefundext.examstatus IS $COMM$Попов В.О. 10.12.04 Признак проведения технической экспертизы Y-проведена успешно, N-проведена с ошибкой, null-не проведена$COMM$;
COMMENT ON COLUMN ais.agrrefundext.examdate IS $COMM$Попов В.О. 10.12.04 Дата проведения технической экспертизы$COMM$;
COMMENT ON COLUMN ais.agrrefundext.refextisn IS $COMM$Попов В.О. 28.03.07 Ссылка обработанного убытка на AgrRefundExt.isn убытка интегрированного счета (ЛПУ по прикреплению)$COMM$;
COMMENT ON COLUMN ais.agrrefundext.claimcondisn IS $COMM$Условие страхования, которое учитывает убытки по застрахованным при расчете премий, Попов В.О. 2012.07.12, задача 33042179203$COMM$;
COMMENT ON COLUMN ais.agrrefundext.pharm_prescrdate IS $COMM$Дата рецепта, Попов В.О. 2009.04.06, задача 8085856303$COMM$;
COMMENT ON COLUMN ais.agrrefundext.payssetisn IS $COMM$Попов В.О. 30.08.06 ISN набора платежей для застрахованного, совпадает с agrrefundext.isn для первой записи платежа$COMM$;
COMMENT ON COLUMN ais.agrrefundext.claimoldisn IS $COMM$Попов В.О. 23.09.06 Ссылка на предыдущий убыток после перепривязки (ЛПУ по прикреплению)$COMM$;
COMMENT ON COLUMN ais.agrrefundext.detriskclassisn IS $COMM$Попов В.О. 23.09.06 Ссылка на детальный риск (ЛПУ по прикреплению)$COMM$;
COMMENT ON COLUMN ais.agrrefundext.pharm_drugcode IS $COMM$Код медикамента, выписанного по рецепту, Попов В.О. 2009.04.06, задача 8085856303$COMM$;
COMMENT ON COLUMN ais.agrrefundext.id IS $COMM$Регистрационный номер$COMM$;
COMMENT ON COLUMN ais.agrrefundext.extid IS $COMM$Внешний референс$COMM$;
COMMENT ON COLUMN ais.agrrefundext.code IS $COMM$Код$COMM$;
COMMENT ON COLUMN ais.agrrefundext.name IS $COMM$Имя застрахованного$COMM$;
COMMENT ON COLUMN ais.agrrefundext.description IS $COMM$Краткое описание$COMM$;
COMMENT ON COLUMN ais.agrrefundext.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrrefundext.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agrrefundext.currisn IS $COMM$FK(CURRENCY).Валюта услуги, наследуется из AGRREFUND$COMM$;
COMMENT ON COLUMN ais.agrrefundext.refundisn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrrefundext.agrisn IS $COMM$Машинный номер объекта. Устанавливается по умолчанию равным
SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrrefundext.objisn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrrefundext.riskisn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrrefundext.classisn IS $COMM$FK(DICTI). Тип риска$COMM$;
COMMENT ON COLUMN ais.agrrefundext.condisn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrrefundext.classisn2 IS $COMM$Врачебная примочка$COMM$;
COMMENT ON COLUMN ais.agrrefundext.datebeg IS $COMM$Дата начала госпитализации$COMM$;
COMMENT ON COLUMN ais.agrrefundext.dateend IS $COMM$Дата окончания госпитализации$COMM$;
COMMENT ON COLUMN ais.agrrefundext.remark2 IS $COMM$Дополнительное примечание (окончательный диагноз, например)$COMM$;
COMMENT ON COLUMN ais.agrrefundext.subjisn IS $COMM$FK(DICTI,СУБЪЕКТ). Машинный номер объекта, совпадает с ISN соответствующей записи в словаре DICTI.
Субъект услуги (врач ТИМа и пр.)$COMM$;
COMMENT ON COLUMN ais.agrrefundext.garletterisn IS $COMM$Попов В.О. 04.08.04 Ссылка на гарантийное письмо AGREEMENT.ISN$COMM$;
COMMENT ON COLUMN ais.agrrefundext.pharm_subjisn IS $COMM$ЛПУ, выписавшее рецепт, Попов В.О. 2009.04.06, задача 8085856303 $COMM$;
COMMENT ON COLUMN ais.agrrefundext.pharm_price IS $COMM$Цена лекарства, Попов В.О. 2009.04.06, задача 8085856303$COMM$;
COMMENT ON COLUMN ais.agrrefundext.addisn IS $COMM$ЛПУ по прикреплению, Номер аддендума условия страхования: либо действующее AgrCond.AddIsn, либо архивное – AgrCond.NewAddIsn; Попов В.О., задача 12241034603$COMM$;
COMMENT ON COLUMN ais.agrrefundext.pharm_id IS $COMM$Номер аптеки, Попов В.О. 2009.04.06, задача 8085856303$COMM$;
COMMENT ON COLUMN ais.agrrefundext.groupisn IS $COMM$Попов В.О. 30.09.06 План хоз. дог. с ЛПУ (ЛПУ по прикреплению)$COMM$;
COMMENT ON COLUMN ais.agrrefundext.groupverno IS $COMM$Попов В.О. 30.09.06 Версия плана (ЛПУ по прикреплению)$COMM$;
COMMENT ON COLUMN ais.agrrefundext.mkbisn IS $COMM$Идентификатор диагноза МКБ10, Зайчиков А.А., задача 4896200203$COMM$;
COMMENT ON COLUMN ais.agrrefundext.mkbname IS $COMM$Диагноз по МКБ10, Зайчиков А.А., задача 4896200203$COMM$;
COMMENT ON COLUMN ais.agrrefundext.mkbcode IS $COMM$Код МКБ10, Зайчиков А.А., задача 4896200203$COMM$;
COMMENT ON COLUMN ais.agrrefundext.cardname IS $COMM$№ истории болезни, Зайчиков А.А., задача 4896200203$COMM$;
COMMENT ON COLUMN ais.agrrefundext.mkbmain IS $COMM$Основная МКБ, Зайчиков А.А., задача 4896200203$COMM$;
COMMENT ON COLUMN ais.agrrefundext.surfacetooth IS $COMM$Поверхность зуба, Зайчиков А.А., задача 4896200203$COMM$;
COMMENT ON COLUMN ais.agrrefundext.branch IS $COMM$Отделение, Зайчиков А.А., задача 4896200203$COMM$;
COMMENT ON COLUMN ais.agrrefundext.divextisn IS $COMM$Ссылка на исходную ЧС AgrRefundExt.Isn у деленных ЧСЛПУ по прикреплению; Попов В.О., задача 12241034603$COMM$;


CREATE TABLE ais.agrrefundhist (
    isn                              NUMERIC,
    currisn                          NUMERIC,
    refundisn                        NUMERIC,
    extid                            VARCHAR(100),
    extdate                          TIMESTAMP,
    losssum                          NUMERIC(20,2),
    claimsum                         NUMERIC(20,2),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    refundsum                        NUMERIC,
    newlossum                        NUMERIC,
    franchsum                        NUMERIC,
    franchcurrisn                    NUMERIC,
    deptisn                          NUMERIC,
    docclassisn                      NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrrefundhist IS $COMM$История убытка для регистрации внешних документов, изменяющих сумму убытка.$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.deptisn IS $COMM$Rusov 14.11.2011 FK(DICTI). Подразделение куратора $COMM$;
COMMENT ON COLUMN ais.agrrefundhist.docclassisn IS $COMM$Rusov 14.11.2011 FK(DICTI). Тип документа, обоснование для внесения или изменения данных$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.franchsum IS $COMM$Rusov 03.05.2007 Сумма франшизы для авсолютной франшизы или минимальная сумма франшизы в валюте
условия$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.refundsum IS $COMM$Rusov 02.04.2007 Сумма к оплате с учетом франшизы в валюте возмещения$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.isn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.currisn IS $COMM$FK(CURRENCY). Валюта возмещения$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.refundisn IS $COMM$FK(AGRREFUND). Указатель возмещения$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.extid IS $COMM$Внешний референс$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.extdate IS $COMM$Дата документа$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.losssum IS $COMM$Сумма убытка в валюте возмещения$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.claimsum IS $COMM$Заявленная сумма убытка в валюте возмещения$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.updated IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agrrefundhist.franchcurrisn IS $COMM$Rusov 03.05.2007 FK(CURRENCY). Указатель валюты франшизы$COMM$;


CREATE TABLE ais.agrrisk (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    rptclassisn                      NUMERIC,
    classisn                         NUMERIC,
    agrisn                           NUMERIC,
    id                               VARCHAR(20),
    name                             VARCHAR(255),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    ruleisn                          NUMERIC,
    insclassisn                      NUMERIC,
    status                           VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrrisk IS $COMM$Страховой риск в смысле потенциально возможного страхового случая, защита от которого
предостваляется в соответсвии с условиями договора.
Множество допустимых для данного договора рисков ограничивается одним или несколькими правилами страхования.
Допустимость риска для объекта договора определяется наличием соответствующего условия страхования (AGRCOND).
Риски группируются (по ParentISN). Группировка осуществляется автоматически в соответствии с правилами страхования при вставке
процедурой INSERTRISK, поэтому вставлять риски в договор непосредственно оператором INSERT запрещено.
Аддитивные реквизиты (премия) могут быть заданы либо только для группы в целом, либо для каждого члена группы в отдельности.
$COMM$;
COMMENT ON COLUMN ais.agrrisk.isn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrrisk.parentisn IS $COMM$FK(AGRRISK). Ссылка на групповой риск. Группировка рисков позволяет однократно
указвать условия для всех рисков группы.
Групповой риск создается автоматически процедурой INSERTRISK при вставке рисков (наследуется из правила)$COMM$;
COMMENT ON COLUMN ais.agrrisk.rptclassisn IS $COMM$FK(DICTI). Класс отчетности, устанавливается автоматически по классу риска$COMM$;
COMMENT ON COLUMN ais.agrrisk.classisn IS $COMM$FK(DICTI,РИСК). Класс риска по классификатору рисков.$COMM$;
COMMENT ON COLUMN ais.agrrisk.agrisn IS $COMM$FK(AGREEMENT). Ссылка на договор$COMM$;
COMMENT ON COLUMN ais.agrrisk.id IS $COMM$Номер параграфа, уникальный в пределах договора в формате 999.[99.]$COMM$;
COMMENT ON COLUMN ais.agrrisk.name IS $COMM$Полное название риска как в тексте договора. По умолчанию копируется из
классификатора.$COMM$;
COMMENT ON COLUMN ais.agrrisk.remark IS $COMM$Дополнительная информация, которая появляется в соответсвующем объекту параграфе
договора.$COMM$;
COMMENT ON COLUMN ais.agrrisk.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrrisk.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agrrisk.ruleisn IS $COMM$FK(RULE). Указатель правила, из которого вставлен риск$COMM$;
COMMENT ON COLUMN ais.agrrisk.insclassisn IS $COMM$FK(DICTI). Указатель вида страхования. Наследуется из правила.$COMM$;
COMMENT ON COLUMN ais.agrrisk.status IS $COMM$Статус риска. Определяет О-обязательность, Д/null-дополнительность или
Н-недопустимость риска$COMM$;


CREATE TABLE ais.agrrole (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    currisn                          NUMERIC,
    classisn                         NUMERIC,
    subjisn                          NUMERIC,
    sumclassisn                      NUMERIC,
    sumclassisn2                     NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    orderno                          NUMERIC(38),
    base                             NUMERIC(17,12),
    baseloss                         NUMERIC,
    sharepc                          NUMERIC(9,6),
    sharesum                         NUMERIC(20,2),
    calcflg                          VARCHAR(1) DEFAULT null,
    planfact                         VARCHAR(1) DEFAULT null,
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    formula                          VARCHAR(20),
    agentcondisn                     NUMERIC,
    objisn                           NUMERIC,
    riskisn                          NUMERIC,
    refundisn                        NUMERIC,
    agentagrisn                      NUMERIC,
    deptisn                          NUMERIC,
    collectflg                       VARCHAR(1),
    claimisn                         NUMERIC,
    addisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrrole IS $COMM$Роль субъекта по отношению к договору: страхователь, агент, перестраховщик.
У договора м.б. несколько субъектов, один субъект может выступать в нескольких ролях. [Вид взаимоотношений] с субъектом
определяет роль данного субъекта по умолчанию.$COMM$;
COMMENT ON COLUMN ais.agrrole.claimisn IS $COMM$FK(AgrClaim) Ссылка на убыток$COMM$;
COMMENT ON COLUMN ais.agrrole.addisn IS $COMM$FK(AGREEMENT). Указатель на аддендум$COMM$;
COMMENT ON COLUMN ais.agrrole.deptisn IS $COMM$FK(Subdept) подразделение для роли "продавец" (определяется по subjisn)$COMM$;
COMMENT ON COLUMN ais.agrrole.collectflg IS $COMM$Флаг инкассации премии$COMM$;
COMMENT ON COLUMN ais.agrrole.isn IS $COMM$Машинный номер: SEQ_AGRROLE.nextval$COMM$;
COMMENT ON COLUMN ais.agrrole.agrisn IS $COMM$FK(AGREEMENT). Указатель договора$COMM$;
COMMENT ON COLUMN ais.agrrole.currisn IS $COMM$FK(CURRENCY). Указатель валюты$COMM$;
COMMENT ON COLUMN ais.agrrole.classisn IS $COMM$FK(DICTI). Указатель роли участника$COMM$;
COMMENT ON COLUMN ais.agrrole.subjisn IS $COMM$FK(DICTI). Указатель участника$COMM$;
COMMENT ON COLUMN ais.agrrole.sumclassisn IS $COMM$FK(DICTI). Указатель класса суммы$COMM$;
COMMENT ON COLUMN ais.agrrole.sumclassisn2 IS $COMM$FK(DICTI). Указатель подкласса суммы$COMM$;
COMMENT ON COLUMN ais.agrrole.datebeg IS $COMM$Дата начала$COMM$;
COMMENT ON COLUMN ais.agrrole.dateend IS $COMM$Дата окончания$COMM$;
COMMENT ON COLUMN ais.agrrole.orderno IS $COMM$Порядковый номер: < 0 - входящие участники, > 0 - исходящие$COMM$;
COMMENT ON COLUMN ais.agrrole.base IS $COMM$Коэффициент относительно 100-процентно брутто-премии$COMM$;
COMMENT ON COLUMN ais.agrrole.baseloss IS $COMM$Коэффициент относительно 100-процентного убытка$COMM$;
COMMENT ON COLUMN ais.agrrole.sharepc IS $COMM$Доля в процентах$COMM$;
COMMENT ON COLUMN ais.agrrole.sharesum IS $COMM$Сумма доли$COMM$;
COMMENT ON COLUMN ais.agrrole.calcflg IS $COMM$Флаг включения в начисления$COMM$;
COMMENT ON COLUMN ais.agrrole.planfact IS $COMM$Индикатор: P-план, F-факт$COMM$;
COMMENT ON COLUMN ais.agrrole.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.agrrole.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrrole.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agrrole.formula IS $COMM$Строка вида S0 - S1. Даёт сумму, с которой считается доля ИГС$COMM$;
COMMENT ON COLUMN ais.agrrole.agentcondisn IS $COMM$FK(AGENTCOND). Указатель условия агентского соглашения)$COMM$;
COMMENT ON COLUMN ais.agrrole.objisn IS $COMM$FK(AgrObject). Cсылка на объект$COMM$;
COMMENT ON COLUMN ais.agrrole.riskisn IS $COMM$FK(AgrRisk).  Cсылка на риск, если не заполнена - от риска не зависит (относится ко всем рискам).$COMM$;
COMMENT ON COLUMN ais.agrrole.refundisn IS $COMM$FK(AgrRefund) Ссылка на претензию$COMM$;
COMMENT ON COLUMN ais.agrrole.agentagrisn IS $COMM$FK(Agreement) Ссылка на агентский договор$COMM$;


CREATE TABLE ais.agrservice (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    parentisn                        NUMERIC,
    classisn                         NUMERIC,
    code                             VARCHAR(30),
    name                             VARCHAR(255),
    price                            NUMERIC(20,2),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    status                           VARCHAR(1) DEFAULT 'N',
    createdby                        NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    flag                             VARCHAR(10),
    mnncode                          NUMERIC,
    addisn                           NUMERIC,
    newaddisn                        NUMERIC,
    remark                           VARCHAR(1000)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrservice IS $COMM$Услуга, оказываемая ЛПУ по договору о медобслуживании$COMM$;
COMMENT ON COLUMN ais.agrservice.addisn IS $COMM$аддендум, по которому добавлена услуга; null - для услуг по договору$COMM$;
COMMENT ON COLUMN ais.agrservice.newaddisn IS $COMM$аддендум, с которым услуга изменена$COMM$;
COMMENT ON COLUMN ais.agrservice.mnncode IS $COMM$Код справочника МНН, Попов В.О. 2009.04.21, задача 8195524803$COMM$;
COMMENT ON COLUMN ais.agrservice.isn IS $COMM$Машинный номер: SEQ_AGRSERVICE.next$COMM$;
COMMENT ON COLUMN ais.agrservice.agrisn IS $COMM$FK(AGREEMENT). Указатель договора с ЛПУ$COMM$;
COMMENT ON COLUMN ais.agrservice.parentisn IS $COMM$FK(AGRSERVICE). Указатель категории услуг$COMM$;
COMMENT ON COLUMN ais.agrservice.classisn IS $COMM$FK(DICTI). Указатель класса услуги$COMM$;
COMMENT ON COLUMN ais.agrservice.code IS $COMM$Код услуги в соответсвие с кодификатором ЛПУ$COMM$;
COMMENT ON COLUMN ais.agrservice.name IS $COMM$Наименование услуги$COMM$;
COMMENT ON COLUMN ais.agrservice.price IS $COMM$Цена услуги по прайс-листу ЛПУ$COMM$;
COMMENT ON COLUMN ais.agrservice.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrservice.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agrservice.status IS $COMM$Статус мед услуги
Y - выверенная
N - новая
A - архивная$COMM$;
COMMENT ON COLUMN ais.agrservice.createdby IS $COMM$Создатель$COMM$;
COMMENT ON COLUMN ais.agrservice.created IS $COMM$Время создания$COMM$;


CREATE TABLE ais.agrsum (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    agragrisn                        NUMERIC,
    addisn                           NUMERIC,
    agrisn                           NUMERIC,
    agrparisn                        NUMERIC,
    subjisn                          NUMERIC,
    currisn                          NUMERIC,
    rateisn                          NUMERIC,
    dateval                          TIMESTAMP,
    name                             VARCHAR(255),
    dbcr                             VARCHAR(1),
    amount                           NUMERIC(20,2),
    amountagragr                     NUMERIC(20,2),
    amountagr                        NUMERIC(20,2),
    amountpar                        NUMERIC(20,2),
    status                           VARCHAR(1),
    discr                            VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrsum IS $COMM$Ввиду наличия у суммы большого числа реквизитов для централизации обработки и контроля
целостности
все суммы договора (страховые суммы, лимиты ответственности, франшизы, премии, возмещения, возвраты)
сведены в общую таблицу.
К какой таблице относится данная сумма определяет дискриминатор [Discr].
Использование сквозного ISN для всех элементов договора позволяет искать суммы без ограничения на дискриминатор.$COMM$;
COMMENT ON COLUMN ais.agrsum.rateisn IS $COMM$FK(CURRATE). Указатель на курс суммы, в соответствии с которым рассчитано покрытие.$COMM$;
COMMENT ON COLUMN ais.agrsum.dateval IS $COMM$Дата валютирования, на с которую рассчитано покрытие.$COMM$;
COMMENT ON COLUMN ais.agrsum.name IS $COMM$Название суммы, уточняющее ее тип в конкретном случае.$COMM$;
COMMENT ON COLUMN ais.agrsum.dbcr IS $COMM$Индикатор дебета/кредита (Д/К) по отношению к субъекту взаиморасчетов: Д-нам платят, К-мы
платим, null-не участвует во взаиморасчетах.$COMM$;
COMMENT ON COLUMN ais.agrsum.amount IS $COMM$Сумма в валюте. Ставка или сумма в валюте должна быть задана.$COMM$;
COMMENT ON COLUMN ais.agrsum.amountagragr IS $COMM$Сумма в валюте договора$COMM$;
COMMENT ON COLUMN ais.agrsum.amountagr IS $COMM$Сумма в валюте элемента договора, к которому относится запись$COMM$;
COMMENT ON COLUMN ais.agrsum.amountpar IS $COMM$Сумма в валюте родительского элемента договора$COMM$;
COMMENT ON COLUMN ais.agrsum.status IS $COMM$Статус:
N-в работе,
Y-активная, учитывается в статистике и начислениях,
P-начислена (план)
F-оплачена (факт)
null-архив$COMM$;
COMMENT ON COLUMN ais.agrsum.discr IS $COMM$Дискриминатор записи, однозначно определяющий таблицу, в которой надо искать
запись-владельца:
A - AGREEMENT, O - AGROBJECT, R - AGRREFUND, P - AGRPERIOD, C - AGRCOND, D - аддендум$COMM$;
COMMENT ON COLUMN ais.agrsum.updated IS $COMM$Дата создания или изменения$COMM$;
COMMENT ON COLUMN ais.agrsum.updatedby IS $COMM$Автор создания или изменения$COMM$;
COMMENT ON COLUMN ais.agrsum.isn IS $COMM$Машинный номер: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrsum.classisn IS $COMM$FK(DICTI,AgrSum). Тип суммы (страховая сумма, франшиза, премия...).$COMM$;
COMMENT ON COLUMN ais.agrsum.agragrisn IS $COMM$FK(AGREEMENT). Указатель на договор, заполняется автоматически$COMM$;
COMMENT ON COLUMN ais.agrsum.addisn IS $COMM$FK(AGREEMENT). Указатель на аддендум, заполняется автоматически$COMM$;
COMMENT ON COLUMN ais.agrsum.agrisn IS $COMM$FK(AGR...). Ссылка на элемент договора. Тип элемента (таблица AGR...) задается
дискриминатором Discr.$COMM$;
COMMENT ON COLUMN ais.agrsum.agrparisn IS $COMM$FK(AGR...). Указатель родительского элемента договора для распространения покрытия:
AGRPERIOD(AGRCOND), AGRCLAIM(AGRREFUND), заполняется автоматически$COMM$;
COMMENT ON COLUMN ais.agrsum.subjisn IS $COMM$FK(SUBJECT). Указатель субъекта взаиморасчетов (один из субъектов договора).
Используется при начислениях.
Если субъектом является САО ИГС (текущая фирма - init.FirmISN), сумма относится ко всем дебиторам/кредиторам (в зависимости от
признака дебета/кредита).$COMM$;
COMMENT ON COLUMN ais.agrsum.currisn IS $COMM$FK(DICTI,Currency). Валюта суммы.$COMM$;


CREATE TABLE ais.agrtariff (
    isn                              NUMERIC,
    condisn                          NUMERIC,
    agrisn                           NUMERIC,
    tariffisn                        NUMERIC DEFAULT NULL,
    basetariff                       NUMERIC(9,6),
    adjustment                       NUMERIC(9,6),
    discount                         NUMERIC(9,6),
    tariff                           NUMERIC(20,6),
    franchtariff                     NUMERIC(9,6),
    franchsum                        NUMERIC(20,2),
    franchmaxsum                     NUMERIC(20,2),
    roundm                           NUMERIC(38),
    x1                               NUMERIC,
    x2                               NUMERIC,
    x3                               NUMERIC,
    x4                               NUMERIC,
    x5                               NUMERIC,
    remark                           VARCHAR(4000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    tariffclassisn                   NUMERIC,
    currisn                          NUMERIC,
    tariffnameisn                    NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.agrtariff.tariffnameisn IS $COMM$FK(DICTI) Метка узла тарифного дерева из СРТ, для коэффициента расчета.$COMM$;
COMMENT ON COLUMN ais.agrtariff.isn IS $COMM$Машинный номер,$COMM$;
COMMENT ON COLUMN ais.agrtariff.agrisn IS $COMM$FK(AGREEMENT). Указатель договора, принудительно наследуется из периода.$COMM$;
COMMENT ON COLUMN ais.agrtariff.tariffisn IS $COMM$Машинный номер записи: SEQ_RULTARIFF.nextval$COMM$;
COMMENT ON COLUMN ais.agrtariff.basetariff IS $COMM$Базовая тарифная ставка в %%$COMM$;
COMMENT ON COLUMN ais.agrtariff.adjustment IS $COMM$Поправочный коэффициент, на который умножается базовый тариф$COMM$;
COMMENT ON COLUMN ais.agrtariff.discount IS $COMM$Скидка/надбавка в %%$COMM$;
COMMENT ON COLUMN ais.agrtariff.tariff IS $COMM$Тарифная ставка в %%, по умолчанию: BaseTariff * Adjustment * (1+Discount/100), но
может устанавливаться вручную.$COMM$;
COMMENT ON COLUMN ais.agrtariff.franchtariff IS $COMM$Ставка франшизы в %%$COMM$;
COMMENT ON COLUMN ais.agrtariff.franchsum IS $COMM$Сумма франшизы абсолютная или минимальная, если задана максимальная$COMM$;
COMMENT ON COLUMN ais.agrtariff.franchmaxsum IS $COMM$Максимальная франшиза$COMM$;
COMMENT ON COLUMN ais.agrtariff.roundm IS $COMM$Масштаб округления тарифа, соответствует параметру m ф-ии ROUND(n,m)$COMM$;
COMMENT ON COLUMN ais.agrtariff.x1 IS $COMM$Параметры тарифов. Интерпретация зависит от типа тарифа$COMM$;
COMMENT ON COLUMN ais.agrtariff.remark IS $COMM$Наименование тарифа или примечание относительно особенностей использования данного
тарифа$COMM$;
COMMENT ON COLUMN ais.agrtariff.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrtariff.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agrtariff.tariffclassisn IS $COMM$Ссылка на класс тарифа, наследуеся из RULTARIFF ()$COMM$;
COMMENT ON COLUMN ais.agrtariff.currisn IS $COMM$FK(CURRENCY). Указатель валюты тарифа. (AL 18/04/03 )$COMM$;


CREATE TABLE ais.agrtparam (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    classisn                         NUMERIC,
    val                              VARCHAR(255),
    discr                            VARCHAR(1) DEFAULT 'I',
    datatype                         VARCHAR(10),
    no                               NUMERIC,
    groupid                          NUMERIC,
    updatedby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
    parentisn                        NUMERIC DEFAULT null
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrtparam IS $COMM$Тарифные параметры договора (котировки) - AL 9/08/06$COMM$;
COMMENT ON COLUMN ais.agrtparam.updatedby IS $COMM$Автор изменения записи.$COMM$;
COMMENT ON COLUMN ais.agrtparam.updated IS $COMM$Дата изменения записи.$COMM$;
COMMENT ON COLUMN ais.agrtparam.createdby IS $COMM$Автор создания записи.$COMM$;
COMMENT ON COLUMN ais.agrtparam.created IS $COMM$Дата создания записи.$COMM$;
COMMENT ON COLUMN ais.agrtparam.isn IS $COMM$Машинный номер записи: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.agrtparam.agrisn IS $COMM$FK(AGREEMENT). Ссылка на договор$COMM$;
COMMENT ON COLUMN ais.agrtparam.classisn IS $COMM$FK(DICTI). Класс параметра$COMM$;
COMMENT ON COLUMN ais.agrtparam.val IS $COMM$Значение параметра$COMM$;
COMMENT ON COLUMN ais.agrtparam.discr IS $COMM$Вид параметра: I - входной, O - выходной$COMM$;
COMMENT ON COLUMN ais.agrtparam.datatype IS $COMM$Тип данных параметра, наследуется по ClassISN->Dicti.Code, типы перечислены в Dicti (ParentISN=C.Get('PrmDataType')$COMM$;
COMMENT ON COLUMN ais.agrtparam.no IS $COMM$Порядковый номер для нескольких параметров одного класса$COMM$;
COMMENT ON COLUMN ais.agrtparam.groupid IS $COMM$Номер группы параметров (для расчета нескольких объектов)$COMM$;


CREATE TABLE ais.agrx (
    isn                              NUMERIC,
    reisn                            NUMERIC,
    sectisn                          NUMERIC,
    agrisn                           NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    objisn                           NUMERIC DEFAULT 0,
    riskisn                          NUMERIC DEFAULT 0,
    retentionsum                     NUMERIC(20,2),
    xpc                              NUMERIC(9,6),
    synisn                           NUMERIC,
    gr                               NUMERIC,
    grp                              NUMERIC,
    shareneorig                      NUMERIC(9,6),
    limitsum                         NUMERIC,
    tiv                              NUMERIC,
    psum2                            NUMERIC,
    calcdate                         TIMESTAMP
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.agrx IS $COMM$Отношение между договорами перестрахования (облигаторами) и перестрахованными по ним договорами.
Один договор может быть перестрахован по нескольким облигаторам. Изменение условий прямого договора может повлиять на характер
перестрахования, что отмечается датами начала и окончания.$COMM$;
COMMENT ON COLUMN ais.agrx.tiv IS $COMM$@purpose=результат функции getcrosscover(getaisumx(,objisn,riskisn))$COMM$;
COMMENT ON COLUMN ais.agrx.psum2 IS $COMM$@purpose=результат функции psum2$COMM$;
COMMENT ON COLUMN ais.agrx.shareneorig IS $COMM$@purpose=Доля неоригинальности$COMM$;
COMMENT ON COLUMN ais.agrx.calcdate IS $COMM$@purpose=Дата последнего расчета функ.данных. Для сверки необходимости пересчета$COMM$;
COMMENT ON COLUMN ais.agrx.limitsum IS $COMM$@purpose=Сумма лимита, результат функции getcrosscover(getaisumx(,objsin,riskisn,X)) $COMM$;
COMMENT ON COLUMN ais.agrx.isn IS $COMM$Машинный номер, SEQ_AGRX.nextval$COMM$;
COMMENT ON COLUMN ais.agrx.reisn IS $COMM$FK(Agreement). Указатель формуляра договора перестрахования, наследуется из секции$COMM$;
COMMENT ON COLUMN ais.agrx.sectisn IS $COMM$FK(ReSection). Указатель секции договора перестрахования$COMM$;
COMMENT ON COLUMN ais.agrx.agrisn IS $COMM$FK(Agreement). Указатель формуляра перестрахованного договора$COMM$;
COMMENT ON COLUMN ais.agrx.datebeg IS $COMM$Дата начала действия условий перестрахования данной секции для прямого договора,
наследуется из секции$COMM$;
COMMENT ON COLUMN ais.agrx.dateend IS $COMM$Дата окончания действия условий перестрахования данной секции для прямого договора,
наследуется из секции$COMM$;
COMMENT ON COLUMN ais.agrx.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.agrx.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.agrx.objisn IS $COMM$FK(AGROBJECT). Указатель перестрахованного объекта (обычно группового), если это не все
объекты$COMM$;
COMMENT ON COLUMN ais.agrx.riskisn IS $COMM$FK(AGRRISK). Указатель перестрахованного риска (обычно группового), если не все риски$COMM$;
COMMENT ON COLUMN ais.agrx.retentionsum IS $COMM$Сумма собственного удержания для данного прямого договора, перестрахованного по XL.
В этом случае собственное удержание XL является границей сверху, но не задает точного значения для всех прямых договоров.$COMM$;
COMMENT ON COLUMN ais.agrx.xpc IS $COMM$Доля по данному прямому договору, перестрахованная по данному перестраховочному$COMM$;
COMMENT ON COLUMN ais.agrx.synisn IS $COMM$FK(AGRX). Указатель AGRX при кумуляции риска$COMM$;


CREATE TABLE ais.aircraft (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    code                             VARCHAR(10),
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(255),
    maxweight                        NUMERIC,
    maxseats                         NUMERIC(38),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    active                           VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.aircraft IS $COMM$Справочник типов воздушных судов$COMM$;
COMMENT ON COLUMN ais.aircraft.isn IS $COMM$FK(DICTI). Указатель элемента словаря$COMM$;
COMMENT ON COLUMN ais.aircraft.parentisn IS $COMM$FK(AIRCRAFT). Групповой тип$COMM$;
COMMENT ON COLUMN ais.aircraft.code IS $COMM$Поисковый внешний код класса (для пользователя). Уникален в пределах суперкласса.$COMM$;
COMMENT ON COLUMN ais.aircraft.shortname IS $COMM$Краткое поисковое название класса для показа в LookUp.$COMM$;
COMMENT ON COLUMN ais.aircraft.fullname IS $COMM$Полное название класса для отчетов, пояснений. Использование зависит от
особенностей суперкласса.$COMM$;
COMMENT ON COLUMN ais.aircraft.maxweight IS $COMM$Максимальный взлетный вес (т.)$COMM$;
COMMENT ON COLUMN ais.aircraft.maxseats IS $COMM$Число посадочных мест$COMM$;
COMMENT ON COLUMN ais.aircraft.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.aircraft.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.ais2rca (
    isn                              NUMERIC,
    aisisn                           NUMERIC,
    rsaisn                           NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.ais2rca IS $COMM$@created=01.09.2010 @createdby=Бородин А.Ю. @seq=SEQ_DICX @purpose=Связь справочника марок и моделей РСА и АИС$COMM$;
COMMENT ON COLUMN ais.ais2rca.isn IS $COMM$@purpose=Уникальный идентификатор$COMM$;
COMMENT ON COLUMN ais.ais2rca.aisisn IS $COMM$@fk=AIS.CARMODEL(RESTRICT) @purpose=Ссылка на модификацию АИС$COMM$;
COMMENT ON COLUMN ais.ais2rca.rsaisn IS $COMM$@fk=AIS.RSAMODEL(RESTRICT) @purpose=Ссылка на модель РСА$COMM$;
COMMENT ON COLUMN ais.ais2rca.updated IS $COMM$@purpose=Дата изменения$COMM$;
COMMENT ON COLUMN ais.ais2rca.updatedby IS $COMM$@purpose=Пользователь$COMM$;


CREATE TABLE ais.audacalc (
    isn                              NUMERIC,
    currisn                          NUMERIC,
    subjisn                          NUMERIC,
    stationname                      VARCHAR(255),
    id                               VARCHAR(20),
    signed                           TIMESTAMP,
    claimsum                         NUMERIC(20,2),
    refundsum                        NUMERIC(20,2),
    rejectsum                        NUMERIC(20,2),
    persentage                       NUMERIC(9,6),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    status                           VARCHAR(1) DEFAULT 'N',
    remark                           VARCHAR(1000),
    no                               NUMERIC(38),
    dateval                          TIMESTAMP,
    createdby                        NUMERIC,
    created                          TIMESTAMP,
    calcdate                         TIMESTAMP,
    calcid                           VARCHAR(20),
    calcamount                       NUMERIC,
    calcuph                          NUMERIC,
    calcucost                        NUMERIC,
    actdate                          TIMESTAMP,
    actremark                        VARCHAR(1000),
    actexpert                        NUMERIC,
    orddate                          TIMESTAMP,
    ordremark                        VARCHAR(1000),
    ordpartsst                       VARCHAR(500),
    prfilename                       VARCHAR(40),
    emplisn                          NUMERIC,
    rultariffisn                     NUMERIC,
    okrsum                           NUMERIC(20,2),
    prncalc                          VARCHAR(1) DEFAULT 'Y',
    prntools                         VARCHAR(1) DEFAULT 'N',
    okrsumcurrisn                    NUMERIC,
    calcmode                         VARCHAR(1),
    causeisn                         NUMERIC,
    calcucostcurrisn                 NUMERIC,
    modelisn                         NUMERIC,
    classisn                         NUMERIC,
    parentisn                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.audacalc.parentisn IS $COMM$Ссылка на документ, на основании которого создан данный документ.Русов Р.В. 26,11,2007$COMM$;
COMMENT ON COLUMN ais.audacalc.classisn IS $COMM$FK(DICTI). Тип документа Русов Р.В. 12.11.2007$COMM$;
COMMENT ON COLUMN ais.audacalc.causeisn IS $COMM$Причина выбора неприоритетной СТО (Угринович А.Н.)$COMM$;
COMMENT ON COLUMN ais.audacalc.calcucostcurrisn IS $COMM$(FK CURRENCY) Валюта стоимости единицы работы (Русов Р.В.)$COMM$;
COMMENT ON COLUMN ais.audacalc.modelisn IS $COMM$FK(CARMODEL). Указатель модели автотранспорта(Русов Р.В.)$COMM$;
COMMENT ON COLUMN ais.audacalc.currisn IS $COMM$(FK CURRENCY) Валюта калькуляции$COMM$;
COMMENT ON COLUMN ais.audacalc.subjisn IS $COMM$Ссылка на СТО$COMM$;
COMMENT ON COLUMN ais.audacalc.stationname IS $COMM$Название станции технического обслуживания. Если SubjISN != NULL => копируется из справочника$COMM$;
COMMENT ON COLUMN ais.audacalc.id IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalc.signed IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalc.claimsum IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalc.refundsum IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalc.rejectsum IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalc.persentage IS $COMM$наценка на детали (Андрияхин А. 22.04.2013)$COMM$;
COMMENT ON COLUMN ais.audacalc.status IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalc.remark IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalc.no IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalc.dateval IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalc.createdby IS $COMM$Владелец записи$COMM$;
COMMENT ON COLUMN ais.audacalc.created IS $COMM$Дата создания$COMM$;
COMMENT ON COLUMN ais.audacalc.calcdate IS $COMM$Дата калькуляции$COMM$;
COMMENT ON COLUMN ais.audacalc.calcid IS $COMM$Номер калькуляции$COMM$;
COMMENT ON COLUMN ais.audacalc.calcamount IS $COMM$Сумма калькуляции$COMM$;
COMMENT ON COLUMN ais.audacalc.calcuph IS $COMM$Норма, ед/час$COMM$;
COMMENT ON COLUMN ais.audacalc.calcucost IS $COMM$Стоимость единицы работы$COMM$;
COMMENT ON COLUMN ais.audacalc.actdate IS $COMM$Дата акта осмотра ТС$COMM$;
COMMENT ON COLUMN ais.audacalc.actremark IS $COMM$Примечание экперта$COMM$;
COMMENT ON COLUMN ais.audacalc.actexpert IS $COMM$(FK SUBJECT) Экперт$COMM$;
COMMENT ON COLUMN ais.audacalc.orddate IS $COMM$Дата направления на СТО$COMM$;
COMMENT ON COLUMN ais.audacalc.ordremark IS $COMM$Примечание куратора$COMM$;
COMMENT ON COLUMN ais.audacalc.ordpartsst IS $COMM$Предписание по сохранению замененных деталей$COMM$;
COMMENT ON COLUMN ais.audacalc.prfilename IS $COMM$Название файла импорт AUDAPAD$COMM$;
COMMENT ON COLUMN ais.audacalc.emplisn IS $COMM$Куратор$COMM$;
COMMENT ON COLUMN ais.audacalc.rultariffisn IS $COMM$(FK RULTARIFF) Ссылка на тарифные коэффициенты для расчета калькуляции$COMM$;
COMMENT ON COLUMN ais.audacalc.okrsum IS $COMM$Стоимость лакокрасочных материалов по факту. Если пусто то стомость равна REFUNDSUM$COMM$;
COMMENT ON COLUMN ais.audacalc.prncalc IS $COMM$Печатать в калькуляции примечание эксперта?$COMM$;
COMMENT ON COLUMN ais.audacalc.prntools IS $COMM$Печатать в направлении примечание эксперта?$COMM$;
COMMENT ON COLUMN ais.audacalc.okrsumcurrisn IS $COMM$(FK CURRENCY) Валюта суммы окраски$COMM$;
COMMENT ON COLUMN ais.audacalc.calcmode IS $COMM$Вид загрузки. -- Rusov 25/10/06 'A' - по AZT 'P' - по производителю$COMM$;


CREATE TABLE ais.audacalcline (
    isn                              NUMERIC,
    invoiceisn                       NUMERIC,
    id                               VARCHAR(20),
    shortname                        VARCHAR(40),
    amount                           NUMERIC(20,2),
    includeamount                    NUMERIC(20,2),
    percentage                       NUMERIC(9,6),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    currisn                          NUMERIC,
    curroutisn                       NUMERIC,
    refundisn                        NUMERIC,
    remark                           VARCHAR(1000),
    signed                           TIMESTAMP,
    vat                              NUMERIC(9,6),
    discount                         NUMERIC(9,6),
    classisn                         NUMERIC,
    mask                             BIGINT DEFAULT 1,
    fullname                         VARCHAR(200),
    workunits                        NUMERIC,
    amountsto                        NUMERIC,
    spf                              NUMERIC,
    partid                           VARCHAR(20),
    nextisn                          NUMERIC,
    fid                              VARCHAR(40),
    docid                            NUMERIC,
    unitclassisn                     NUMERIC,
    conventionalunit                 NUMERIC,
    modifictype                      VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.audacalcline.amountsto IS $COMM$Стоимость прайс-листа СТО. Заполняется из справочника цен СТО. AL 22/05/06$COMM$;
COMMENT ON COLUMN ais.audacalcline.spf IS $COMM$Spare Part Factor - Коэффициент запчастей (КЗЧ). AL 22/05/06$COMM$;
COMMENT ON COLUMN ais.audacalcline.partid IS $COMM$Код детали в AudaPad. -- Rusov 22/09/06$COMM$;
COMMENT ON COLUMN ais.audacalcline.modifictype IS $COMM$Rusov 15-03-2013.Признак ручной модификации строки экспертом при составлении калькуляции в AudaTex. «P» - Модификация стоимости, «C» - Модификация кода детали, «W» - Модификация работ$COMM$;
COMMENT ON COLUMN ais.audacalcline.invoiceisn IS $COMM$(FK AUDACALC)$COMM$;
COMMENT ON COLUMN ais.audacalcline.id IS $COMM$Заводской номер$COMM$;
COMMENT ON COLUMN ais.audacalcline.shortname IS $COMM$Описание ремонтной работы$COMM$;
COMMENT ON COLUMN ais.audacalcline.amount IS $COMM$Заявленная сумма$COMM$;
COMMENT ON COLUMN ais.audacalcline.includeamount IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalcline.percentage IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalcline.currisn IS $COMM$FK(DICTI,ВАЛЮТА). Машинный номер объекта, совпадает с ISN соответствующей записи в словаре DICTI.$COMM$;
COMMENT ON COLUMN ais.audacalcline.curroutisn IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalcline.refundisn IS $COMM$Ссылка на претензию$COMM$;
COMMENT ON COLUMN ais.audacalcline.remark IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalcline.signed IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalcline.vat IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalcline.discount IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalcline.classisn IS $COMM$Не используется$COMM$;
COMMENT ON COLUMN ais.audacalcline.mask IS $COMM$Маска вида документа/вида ремонтного воздействия$COMM$;
COMMENT ON COLUMN ais.audacalcline.fullname IS $COMM$Описание работы, детали и т.п.$COMM$;
COMMENT ON COLUMN ais.audacalcline.workunits IS $COMM$Количество единиц работы$COMM$;
COMMENT ON COLUMN ais.audacalcline.unitclassisn IS $COMM$FK(DICTI). Класс работ или деталей. -- Rusov 12/11/07$COMM$;
COMMENT ON COLUMN ais.audacalcline.conventionalunit IS $COMM$Кол-во расходных материалов на окраску одной условной детали Rusov 18.02.2008$COMM$;
COMMENT ON COLUMN ais.audacalcline.nextisn IS $COMM$Ссылка на позицию документа присланного из СТО.Rusov 19.09.2007$COMM$;
COMMENT ON COLUMN ais.audacalcline.fid IS $COMM$Код детали, уникальный в EveryCar$COMM$;
COMMENT ON COLUMN ais.audacalcline.docid IS $COMM$Номер документа присланного из СТО. Rusov 18,09,2007$COMM$;


CREATE TABLE ais.bso_agrid (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    agrisn                           NUMERIC,
    objisn                           NUMERIC,
    previsn                          NUMERIC,
    subjisn                          NUMERIC,
    statusisn                        NUMERIC,
    agrid                            VARCHAR(20),
    datesign                         TIMESTAMP,
    docser                           VARCHAR(20),
    docno                            NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC,
    headisn                          NUMERIC,
    dateclose                        TIMESTAMP,
    deptisn                          NUMERIC,
    addisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.bso_agrid IS $COMM$Таблица бланков БСО
<<Новое>>
Морин М.А.$COMM$;
COMMENT ON COLUMN ais.bso_agrid.addisn IS $COMM$FK(AGREEMENT). Указатель на аддендум$COMM$;
COMMENT ON COLUMN ais.bso_agrid.classisn IS $COMM$FK(Dicti), тип номера (договорб знак)$COMM$;
COMMENT ON COLUMN ais.bso_agrid.agrisn IS $COMM$Машинный номер объекта. Устанавливается по умолчанию равным SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.bso_agrid.objisn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.bso_agrid.subjisn IS $COMM$FK(DICTI,СУБЪЕКТ). Машинный номер объекта, совпадает с ISN соответствующей записи в словаре DICTI.$COMM$;
COMMENT ON COLUMN ais.bso_agrid.statusisn IS $COMM$Машинный номер объекта.
Устанавливается по умолчанию равным SEQ_DICTI.nextval.
Совпадает с ISN соответствующего объекта, имеющего отдельную таблицу для хранения дополнительных полей.$COMM$;
COMMENT ON COLUMN ais.bso_agrid.headisn IS $COMM$указывает на последнию операцию (AGRIDHEAD)$COMM$;
COMMENT ON COLUMN ais.bso_agrid.dateclose IS $COMM$дата закрытия отчетного периода$COMM$;
COMMENT ON COLUMN ais.bso_agrid.deptisn IS $COMM$ССЫЛКА НА подразделение за которым числется в случае если subjisn вне оргструктуры$COMM$;


CREATE TABLE ais.bso_agriddoc (
    isn                              NUMERIC,
    datereg                          TIMESTAMP,
    id                               VARCHAR(40),
    status                           VARCHAR(1),
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    subjisn                          NUMERIC,
    vatsum                           NUMERIC,
    vat                              NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.bso_agriddoc.vatsum IS $COMM$сумма за партию с НДС$COMM$;
COMMENT ON COLUMN ais.bso_agriddoc.vat IS $COMM$процент НДС (Value added tax)$COMM$;
COMMENT ON COLUMN ais.bso_agriddoc.isn IS $COMM$SEQ_AGRIDHEAD$COMM$;
COMMENT ON COLUMN ais.bso_agriddoc.datereg IS $COMM$дата регистрации$COMM$;
COMMENT ON COLUMN ais.bso_agriddoc.id IS $COMM$рег. номер$COMM$;


CREATE TABLE ais.bso_agridhead (
    isn                              NUMERIC,
    datereg                          TIMESTAMP DEFAULT current_timestamp,
    docser                           VARCHAR(20),
    emplisn                          NUMERIC,
    subjisn                          NUMERIC,
    classisn                         NUMERIC,
    operisn                          NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    id                               VARCHAR(20),
    price                            NUMERIC,
    dateclose                        TIMESTAMP,
    subjdeptisn                      NUMERIC,
    empldeptisn                      NUMERIC,
    status                           VARCHAR(1),
    docisn                           NUMERIC,
    listno                           TEXT,
    redisn                           NUMERIC,
    type_oper                        NUMERIC,
    dateval                          TIMESTAMP
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.bso_agridhead.datereg IS $COMM$Дата операции$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.docser IS $COMM$Серия$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.emplisn IS $COMM$кто выдал$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.subjisn IS $COMM$кому выдан$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.classisn IS $COMM$тип бланка$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.operisn IS $COMM$операция$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.id IS $COMM$№ накладной$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.price IS $COMM$стоимоть бланка$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.dateclose IS $COMM$дата закрытия отчетного периода$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.subjdeptisn IS $COMM$подразделение кому$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.empldeptisn IS $COMM$подразделение от кого$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.status IS $COMM$для отметки закрытого периода: null-не выгружалась, Y-в бухгалтерии, S-удалена (была в состоянии Y)$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.docisn IS $COMM$(FK) AGRIDDOC$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.redisn IS $COMM$ссылка на стороняемую(забракованную) операцию$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.type_oper IS $COMM$-3 ОПЕРАЦИЯ БЕЗ ОРГАНИЗАЦИИ ШАПКИ; -2 УДАЛЕНИЕ; -1: сторняемая операция; 0: нормальная; 1: сторняющая операция$COMM$;
COMMENT ON COLUMN ais.bso_agridhead.dateval IS $COMM$дата валютирования (заполняется только при Insert)$COMM$;


CREATE TABLE ais.bso_agridx (
    isn                              NUMERIC,
    disn                             NUMERIC,
    hisn                             NUMERIC,
    statusisn                        NUMERIC,
    operisn                          NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC,
    dateoper                         TIMESTAMP,
    dateval                          TIMESTAMP DEFAULT current_timestamp,
    emplisn                          NUMERIC,
    subjisn                          NUMERIC,
    empldeptisn                      NUMERIC,
    subjdeptisn                      NUMERIC,
    agrisn                           NUMERIC,
    redisn                           NUMERIC,
    type_oper                        NUMERIC,
    addisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.bso_agridx.isn IS $COMM$Счетчик$COMM$;
COMMENT ON COLUMN ais.bso_agridx.disn IS $COMM$Agrid.Isn$COMM$;
COMMENT ON COLUMN ais.bso_agridx.hisn IS $COMM$Agridhead.Isn$COMM$;
COMMENT ON COLUMN ais.bso_agridx.statusisn IS $COMM$Статус бланка на момент формирования$COMM$;
COMMENT ON COLUMN ais.bso_agridx.operisn IS $COMM$Тип операции на момент формирования$COMM$;
COMMENT ON COLUMN ais.bso_agridx.dateoper IS $COMM$Дата:Время операции (равна bso_agridhead.datereg и bso_agrid.datesign и в случае операции по использованию agreement.dateissue)$COMM$;
COMMENT ON COLUMN ais.bso_agridx.dateval IS $COMM$Дата валютирования$COMM$;
COMMENT ON COLUMN ais.bso_agridx.emplisn IS $COMM$кто выдал$COMM$;
COMMENT ON COLUMN ais.bso_agridx.subjisn IS $COMM$кто принял$COMM$;
COMMENT ON COLUMN ais.bso_agridx.empldeptisn IS $COMM$подразделение кто выдал$COMM$;
COMMENT ON COLUMN ais.bso_agridx.subjdeptisn IS $COMM$подразделение кто получил$COMM$;
COMMENT ON COLUMN ais.bso_agridx.agrisn IS $COMM$ссылка на договор если есть$COMM$;
COMMENT ON COLUMN ais.bso_agridx.redisn IS $COMM$ссылка на operisn сторнируемой операции$COMM$;
COMMENT ON COLUMN ais.bso_agridx.type_oper IS $COMM$-2 УДАЛЕНИЕ -1: сторнированная операция; 0 - нормальная пока; 1- сторнирующая$COMM$;
COMMENT ON COLUMN ais.bso_agridx.addisn IS $COMM$FK(AGREEMENT). Указатель на аддендум$COMM$;


CREATE TABLE ais.buhbody_t (
    isn                              NUMERIC,
    headisn                          NUMERIC,
    currisn                          NUMERIC,
    subaccisn                        NUMERIC,
    parentisn                        NUMERIC,
    groupisn                         NUMERIC,
    deptisn                          NUMERIC,
    subjisn                          NUMERIC,
    classisn                         NUMERIC,
    fid                              NUMERIC(38),
    code                             VARCHAR(10),
    dateval                          TIMESTAMP,
    damount                          NUMERIC(20,2),
    damountrub                       NUMERIC(20,2),
    camount                          NUMERIC(20,2),
    camountrub                       NUMERIC(20,2),
    remain                           NUMERIC(20,2),
    remainrub                        NUMERIC(20,2),
    status                           VARCHAR(1),
    quitstatus                       VARCHAR(1),
    subjname                         VARCHAR(255),
    remark                           VARCHAR(255),
    agrid                            VARCHAR(20),
    docid                            VARCHAR(25),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    oprisn                           NUMERIC,
    daterate                         TIMESTAMP,
    docisn                           NUMERIC,
    datequit                         TIMESTAMP,
    subkindisn                       NUMERIC,
    damountusd                       NUMERIC(20,2),
    camountusd                       NUMERIC(20,2),
    convisn                          NUMERIC,
    agrisn                           NUMERIC,
    planisn                          NUMERIC,
    subnumisn                        NUMERIC,
    docitemisn                       NUMERIC,
    fobjisn                          NUMERIC,
    dateevent                        TIMESTAMP
)
;
--WARNING: No primary key defined for ais.buhbody_t

COMMENT ON TABLE ais.buhbody_t IS $COMM$Непосредственно полупроводки$COMM$;
COMMENT ON COLUMN ais.buhbody_t.subjname IS $COMM$Имя клиента с которым связана эта сумма$COMM$;
COMMENT ON COLUMN ais.buhbody_t.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.buhbody_t.agrid IS $COMM$Номер договора или другой бизнес документ, являющейся причиной появления данной
проводки$COMM$;
COMMENT ON COLUMN ais.buhbody_t.docid IS $COMM$Номер счета - убытка  ( платежный документ/документы, с которым связана данная сумма)$COMM$;
COMMENT ON COLUMN ais.buhbody_t.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.buhbody_t.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.buhbody_t.oprisn IS $COMM$Ссылка на операцию$COMM$;
COMMENT ON COLUMN ais.buhbody_t.daterate IS $COMM$Дата пересчета рублевого эквивалента.
Используется для автоматической поддержки.$COMM$;
COMMENT ON COLUMN ais.buhbody_t.docisn IS $COMM$FK(DOCS). Указатель документа, на основании которого сделана полупроводка$COMM$;
COMMENT ON COLUMN ais.buhbody_t.datequit IS $COMM$Дата квитовки (отслеживается по группе квитовки)$COMM$;
COMMENT ON COLUMN ais.buhbody_t.subkindisn IS $COMM$Ссылка на аналитику$COMM$;
COMMENT ON COLUMN ais.buhbody_t.convisn IS $COMM$Ссылка на головку проводки$COMM$;
COMMENT ON COLUMN ais.buhbody_t.agrisn IS $COMM$FK(AGREEMENT). Указатель договора страхования$COMM$;
COMMENT ON COLUMN ais.buhbody_t.planisn IS $COMM$FK(DICTI) Класс плана счетов, к которому относится проводка$COMM$;
COMMENT ON COLUMN ais.buhbody_t.dateevent IS $COMM$Дата события$COMM$;
COMMENT ON COLUMN ais.buhbody_t.currisn IS $COMM$Ссылка на валюту$COMM$;
COMMENT ON COLUMN ais.buhbody_t.subaccisn IS $COMM$Ссылка на субсчет$COMM$;
COMMENT ON COLUMN ais.buhbody_t.parentisn IS $COMM$Ссылка на первичную проводку, используется для привязки проводок по отведению
курсовой разницы.$COMM$;
COMMENT ON COLUMN ais.buhbody_t.groupisn IS $COMM$FK(DOCGRP). Ссылка на группу квитовки$COMM$;
COMMENT ON COLUMN ais.buhbody_t.deptisn IS $COMM$FK(SUBDEPT). Ссылка на подразделение$COMM$;
COMMENT ON COLUMN ais.buhbody_t.subjisn IS $COMM$FK(SUBJECT). Ссылка на субъекта$COMM$;
COMMENT ON COLUMN ais.buhbody_t.classisn IS $COMM$FK(DICTI). Объект проводки$COMM$;
COMMENT ON COLUMN ais.buhbody_t.fid IS $COMM$FID - ссылка на P_N$COMM$;
COMMENT ON COLUMN ais.buhbody_t.damount IS $COMM$Сумма в валюте по дебету$COMM$;
COMMENT ON COLUMN ais.buhbody_t.damountrub IS $COMM$Рублевое покрытие по дебету$COMM$;
COMMENT ON COLUMN ais.buhbody_t.camount IS $COMM$Сумма в валюте по кредиту$COMM$;
COMMENT ON COLUMN ais.buhbody_t.camountrub IS $COMM$Рублевое покрытие по кредиту$COMM$;
COMMENT ON COLUMN ais.buhbody_t.remain IS $COMM$Остаток в валюте$COMM$;
COMMENT ON COLUMN ais.buhbody_t.remainrub IS $COMM$Остаток в рублях$COMM$;
COMMENT ON COLUMN ais.buhbody_t.status IS $COMM$Статус в теле проводки является удобным местом для
 отражения архивации полупроводки (Null - архивная, У - удаленная, П - плановая,  А - активная)$COMM$;
COMMENT ON COLUMN ais.buhbody_t.quitstatus IS $COMM$В зависимости от признака квитовки может определяться и возможнсть отведения
курсовой разницы.$COMM$;
COMMENT ON COLUMN ais.buhbody_t.isn IS $COMM$Машинный номер объекта. Устанавливается по умолчанию равным SEQ_BUHBODY.nextval$COMM$;
COMMENT ON COLUMN ais.buhbody_t.headisn IS $COMM$Ссылка на головку$COMM$;


CREATE TABLE ais.buhgrant (
    isn                              NUMERIC,
    emplisn                          NUMERIC,
    classisn                         NUMERIC,
    buhbeg                           TIMESTAMP,
    buhend                           TIMESTAMP,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    remark                           VARCHAR(255),
    status                           VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.buhgrant IS $COMM$Права сотрудника по проводкам с датой валютирования из заданного диапазона на заданный
временной интервал с ограничениям по счетам или операциям$COMM$;
COMMENT ON COLUMN ais.buhgrant.isn IS $COMM$Машинный номер: SEQ_BUHGRANT.nextval$COMM$;
COMMENT ON COLUMN ais.buhgrant.emplisn IS $COMM$FK(SUBJECT). Указатель сотрудника$COMM$;
COMMENT ON COLUMN ais.buhgrant.classisn IS $COMM$FK(DICTI). Указатель класса объекта защиты (субсчет, операция )$COMM$;
COMMENT ON COLUMN ais.buhgrant.buhbeg IS $COMM$Начало разрешенного диапазона дат проводок$COMM$;
COMMENT ON COLUMN ais.buhgrant.buhend IS $COMM$Конец разрешенного диапазона дат проводок$COMM$;
COMMENT ON COLUMN ais.buhgrant.datebeg IS $COMM$Начало срока действия разрешения$COMM$;
COMMENT ON COLUMN ais.buhgrant.dateend IS $COMM$Конец срока действия разрешения$COMM$;
COMMENT ON COLUMN ais.buhgrant.remark IS $COMM$Коментарий$COMM$;
COMMENT ON COLUMN ais.buhgrant.status IS $COMM$Статус: B-базовый, A-дополнительный, временно выданный$COMM$;
COMMENT ON COLUMN ais.buhgrant.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.buhgrant.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.buhhead_t (
    isn                              NUMERIC,
    currisn                          NUMERIC,
    oprisn                           NUMERIC,
    fid                              NUMERIC(38),
    dateval                          TIMESTAMP,
    remark                           VARCHAR(255),
    status                           VARCHAR(1),
    created                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC(38),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    docisn                           NUMERIC,
    planisn                          NUMERIC,
    templeisn                        NUMERIC,
    bitledgers                       NUMERIC,
    docitemisn                       NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.buhhead_t IS $COMM$Хранится информация о валюте, операции,
дате проводки, о ее предворительности
Главный вопрос -  как определять множество остатков
для синхронизации
$COMM$;
COMMENT ON COLUMN ais.buhhead_t.currisn IS $COMM$Ссылка на валюту$COMM$;
COMMENT ON COLUMN ais.buhhead_t.oprisn IS $COMM$Ссылка на  операцию$COMM$;
COMMENT ON COLUMN ais.buhhead_t.fid IS $COMM$FID-должен определять CODE_PROV$COMM$;
COMMENT ON COLUMN ais.buhhead_t.dateval IS $COMM$Дата проводки$COMM$;
COMMENT ON COLUMN ais.buhhead_t.remark IS $COMM$Коментарий$COMM$;
COMMENT ON COLUMN ais.buhhead_t.status IS $COMM$Статус$COMM$;
COMMENT ON COLUMN ais.buhhead_t.created IS $COMM$Дата создания$COMM$;
COMMENT ON COLUMN ais.buhhead_t.createdby IS $COMM$Создатель записи$COMM$;
COMMENT ON COLUMN ais.buhhead_t.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.buhhead_t.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.buhhead_t.docisn IS $COMM$FK(DOCS). Указатель документа, на основании которого сделана проваодка$COMM$;
COMMENT ON COLUMN ais.buhhead_t.planisn IS $COMM$FK(DICTI). Класс плана счетов, к которому относится проводка$COMM$;


CREATE TABLE ais.buhsubacc_t (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    code                             VARCHAR(10),
    id                               VARCHAR(10),
    n_children                       NUMERIC(38),
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(255),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    datemod                          TIMESTAMP,
    datequit                         TIMESTAMP,
    active                           VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    currisn                          NUMERIC,
    deptflag                         VARCHAR(1),
    humanflag                        VARCHAR(1),
    buhkindisn                       NUMERIC,
    emplisn                          NUMERIC,
    remark                           VARCHAR(255),
    classisn                         NUMERIC,
    bitledgers                       NUMERIC,
    actpas                           VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.buhsubacc_t.actpas IS $COMM$Признак активности/пассивности субчета. =А - активный, =П - пассивный, =null активно-пассивный$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.code IS $COMM$Код аналитического учета$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.id IS $COMM$Полный номер субсчета$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.shortname IS $COMM$Краткое имя$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.fullname IS $COMM$Полное название$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.datebeg IS $COMM$Начало действия$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.dateend IS $COMM$Конец действия$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.datemod IS $COMM$Граничная дата изменений.
Суммы с датой проводки меньше данной не могут быть модифицированы или удалены.$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.datequit IS $COMM$Граничная дата квитовки.
Суммы из разных периодов, определенных в этом поле не могут квитоваться$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.active IS $COMM$Признак активности$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.buhkindisn IS $COMM$FK(BUHKIND) Ссылка на вид аналитики$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.emplisn IS $COMM$Ответственный за счет$COMM$;
COMMENT ON COLUMN ais.buhsubacc_t.classisn IS $COMM$FK(DICTI). Класс (тип) плана счетов: основной ИГС, другой компании и т.д.$COMM$;


CREATE TABLE ais.cardamage (
    isn                              NUMERIC,
    surveyisn                        NUMERIC,
    partisn                          NUMERIC,
    damageisn                        NUMERIC,
    degree                           SMALLINT,
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    partname                         VARCHAR(200)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.cardamage IS $COMM$Список повреждений, обнаруженных при осмотре транспортного средства$COMM$;
COMMENT ON COLUMN ais.cardamage.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.cardamage.partname IS $COMM$Наименование типа детали$COMM$;
COMMENT ON COLUMN ais.cardamage.isn IS $COMM$Машинный номер: SEQ_CARDAMAGE$COMM$;
COMMENT ON COLUMN ais.cardamage.surveyisn IS $COMM$FK(CARSURVEY). Указатель акта осмотра$COMM$;
COMMENT ON COLUMN ais.cardamage.partisn IS $COMM$FK(DICTI). Указатель типа детали$COMM$;
COMMENT ON COLUMN ais.cardamage.damageisn IS $COMM$FK(DICTI). Указатель характера повреждения$COMM$;
COMMENT ON COLUMN ais.cardamage.degree IS $COMM$Степень повреждения (в процентах)$COMM$;
COMMENT ON COLUMN ais.cardamage.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.cardamage.updated IS $COMM$Дата изменения$COMM$;


CREATE TABLE ais.carequip (
    isn                              NUMERIC,
    surveyisn                        NUMERIC,
    classisn                         NUMERIC,
    manufactisn                      NUMERIC,
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.carequip IS $COMM$Список доп. оборудования (в т. ч. противоугонных систем) транспортного средства$COMM$;
COMMENT ON COLUMN ais.carequip.isn IS $COMM$Машинный номер: SEQ_CAREQUIP$COMM$;
COMMENT ON COLUMN ais.carequip.surveyisn IS $COMM$FK(CARSURVEY). Указатель акта осмотра$COMM$;
COMMENT ON COLUMN ais.carequip.classisn IS $COMM$FK(DICTI). Ссылка на тип оборудования$COMM$;
COMMENT ON COLUMN ais.carequip.manufactisn IS $COMM$FK(SUBJECT). Ссылка на производителя оборудования$COMM$;
COMMENT ON COLUMN ais.carequip.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.carequip.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.carequip.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.carmodel (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    enginetype                       VARCHAR(1) DEFAULT 'P',
    enginepowerkw                    NUMERIC(38),
    enginepowerhp                    NUMERIC,
    enginevolume                     NUMERIC(38),
    enginemaxvol                     NUMERIC(38),
    doors                            NUMERIC(38),
    seats                            NUMERIC(38),
    maxload                          NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    modification                     VARCHAR(40),
    bodytype                         VARCHAR(40),
    createdby                        NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
    tariffgroupisn                   NUMERIC,
    transmission                     VARCHAR(40),
    typeisn                          NUMERIC,
    protectionisn                    NUMERIC,
    grossweigh                       NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    netweight                        NUMERIC,
    warrantyperiod                   NUMERIC,
    warrantykilometrage              NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.carmodel IS $COMM$Классификатор моделей автотранспорта. Иерархия тип-марка-модель-модификация (тип кузова)
является частью классификатора объектов страхования в словаре DICTI$COMM$;
COMMENT ON COLUMN ais.carmodel.protectionisn IS $COMM$FK(Dicti) Класс защиты. Yunin V.A. 11/05/05$COMM$;
COMMENT ON COLUMN ais.carmodel.warrantyperiod IS $COMM$Гарантийный период (в месяцах) (Yunin V.A. 13/12/10)$COMM$;
COMMENT ON COLUMN ais.carmodel.warrantykilometrage IS $COMM$Гарантийный пробег (в км.) (Yunin V.A. 13/12/10)$COMM$;
COMMENT ON COLUMN ais.carmodel.isn IS $COMM$Машинный номер: SEQ_CARMODEL.nextval$COMM$;
COMMENT ON COLUMN ais.carmodel.classisn IS $COMM$FK(DICTI). Указатель марки-модели-модификации автотранспорта$COMM$;
COMMENT ON COLUMN ais.carmodel.enginetype IS $COMM$Тип двигателя: D-дизель, P-бензиновый$COMM$;
COMMENT ON COLUMN ais.carmodel.enginepowerkw IS $COMM$Мощность двигателя, кВт$COMM$;
COMMENT ON COLUMN ais.carmodel.enginepowerhp IS $COMM$Мощность двигателя, л.с.$COMM$;
COMMENT ON COLUMN ais.carmodel.enginevolume IS $COMM$Объем двигателя, куб.см.$COMM$;
COMMENT ON COLUMN ais.carmodel.enginemaxvol IS $COMM$Максимальный объем двигателя, куб.см., если задается диапазон$COMM$;
COMMENT ON COLUMN ais.carmodel.doors IS $COMM$Количество дверей$COMM$;
COMMENT ON COLUMN ais.carmodel.seats IS $COMM$Число посадочных мест$COMM$;
COMMENT ON COLUMN ais.carmodel.maxload IS $COMM$Грузоподъемность, т.$COMM$;
COMMENT ON COLUMN ais.carmodel.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.carmodel.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.carmodel.modification IS $COMM$Модификация модели$COMM$;
COMMENT ON COLUMN ais.carmodel.bodytype IS $COMM$Тип кузова$COMM$;
COMMENT ON COLUMN ais.carmodel.createdby IS $COMM$Создатель$COMM$;
COMMENT ON COLUMN ais.carmodel.created IS $COMM$Дата создания записи, трактуется как дата начала сотрудничества$COMM$;
COMMENT ON COLUMN ais.carmodel.tariffgroupisn IS $COMM$FK(DICTI). Ссылка на тарифную группу$COMM$;
COMMENT ON COLUMN ais.carmodel.transmission IS $COMM$Текстовое описание коробки передач$COMM$;
COMMENT ON COLUMN ais.carmodel.typeisn IS $COMM$FK(DICTI). Тип ТС. --Yunin V.A. 14/09/04$COMM$;
COMMENT ON COLUMN ais.carmodel.datebeg IS $COMM$Началало производства модификации -- Rusov 04.04.06$COMM$;
COMMENT ON COLUMN ais.carmodel.dateend IS $COMM$Окончание производства модификации -- Rusov 04.04.06$COMM$;
COMMENT ON COLUMN ais.carmodel.netweight IS $COMM$Масса ТС без нагрузки (т). -- Yunin V.A. 10/05/07$COMM$;
COMMENT ON COLUMN ais.carmodel.grossweigh IS $COMM$Максимально разрешенная масса (т.)  -- Yunin V.A. 15/12/05$COMM$;


CREATE TABLE ais.carsurvey (
    isn                              NUMERIC,
    emplisn                          NUMERIC,
    carisn                           NUMERIC,
    agrisn                           NUMERIC,
    datereg                          TIMESTAMP,
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    refundisn                        NUMERIC,
    calcisn                          NUMERIC,
    objisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.carsurvey IS $COMM$Акт осмотра транспортного средства$COMM$;
COMMENT ON COLUMN ais.carsurvey.objisn IS $COMM$FK(AGROBJECT)Ссылка на объект/*18.05.05 SR*/$COMM$;
COMMENT ON COLUMN ais.carsurvey.isn IS $COMM$Машинный номер: SEQ_CARSURVEY$COMM$;
COMMENT ON COLUMN ais.carsurvey.emplisn IS $COMM$FK(SUBHUMAN). Указатель сотрудника, проводившего осмотр$COMM$;
COMMENT ON COLUMN ais.carsurvey.carisn IS $COMM$FK(OBJCAR). Ссылка на транспортное средство$COMM$;
COMMENT ON COLUMN ais.carsurvey.agrisn IS $COMM$FK(Agreement). Ссылка на договор$COMM$;
COMMENT ON COLUMN ais.carsurvey.datereg IS $COMM$Дата проведения осмотра$COMM$;
COMMENT ON COLUMN ais.carsurvey.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.carsurvey.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.carsurvey.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.carsurvey.refundisn IS $COMM$Ссылка на претензию$COMM$;
COMMENT ON COLUMN ais.carsurvey.calcisn IS $COMM$Ссылка на калькуляцию$COMM$;


CREATE TABLE ais.cartarif (
    isn                              NUMERIC,
    modelisn                         NUMERIC,
    countryisn                       NUMERIC,
    currisn                          NUMERIC,
    price                            NUMERIC(20,2),
    tarifno                          NUMERIC(38),
    risklevel                        NUMERIC(38),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    status                           VARCHAR(1),
    synisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.cartarif IS $COMM$Тарифные характеристики модели автотранспорта в зависимости от страны производителя$COMM$;
COMMENT ON COLUMN ais.cartarif.isn IS $COMM$Машинный номер: SEQ_CARPRICE.nextval$COMM$;
COMMENT ON COLUMN ais.cartarif.modelisn IS $COMM$FK(CARMODEL). Указатель модели автотранспорта$COMM$;
COMMENT ON COLUMN ais.cartarif.countryisn IS $COMM$FK(COUNTRY). Указатель страны-производителя$COMM$;
COMMENT ON COLUMN ais.cartarif.currisn IS $COMM$FK(CURRENCY). Валюта стоимости$COMM$;
COMMENT ON COLUMN ais.cartarif.price IS $COMM$Цена$COMM$;
COMMENT ON COLUMN ais.cartarif.tarifno IS $COMM$Тарифный разряд$COMM$;
COMMENT ON COLUMN ais.cartarif.risklevel IS $COMM$Уровень риска$COMM$;
COMMENT ON COLUMN ais.cartarif.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.cartarif.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.cartarif.status IS $COMM$N - не выверен
Y - выверен
NULL - архивный
S - (зарезервировано)$COMM$;
COMMENT ON COLUMN ais.cartarif.synisn IS $COMM$Для вкл. механизма синонимов --Yunin V.A. 07/10/04$COMM$;


CREATE TABLE ais.cc_outb_list_client (
    isn                              NUMERIC,
    listisn                          NUMERIC,
    subjisn                          NUMERIC,
    objisn                           NUMERIC,
    claimisn                         NUMERIC,
    srisn                            NUMERIC,
    agrisn                           NUMERIC,
    docisn                           NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.cc_outb_list_client IS $COMM$Информация о клиентах в списках + причины на основе которой они туда попали$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_client.isn IS $COMM$Машинный номер списка (SEQ_CC_OUTB_CLIENT)$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_client.listisn IS $COMM$Ссылка на список FK(CC_OUTB_LIST_INFO)$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_client.subjisn IS $COMM$Ссылка на клиента$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_client.objisn IS $COMM$Ссылка на объект$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_client.claimisn IS $COMM$Ссылка на убыток$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_client.srisn IS $COMM$Ссылка на заявку(Queue)$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_client.agrisn IS $COMM$Ссылка на договор$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_client.docisn IS $COMM$Ссылка на документ$COMM$;


CREATE TABLE ais.cc_outb_list_info (
    isn                              NUMERIC,
    typeisn                          NUMERIC,
    listname                         VARCHAR(255),
    listdesc                         VARCHAR(1023),
    datemake                         TIMESTAMP,
    makeflg                          VARCHAR(1),
    unloadflg                        VARCHAR(1),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.cc_outb_list_info IS $COMM$Информация о сформированных списках, либо запросов на отложенное формирование$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_info.isn IS $COMM$Машинный номер списка (SEQ_CC_OUTB_LIST)$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_info.typeisn IS $COMM$Тип списка FK(CC_OUTB_TYPE_INFO)$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_info.listname IS $COMM$Название списка$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_info.listdesc IS $COMM$Описание списка$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_info.datemake IS $COMM$Дата формирования(для ночного автомата)$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_info.makeflg IS $COMM$Флаг того что сформирован$COMM$;
COMMENT ON COLUMN ais.cc_outb_list_info.unloadflg IS $COMM$Флаг того что выгружен в Genesys$COMM$;


CREATE TABLE ais.cc_outb_list_param (
    isn                              NUMERIC,
    listisn                          NUMERIC,
    paramname                        VARCHAR(255),
    paramval                         VARCHAR(255)
)
DISTRIBUTED BY (isn);



CREATE TABLE ais.cc_outb_type_param (
    isn                              NUMERIC,
    typeisn                          NUMERIC,
    paramname                        VARCHAR(255),
    paramdesc                        VARCHAR(255),
    defaultval                       VARCHAR(255)
)
DISTRIBUTED BY (isn);



CREATE TABLE ais.city (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    countryisn                       NUMERIC,
    regionisn                        NUMERIC,
    phone                            VARCHAR(6),
    code                             VARCHAR(3),
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(40),
    active                           VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    postcode                         VARCHAR(12),
    synisn                           NUMERIC,
    socr                             VARCHAR(10),
    gnicode                          VARCHAR(11),
    gninmb                           VARCHAR(4),
    ocatd                            VARCHAR(11),
    parentisn                        NUMERIC,
    latitude                         NUMERIC,
    longitude                        NUMERIC,
    population                       NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.city IS $COMM$Классификатор городов (и других населенных пунктов) мира. Город относится ровно к одной стране и
может входить в один регион страны.
DICTI.Code - 3-х буквенная русская кодировка для часто используемых городов.
DICTI.ShortName - русское название.
DICTI.$COMM$;
COMMENT ON COLUMN ais.city.isn IS $COMM$FK(DICTI,ГОРОД). Машинный номер объекта, совпадает с ISN соответствующей записи в словаре
DICTI.$COMM$;
COMMENT ON COLUMN ais.city.classisn IS $COMM$FK(DICTI,ТНП).Тип населенного пункта: город, поселок, село ...$COMM$;
COMMENT ON COLUMN ais.city.countryisn IS $COMM$FK(DICTI,СТРАНА). Страна, к которой относится город.$COMM$;
COMMENT ON COLUMN ais.city.regionisn IS $COMM$FK(DICTI,РЕГИОН). Регион страны, в который входит город.$COMM$;
COMMENT ON COLUMN ais.city.phone IS $COMM$Телефонный код города (без кода страны).$COMM$;
COMMENT ON COLUMN ais.city.postcode IS $COMM$Почтовый индекс$COMM$;
COMMENT ON COLUMN ais.city.synisn IS $COMM$FK(CITY). Указатель синонима, по умолчанию устанавливается на себя$COMM$;
COMMENT ON COLUMN ais.city.socr IS $COMM$Аббревиатура типа населенного пункта: г,с,пгт...$COMM$;
COMMENT ON COLUMN ais.city.gnicode IS $COMM$Цифровой код по справочнику ГНИ$COMM$;
COMMENT ON COLUMN ais.city.gninmb IS $COMM$Номер налоговой инспекции$COMM$;
COMMENT ON COLUMN ais.city.ocatd IS $COMM$Цифровой код ОКАТД (классификатор адм.-терр.деления)$COMM$;
COMMENT ON COLUMN ais.city.parentisn IS $COMM$FK(CITY). Ссылка на основной город для нас.пункта-спутника (типа Зеленоград)$COMM$;
COMMENT ON COLUMN ais.city.latitude IS $COMM$широта$COMM$;
COMMENT ON COLUMN ais.city.longitude IS $COMM$долгота$COMM$;
COMMENT ON COLUMN ais.city.population IS $COMM$население$COMM$;


CREATE TABLE ais.claimanketa (
    isn                              NUMERIC,
    claimisn                         NUMERIC,
    ans1                             VARCHAR(255),
    ans2                             VARCHAR(255),
    ans3                             VARCHAR(255),
    ans4                             VARCHAR(255),
    ans5                             VARCHAR(255),
    ans6                             VARCHAR(255),
    ans7                             VARCHAR(255),
    ans8                             VARCHAR(255),
    ans9                             VARCHAR(255),
    ans10                            VARCHAR(255),
    ans11                            VARCHAR(255),
    ans12                            VARCHAR(255),
    ans13                            VARCHAR(255),
    ans14                            VARCHAR(255),
    ans15                            VARCHAR(255),
    ans16                            VARCHAR(255),
    ans17                            VARCHAR(255),
    ans18                            VARCHAR(255),
    ans19                            VARCHAR(255),
    ans20                            VARCHAR(255),
    ans21                            VARCHAR(255),
    ans22                            VARCHAR(255),
    ans23                            VARCHAR(255),
    status                           VARCHAR(1),
    anketatype                       VARCHAR(3),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    ans24                            VARCHAR(255),
    fid                              VARCHAR(255),
    ans25                            VARCHAR(255),
    ans26                            VARCHAR(255),
    ans27                            VARCHAR(255),
    ans28                            VARCHAR(255),
    ans29                            VARCHAR(255),
    ans30                            VARCHAR(255),
    ans31                            VARCHAR(255),
    ans32                            VARCHAR(255),
    ans33                            VARCHAR(255),
    ans34                            VARCHAR(255),
    ans35                            VARCHAR(255),
    ans36                            VARCHAR(255),
    ans37                            VARCHAR(255),
    ans38                            VARCHAR(255),
    ans39                            VARCHAR(255),
    ans40                            VARCHAR(255),
    ans41                            VARCHAR(255),
    ans42                            VARCHAR(255),
    ans43                            VARCHAR(255),
    ans44                            VARCHAR(255),
    ans45                            VARCHAR(255),
    ans46                            VARCHAR(255),
    ans47                            VARCHAR(255),
    ans48                            VARCHAR(255),
    gk_isn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.claimanketa IS $COMM$В таблице сохраняется информация об ответах респондентов на вопросы о качестве урегулирования убытков$COMM$;
COMMENT ON COLUMN ais.claimanketa.gk_isn IS $COMM$ISN задачи горячего контакта$COMM$;
COMMENT ON COLUMN ais.claimanketa.isn IS $COMM$ISN анкеты$COMM$;
COMMENT ON COLUMN ais.claimanketa.claimisn IS $COMM$Ссылка на убыток$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans1 IS $COMM$Ответ на вопрос 1.1 (Ремонт на СТОА)$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans2 IS $COMM$Ответ на вопрос 1.2 (Ремонт на СТОА)$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans3 IS $COMM$Ответ на вопрос 1.3 (Ремонт на СТОА)$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans4 IS $COMM$Ответ на вопрос 1.1 (Выплата по факту)$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans5 IS $COMM$Ответ на вопрос 1.2 (Выплата по факту)$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans6 IS $COMM$Ответ на вопрос 1.3 (Выплата по факту)$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans7 IS $COMM$Ответ на вопрос 1.1 (Выплата по калькуляции)$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans8 IS $COMM$Ответ на вопрос 1.2 (Выплата по калькуляции)$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans9 IS $COMM$Ответ на вопрос 1.3 (Выплата по калькуляции)$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans10 IS $COMM$Ответ на вопрос 1.4 (Выплата по калькуляции)$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans11 IS $COMM$Ответ на вопрос 2.1$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans12 IS $COMM$Ответ на вопрос 2.2$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans13 IS $COMM$Ответ на вопрос 2.3$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans14 IS $COMM$Ответ на вопрос 3.1$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans15 IS $COMM$Ответ на вопрос 3.2$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans16 IS $COMM$Ответ на вопрос 3.3$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans17 IS $COMM$Ответ на вопрос 3.4$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans18 IS $COMM$Ответ на вопрос 3.5$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans19 IS $COMM$Ответ на вопрос 3.6$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans20 IS $COMM$Ответ на вопрос 3.7$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans21 IS $COMM$Ответ на вопрос 3.8$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans22 IS $COMM$Ответ на вопрос 3.9$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans23 IS $COMM$Ответ на вопрос 3.10$COMM$;
COMMENT ON COLUMN ais.claimanketa.status IS $COMM$Статус анкеты$COMM$;
COMMENT ON COLUMN ais.claimanketa.updated IS $COMM$Дата изменения записи$COMM$;
COMMENT ON COLUMN ais.claimanketa.updatedby IS $COMM$Автор изменения записи$COMM$;
COMMENT ON COLUMN ais.claimanketa.ans24 IS $COMM$Ответ на вопрос 3.8 bis$COMM$;


CREATE TABLE ais.claiminvoice (
    isn                              NUMERIC,
    currisn                          NUMERIC,
    subjisn                          NUMERIC,
    stationname                      VARCHAR(255),
    id                               VARCHAR(20),
    signed                           TIMESTAMP,
    claimsum                         NUMERIC(20,2),
    refundsum                        NUMERIC(20,2),
    rejectsum                        NUMERIC(20,2),
    persentage                       NUMERIC(9,6),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    status                           VARCHAR(1) DEFAULT 'N',
    remark                           VARCHAR(1000),
    no                               NUMERIC(38),
    dateval                          TIMESTAMP,
    createdby                        NUMERIC,
    created                          TIMESTAMP,
    received                         TIMESTAMP,
    nrzu                             VARCHAR(1) DEFAULT 'N',
    datesps                          TIMESTAMP,
    emplisn                          NUMERIC,
    classisn                         NUMERIC,
    causeisn                         NUMERIC,
    calcuph                          NUMERIC,
    rultariffisn                     NUMERIC,
    parentisn                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.claiminvoice.parentisn IS $COMM$(FK CLAIMINVOICE)$COMM$;
COMMENT ON COLUMN ais.claiminvoice.classisn IS $COMM$FK(DICTI). Тип документа Русов Р.В. 04.01.2008$COMM$;
COMMENT ON COLUMN ais.claiminvoice.emplisn IS $COMM$Куратор$COMM$;
COMMENT ON COLUMN ais.claiminvoice.calcuph IS $COMM$Норма, ед/час$COMM$;
COMMENT ON COLUMN ais.claiminvoice.rultariffisn IS $COMM$(FK stotariff_t) Ссылка на тарифные коэффициенты для расчета калькуляции$COMM$;
COMMENT ON COLUMN ais.claiminvoice.causeisn IS $COMM$Причина выбора неприоритетной СТО (Русов Р.В.)$COMM$;
COMMENT ON COLUMN ais.claiminvoice.datesps IS $COMM$Дата согласования предварительного счета (Smirnov 14/10/05)$COMM$;
COMMENT ON COLUMN ais.claiminvoice.nrzu IS $COMM$Признак "Не учитывается в РЗУ"/*17.05.05*/$COMM$;
COMMENT ON COLUMN ais.claiminvoice.no IS $COMM$Порядковый номер документа в рамках фирмы, подразделения, сотрудник
Внутренний Ингосстраховский номер$COMM$;
COMMENT ON COLUMN ais.claiminvoice.dateval IS $COMM$Дата пересчета курса при калькуляции по счету$COMM$;
COMMENT ON COLUMN ais.claiminvoice.createdby IS $COMM$Создатель$COMM$;
COMMENT ON COLUMN ais.claiminvoice.created IS $COMM$Дата создания$COMM$;
COMMENT ON COLUMN ais.claiminvoice.received IS $COMM$Дата получения счета$COMM$;
COMMENT ON COLUMN ais.claiminvoice.currisn IS $COMM$Машинный номер объекта.
Устанавливается по умолчанию равным SEQ_DICTI.nextval.
Совпадает с ISN соответствующего объекта, имеющего отдельную таблицу для хранения дополнительных полей.$COMM$;
COMMENT ON COLUMN ais.claiminvoice.subjisn IS $COMM$Ссылка на СТО$COMM$;
COMMENT ON COLUMN ais.claiminvoice.stationname IS $COMM$Название станции технического обслуживания. Если SubjISN != NULL =>
копируется из справочника$COMM$;
COMMENT ON COLUMN ais.claiminvoice.id IS $COMM$Идентификатор счета-калькуляции$COMM$;
COMMENT ON COLUMN ais.claiminvoice.signed IS $COMM$Дата счета$COMM$;
COMMENT ON COLUMN ais.claiminvoice.claimsum IS $COMM$Итоговая сумма калькуляции стоимости работ и материалов$COMM$;
COMMENT ON COLUMN ais.claiminvoice.refundsum IS $COMM$Сумма к выплате$COMM$;
COMMENT ON COLUMN ais.claiminvoice.rejectsum IS $COMM$Итоговая сумма по позициям калькуляции, по которым частично или полностью
отказано в оплате$COMM$;
COMMENT ON COLUMN ais.claiminvoice.persentage IS $COMM$RefundSum = ClaimSum - RejectedSum*Persentage$COMM$;
COMMENT ON COLUMN ais.claiminvoice.status IS $COMM$Закрыт Y - все расчеты по нему завершены
нет  N - процесс присоединения сумм начислений по претензиям не завершен$COMM$;


CREATE TABLE ais.claiminvoiceline (
    isn                              NUMERIC,
    invoiceisn                       NUMERIC,
    id                               VARCHAR(20),
    shortname                        VARCHAR(40),
    amount                           NUMERIC(20,2),
    includeamount                    NUMERIC(20,2),
    percentage                       NUMERIC(9,6),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    currisn                          NUMERIC,
    curroutisn                       NUMERIC,
    refundisn                        NUMERIC,
    remark                           VARCHAR(3000),
    signed                           TIMESTAMP,
    vat                              NUMERIC(9,6),
    discount                         NUMERIC(9,6),
    classisn                         NUMERIC,
    ssd                              VARCHAR(1),
    unitclassisn                     NUMERIC,
    unitprice                        NUMERIC,
    unitqnt                          NUMERIC,
    processclassisn                  NUMERIC,
    mask                             BIGINT,
    expertvalue                      NUMERIC,
    code                             VARCHAR(60),
    conventionalunit                 NUMERIC,
    docid                            NUMERIC,
    partid                           VARCHAR(40),
    nextisn                          NUMERIC,
    amountsto                        NUMERIC,
    checkline                        VARCHAR(1),
    modifictype                      VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.claiminvoiceline.partid IS $COMM$Код детали в AudaTex. -- Rusov 16/02/2012$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.nextisn IS $COMM$Ссылка на позицию документа присланного из СТО.Rusov 16/02/2012$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.amountsto IS $COMM$Стоимость прайс-листа СТО. Заполняется из справочника цен СТО. Rusov 16/02/2012$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.checkline IS $COMM$Признак что строка проверена.Rusov 15/05/2012$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.modifictype IS $COMM$Rusov 15-03-2013.Признак ручной модификации строки экспертом при составлении калькуляции в AudaTex. «P» - Модификация стоимости, «C» - Модификация кода детали, «W» - Модификация работ$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.ssd IS $COMM$Согласование скрытого дефекта   -- 31.05.05 SR$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.unitclassisn IS $COMM$FK(DICTI). Класс работ или деталей. -- AL 3/08/06$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.unitprice IS $COMM$Цена единицы работы (СНЧ) или детали.  -- AL 3/08/06$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.unitqnt IS $COMM$Количество единиц работы (НЧ) или деталей.  -- AL 3/08/06$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.processclassisn IS $COMM$Класс воздействия -- Rusov 13/02/07$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.mask IS $COMM$Маска вида документа/вида ремонтного воздействия -- Rusov 07.06.2007$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.expertvalue IS $COMM$Экспертная оценка 29.04.2008 Русов Р.В.$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.docid IS $COMM$Номер документа присланного из СТО. Rusov 16/02/2012$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.id IS $COMM$№ позиции$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.shortname IS $COMM$Описание ремонтной работы$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.amount IS $COMM$Заявленная сумма$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.includeamount IS $COMM$Сумма отказа или выплаты (в зависимости от знака)$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.percentage IS $COMM$Процент от AMOUNT отказа или выплаты в зависимости от знака$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.currisn IS $COMM$FK(DICTI,ВАЛЮТА). Машинный номер объекта, совпадает с ISN соответствующей
записи в словаре DICTI.$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.curroutisn IS $COMM$FK(DICTI,ВАЛЮТА). Машинный номер объекта, совпадает с ISN соответствующей
записи в словаре DICTI.$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.refundisn IS $COMM$Ссылка на претензию$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.signed IS $COMM$Дата курса при калькуляциях- наследуется из ClaimInvoice.DateVal$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.vat IS $COMM$НДС в процентах$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.discount IS $COMM$Процент скидки/надбавки$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.classisn IS $COMM$FK(DICTI). Указатель класса выплаты (ремонт,эвакуация,оценка,утеря тов.стоим.)$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.code IS $COMM$Код детали\работ - Rusov 16/02/2012$COMM$;
COMMENT ON COLUMN ais.claiminvoiceline.conventionalunit IS $COMM$Кол-во расходных материалов на окраску одной условной детали - Rusov 16/02/2012$COMM$;


CREATE TABLE ais.claiminvoicesto (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    stationname                      VARCHAR(255),
    id                               VARCHAR(20),
    signed                           TIMESTAMP DEFAULT current_timestamp,
    claimsum                         NUMERIC(20,2),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    status                           VARCHAR(1) DEFAULT 'N',
    remark                           VARCHAR(1000),
    createdby                        NUMERIC,
    created                          TIMESTAMP
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.claiminvoicesto IS $COMM$Счета из СТО$COMM$;
COMMENT ON COLUMN ais.claiminvoicesto.subjisn IS $COMM$Ссылка на СТО$COMM$;
COMMENT ON COLUMN ais.claiminvoicesto.stationname IS $COMM$Название станции технического обслуживания. Если SubjISN != NULL =>
копируется из справочника$COMM$;
COMMENT ON COLUMN ais.claiminvoicesto.id IS $COMM$Идентификатор счета-калькуляции$COMM$;
COMMENT ON COLUMN ais.claiminvoicesto.signed IS $COMM$Дата счета$COMM$;
COMMENT ON COLUMN ais.claiminvoicesto.claimsum IS $COMM$Итоговая сумма калькуляции стоимости работ и материалов$COMM$;
COMMENT ON COLUMN ais.claiminvoicesto.status IS $COMM$Закрыт Y - все расчеты по нему завершены
нет  N - процесс присоединения сумм начислений по претензиям не завершен$COMM$;
COMMENT ON COLUMN ais.claiminvoicesto.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.claiminvoicesto.createdby IS $COMM$Куратор$COMM$;
COMMENT ON COLUMN ais.claiminvoicesto.created IS $COMM$Дата создания$COMM$;


CREATE TABLE ais.claiminvoicestoline (
    isn                              NUMERIC,
    invoicestoisn                    NUMERIC,
    fio                              VARCHAR(100),
    agrid                            VARCHAR(20),
    carno                            VARCHAR(20),
    marka                            VARCHAR(40),
    vsumma                           VARCHAR(20),
    summa                            NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    currisn                          NUMERIC,
    invoiceisn                       NUMERIC,
    remark                           VARCHAR(100),
    no                               VARCHAR(20),
    claimid                          VARCHAR(40)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.claiminvoicestoline IS $COMM$Список счетов-калькуляций загруженных из СТО (XLS файл). В счетах найденных по одному из двух вариантов (по №полиса и сумме или по №ТС и сумме), проставляется INVOICEISN.$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.claimid IS $COMM$№ Убытка (из XLS файла)$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.invoicestoisn IS $COMM$FK(CLAIMINVOICESTO)$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.fio IS $COMM$ФИО (из XLS файла)$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.agrid IS $COMM$№ Полиса (из XLS файла)$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.carno IS $COMM$Рег. № ТС (из XLS файла)$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.marka IS $COMM$Марка, модель ТС (из XLS файла)$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.vsumma IS $COMM$Сумма (из XLS файла)$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.summa IS $COMM$Сумма (преобразована из VSUMMA)$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.currisn IS $COMM$FK(DICTI,ВАЛЮТА). Машинный номер объекта, совпадает с ISN соответствующей
записи в словаре DICTI.$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.invoiceisn IS $COMM$FK(CLAIMINVOICE). Если NULL то не найден счет с такими параметрами: № полиса, № ТС, Сумма$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.claiminvoicestoline.no IS $COMM$Порядковый номер документа в рамках счета СТО$COMM$;


CREATE TABLE ais.claimrefundcar (
    isn                              NUMERIC,
    nextagrisn                       NUMERIC,
    guilty                           VARCHAR(1),
    totalloss                        VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    parentisn                        NUMERIC,
    claimantclassisn                 NUMERIC,
    claimantdisabled                 VARCHAR(1) DEFAULT 'N',
    claimantstudent                  VARCHAR(1) DEFAULT 'N',
    subjbonusisn                     NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.claimrefundcar.created IS $COMM$Дата создания -- Rusov 01.11.2012$COMM$;
COMMENT ON COLUMN ais.claimrefundcar.createdby IS $COMM$Создатель -- Rusov 01.11.2012$COMM$;
COMMENT ON COLUMN ais.claimrefundcar.isn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.claimrefundcar.nextagrisn IS $COMM$Ссылка на договор в котором данный убыток повлиял на бонус-малус$COMM$;
COMMENT ON COLUMN ais.claimrefundcar.guilty IS $COMM$Признак вины страхователя Y/N$COMM$;
COMMENT ON COLUMN ais.claimrefundcar.totalloss IS $COMM$Признак полной гибели ТС Y/N$COMM$;
COMMENT ON COLUMN ais.claimrefundcar.parentisn IS $COMM$FK(CLAIMREFUNDCAR). Ссылка на аккумулирующую претензию, на которую относятся
выплаты по группе претензий, если их невозможно однозначно разделить.$COMM$;
COMMENT ON COLUMN ais.claimrefundcar.claimantclassisn IS $COMM$FK(Dicti) Тип потерпевшего$COMM$;
COMMENT ON COLUMN ais.claimrefundcar.claimantdisabled IS $COMM$Потерпевший-инвалид$COMM$;
COMMENT ON COLUMN ais.claimrefundcar.claimantstudent IS $COMM$Потерпевший-учащийся$COMM$;


CREATE TABLE ais.claimstolen (
    isn                              NUMERIC,
    objcarisn                        NUMERIC,
    refundisn                        NUMERIC,
    subjisn                          NUMERIC,
    address                          VARCHAR(255),
    policestation                    VARCHAR(255),
    id                               VARCHAR(20),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    policestationadr                 VARCHAR(255),
    iid                              VARCHAR(20)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.claimstolen.iid IS $COMM$Внутренний номер  -- 01.03.05 SR$COMM$;
COMMENT ON COLUMN ais.claimstolen.refundisn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.claimstolen.subjisn IS $COMM$FK(DICTI,СУБЪЕКТ). ОВД милиции (из справочника)$COMM$;
COMMENT ON COLUMN ais.claimstolen.address IS $COMM$Адрес происшествия$COMM$;
COMMENT ON COLUMN ais.claimstolen.policestation IS $COMM$ОВД милиции (ручной ввод)$COMM$;
COMMENT ON COLUMN ais.claimstolen.id IS $COMM$№ уголовного дела$COMM$;
COMMENT ON COLUMN ais.claimstolen.datebeg IS $COMM$Дата нач. угол. дела$COMM$;
COMMENT ON COLUMN ais.claimstolen.dateend IS $COMM$Дата окончания угол. дела$COMM$;
COMMENT ON COLUMN ais.claimstolen.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.claimstolen.policestationadr IS $COMM$Адрес ОВД милиции$COMM$;
COMMENT ON COLUMN ais.claimstolen.isn IS $COMM$Машинный номер: SEQ_CLAIMSTOLEN.nextval$COMM$;
COMMENT ON COLUMN ais.claimstolen.objcarisn IS $COMM$FK(OBJAGR). Указатель заголовка физического объекта$COMM$;


CREATE TABLE ais.claimsurvey (
    isn                              NUMERIC,
    id                               VARCHAR(40),
    claimisn                         NUMERIC,
    classisn                         NUMERIC,
    subjisn                          NUMERIC,
    daterequest                      TIMESTAMP,
    status                           VARCHAR(1),
    datesurvey                       TIMESTAMP,
    dateevent                        TIMESTAMP,
    branchisn                        NUMERIC,
    industryisn                      NUMERIC,
    emplisn                          NUMERIC,
    deptisn                          NUMERIC,
    claimsum                         NUMERIC,
    currisn                          NUMERIC,
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    formisn                          NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE ais.claimsurveyobj (
    claimsurveyisn                   NUMERIC,
    objectisn                        NUMERIC,
    objecttypeisn                    NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    isn                              NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE ais.claimtotalloss (
    isn                              NUMERIC,
    refundisn                        NUMERIC,
    objcarisn                        NUMERIC,
    id                               VARCHAR(20),
    signed                           TIMESTAMP,
    price                            NUMERIC(20,2),
    currisn                          NUMERIC,
    commission                       NUMERIC(9,6),
    status                           VARCHAR(1) DEFAULT 'N',
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    subjisn                          NUMERIC,
    no                               NUMERIC(38),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    remark                           VARCHAR(2000),
    shopisn                          NUMERIC,
    incomesum                        NUMERIC(20,2),
    commisionsum                     NUMERIC(20,2),
    vat                              NUMERIC(20,2),
    iid                              VARCHAR(20),
    conveyance_docid                 VARCHAR(20),
    conveyance_date                  TIMESTAMP,
    carcost                          NUMERIC(20,2),
    roleisn                          NUMERIC,
    oebs_id                          VARCHAR(20),
    saletype                         NUMERIC,
    claimsum                         NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    terrisn                          NUMERIC,
    deptisn                          NUMERIC,
    paymentcash                      VARCHAR(1) DEFAULT 'N' 
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.claimtotalloss IS $COMM$Журнал тотальных убытков$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.conveyance_docid IS $COMM$Номер акта передачи ТС от страхователя в ИГС (Smirnov 14/07/06)$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.conveyance_date IS $COMM$Дата акта передачи ТС от страхователя в ИГС (Smirnov 14/07/06)$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.carcost IS $COMM$Рыночная стоимость «тотальной» автомашины после ДТП (Smirnov 14/07/06)$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.roleisn IS $COMM$FK(Dicti) - Роль ИГС в процессе реализации (Smirnov 14/07/06)$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.saletype IS $COMM$FK(Dicti) - Способ реализации(Rusov 27.08.2009)$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.claimsum IS $COMM$Оценка(Rusov 27.08.2009)$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.terrisn IS $COMM$Место хранения - A.Dukhanin 25.01.2013$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.created IS $COMM$дата создания -- Andriyahin 01.11.2012$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.deptisn IS $COMM$Подразделение - A.Dukhanin 25.01.2013$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.iid IS $COMM$Внутренний номер  -- 01.03.05 SR$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.paymentcash IS $COMM$Оплата через кассу; 'N' - нет, 'Y' - да - A.Dukhanin 25.01.2013$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.createdby IS $COMM$автор создания -- Andriyahin 01.11.2012$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.isn IS $COMM$Машинный номер, SEQ_CLAIMTOTALLOSS$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.refundisn IS $COMM$FK(AGRREFUND). Ссылка на претензию$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.objcarisn IS $COMM$FK(OBJCAR).Ссылка на физический объект - ТС$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.id IS $COMM$№ акта продажи из магазина$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.signed IS $COMM$Дата продажи по акту$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.price IS $COMM$Цена магазина$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.currisn IS $COMM$FK(CURRENCY). Валюта продажи.$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.commission IS $COMM$Комиссия магазина$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.status IS $COMM$Состояние продажи:
N - не продан,
Y - продан
null - аннулирован (сторно)$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.subjisn IS $COMM$FK(SUBJECT). Указатель доверенного лица (получателя доверенности) для
реализации а/м через комиссионный магазин$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.no IS $COMM$Порядковый номер документа (доверенности)$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.datebeg IS $COMM$Дата начала действия доверенности$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.dateend IS $COMM$Дата окончания действия доверенности$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.shopisn IS $COMM$FK(DICTI,СУБЪЕКТ). Машинный номер объекта, совпадает с ISN соответствующей
записи в словаре DICTI.$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.incomesum IS $COMM$Доход Ингостраха$COMM$;
COMMENT ON COLUMN ais.claimtotalloss.commisionsum IS $COMM$Абсолютная сумма комиссии магазина$COMM$;


CREATE TABLE ais.clientdirectivity (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    objisn                           NUMERIC,
    datecalc                         TIMESTAMP,
    clientvalue                      NUMERIC,
    sumclientdir                     NUMERIC,
    typeclientdir                    VARCHAR(1000),
    emplisn                          NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    roleclassisn                     NUMERIC,
    estimate                         NUMERIC,
    estcurrisn                       NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.clientdirectivity IS $COMM$История расчета ценности клиента$COMM$;
COMMENT ON COLUMN ais.clientdirectivity.roleclassisn IS $COMM$Указатель роли субъекта$COMM$;
COMMENT ON COLUMN ais.clientdirectivity.isn IS $COMM$Уникальный машинный ISN по SEQ_CLIENTDIRECTIVITY.nextval$COMM$;
COMMENT ON COLUMN ais.clientdirectivity.subjisn IS $COMM$Указатель клиента$COMM$;
COMMENT ON COLUMN ais.clientdirectivity.objisn IS $COMM$Указатель объекта расчета ценности клиента$COMM$;
COMMENT ON COLUMN ais.clientdirectivity.datecalc IS $COMM$Дата расчета ценности клиента$COMM$;
COMMENT ON COLUMN ais.clientdirectivity.clientvalue IS $COMM$Показатель ценности клиента$COMM$;
COMMENT ON COLUMN ais.clientdirectivity.sumclientdir IS $COMM$Сумма клиентоориентированного решения$COMM$;
COMMENT ON COLUMN ais.clientdirectivity.typeclientdir IS $COMM$Тип клиентоориентированного решения$COMM$;
COMMENT ON COLUMN ais.clientdirectivity.emplisn IS $COMM$Куратор клиентоориентированного решения$COMM$;
COMMENT ON COLUMN ais.clientdirectivity.updated IS $COMM$Дата создания или последнего изменения записи.

Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.clientdirectivity.updatedby IS $COMM$Автор создания или последнего изменения записи.

Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;


CREATE TABLE ais.crgdoc (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    routeisn                         NUMERIC,
    classisn                         NUMERIC,
    id                               VARCHAR(50),
    docdate                          TIMESTAMP,
    name                             VARCHAR(40),
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    refundisn                        NUMERIC,
    dateend                          TIMESTAMP,
    objisn                           NUMERIC,
    subjisn                          NUMERIC,
    addisn                           NUMERIC,
    newaddisn                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    claimisn                         NUMERIC,
    regionisn                        NUMERIC,
    currisn                          NUMERIC,
    amount                           NUMERIC(20,2),
    docisn                           NUMERIC,
    amountmax                        NUMERIC(20,2),
    subclassisn                      NUMERIC,
    chgflg                           VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.crgdoc IS $COMM$Грузовой документ: коносамент, CMR, транспортная накладная. Может относиться целиком к полису
или к конкретному транспортому средству CRGTRANSP.$COMM$;
COMMENT ON COLUMN ais.crgdoc.chgflg IS $COMM$Режим обновления: 'T' - ЕАИС ТО, 'D' - ДК/Талон ТО$COMM$;
COMMENT ON COLUMN ais.crgdoc.currisn IS $COMM$??? Сарваров ??? Используется в экспертной оценке по убытку$COMM$;
COMMENT ON COLUMN ais.crgdoc.amount IS $COMM$??? Сарваров ??? Используется в экспертной оценке по убытку$COMM$;
COMMENT ON COLUMN ais.crgdoc.docisn IS $COMM$??? Сарваров ??? Используется в экспертной оценке по убытку$COMM$;
COMMENT ON COLUMN ais.crgdoc.isn IS $COMM$Машинный номер: SEQ_CRGDOC.nextval$COMM$;
COMMENT ON COLUMN ais.crgdoc.agrisn IS $COMM$FK(AGREEMENT). Ссылка на договор. Наследуется из сегмента маршрута, если есть$COMM$;
COMMENT ON COLUMN ais.crgdoc.routeisn IS $COMM$FK(CRGROUTE). Указатель на сегмент маршрута$COMM$;
COMMENT ON COLUMN ais.crgdoc.classisn IS $COMM$FK(DICTI). Тип грузового документа: коносамент, CMR, товарно-транспортная
накландная$COMM$;
COMMENT ON COLUMN ais.crgdoc.id IS $COMM$Номер документа, возможно несколько$COMM$;
COMMENT ON COLUMN ais.crgdoc.docdate IS $COMM$Дата документа$COMM$;
COMMENT ON COLUMN ais.crgdoc.name IS $COMM$Наименование документа$COMM$;
COMMENT ON COLUMN ais.crgdoc.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.crgdoc.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.crgdoc.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.crgdoc.refundisn IS $COMM$FK(AGRREFUND). Ссылка на претензию$COMM$;
COMMENT ON COLUMN ais.crgdoc.dateend IS $COMM$Дата окончания действия документа. В экспертной оценке - предполагаемая дата оплаты$COMM$;
COMMENT ON COLUMN ais.crgdoc.objisn IS $COMM$FK(OBJAGR). Ссылка на застрахованный объект$COMM$;
COMMENT ON COLUMN ais.crgdoc.subjisn IS $COMM$FK(SUBJECT). Доверенное лицо или другой контрагент по документу$COMM$;
COMMENT ON COLUMN ais.crgdoc.addisn IS $COMM$FK(AGREEMENT). Аддендум, в котором начал действовать документ$COMM$;
COMMENT ON COLUMN ais.crgdoc.newaddisn IS $COMM$FK(AGREEMENT). Аддендум, прекративший действие документа$COMM$;
COMMENT ON COLUMN ais.crgdoc.created IS $COMM$Дата создания записи$COMM$;
COMMENT ON COLUMN ais.crgdoc.createdby IS $COMM$Создатель записи$COMM$;
COMMENT ON COLUMN ais.crgdoc.claimisn IS $COMM$FK(AGRCLAIM). Ссылка на убыток$COMM$;
COMMENT ON COLUMN ais.crgdoc.regionisn IS $COMM$FK(DICTI). Ссылка на территорию регисрации объекта (AL 19/06/03)$COMM$;
COMMENT ON COLUMN ais.crgdoc.amountmax IS $COMM$Экспертная оценка - максимальная сумма (Smirnov 31/10/08)$COMM$;
COMMENT ON COLUMN ais.crgdoc.subclassisn IS $COMM$FK(DICTI). Подтип документа. В экспертной оценке - ЭО, прогноз или ЭО + прогноз (Smirnov 31/10/08)$COMM$;


CREATE TABLE ais.crgpoint (
    isn                              NUMERIC,
    countryisn                       NUMERIC,
    agrisn                           NUMERIC,
    classisn                         NUMERIC,
    pointisn                         NUMERIC,
    shortname                        VARCHAR(40),
    id                               VARCHAR(20),
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.crgpoint IS $COMM$Пункт маршрута: город, порт, аэропорт, склад...$COMM$;
COMMENT ON COLUMN ais.crgpoint.isn IS $COMM$Машинный номер: SEQ_CRGPOINT.nextval. Наследуется из DICTI.$COMM$;
COMMENT ON COLUMN ais.crgpoint.countryisn IS $COMM$FK(COUNTRY). Указатель страны пункта маршрута$COMM$;
COMMENT ON COLUMN ais.crgpoint.agrisn IS $COMM$FK(AGREEMENT). Ссылка на договор$COMM$;
COMMENT ON COLUMN ais.crgpoint.classisn IS $COMM$FK(DICTI). Тип грузового документа: коносамент, CMR, товарно-транспортная
накландная$COMM$;
COMMENT ON COLUMN ais.crgpoint.pointisn IS $COMM$FK(DICTI). Указатель пункта маршрута: город, порт$COMM$;
COMMENT ON COLUMN ais.crgpoint.shortname IS $COMM$Наименование пункта маршрута$COMM$;
COMMENT ON COLUMN ais.crgpoint.id IS $COMM$Порядковый номер, возможно через точку$COMM$;
COMMENT ON COLUMN ais.crgpoint.remark IS $COMM$Адрес пункта маршрута$COMM$;
COMMENT ON COLUMN ais.crgpoint.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.crgpoint.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.crgroute (
    isn                              NUMERIC,
    fromisn                          NUMERIC,
    toisn                            NUMERIC,
    classisn                         NUMERIC,
    objisn                           NUMERIC,
    countryisn                       NUMERIC,
    id                               VARCHAR(255),
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.crgroute IS $COMM$Пункт маршрута: город, порт, аэропорт, склад...$COMM$;
COMMENT ON COLUMN ais.crgroute.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.crgroute.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.crgroute.isn IS $COMM$FK(AGRLIMIT). Указатель заголовка ограничения$COMM$;
COMMENT ON COLUMN ais.crgroute.fromisn IS $COMM$FK(CRGPOINT). Указатель пункта отправления$COMM$;
COMMENT ON COLUMN ais.crgroute.toisn IS $COMM$FK(CRGPOINT). Указатель пункта назначения$COMM$;
COMMENT ON COLUMN ais.crgroute.classisn IS $COMM$FK(DICTI). Вид транспорта$COMM$;
COMMENT ON COLUMN ais.crgroute.objisn IS $COMM$FK(OBJAGR). Указатель на транспортное средство - физический объект$COMM$;
COMMENT ON COLUMN ais.crgroute.countryisn IS $COMM$FK(COUNTRY). Указатель страны принадлежности транспортного средства. Наследуется
из OBJAGR$COMM$;
COMMENT ON COLUMN ais.crgroute.id IS $COMM$Идентификатор транспортного средства: бортовой номер, название судна. Наследуется из
OBJAGR$COMM$;
COMMENT ON COLUMN ais.crgroute.remark IS $COMM$Примечание: перевозчик...$COMM$;


CREATE TABLE ais.currate (
    isn                              NUMERIC,
    currfromisn                      NUMERIC,
    currtoisn                        NUMERIC,
    classisn                         NUMERIC,
    dateval                          TIMESTAMP,
    codefrom                         VARCHAR(3) DEFAULT 'RUR',
    codeto                           VARCHAR(3) DEFAULT 'RUR',
    rate                             NUMERIC(22,10),
    scale                            NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.currate IS $COMM$Задает коэффициент пересчета исходной валюты в целевую с учетом масштаба целевой валюты:
Sц=Sи*Rate*Scale на даты, начиная с DateVal(i) до DateVal(i+1)-1.

Может быть несколько типов курсов: ЦБ, ММВБ и т.д.$COMM$;
COMMENT ON COLUMN ais.currate.isn IS $COMM$Машинный номер записи. Устанавливается по умолчанию равным SEQ_CURRATE.nextval$COMM$;
COMMENT ON COLUMN ais.currate.currfromisn IS $COMM$FK(DICTI,ВАЛЮТА). Исходная валюта кросскурса.$COMM$;
COMMENT ON COLUMN ais.currate.currtoisn IS $COMM$FK(DICTI,ВАЛЮТА). Целевая валюта кросскурса. По умолчанию - локальная валюта.$COMM$;
COMMENT ON COLUMN ais.currate.classisn IS $COMM$FK(DICTI,КУРС). Класс кросскурса: ЦБ, ММВБ, London ...$COMM$;
COMMENT ON COLUMN ais.currate.dateval IS $COMM$Дата, начиная с которой введен данный курс (дата валютирования).
Промежуточные даты не хранятся.$COMM$;
COMMENT ON COLUMN ais.currate.codefrom IS $COMM$Код исходной валюты кросскурса.
Функционально зависит от CurrFromISN: CodeFrom = DICTI(CurrFromISN).Code
Предназначен для упрощения ввода из приложения: допустимость кода и установка соответствующего CurrFromISN осуществляется
автоматически.$COMM$;
COMMENT ON COLUMN ais.currate.codeto IS $COMM$Код целевой валюты кросскурса.
Функционально зависит от CurrToISN: CodeTo = DICTI(CurrToISN).Code
Предназначен для упрощения ввода из приложения: допустимость кода и установка соответствующего CurrToISN осуществляется
автоматически.$COMM$;
COMMENT ON COLUMN ais.currate.rate IS $COMM$Кросс-курс целевой валюты по отношению к исходной:
Сумма в целевой валюте = Сумма в исходной валюте * Курс валюты / Масштаб курса$COMM$;
COMMENT ON COLUMN ais.currate.scale IS $COMM$Масштаб курса: количество единиц исходной валюты в расчете на единицу кросс-курса к
целевой валюте.
Функционально зависит от исходной валюты: Scale = CURRENCY(CurrFromISN).Scale
Устанавливается автоматически.$COMM$;
COMMENT ON COLUMN ais.currate.updated IS $COMM$Дата создания или последнего изменения записи.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.currate.updatedby IS $COMM$Автор создания или последнего изменения записи.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;


CREATE TABLE ais.dictext (
    isn                              NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    txt                              TEXT
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.dictext IS $COMM$Длинный текст для элементов словаря$COMM$;
COMMENT ON COLUMN ais.dictext.txt IS $COMM$Текст для элемента словаря$COMM$;
COMMENT ON COLUMN ais.dictext.isn IS $COMM$Машинный номер: SEQ_DICTI.nextval$COMM$;
COMMENT ON COLUMN ais.dictext.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.dictext.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.dicti (
    isn                              NUMERIC,
    parentisn                        NUMERIC DEFAULT 0,
    code                             VARCHAR(10),
    n_children                       NUMERIC,
    filterisn                        NUMERIC,
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(255),
    tablename                        VARCHAR(32),
    constname                        VARCHAR(32),
    active                           VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    synisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.dicti IS $COMM$Сборная таблица кодификаторов. Запись таблицы определяет некоторый класс объектов.
Класс может относиться к некоторому суперклассу, что дает возможность построения иерархических справочников.
Класс может иметь синоним - например, задающий новое наименование.
Класс может иметь длинное примечание или OLE-объект в таблице DICREMARK.$COMM$;
COMMENT ON COLUMN ais.dicti.isn IS $COMM$Машинный номер объекта.
Устанавливается по умолчанию равным SEQ_DICTI.nextval.
Совпадает с ISN соответствующего объекта, имеющего отдельную таблицу для хранения дополнительных полей.$COMM$;
COMMENT ON COLUMN ais.dicti.parentisn IS $COMM$FK(DICT). Суперкласс, членом которого является данный класс. По умолчанию: 0
(корневой суперкласс).$COMM$;
COMMENT ON COLUMN ais.dicti.code IS $COMM$Поисковый внешний код класса (для пользователя). Уникален в пределах суперкласса.$COMM$;
COMMENT ON COLUMN ais.dicti.n_children IS $COMM$Число детей класса, при добавлении и удалении подчиненных записей изменяется
автоматически.

NULL - лист дерева, детей нет и быть не может,
0 - потенциальный узел, детей нет,
>0 - узел с детьми, раскрывается в иерархическом справочнике (TreeView) (число детей=N_Children),
<0 - узел с детьми, не раскрывается (число детей=-(N_Children+1)).

Отрицательное значение является признаком запрета раскрытия класса в иерархическом справочнике. У такого класса дети могут быть,
но они не будут показаны в TreeView.

Это используется при большом числе объектов (города, субъекты), поскольку показывать их списком бесполезно - это лишние
накладные расходы по памяти рабочей станции и нагрузка на сеть.

В словарь DICTI эти объекты заносятся с целью облегчения декодирования FK(ISN) и выполнения стандартных действий
(LookUp,синонимы, архив, иерархия).$COMM$;
COMMENT ON COLUMN ais.dicti.filterisn IS $COMM$FK(DICTI) для фильтрации членов класса, например, подразделение для классификаторов
рисков и объектов.$COMM$;
COMMENT ON COLUMN ais.dicti.shortname IS $COMM$Краткое поисковое название класса для показа в LookUp.$COMM$;
COMMENT ON COLUMN ais.dicti.fullname IS $COMM$Полное название класса для отчетов, пояснений. Использование зависит от особенностей
суперкласса.$COMM$;
COMMENT ON COLUMN ais.dicti.tablename IS $COMM$Имя таблицы, в которой хранятся дополнительные поля объекта.$COMM$;
COMMENT ON COLUMN ais.dicti.constname IS $COMM$Имя константы для DELPHI & PL/SQL$COMM$;
COMMENT ON COLUMN ais.dicti.active IS $COMM$Признак активности объекта:
N - новый, не проверен администратором,
Y - проверен администратором,
NULL-считается архивным и сохраняется для декодировки ссылок из ранее созданных объектов, но недоступен для ссылок из вновь
создаваемых объектов и не показывается в LookUp.
При создании нового объекта: если задан NULL - устанавливается N, если задан X - устанавливается в NULL.
Удалять можно только архивный объект.$COMM$;
COMMENT ON COLUMN ais.dicti.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.dicti.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.dicti.synisn IS $COMM$FK(DICT). Ссылка на синоним объекта или на сам объект, если такового нет.
По умолчанию - ссылка на самого себя.
При формировании отчетов группировка должна вестись именно по этому полю, а не по ISN.$COMM$;


CREATE TABLE ais.dicx (
    isn                              NUMERIC,
    classisn1                        NUMERIC,
    classisn2                        NUMERIC,
    filterisn                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    classisn                         NUMERIC,
    code                             VARCHAR(10),
    classisn3                        NUMERIC,
    classisn4                        NUMERIC,
    classisn5                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.dicx IS $COMM$Справочник связей задает множество отношений многие-ко-многим между элементами словаря DICTI,
зависящим от подразделения.$COMM$;
COMMENT ON COLUMN ais.dicx.classisn3 IS $COMM$FK(DICTI). Указатель класса3$COMM$;
COMMENT ON COLUMN ais.dicx.classisn4 IS $COMM$FK(DICTI). Указатель класса4$COMM$;
COMMENT ON COLUMN ais.dicx.classisn5 IS $COMM$FK(DICTI). Указатель класса5$COMM$;
COMMENT ON COLUMN ais.dicx.isn IS $COMM$Машинный номер: SEQ_DICX.nextval$COMM$;
COMMENT ON COLUMN ais.dicx.classisn1 IS $COMM$FK(DICTI). Указатель класса1$COMM$;
COMMENT ON COLUMN ais.dicx.classisn2 IS $COMM$FK(DICTI). Указатель класса2$COMM$;
COMMENT ON COLUMN ais.dicx.filterisn IS $COMM$FK(SUBDEPT). Фильтр по подразделениям, если null - ограничение общего пользования$COMM$;
COMMENT ON COLUMN ais.dicx.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.dicx.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.dicx.classisn IS $COMM$FK(DICTI). Указатель класса отношения$COMM$;


CREATE TABLE ais.dicxheader (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    ind                              NUMERIC,
    typeisn                          NUMERIC,
    root                             NUMERIC,
    fieldname                        VARCHAR(40),
    fieldorder                       VARCHAR(40),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    remark                           VARCHAR(1000)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.dicxheader IS $COMM$Подробное описание полей DICX в отношении$COMM$;
COMMENT ON COLUMN ais.dicxheader.root IS $COMM$Стартовый Root Dicti$COMM$;
COMMENT ON COLUMN ais.dicxheader.fieldname IS $COMM$Какое поле отображать - (shortname,code,code+shortname)$COMM$;
COMMENT ON COLUMN ais.dicxheader.fieldorder IS $COMM$По какому полю упорядочивать - (shortname,code,code+shortname)$COMM$;
COMMENT ON COLUMN ais.dicxheader.remark IS $COMM$Коментарий$COMM$;
COMMENT ON COLUMN ais.dicxheader.classisn IS $COMM$FK(DICTI) ClassIsn из Dicx$COMM$;
COMMENT ON COLUMN ais.dicxheader.ind IS $COMM$ClassIsn1 - 1, ClassIsn2- 2, FilterIsn - 3$COMM$;
COMMENT ON COLUMN ais.dicxheader.typeisn IS $COMM$Тип отображаемого объекта (NotVisible, Dicti(default - null), Subject, Subdept ....)$COMM$;
COMMENT ON COLUMN ais.dicxheader.isn IS $COMM$PK(seq_dicx)$COMM$;


CREATE TABLE ais.docext (
    isn                              NUMERIC,
    st                               VARCHAR(5),
    kbk                              VARCHAR(40),
    okato                            VARCHAR(40),
    onp                              VARCHAR(40),
    nperiod                          VARCHAR(40),
    nid                              VARCHAR(40),
    ndate                            TIMESTAMP,
    ntype                            VARCHAR(40),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    docinfo                          VARCHAR(255),
    amount                           NUMERIC,
    cnt                              NUMERIC,
    nds                              NUMERIC,
    nds_print                        NUMERIC,
    refundisn                        NUMERIC,
    claiminvoiceisn                  BIGINT
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.docext.st IS $COMM$статус составителя расчетного документа$COMM$;
COMMENT ON COLUMN ais.docext.kbk IS $COMM$код бюджетной классификации$COMM$;
COMMENT ON COLUMN ais.docext.okato IS $COMM$код ОКАТО$COMM$;
COMMENT ON COLUMN ais.docext.onp IS $COMM$основание налогового платежа$COMM$;
COMMENT ON COLUMN ais.docext.nperiod IS $COMM$налоговый период$COMM$;
COMMENT ON COLUMN ais.docext.nid IS $COMM$номер налогового документа$COMM$;
COMMENT ON COLUMN ais.docext.ndate IS $COMM$дата налогового документа$COMM$;
COMMENT ON COLUMN ais.docext.ntype IS $COMM$тип налогового платежа$COMM$;
COMMENT ON COLUMN ais.docext.amount IS $COMM$цена ед.  (денежные документы)$COMM$;
COMMENT ON COLUMN ais.docext.cnt IS $COMM$количество уд. (денежные документы)$COMM$;
COMMENT ON COLUMN ais.docext.nds IS $COMM$Сумма НДС$COMM$;
COMMENT ON COLUMN ais.docext.nds_print IS $COMM$чекбокс на клиенте (0 = не печатать, 1 = печатать)$COMM$;
COMMENT ON COLUMN ais.docext.refundisn IS $COMM$ссылка на претензию$COMM$;
COMMENT ON COLUMN ais.docext.docinfo IS $COMM$@purpose=Дополнительная информация$COMM$;


CREATE TABLE ais.docfile (
    isn                              NUMERIC,
    docisn                           NUMERIC,
    discr                            VARCHAR(1),
    classisn                         NUMERIC,
    docdate                          TIMESTAMP,
    title                            VARCHAR(100),
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC DEFAULT 0,
    fid                              NUMERIC,
    emplisn                          NUMERIC,
    status                           VARCHAR(1),
    messageisn                       NUMERIC,
    datereceive                      TIMESTAMP,
    id                               VARCHAR(100),
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    status_updated                   TIMESTAMP,
    categoryisn                      NUMERIC,
    fulldescr                        TEXT
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.docfile IS $COMM$Описание документов, который может содержать несколько OLE-объектов. Сами объекты храняться в DOCIMAGE. Аналог. OLEDOC.$COMM$;
COMMENT ON COLUMN ais.docfile.created IS $COMM$Дата создания, регистрируется автоматически (Smirnov 31/08/09)$COMM$;
COMMENT ON COLUMN ais.docfile.createdby IS $COMM$Создатель, регистрируется автоматически (Smirnov 31/08/09)$COMM$;
COMMENT ON COLUMN ais.docfile.categoryisn IS $COMM$Категория документа (c.get('XPACKDOC_CATEGORIES')) (Smirnov 22/10/12)$COMM$;
COMMENT ON COLUMN ais.docfile.fulldescr IS $COMM$Полное описание типа документа. Оригинал хранится в dictext (Smirnov 30/10/12)$COMM$;
COMMENT ON COLUMN ais.docfile.id IS $COMM$Номер документа (Smirnov 21/06/06)$COMM$;
COMMENT ON COLUMN ais.docfile.datereceive IS $COMM$Дата получения документа (Smirnov 13/02/06)$COMM$;
COMMENT ON COLUMN ais.docfile.fid IS $COMM$Ссылка на внешний объект(например факс)$COMM$;
COMMENT ON COLUMN ais.docfile.emplisn IS $COMM$Куратор привязки$COMM$;
COMMENT ON COLUMN ais.docfile.status IS $COMM$Статус: null-аннулирована, остальные из справочника ParentISN =  .$COMM$;
COMMENT ON COLUMN ais.docfile.docisn IS $COMM$FK(<Таблица>). Ссылка на объект, образом которого является документ.$COMM$;
COMMENT ON COLUMN ais.docfile.discr IS $COMM$Дискриминатор объекта:
A-Agreement
D-Docs
R-Rule (а с ним и весь DICTI)
Q-Queue, F-AgrRefund, T-emp_testq.isn$COMM$;
COMMENT ON COLUMN ais.docfile.classisn IS $COMM$FK(DICTI). Ссылка на класс документа.$COMM$;
COMMENT ON COLUMN ais.docfile.docdate IS $COMM$Дата документа$COMM$;
COMMENT ON COLUMN ais.docfile.title IS $COMM$Заголовок документа$COMM$;
COMMENT ON COLUMN ais.docfile.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.docfile.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.docfile.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.docgrp (
    isn                              NUMERIC,
    hst                              NUMERIC(38),
    id                               NUMERIC(38),
    status                           VARCHAR(1) DEFAULT 0,
    dateupd                          TIMESTAMP DEFAULT current_timestamp,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    dateupdbuh                       TIMESTAMP,
    classisn                         NUMERIC,
    queisn                           NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.docgrp IS $COMM$Группа квитовки, объединяющая фактические и соответсвующие им плановые документы и суммы$COMM$;
COMMENT ON COLUMN ais.docgrp.isn IS $COMM$Машинный номер группы квитовки: SEQ_DOCGRP.nextval$COMM$;
COMMENT ON COLUMN ais.docgrp.hst IS $COMM$Номер сервера-владельца$COMM$;
COMMENT ON COLUMN ais.docgrp.id IS $COMM$Внешний номер группы квитовки$COMM$;
COMMENT ON COLUMN ais.docgrp.status IS $COMM$Статус группы: 0-живая, 1-удаленная$COMM$;
COMMENT ON COLUMN ais.docgrp.dateupd IS $COMM$Дата внешнего изменения, отслеживается процессом синхронизации$COMM$;
COMMENT ON COLUMN ais.docgrp.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.docgrp.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.docgrp.dateupdbuh IS $COMM$Дата внешнего изменения  - используется процессом
синхронизации результатов балансовой квитовки. (до объединения
процессов обработки балансовой и забалансовой квитовки)$COMM$;
COMMENT ON COLUMN ais.docgrp.classisn IS $COMM$FK Dicti. Тип группы (алгоритм, по которому была сформирована группа)$COMM$;
COMMENT ON COLUMN ais.docgrp.queisn IS $COMM$Уникальный машинный ISN по SEQ_QUEUE.nextval$COMM$;
COMMENT ON COLUMN ais.docgrp.created IS $COMM$Дата создания записи$COMM$;
COMMENT ON COLUMN ais.docgrp.createdby IS $COMM$Автор создания записи$COMM$;


CREATE TABLE ais.docimage (
    isn                              NUMERIC,
    fileisn                          NUMERIC,
    doctype                          VARCHAR(40),
    pageno                           NUMERIC,
    updated                          TIMESTAMP,
    oleobject                        BYTEA,
    updatedby                        NUMERIC,
    remark                           VARCHAR(255),
    filename                         VARCHAR(200),
    oleobject_checksum               NUMERIC,
    filename_checksum                NUMERIC,
    parentisn                        NUMERIC,
    synisn                           NUMERIC,
    oleobject_simple                 BYTEA,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    oleobject2                       BYTEA,
    storetype                        BIGINT,
    filesize                         NUMERIC,
    origdate                         TIMESTAMP,
    status                           VARCHAR(1),
    external_storage_id              VARCHAR(100),
    external_put_date                TIMESTAMP,
    partid                           NUMERIC,
    xmlmetadataexif                  TEXT
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.docimage IS $COMM$Необязательный OLE-объект для документов. Используется по разному для различных классов.$COMM$;
COMMENT ON COLUMN ais.docimage.filesize IS $COMM$Размер файла (BLOB-поля) в байтах (Smirnov 23/03/10)$COMM$;
COMMENT ON COLUMN ais.docimage.origdate IS $COMM$Дата создания документа (дата снимка) (Smirnov 23/03/10)$COMM$;
COMMENT ON COLUMN ais.docimage.status IS $COMM$Статус файла. NULL или Н(rus)-активный файл, А(rus)-аннулированный, В(rus)-устаревший (старая версия) (Smirnov 30/03/10)$COMM$;
COMMENT ON COLUMN ais.docimage.external_storage_id IS $COMM$Идентификатор данных во внешнем хранилище (пока только Centera) (Smirnov 28/03/11)$COMM$;
COMMENT ON COLUMN ais.docimage.external_put_date IS $COMM$Дата, после которой возможно перемещение BLOB во внешнее хранилище (Smirnov 14/04/11)$COMM$;
COMMENT ON COLUMN ais.docimage.fileisn IS $COMM$FK(DOCFILE). Ссылка на заголовок документа.$COMM$;
COMMENT ON COLUMN ais.docimage.doctype IS $COMM$Тип файла - расширение без точки. Автоматический uppercase$COMM$;
COMMENT ON COLUMN ais.docimage.pageno IS $COMM$Порядковый номер страницы документа. При удалении непоследней страницы все перенумеровывается$COMM$;
COMMENT ON COLUMN ais.docimage.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.docimage.oleobject IS $COMM$Содержимое файла. В старых записях в начале содержится заголовок OLE-контейнера$COMM$;
COMMENT ON COLUMN ais.docimage.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.docimage.remark IS $COMM$Примечание --Yunin V.A. 20/10/04$COMM$;
COMMENT ON COLUMN ais.docimage.filename IS $COMM$Имя файла без каталога. Автоматический uppercase$COMM$;
COMMENT ON COLUMN ais.docimage.oleobject_checksum IS $COMM$CRC32-сумма содержимого файла$COMM$;
COMMENT ON COLUMN ais.docimage.filename_checksum IS $COMM$CRC32-сумма имени файла$COMM$;
COMMENT ON COLUMN ais.docimage.parentisn IS $COMM$Ссылка на первичный документ (Smirnov 24/01/06)$COMM$;
COMMENT ON COLUMN ais.docimage.synisn IS $COMM$Ссылка на запись, из которой берется BLOB (Smirnov 15/07/08)$COMM$;
COMMENT ON COLUMN ais.docimage.oleobject_simple IS $COMM$Текстовое представление содержимого OLE_OBJECT (Smirnov 23/12/08)$COMM$;
COMMENT ON COLUMN ais.docimage.created IS $COMM$Дата создания, регистрируется автоматически (Smirnov 31/08/09)$COMM$;
COMMENT ON COLUMN ais.docimage.createdby IS $COMM$Создатель, регистрируется автоматически (Smirnov 31/08/09)$COMM$;
COMMENT ON COLUMN ais.docimage.oleobject2 IS $COMM$Содержимое файла$COMM$;
COMMENT ON COLUMN ais.docimage.storetype IS $COMM$Место расположения BLOB-поля (c.get('XPACKDOC_STORETYPES'))$COMM$;


CREATE TABLE ais.doclimit (
    isn                              NUMERIC,
    currisn                          NUMERIC,
    deptisn                          NUMERIC,
    dateval                          TIMESTAMP,
    amountp                          NUMERIC(20,2),
    rub4val                          VARCHAR(1),
    status                           VARCHAR(2) DEFAULT '00',
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    discr                            VARCHAR(1) DEFAULT 'L',
    parentisn                        NUMERIC,
    createdby                        NUMERIC(38),
    no                               NUMERIC,
    docisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.doclimit IS $COMM$Лимит платежей для подразделения в заданной валюте на неделю$COMM$;
COMMENT ON COLUMN ais.doclimit.parentisn IS $COMM$FK(DOCLIMIT). Ссылка на план$COMM$;
COMMENT ON COLUMN ais.doclimit.createdby IS $COMM$Создатель$COMM$;
COMMENT ON COLUMN ais.doclimit.isn IS $COMM$Машинный номер записи: SEQ_DOCLIMIT.nextval$COMM$;
COMMENT ON COLUMN ais.doclimit.currisn IS $COMM$FK(CURRENCY). Указатель валюты лимита$COMM$;
COMMENT ON COLUMN ais.doclimit.deptisn IS $COMM$FK(SUBDEPT). Указатель подразделения$COMM$;
COMMENT ON COLUMN ais.doclimit.dateval IS $COMM$Предполагаемая дата оплаты$COMM$;
COMMENT ON COLUMN ais.doclimit.amountp IS $COMM$Плановая сумма в валюте$COMM$;
COMMENT ON COLUMN ais.doclimit.rub4val IS $COMM$Флаг рубли-за-валюту: Y$COMM$;
COMMENT ON COLUMN ais.doclimit.status IS $COMM$Состояние в соотв.с технологией обработки:
00 - без визы: в работе, с номером
ГО - подписан: руководителем подразделения (ГОтов)
ПН - включен в план
$COMM$;
COMMENT ON COLUMN ais.doclimit.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.doclimit.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.doclimit.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.doclimit.discr IS $COMM$Дискриминатор лимита: P-план, L-заявка на доп.лимит$COMM$;


CREATE TABLE ais.docline (
    isn                              NUMERIC,
    listisn                          NUMERIC,
    docisn                           NUMERIC,
    docid                            NUMERIC,
    datedoc                          TIMESTAMP,
    lineno                           NUMERIC,
    doctype                          VARCHAR(2),
    corruch                          VARCHAR(9),
    corracc                          VARCHAR(20),
    amount                           NUMERIC,
    dbcr                             VARCHAR(1),
    status                           VARCHAR(1),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    headisn                          NUMERIC,
    transactno                       NUMERIC(38),
    sndinn                           VARCHAR(15),
    sndname                          VARCHAR(1000),
    sndacc                           VARCHAR(40),
    sndbic                           INT,
    sndbank                          VARCHAR(1000),
    rcvinn                           VARCHAR(15),
    rcvname                          VARCHAR(1000),
    rcvacc                           VARCHAR(40),
    rcvbic                           INT,
    rcvbank                          VARCHAR(1000),
    flagauto                         VARCHAR(1),
    created                          TIMESTAMP DEFAULT current_timestamp,
    docidin                          VARCHAR(50),
    docidoriginal                    VARCHAR(50)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.docline.isn IS $COMM$Машинный номер: SEQ_DOCLINE.nextval$COMM$;
COMMENT ON COLUMN ais.docline.listisn IS $COMM$FK(DOCLIST). Указатель списка документов$COMM$;
COMMENT ON COLUMN ais.docline.docisn IS $COMM$FK(DOCS). Ссылка на документ, соответствующий строке выписки$COMM$;
COMMENT ON COLUMN ais.docline.docid IS $COMM$Внешний номер документа$COMM$;
COMMENT ON COLUMN ais.docline.datedoc IS $COMM$Дата документа, на которую следует ориентироваться при расчете покрытия$COMM$;
COMMENT ON COLUMN ais.docline.lineno IS $COMM$Номер строки в списке$COMM$;
COMMENT ON COLUMN ais.docline.doctype IS $COMM$Тип документа (тип операции с расчетным счетом)$COMM$;
COMMENT ON COLUMN ais.docline.corruch IS $COMM$Код участника банка - корреспондента$COMM$;
COMMENT ON COLUMN ais.docline.corracc IS $COMM$Лиц.счет банка - корреспондента$COMM$;
COMMENT ON COLUMN ais.docline.amount IS $COMM$Сумма документа$COMM$;
COMMENT ON COLUMN ais.docline.dbcr IS $COMM$Признак 'Д'-дебета/'К'-кредита: списание/поступление на расч.счет$COMM$;
COMMENT ON COLUMN ais.docline.status IS $COMM$Статус строки:
null-архив (проводки сделаны)
Y-скрыжена (но необязательно DocISN задан)
N-не скрыжена, но не представляет интереса (не по осн.деятельности)$COMM$;
COMMENT ON COLUMN ais.docline.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.docline.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.docline.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.docline.headisn IS $COMM$FK(BUHHEAD). Указатель головки проводки, соответсвующей данной строке выписки$COMM$;
COMMENT ON COLUMN ais.docline.transactno IS $COMM$Номер транзакции электронных платежей$COMM$;
COMMENT ON COLUMN ais.docline.sndinn IS $COMM$ИНН плательщика$COMM$;
COMMENT ON COLUMN ais.docline.sndname IS $COMM$Наименование плательщика$COMM$;
COMMENT ON COLUMN ais.docline.sndacc IS $COMM$Счет плательщика: Расч.счет/Корр.счет банка$COMM$;
COMMENT ON COLUMN ais.docline.sndbic IS $COMM$БИК плательщика: банк/РКЦ$COMM$;
COMMENT ON COLUMN ais.docline.sndbank IS $COMM$Банк плательщика$COMM$;
COMMENT ON COLUMN ais.docline.rcvinn IS $COMM$ИНН получателя$COMM$;
COMMENT ON COLUMN ais.docline.rcvname IS $COMM$Наименование получателя$COMM$;
COMMENT ON COLUMN ais.docline.rcvacc IS $COMM$Счет получателя: Расч.счет/Корр.счет банка$COMM$;
COMMENT ON COLUMN ais.docline.rcvbic IS $COMM$БИК получателя: банк/РКЦ$COMM$;
COMMENT ON COLUMN ais.docline.rcvbank IS $COMM$Банк получателя$COMM$;
COMMENT ON COLUMN ais.docline.flagauto IS $COMM$Флаг обработки выписки автоматическим процессом расшифровки. NULL -обработан$COMM$;
COMMENT ON COLUMN ais.docline.created IS $COMM$Дата создания$COMM$;
COMMENT ON COLUMN ais.docline.docidin IS $COMM$номер документа из файла с выпиской$COMM$;
COMMENT ON COLUMN ais.docline.docidoriginal IS $COMM$исходный номер документа из файла с выпиской$COMM$;


CREATE TABLE ais.doclist (
    isn                              NUMERIC,
    accisn                           NUMERIC,
    bankdate                         TIMESTAMP,
    amountold                        NUMERIC(20,2),
    amountnew                        NUMERIC(20,2),
    totaldb                          NUMERIC(20,2),
    totalcr                          NUMERIC(20,2),
    status                           VARCHAR(1),
    discr                            VARCHAR(1) DEFAULT 'I',
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    currisn                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.doclist IS $COMM$Заголовок списка документов: банковская выписка, сопроводительный список платежек, сводный
кассовый ордер$COMM$;
COMMENT ON COLUMN ais.doclist.isn IS $COMM$Машинный номер: SEQ_DOCLIST.nextval$COMM$;
COMMENT ON COLUMN ais.doclist.accisn IS $COMM$FK(SUBACC). Ссылка на расчетный счет, соответсвующий выписке.$COMM$;
COMMENT ON COLUMN ais.doclist.bankdate IS $COMM$Банковская дата выписки.$COMM$;
COMMENT ON COLUMN ais.doclist.amountold IS $COMM$Входящее сальдо. Только для входящих документов.$COMM$;
COMMENT ON COLUMN ais.doclist.amountnew IS $COMM$Исходящее сальдо. Только для входящих документов:
AmountNew = AmountOld + TotalCr - TotalDb$COMM$;
COMMENT ON COLUMN ais.doclist.totaldb IS $COMM$Дебетовые обороты. Только для входящих документов.$COMM$;
COMMENT ON COLUMN ais.doclist.totalcr IS $COMM$Кредитовые обороты. Только для входящих документов.$COMM$;
COMMENT ON COLUMN ais.doclist.status IS $COMM$Статус выписки:
N-в работе,
Y-введена (готова к отсылке/квитовке),
D-готов дебет,
C-готов кредит,
null-архив (отослана/сквитована)
D+C=Y$COMM$;
COMMENT ON COLUMN ais.doclist.discr IS $COMM$Дискриминатор списка:
I-входящий (выписка),
O-исходящий (список платежек)$COMM$;
COMMENT ON COLUMN ais.doclist.updated IS $COMM$Дата изменения, заполняется автоматически$COMM$;
COMMENT ON COLUMN ais.doclist.updatedby IS $COMM$Автор изменения, заполняется автоматически.$COMM$;
COMMENT ON COLUMN ais.doclist.currisn IS $COMM$FK(CURRENCY). Указатель валюты выписки$COMM$;


CREATE TABLE ais.docs_t (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    agrisn                           NUMERIC,
    currisn                          NUMERIC,
    accisn                           NUMERIC,
    accnostroisn                     NUMERIC,
    payformisn                       NUMERIC,
    groupisn                         NUMERIC,
    subacc                           NUMERIC(38),
    no                               NUMERIC(38),
    id                               VARCHAR(20),
    fid                              NUMERIC(38),
    signed                           TIMESTAMP,
    status                           VARCHAR(2) DEFAULT '00',
    doc_type                         VARCHAR(7),
    mfo_rem                          INT,
    rem_acc_no                       VARCHAR(40),
    debit_acc_id                     VARCHAR(40),
    remisn                           NUMERIC,
    rem_inn                          VARCHAR(15),
    remittant_name                   VARCHAR(1000),
    rem_bank_name                    VARCHAR(1000),
    rem_city                         VARCHAR(40),
    mfo_rec                          INT,
    rec_acc_no                       VARCHAR(40),
    credit_acc_id                    VARCHAR(40),
    credit_acc_id2                   VARCHAR(40),
    recisn                           NUMERIC,
    rec_inn                          VARCHAR(15),
    receipient_name                  VARCHAR(1000),
    rec_bank_name                    VARCHAR(1000),
    rec_city                         VARCHAR(40),
    credit_amount                    NUMERIC,
    credit_amount2                   NUMERIC,
    amount_cur_1                     NUMERIC,
    payment_purpose                  VARCHAR(1000),
    payment_date                     TIMESTAMP,
    payment_order                    NUMERIC,
    datecre                          TIMESTAMP,
    usercre                          NUMERIC,
    deptown                          NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    dateaut                          TIMESTAMP,
    useraut                          NUMERIC,
    recaccisn                        NUMERIC,
    remainder                        NUMERIC(23,5),
    okpo                             NUMERIC,
    okohx                            VARCHAR(255),
    address                          VARCHAR(255),
    city                             VARCHAR(40),
    country                          VARCHAR(40),
    postcode                         VARCHAR(10),
    langisn                          NUMERIC DEFAULT 197,
    bankcountry                      VARCHAR(40),
    bankaddress                      VARCHAR(255),
    flg                              VARCHAR(1),
    buhaccisn                        NUMERIC,
    userown                          NUMERIC,
    losses                           VARCHAR(20),
    paymediaisn                      NUMERIC,
    subjaccisn                       NUMERIC,
    subjisn                          NUMERIC,
    subjinfo                         VARCHAR(255),
    docno                            NUMERIC(38),
    cardtypeisn                      NUMERIC,
    accisn2                          NUMERIC,
    printnumber                      NUMERIC,
    doclimitisn                      NUMERIC,
    updversion                       NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    policyyear                       TIMESTAMP,
    kindaccisn                       NUMERIC,
    rem_kpp                          VARCHAR(15),
    rec_kpp                          VARCHAR(15),
    buhclassisn                      NUMERIC,
    planisn                          NUMERIC,
    securitylevel                    NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.docs_t.planisn IS $COMM$FK(DICTI). Указатель класса плана счетов бухгалтерии$COMM$;
COMMENT ON COLUMN ais.docs_t.securitylevel IS $COMM$Минимальный уровень доступа к данному документу$COMM$;
COMMENT ON COLUMN ais.docs_t.isn IS $COMM$Машинный номер документа: DOCS_SEQ.nextval$COMM$;
COMMENT ON COLUMN ais.docs_t.parentisn IS $COMM$FK(DOCS). Ссылка на родительский документ, например, из фактического на плановый.$COMM$;
COMMENT ON COLUMN ais.docs_t.currisn IS $COMM$FK(CURRENCY). Указатель валюты документа$COMM$;
COMMENT ON COLUMN ais.docs_t.accisn IS $COMM$FK(SUBACC). Указатель банковского счета Ингосстраха$COMM$;
COMMENT ON COLUMN ais.docs_t.accnostroisn IS $COMM$FK(SUBACC). Указатель корсчета банка при расчетах через банк-корреспондент$COMM$;
COMMENT ON COLUMN ais.docs_t.payformisn IS $COMM$FK(DICTI). Форма оплаты: банк, касса, чек...$COMM$;
COMMENT ON COLUMN ais.docs_t.subacc IS $COMM$Субсчет плана счетов, соответсвующий виду страхования$COMM$;
COMMENT ON COLUMN ais.docs_t.no IS $COMM$Порядковый номер документа в рамках фирмы, подразделения, сотрудника$COMM$;
COMMENT ON COLUMN ais.docs_t.id IS $COMM$Внешний идентификатор документа$COMM$;
COMMENT ON COLUMN ais.docs_t.fid IS $COMM$Внешний машинный номер$COMM$;
COMMENT ON COLUMN ais.docs_t.signed IS $COMM$Дата подписания (дата документа)$COMM$;
COMMENT ON COLUMN ais.docs_t.status IS $COMM$Состояние в соотв.с технологией обработки:
ШБ - ШаБлон
КЗ - отказ банка
ОШ - ОШибка в реквизитах
-1  - создан системой: новый, без номера
00 - без визы: в работе, с номером
ГО - подписан: руководителем подразделения (ГОтов)
ПН - заПлаНирован
РЦ - в бухгалтерии: готов к отправке
ВП - квитуется: отправлен в бухгалтерию (ВыПущен)
ОЛ - ОтЛожен до лучших времен
БК - в БанКе
ВЯ - до ВыЯснения после выписки
99  - оплачен (скрыжен, полностью сквитован)
98-отказ банка получателя
АН - АНнулирован$COMM$;
COMMENT ON COLUMN ais.docs_t.doc_type IS $COMM$Тип документа:
01-исходящее платежное поручение,
02-входящее платежное поручение,
05-расходный кассовый ордер,
06-приходный кассовый ордер,
07-расходный ордер внутренней кассы
08-приходный ордер внутренней кассы
21-исходящий счет
$COMM$;
COMMENT ON COLUMN ais.docs_t.mfo_rem IS $COMM$МФО плательщика: банк/РКЦ$COMM$;
COMMENT ON COLUMN ais.docs_t.rem_acc_no IS $COMM$Счет плательщика: Расч.счет/Корр.счет банка$COMM$;
COMMENT ON COLUMN ais.docs_t.debit_acc_id IS $COMM$р/с плательщика в плане счетов своего банка$COMM$;
COMMENT ON COLUMN ais.docs_t.rem_inn IS $COMM$ИНН плательщика$COMM$;
COMMENT ON COLUMN ais.docs_t.remittant_name IS $COMM$Наименование плательщика$COMM$;
COMMENT ON COLUMN ais.docs_t.rem_bank_name IS $COMM$Банк плательщика$COMM$;
COMMENT ON COLUMN ais.docs_t.rem_city IS $COMM$Город банка плательщика$COMM$;
COMMENT ON COLUMN ais.docs_t.mfo_rec IS $COMM$МФО получателя: банк/РКЦ$COMM$;
COMMENT ON COLUMN ais.docs_t.rec_acc_no IS $COMM$Счет получателя: Расч.счет/Корр.счет банка$COMM$;
COMMENT ON COLUMN ais.docs_t.credit_acc_id IS $COMM$р/с получателя в плане счетов банка получателя$COMM$;
COMMENT ON COLUMN ais.docs_t.credit_acc_id2 IS $COMM$Дополнительный р-с получателя, например комиссия РКЦ за телеграфный перевод$COMM$;
COMMENT ON COLUMN ais.docs_t.rec_inn IS $COMM$ИНН получателя$COMM$;
COMMENT ON COLUMN ais.docs_t.receipient_name IS $COMM$Наименование получателя$COMM$;
COMMENT ON COLUMN ais.docs_t.rec_bank_name IS $COMM$Банк получателя$COMM$;
COMMENT ON COLUMN ais.docs_t.rec_city IS $COMM$Город банка получателя$COMM$;
COMMENT ON COLUMN ais.docs_t.credit_amount IS $COMM$Сумма кредита 1$COMM$;
COMMENT ON COLUMN ais.docs_t.credit_amount2 IS $COMM$Сумма кредита 2$COMM$;
COMMENT ON COLUMN ais.docs_t.amount_cur_1 IS $COMM$Сумма документа$COMM$;
COMMENT ON COLUMN ais.docs_t.payment_purpose IS $COMM$Назначение платежа$COMM$;
COMMENT ON COLUMN ais.docs_t.payment_date IS $COMM$Срок платежа$COMM$;
COMMENT ON COLUMN ais.docs_t.payment_order IS $COMM$Очередность платежа$COMM$;
COMMENT ON COLUMN ais.docs_t.datecre IS $COMM$Дата создания документа в БД, устанавливается автоматически$COMM$;
COMMENT ON COLUMN ais.docs_t.usercre IS $COMM$FK(SUBHUMAN). Создатель, устанавливается автоматически$COMM$;
COMMENT ON COLUMN ais.docs_t.deptown IS $COMM$Подразделение - владелец, устанавливается автоматически, если не задано$COMM$;
COMMENT ON COLUMN ais.docs_t.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.docs_t.updatedby IS $COMM$FK(SUBHUMAN). Автор изменения$COMM$;
COMMENT ON COLUMN ais.docs_t.dateaut IS $COMM$Дата авторизации записи (изменения Status), устанавливается автоматически$COMM$;
COMMENT ON COLUMN ais.docs_t.useraut IS $COMM$FK(SUBHUMAN).  Авторизатор, устанавливается автоматически$COMM$;
COMMENT ON COLUMN ais.docs_t.recaccisn IS $COMM$FK(SUBACC). Указатель банковского счета получателя$COMM$;
COMMENT ON COLUMN ais.docs_t.remainder IS $COMM$Несквитованный  остаток в валюте документа$COMM$;
COMMENT ON COLUMN ais.docs_t.okpo IS $COMM$Код ОКПО$COMM$;
COMMENT ON COLUMN ais.docs_t.okohx IS $COMM$Коды ОКОНХ$COMM$;
COMMENT ON COLUMN ais.docs_t.address IS $COMM$Адрес$COMM$;
COMMENT ON COLUMN ais.docs_t.city IS $COMM$Город$COMM$;
COMMENT ON COLUMN ais.docs_t.country IS $COMM$Страна$COMM$;
COMMENT ON COLUMN ais.docs_t.postcode IS $COMM$Почтовый индекс$COMM$;
COMMENT ON COLUMN ais.docs_t.langisn IS $COMM$FK(DICTI). Язык документа, по умолчанию - русский$COMM$;
COMMENT ON COLUMN ais.docs_t.bankcountry IS $COMM$Страна банка получателя$COMM$;
COMMENT ON COLUMN ais.docs_t.bankaddress IS $COMM$Адрес банка получателя$COMM$;
COMMENT ON COLUMN ais.docs_t.flg IS $COMM$Флаг для спецотметок при подготовке отчетности$COMM$;
COMMENT ON COLUMN ais.docs_t.buhaccisn IS $COMM$FK(DICTI). Указатель на базовый расчетный счет плана счетов, со стороны которого
проводка, соответствующая документу, будет иметь одну полупроводку.$COMM$;
COMMENT ON COLUMN ais.docs_t.userown IS $COMM$FK(SUBHUMAN). Владелец документа$COMM$;
COMMENT ON COLUMN ais.docs_t.losses IS $COMM$Номера убытков для выгрузки в Informix$COMM$;
COMMENT ON COLUMN ais.docs_t.paymediaisn IS $COMM$FK(DICTI). Указатель способа оплаты (почтой, телеграфом, электронно)$COMM$;
COMMENT ON COLUMN ais.docs_t.subjaccisn IS $COMM$FK(SUBACC). Указатель лицевого счета получателя - физического лица, который
помещается в назначение платежа$COMM$;
COMMENT ON COLUMN ais.docs_t.subjisn IS $COMM$FK(SUBJECT). Указатель получателя - физического лица, который помещается в назначение
платежа$COMM$;
COMMENT ON COLUMN ais.docs_t.subjinfo IS $COMM$Информация по физлицу (ФИО+лицевой счет)$COMM$;
COMMENT ON COLUMN ais.docs_t.docno IS $COMM$Номер платежного поручения$COMM$;
COMMENT ON COLUMN ais.docs_t.cardtypeisn IS $COMM$FK(DICTI). Указатель типа кредитной карты$COMM$;
COMMENT ON COLUMN ais.docs_t.accisn2 IS $COMM$FK(SUBACC). Указатель альтернативного банковского счета (в рублях для валютного счета)$COMM$;
COMMENT ON COLUMN ais.docs_t.printnumber IS $COMM$Кол-во выводов на печать$COMM$;
COMMENT ON COLUMN ais.docs_t.doclimitisn IS $COMM$FK(DOCLIMIT). Указатель плановой записи или лимита$COMM$;
COMMENT ON COLUMN ais.docs_t.updversion IS $COMM$Номер версии для контроля коллизий$COMM$;
COMMENT ON COLUMN ais.docs_t.datebeg IS $COMM$Дата начала периода начисления$COMM$;
COMMENT ON COLUMN ais.docs_t.dateend IS $COMM$Дата окончания периода начисления$COMM$;
COMMENT ON COLUMN ais.docs_t.policyyear IS $COMM$Страховой год соглашения, по которому сделано начисление$COMM$;
COMMENT ON COLUMN ais.docs_t.kindaccisn IS $COMM$Ссылка на аналитический счет$COMM$;
COMMENT ON COLUMN ais.docs_t.buhclassisn IS $COMM$FK(DICTI). Указатель типа бухгалтерской операции, регистрируемой данным документом: оплата счета в кассу, выдача под отчет, инкассация... По сути - расширение DOC_TYPE$COMM$;


CREATE TABLE ais.docsum (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    classisn                         NUMERIC,
    datepay                          TIMESTAMP,
    classisn2                        NUMERIC,
    parentisn                        NUMERIC,
    amount                           NUMERIC(23,5),
    sumisn                           NUMERIC,
    amountrub                        NUMERIC(23,5),
    docisn                           NUMERIC,
    docisn2                          NUMERIC,
    subjisn                          NUMERIC,
    docid                            VARCHAR(20),
    splitisn                         NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    remainder                        NUMERIC(23,5),
    amountagr                        NUMERIC(23,5),
    amountusd                        NUMERIC(20,2),
    currisn                          NUMERIC,
    groupisn                         NUMERIC,
    agrid                            VARCHAR(20),
    fid                              NUMERIC(38),
    payform                          VARCHAR(1) DEFAULT 4,
    status                           VARCHAR(1) DEFAULT 'Н',
    discr                            VARCHAR(1),
    reaccisn                         NUMERIC,
    agrcurrisn                       NUMERIC,
    directionflg                     VARCHAR(1) DEFAULT 'I',
    accisn                           NUMERIC,
    subaccisn                        NUMERIC,
    debetisn                         NUMERIC,
    creditisn                        NUMERIC,
    doccurrisn                       NUMERIC,
    amountdoc                        NUMERIC(23,5),
    docdate                          TIMESTAMP,
    docrate                          NUMERIC(20,10),
    refundisn                        NUMERIC,
    doc_type                         VARCHAR(7),
    docadjustment                    NUMERIC(9,6),
    indocisn                         NUMERIC,
    regressisn                       NUMERIC,
    kindaccisn                       NUMERIC,
    signed                           TIMESTAMP,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    deptisn                          NUMERIC,
    lineisn                          NUMERIC,
    datepaylast                      TIMESTAMP,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    ragentflg                        VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.docsum IS $COMM$Суммы взаиморасчетов, интересующие бухгалтерию, на основании которых делаются проводки.
Сумма обычно относится к некоторому субъекту, договору, документу.
Может быть плановой или фактической, что определяется дискриминатором.
Фактическая сумма обязательно ссылается на документ, плановая - на субъекта и договор.
Класс суммы определяет бухгалтерскую операцию, вместе с дискриминатором и формой оплаты задает соответствующие проводки.
Класс плановой суммы задается при начислении, фактической - при квитовке.
Знак суммы определяет приход/расход: для фактических - приход > 0, для плановых - расход > 0.
Фактические суммы квитуются с плановыми, образуя группы квитовки (в простейшем случае - пары).
Все суммы группы должны относиться к одному субъекту.
Суммы объединяются в группы посредством ссылки посредством ParentISN на специальную сумму невязки, помещаемую в эту же  таблицу.
Пока сумма несквитована, ее ParentISN=0. Для суммы невязки частичная квитовка обозначается ParentISN=0, полная ParentISN=null.
Сумма невязки определяется как алгебраическая сумма всех сумм группы, взятая со знаком минус.
Таким образом отрицательная сумма невязки соответствует дебиторской задолженности, а положительная - кредиторской.
SQL для подсчета дебиторско-кредиторской задолженности по клиенту:
   select sum(AmountRub) from DOCSUM where ParentISN=0 and SubjISN=:ISN$COMM$;
COMMENT ON COLUMN ais.docsum.ragentflg IS $COMM$Флаг формирования суммы через расчеты с агентами$COMM$;
COMMENT ON COLUMN ais.docsum.isn IS $COMM$Машинный номер записи, по умолчанию генерируется автоматически: SEQ_DOCSUM.nextval$COMM$;
COMMENT ON COLUMN ais.docsum.agrisn IS $COMM$FK(AGREEMENT). Указатель договора, к которому относится сумма.$COMM$;
COMMENT ON COLUMN ais.docsum.classisn IS $COMM$FK(DICTI,СУММА). Тип суммы (как бухгалтерской операции для автоматической генерации
проводок). null-тип не определен (смешанный).$COMM$;
COMMENT ON COLUMN ais.docsum.datepay IS $COMM$Планируемая или фактическая дата платежа.$COMM$;
COMMENT ON COLUMN ais.docsum.classisn2 IS $COMM$FK(DICTI). Указатель подкласса суммы. ClassISN - его родитель$COMM$;
COMMENT ON COLUMN ais.docsum.parentisn IS $COMM$FK(DOCSUM). ссылка на плановую суму, сквитованную с данной$COMM$;
COMMENT ON COLUMN ais.docsum.amount IS $COMM$Сумма в валюте$COMM$;
COMMENT ON COLUMN ais.docsum.sumisn IS $COMM$Машинный номер инстоллмента: SEQ_DOCSUM_SUM.nextval$COMM$;
COMMENT ON COLUMN ais.docsum.amountrub IS $COMM$Сумма покрытия в локальной валюте.$COMM$;
COMMENT ON COLUMN ais.docsum.docisn IS $COMM$FK(DOCS). Указатель документа, в который включена сумма, например, исходящий счет для
начисленной премии, платежное поручение для суммы возмещения.$COMM$;
COMMENT ON COLUMN ais.docsum.docisn2 IS $COMM$Ссылка на бухгалтерское начисление$COMM$;
COMMENT ON COLUMN ais.docsum.subjisn IS $COMM$FK(SUBJECT). Указатель субъекта, к которому относится сумма.$COMM$;
COMMENT ON COLUMN ais.docsum.docid IS $COMM$Внешний номер документа, в который вошла сумма. При задании DocISN устанавливается
автоматически. При отсутствии DocISN может быть задан вручную, независимо от наличия документа в БД.$COMM$;
COMMENT ON COLUMN ais.docsum.splitisn IS $COMM$FK(DOCSUM). Ссылка на исходную сумму, врезультате разбиения которой возникла данная$COMM$;
COMMENT ON COLUMN ais.docsum.updated IS $COMM$Дата изменения, устанавливается автоматически.$COMM$;
COMMENT ON COLUMN ais.docsum.updatedby IS $COMM$Автор изменения, устанавливается автоматически.$COMM$;
COMMENT ON COLUMN ais.docsum.remainder IS $COMM$Несквитованный остаток в валюте документа$COMM$;
COMMENT ON COLUMN ais.docsum.amountagr IS $COMM$Сумма в валюте договора$COMM$;
COMMENT ON COLUMN ais.docsum.amountusd IS $COMM$Сумма покрытия в USD$COMM$;
COMMENT ON COLUMN ais.docsum.currisn IS $COMM$FK(CURRENCY). Указатель валюты суммы.$COMM$;
COMMENT ON COLUMN ais.docsum.agrid IS $COMM$Номер полиса. Поддерживается по AgrISN автоматически. М.б.заполнено при импорте
начислений.$COMM$;
COMMENT ON COLUMN ais.docsum.fid IS $COMM$Внешний машинный номер записи$COMM$;
COMMENT ON COLUMN ais.docsum.payform IS $COMM$FK(DICTI.Code,accPayForm). Форма оплаты, желаемая или фактическая. Наследуется из
DOCS:
1-Касса
2-Чек
3-Кредитная карта
4-Банк
5-Счет
6-Резерв
7-SOVAG
8-Взаиморасчет
9-Спонсорство
$COMM$;
COMMENT ON COLUMN ais.docsum.status IS $COMM$Статус суммы, поддерживается автоматически по ParentISN и Status родителя, DocISN и
DOCS.Status.
Для плановых сумм (Discr=P):
Н - начислена (не включена в документ: DocISN=null)
Д - включена в документ (документ не выпущен: DocISN<>null, DOCS.Status<>ВП)
В - выставлена (документ выпущен)
Ч - частично сквитована (ParentISN<>null, ParentISN(ParentISN)=0)
null - полностью сквитована (ParentISN<>null, ParentISN(ParentISN)=null)$COMM$;
COMMENT ON COLUMN ais.docsum.discr IS $COMM$Дискриминатор типа суммы: P-плановая, F-фактическая$COMM$;
COMMENT ON COLUMN ais.docsum.reaccisn IS $COMM$FK(REACC100). Указатель на начисление (заголовок 100 счета), к которому относится
объект.$COMM$;
COMMENT ON COLUMN ais.docsum.agrcurrisn IS $COMM$FK(CURRENCY). Указатель валюты договора. Наследуется из AGREEMENT по AgrISN$COMM$;
COMMENT ON COLUMN ais.docsum.directionflg IS $COMM$Флаг направления взаиморасчетов:
I-входящие (до ИГС: прямые дела,  входящее факультативное перестрахование),
O-исходящие (после ИГС: исх.факультативное перестрахование)$COMM$;
COMMENT ON COLUMN ais.docsum.accisn IS $COMM$not used$COMM$;
COMMENT ON COLUMN ais.docsum.subaccisn IS $COMM$FK(DICTI). Указатель аналитического счета$COMM$;
COMMENT ON COLUMN ais.docsum.doccurrisn IS $COMM$FK(CURRENCY). Указатель валюты документа. Наследуется из DOCS по DocISN$COMM$;
COMMENT ON COLUMN ais.docsum.amountdoc IS $COMM$Сумма в валюте документа, поддерживается автоматически = Amount * DocRate$COMM$;
COMMENT ON COLUMN ais.docsum.docdate IS $COMM$Дата, на которую определяется курс для пересчета в валюту документа DocRate.
Наследуется из DOCS.Signed$COMM$;
COMMENT ON COLUMN ais.docsum.docrate IS $COMM$Курс пересчета в валюту документа, определяется по дате документа DocDate$COMM$;
COMMENT ON COLUMN ais.docsum.refundisn IS $COMM$FK(AGRREFUND/AGREEMENT/RESECTION). Ссылка на претензию или аддендум, по которому сделано
начисление. А для перестраховочных облигаторов - ссылка на секцию$COMM$;
COMMENT ON COLUMN ais.docsum.doc_type IS $COMM$Тип документа:
01-исходящее платежное поручение,
02-входящее платежное поручение,
05-расходный кассовый ордер,
06-приходный кассовый ордер,
21-исходящий счет
$COMM$;
COMMENT ON COLUMN ais.docsum.docadjustment IS $COMM$Поправка курса пересчета в валюту документа в процентах, наследуется из
DOCS.Credit_Amount$COMM$;
COMMENT ON COLUMN ais.docsum.indocisn IS $COMM$Ссылка на счет-калькуляцию в автостраховании$COMM$;
COMMENT ON COLUMN ais.docsum.regressisn IS $COMM$FK(REGRESS). Указатель регресса, к которому относится сумма$COMM$;
COMMENT ON COLUMN ais.docsum.signed IS $COMM$Дата документа, наследуется из DOCS. Используется для автоматического перерасчета эквивалента как дата курса, если не задана DocDate, фиксирующая курс.$COMM$;
COMMENT ON COLUMN ais.docsum.created IS $COMM$Дата создания$COMM$;
COMMENT ON COLUMN ais.docsum.createdby IS $COMM$Автор$COMM$;
COMMENT ON COLUMN ais.docsum.deptisn IS $COMM$FK(SUBDEPT) Указатель подразделения, наследуется из DOCS$COMM$;
COMMENT ON COLUMN ais.docsum.lineisn IS $COMM$FK(DOCSUM) Указатель строки выписки в результате предварительной расшифровки$COMM$;
COMMENT ON COLUMN ais.docsum.datebeg IS $COMM$Начальная дата периода начисления по условиям договора$COMM$;
COMMENT ON COLUMN ais.docsum.dateend IS $COMM$Конечная дата периода начисления по условиям договора$COMM$;


CREATE TABLE ais.docsum_auto (
    isn                              NUMERIC,
    docisn                           NUMERIC,
    datepay                          TIMESTAMP,
    currisn                          NUMERIC,
    amount                           NUMERIC(23,5),
    classisn                         NUMERIC,
    classisn2                        NUMERIC,
    subjisn                          NUMERIC,
    docid                            VARCHAR(20),
    agrid                            VARCHAR(20),
    payform                          VARCHAR(1),
    fid                              NUMERIC,
    discr                            VARCHAR(1),
    status                           VARCHAR(1),
    remainder                        NUMERIC(23,5),
    amountdoc                        NUMERIC(23,5),
    doccurrisn                       NUMERIC,
    subaccisn                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.docsum_auto.remainder IS $COMM$Несквитованный остаток в валюте документа$COMM$;
COMMENT ON COLUMN ais.docsum_auto.amountdoc IS $COMM$Сумма в валюте документа, поддерживается автоматически = Amount * DocRate$COMM$;
COMMENT ON COLUMN ais.docsum_auto.doccurrisn IS $COMM$FK(CURRENCY). Указатель  валюты документа$COMM$;
COMMENT ON COLUMN ais.docsum_auto.subaccisn IS $COMM$FK(DICTI). Указатель кода аналитики$COMM$;
COMMENT ON COLUMN ais.docsum_auto.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.docsum_auto.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.docsum_auto.isn IS $COMM$Машинный номер SEQ_DOCSUM.nextval$COMM$;
COMMENT ON COLUMN ais.docsum_auto.docisn IS $COMM$FK(DOCS). Указатель документа, в который включена сумма, например, исходящий счет
для начисленной премии, платежное поручение для суммы возмещения.$COMM$;
COMMENT ON COLUMN ais.docsum_auto.datepay IS $COMM$Планируемая или фактическая дата платежа.$COMM$;
COMMENT ON COLUMN ais.docsum_auto.currisn IS $COMM$FK(CURRENCY). Указатель  валюты суммы$COMM$;
COMMENT ON COLUMN ais.docsum_auto.amount IS $COMM$Сумма в валюте$COMM$;
COMMENT ON COLUMN ais.docsum_auto.classisn IS $COMM$FK(DICTI). Указатель класса суммы$COMM$;
COMMENT ON COLUMN ais.docsum_auto.classisn2 IS $COMM$FK(DICTI). Указатель подкласса суммы$COMM$;
COMMENT ON COLUMN ais.docsum_auto.subjisn IS $COMM$FK(SUBJECT). Указатель субъекта суммы$COMM$;
COMMENT ON COLUMN ais.docsum_auto.docid IS $COMM$Внешний номер документа, в который вошла сумма. При задании DocISN устанавливается
автоматически. При отсутствии DocISN может быть задан вручную, независимо от наличия документа в БД.$COMM$;
COMMENT ON COLUMN ais.docsum_auto.agrid IS $COMM$Номер полиса. Поддерживается по AgrISN автоматически. М.б.заполнено при импорте
начислений.$COMM$;
COMMENT ON COLUMN ais.docsum_auto.payform IS $COMM$Форма оплаты$COMM$;
COMMENT ON COLUMN ais.docsum_auto.fid IS $COMM$Внешний машинный номер записи$COMM$;
COMMENT ON COLUMN ais.docsum_auto.discr IS $COMM$Дискриминатор типа суммы: P-плановая, F-фактическая$COMM$;
COMMENT ON COLUMN ais.docsum_auto.status IS $COMM$Статус суммы, поддерживается автоматически по ParentISN и Status родителя, DocISN
и DOCS.Status.
Для плановых сумм (Discr=P):
Н - начислена (не включена в документ: DocISN=null)
Д - включена в документ (документ не выпущен: DocISN<>null, DOCS.Status<>ВП)
В - выставлена (документ выпущен)
Ч - частично сквитована (ParentISN<>null, ParentISN(ParentISN)=0)
null - полностью сквитована (ParentISN<>null, ParentISN(ParentISN)=null)$COMM$;


CREATE TABLE ais.docsumext (
    isn                              NUMERIC,
    docsumisn                        NUMERIC,
    id                               VARCHAR(20),
    datepay                          TIMESTAMP,
    amount                           NUMERIC,
    currisn                          NUMERIC,
    docrate                          NUMERIC,
    discr                            VARCHAR(10),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    classisn                         NUMERIC,
    extref1                          NUMERIC,
    agrid                            VARCHAR(255),
    agrdate                          TIMESTAMP,
    docid                            VARCHAR(255),
    docdate                          TIMESTAMP,
    paspid                           VARCHAR(255),
    paspdate                         TIMESTAMP,
    paspisn                          NUMERIC,
    amountagr                        NUMERIC,
    agrcurrisn                       NUMERIC,
    amountagr1                       NUMERIC,
    amountdoc                        NUMERIC,
    amountdoc1                       NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.docsumext IS $COMM$Используется для привязки квитанции А7 к платежу$COMM$;
COMMENT ON COLUMN ais.docsumext.agrid IS $COMM$@purpose=Номер договора, может быть больше 20 символов$COMM$;
COMMENT ON COLUMN ais.docsumext.agrdate IS $COMM$@purpose=Дата договора$COMM$;
COMMENT ON COLUMN ais.docsumext.docid IS $COMM$@purpose=Номер счета из распоряжения (doc_type=40)$COMM$;
COMMENT ON COLUMN ais.docsumext.docdate IS $COMM$@purpose=Дата счета$COMM$;
COMMENT ON COLUMN ais.docsumext.paspdate IS $COMM$@purpose=Дата паспорта сделки$COMM$;
COMMENT ON COLUMN ais.docsumext.paspid IS $COMM$@purpose=Номер паспорта сделки$COMM$;
COMMENT ON COLUMN ais.docsumext.isn IS $COMM$Уникальный идентификатор SEQ_DOCSUMEXT.$COMM$;
COMMENT ON COLUMN ais.docsumext.docsumisn IS $COMM$FK(DOCSUM) Указатель на плановый платеж.$COMM$;
COMMENT ON COLUMN ais.docsumext.id IS $COMM$Номер документа.$COMM$;
COMMENT ON COLUMN ais.docsumext.datepay IS $COMM$Дата платежа.$COMM$;
COMMENT ON COLUMN ais.docsumext.amount IS $COMM$Сумма платежа.$COMM$;
COMMENT ON COLUMN ais.docsumext.currisn IS $COMM$Валюта платежа.$COMM$;
COMMENT ON COLUMN ais.docsumext.docrate IS $COMM$Курс валюты платежа к валюте полиса.$COMM$;
COMMENT ON COLUMN ais.docsumext.discr IS $COMM$Дискриминатор. (А7 - Форма А-7)$COMM$;
COMMENT ON COLUMN ais.docsumext.updated IS $COMM$Дата последнего обновления$COMM$;
COMMENT ON COLUMN ais.docsumext.updatedby IS $COMM$FK(DICTI) Пользователь сделавший последнее обновление$COMM$;
COMMENT ON COLUMN ais.docsumext.classisn IS $COMM$FK(DICTI) Класс расширения (дискриминатор)$COMM$;
COMMENT ON COLUMN ais.docsumext.extref1 IS $COMM$FK(DICTI) Указатель на любой объект (тип объекта в зависимости от CLASSISN)$COMM$;


CREATE TABLE ais.doctemplate (
    isn                              NUMERIC,
    bankisn                          NUMERIC,
    discr                            VARCHAR(2),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    name                             VARCHAR(255),
    deptisn                          NUMERIC,
    shortname                        VARCHAR(40),
    classisn                         NUMERIC,
    status                           VARCHAR(1),
    langisn                          NUMERIC,
    doctype                          VARCHAR(3),
    synisn                           NUMERIC,
    oleobject                        BYTEA,
    id                               VARCHAR(20),
    securityisn                      NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.doctemplate IS $COMM$OLE-объект - макет документа для формирования печатного  документа на основе полей
некоторого объекта БД$COMM$;
COMMENT ON COLUMN ais.doctemplate.securityisn IS $COMM$FK(DICTI) тип защиты шаблона (Угринович 24.03.06)$COMM$;
COMMENT ON COLUMN ais.doctemplate.id IS $COMM$Код формы (Smirnov 07/11/05)$COMM$;
COMMENT ON COLUMN ais.doctemplate.isn IS $COMM$Машинный номер: SEQ_DOCTEMPLATE.nextval$COMM$;
COMMENT ON COLUMN ais.doctemplate.bankisn IS $COMM$FK(<Таблица>). Ссылка на любой объект из DICTI,
используется
а) в заявлениях платежных переводов дя выбора темплейта$COMM$;
COMMENT ON COLUMN ais.doctemplate.discr IS $COMM$Дискриминатор объекта ( констрейном не  поддерживается)
A (agreement) - договор
C (claim) -- убытки
D (doc)- платежный документ договор
F - Formula One
I (invoice) - счет
K - кадры
L - счет-платеж
P - платежки и заявления на перевод
T (temporary) - временно хранимый шаблон в стадии разработки
O - офисный документ: служебки, заявления, и т.д.$COMM$;
COMMENT ON COLUMN ais.doctemplate.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.doctemplate.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.doctemplate.name IS $COMM$Описание шаблона$COMM$;
COMMENT ON COLUMN ais.doctemplate.deptisn IS $COMM$FK(SUBDEPT). Указатель подразделения, использующего шаблон. Null-шаблон общего
пользования$COMM$;
COMMENT ON COLUMN ais.doctemplate.shortname IS $COMM$Краткое описание шаблона (служит для формирования меню)$COMM$;
COMMENT ON COLUMN ais.doctemplate.status IS $COMM$O - старый формат (без исп. XML, не показ. в АИС2000), А - активный$COMM$;
COMMENT ON COLUMN ais.doctemplate.langisn IS $COMM$FK(Dicti). Язык документа$COMM$;
COMMENT ON COLUMN ais.doctemplate.doctype IS $COMM$OLE-объект - макет документа$COMM$;
COMMENT ON COLUMN ais.doctemplate.synisn IS $COMM$"W95' - MS Word 6.0/95, 'W97' - MS Word 97-2000$COMM$;


CREATE TABLE ais.dtpdata (
    isn                              NUMERIC,
    carisn                           NUMERIC,
    agrclaimisn                      NUMERIC,
    dtpdate                          TIMESTAMP,
    regnum                           VARCHAR(20),
    vin                              VARCHAR(20),
    area                             NUMERIC,
    subarea                          VARCHAR(2),
    acceleration                     NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    accelerationy                    NUMERIC,
    accelerationz                    NUMERIC,
    aisvin                           VARCHAR(20)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.dtpdata.aisvin IS $COMM$@purpose=Идентификационный номер ТС с учетом замены русских букв$COMM$;


CREATE TABLE ais.emp_career (
    isn                              NUMERIC,
    emplisn                          NUMERIC,
    shtatisn                         NUMERIC,
    orderbegisn                      NUMERIC,
    begindate                        TIMESTAMP,
    orderendisn                      NUMERIC,
    enddate                          TIMESTAMP,
    stavka                           NUMERIC(4,3) DEFAULT 1,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    status                           NUMERIC(38),
    parentisn                        NUMERIC,
    uvolisn                          NUMERIC,
    fid                              NUMERIC,
    sovm                             VARCHAR(1),
    trialend                         TIMESTAMP
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.emp_career IS $COMM$Карьера сотрудников$COMM$;
COMMENT ON COLUMN ais.emp_career.isn IS $COMM$Машинный номер объекта.$COMM$;
COMMENT ON COLUMN ais.emp_career.emplisn IS $COMM$ISN сотрудника (FK SUBHUMAN)$COMM$;
COMMENT ON COLUMN ais.emp_career.shtatisn IS $COMM$ISN штатной единицы (FK EMP_SHTAT)$COMM$;
COMMENT ON COLUMN ais.emp_career.orderbegisn IS $COMM$Машинный номер объекта.$COMM$;
COMMENT ON COLUMN ais.emp_career.begindate IS $COMM$Дата начала работы на должности$COMM$;
COMMENT ON COLUMN ais.emp_career.orderendisn IS $COMM$Машинный номер объекта.$COMM$;
COMMENT ON COLUMN ais.emp_career.enddate IS $COMM$Дата окончания работы на должности$COMM$;
COMMENT ON COLUMN ais.emp_career.stavka IS $COMM$Ставка$COMM$;
COMMENT ON COLUMN ais.emp_career.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.emp_career.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.emp_career.status IS $COMM$Статус записи о карьере$COMM$;
COMMENT ON COLUMN ais.emp_career.parentisn IS $COMM$ISN предыдущей записи карьеры.
(FK EMP_CAREER)$COMM$;
COMMENT ON COLUMN ais.emp_career.uvolisn IS $COMM$ISN причины увольнения. (FK DICTI)$COMM$;
COMMENT ON COLUMN ais.emp_career.fid IS $COMM$Указатель внешней системы$COMM$;
COMMENT ON COLUMN ais.emp_career.sovm IS $COMM$Совместительство: null - нет, '1' - первое, '2' - второе и т.д. (Крылов В.)$COMM$;


CREATE TABLE ais.emp_foto (
    isn                              NUMERIC,
    foto                             BYTEA,
    status                           VARCHAR(1),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.emp_foto IS $COMM$Фотографии сотрудников$COMM$;
COMMENT ON COLUMN ais.emp_foto.isn IS $COMM$ISN сотрудника (PK EMPLOYEE)$COMM$;


CREATE TABLE ais.emp_mission (
    isn                              NUMERIC,
    emplisn                          NUMERIC,
    orderisn                         NUMERIC,
    typeisn                          NUMERIC,
    countryisn                       NUMERIC,
    cityisn                          NUMERIC,
    firmisn                          NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.emp_mission IS $COMM$Командировки сотрудников$COMM$;
COMMENT ON COLUMN ais.emp_mission.isn IS $COMM$Машинный номер объекта.$COMM$;
COMMENT ON COLUMN ais.emp_mission.emplisn IS $COMM$ISN сотрудника (FK SUBJECT)$COMM$;
COMMENT ON COLUMN ais.emp_mission.orderisn IS $COMM$Приказ на командировку (FK EMP_ORDER)$COMM$;
COMMENT ON COLUMN ais.emp_mission.typeisn IS $COMM$Вид командировки (FK DICTI)$COMM$;
COMMENT ON COLUMN ais.emp_mission.countryisn IS $COMM$Страна командировки (FK COUNTRY)$COMM$;
COMMENT ON COLUMN ais.emp_mission.cityisn IS $COMM$Город командировки (FK CITY)$COMM$;
COMMENT ON COLUMN ais.emp_mission.firmisn IS $COMM$Фирма, в которую направляется (FK SUBJECT)$COMM$;
COMMENT ON COLUMN ais.emp_mission.datebeg IS $COMM$Дата начала командировки$COMM$;
COMMENT ON COLUMN ais.emp_mission.dateend IS $COMM$Дата окончания командировки$COMM$;
COMMENT ON COLUMN ais.emp_mission.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.emp_mission.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.emp_mission.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;


CREATE TABLE ais.emp_order (
    isn                              NUMERIC,
    typeisn                          NUMERIC,
    ordnumber                        VARCHAR(255),
    orddate                          TIMESTAMP,
    remark                           VARCHAR(255),
    ordtext_old                      TEXT,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    deptisn                          NUMERIC,
    ordtext                          BYTEA
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.emp_order IS $COMM$Приказы$COMM$;
COMMENT ON COLUMN ais.emp_order.ordtext IS $COMM$Текст приказа$COMM$;
COMMENT ON COLUMN ais.emp_order.isn IS $COMM$Машинный номер объекта.$COMM$;
COMMENT ON COLUMN ais.emp_order.typeisn IS $COMM$Тип приказа (FK DICTI)$COMM$;
COMMENT ON COLUMN ais.emp_order.ordnumber IS $COMM$Номер приказа$COMM$;
COMMENT ON COLUMN ais.emp_order.orddate IS $COMM$Дата приказа$COMM$;
COMMENT ON COLUMN ais.emp_order.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.emp_order.ordtext_old IS $COMM$Текст приказа$COMM$;
COMMENT ON COLUMN ais.emp_order.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.emp_order.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.emp_otpusk (
    isn                              NUMERIC,
    emplisn                          NUMERIC,
    typeisn                          NUMERIC,
    orderisn                         NUMERIC,
    periodbeg                        TIMESTAMP,
    periodend                        TIMESTAMP,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    numday                           NUMERIC,
    otp_nom                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.emp_otpusk IS $COMM$Отпуска сотрудников$COMM$;
COMMENT ON COLUMN ais.emp_otpusk.isn IS $COMM$Машинный номер объекта.$COMM$;
COMMENT ON COLUMN ais.emp_otpusk.emplisn IS $COMM$ISN сотрудника (FK SUBJECT)$COMM$;
COMMENT ON COLUMN ais.emp_otpusk.typeisn IS $COMM$Вид отпуска (FK DICTI)$COMM$;
COMMENT ON COLUMN ais.emp_otpusk.orderisn IS $COMM$Приказ на отпуск (FK EMP_ORDER)$COMM$;
COMMENT ON COLUMN ais.emp_otpusk.periodbeg IS $COMM$Начало периода, за который предоставлен отпуск$COMM$;
COMMENT ON COLUMN ais.emp_otpusk.periodend IS $COMM$Окончание периода, за который предоставлен отпуск$COMM$;
COMMENT ON COLUMN ais.emp_otpusk.datebeg IS $COMM$Дата начала отпуска$COMM$;
COMMENT ON COLUMN ais.emp_otpusk.dateend IS $COMM$Дата окончания отпуска$COMM$;
COMMENT ON COLUMN ais.emp_otpusk.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.emp_otpusk.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;


CREATE TABLE ais.emp_shtat (
    isn                              NUMERIC,
    deptisn                          NUMERIC,
    dutyisn                          NUMERIC,
    occupied                         NUMERIC(4,3) DEFAULT 0,
    orderbegisn                      NUMERIC,
    datebeg                          TIMESTAMP,
    orderendisn                      NUMERIC,
    dateend                          TIMESTAMP,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    status                           NUMERIC(38),
    active                           VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.emp_shtat IS $COMM$Штатное расписание$COMM$;
COMMENT ON COLUMN ais.emp_shtat.isn IS $COMM$Машинный номер объекта.$COMM$;
COMMENT ON COLUMN ais.emp_shtat.deptisn IS $COMM$ISN подразделения (FK SUBDEPT)$COMM$;
COMMENT ON COLUMN ais.emp_shtat.dutyisn IS $COMM$ISN должности (FK SUBDUTY)$COMM$;
COMMENT ON COLUMN ais.emp_shtat.occupied IS $COMM$Коэффициент занятости штатной единицы$COMM$;
COMMENT ON COLUMN ais.emp_shtat.orderbegisn IS $COMM$Машинный номер объекта.$COMM$;
COMMENT ON COLUMN ais.emp_shtat.datebeg IS $COMM$Дата создания штатной единицы$COMM$;
COMMENT ON COLUMN ais.emp_shtat.orderendisn IS $COMM$Машинный номер объекта.$COMM$;
COMMENT ON COLUMN ais.emp_shtat.dateend IS $COMM$Дата закрытия$COMM$;
COMMENT ON COLUMN ais.emp_shtat.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.emp_shtat.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.emp_shtat.status IS $COMM$Статус штатной единицы$COMM$;
COMMENT ON COLUMN ais.emp_shtat.active IS $COMM$Активность штатной единицы$COMM$;


CREATE TABLE ais.employee (
    isn                              NUMERIC,
    famstatisn                       NUMERIC,
    educationisn                     NUMERIC,
    nationisn                        NUMERIC,
    scienceisn                       NUMERIC,
    delo                             NUMERIC(38),
    telefon                          VARCHAR(12),
    beginwork                        TIMESTAMP,
    permanwork                       TIMESTAMP,
    status                           NUMERIC(38),
    straxnum                         VARCHAR(20),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    regionisn                        NUMERIC,
    cityisn                          NUMERIC,
    rajon                            VARCHAR(80),
    countryisn                       NUMERIC,
    region                           VARCHAR(255),
    city                             VARCHAR(80),
    country                          VARCHAR(80),
    deptisn                          NUMERIC,
    placework                        VARCHAR(1) DEFAULT 'О'
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.employee IS $COMM$Сотрудники (продолжение SUBJECT и SUBHUMAN)$COMM$;
COMMENT ON COLUMN ais.employee.isn IS $COMM$Машинный номер объекта.$COMM$;
COMMENT ON COLUMN ais.employee.famstatisn IS $COMM$ISN семейного положения. (FK DICTI)$COMM$;
COMMENT ON COLUMN ais.employee.educationisn IS $COMM$ISN образования. (FK DICTI)$COMM$;
COMMENT ON COLUMN ais.employee.nationisn IS $COMM$ISN национальности. (FK DICTI)$COMM$;
COMMENT ON COLUMN ais.employee.scienceisn IS $COMM$ISN ученой степени. (FK DICTI)$COMM$;
COMMENT ON COLUMN ais.employee.delo IS $COMM$№ личного дела$COMM$;
COMMENT ON COLUMN ais.employee.telefon IS $COMM$Телефон$COMM$;
COMMENT ON COLUMN ais.employee.beginwork IS $COMM$Дата начала трудовой деятельности$COMM$;
COMMENT ON COLUMN ais.employee.permanwork IS $COMM$Дата начала непрерывного трудового стажа$COMM$;
COMMENT ON COLUMN ais.employee.status IS $COMM$Статус сотрудника$COMM$;
COMMENT ON COLUMN ais.employee.straxnum IS $COMM$№ страхового свидетельства$COMM$;
COMMENT ON COLUMN ais.employee.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.employee.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.employee.regionisn IS $COMM$ISN региона места рождения (FK REGION)$COMM$;
COMMENT ON COLUMN ais.employee.cityisn IS $COMM$ISN страны рождения (FK CITY)$COMM$;
COMMENT ON COLUMN ais.employee.rajon IS $COMM$Район рождения$COMM$;
COMMENT ON COLUMN ais.employee.countryisn IS $COMM$ISN страны рождения (FK COUNTRY)$COMM$;
COMMENT ON COLUMN ais.employee.region IS $COMM$Регион рождения прописью$COMM$;
COMMENT ON COLUMN ais.employee.city IS $COMM$Город рождения прописью$COMM$;
COMMENT ON COLUMN ais.employee.country IS $COMM$Страна рождения прописью$COMM$;
COMMENT ON COLUMN ais.employee.deptisn IS $COMM$FK(SUBDEPT). Подразделение по штатному расписанию$COMM$;
COMMENT ON COLUMN ais.employee.placework IS $COMM$Место работы (О - основное, Н - неосновное)$COMM$;


CREATE TABLE ais.folder (
    isn                              NUMERIC,
    formisn                          NUMERIC,
    userisn                          NUMERIC,
    shortname                        VARCHAR(40),
    active                           VARCHAR(1),
    canceldate                       TIMESTAMP,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    classisn                         NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.folder IS $COMM$Личные папки пользователей - заголовок$COMM$;
COMMENT ON COLUMN ais.folder.classisn IS $COMM$Ограничение объектов помещаемых в папку$COMM$;
COMMENT ON COLUMN ais.folder.isn IS $COMM$SEQ_FOLDER$COMM$;
COMMENT ON COLUMN ais.folder.formisn IS $COMM$isn -формы отбора$COMM$;
COMMENT ON COLUMN ais.folder.userisn IS $COMM$пользователь$COMM$;
COMMENT ON COLUMN ais.folder.shortname IS $COMM$наименование папки$COMM$;
COMMENT ON COLUMN ais.folder.active IS $COMM$статус (Y- личная папка по умолчанию, N-остальные)$COMM$;
COMMENT ON COLUMN ais.folder.canceldate IS $COMM$время до которого следует хранить информацию о папке$COMM$;
COMMENT ON COLUMN ais.folder.updated IS $COMM$время изм.$COMM$;
COMMENT ON COLUMN ais.folder.updatedby IS $COMM$автор изм.$COMM$;
COMMENT ON COLUMN ais.folder.created IS $COMM$время созд$COMM$;
COMMENT ON COLUMN ais.folder.createdby IS $COMM$автор созд$COMM$;


CREATE TABLE ais.folderitem (
    isn                              NUMERIC,
    folderisn                        NUMERIC,
    objisn                           NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    param                            NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.folderitem IS $COMM$Личные папки пользователей - объекты$COMM$;
COMMENT ON COLUMN ais.folderitem.param IS $COMM$Дополнительный параметр, например, сортировка (Smirnov 23/07/12)$COMM$;
COMMENT ON COLUMN ais.folderitem.isn IS $COMM$SEQ_FOLDERITEM$COMM$;
COMMENT ON COLUMN ais.folderitem.folderisn IS $COMM$FK Folder$COMM$;
COMMENT ON COLUMN ais.folderitem.objisn IS $COMM$объект$COMM$;


CREATE TABLE ais.holiday (
    data                             TIMESTAMP,
    day                              VARCHAR(2),
    day_week                         NUMERIC(38),
    holiday                          NUMERIC(38),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    isn                              NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.holiday.updatedby IS $COMM$Автор изменения. Устанавливается автоматически.  /* 29.06.05 SR */$COMM$;
COMMENT ON COLUMN ais.holiday.updated IS $COMM$Дата изменения. Устанавливается автоматически.   /* 29.06.05 SR */$COMM$;


CREATE TABLE ais.kind (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    code                             VARCHAR(10),
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(255),
    active                           VARCHAR(1) DEFAULT 'N',
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    sqltext                          VARCHAR(2000),
    id                               VARCHAR(20)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.kind IS $COMM$Вид аналитики. Определят справочник для получения значений видов аналитики.$COMM$;
COMMENT ON COLUMN ais.kind.isn IS $COMM$Ссылка на (Dicti) - определяет справочник значений$COMM$;
COMMENT ON COLUMN ais.kind.classisn IS $COMM$Код вида аналитики$COMM$;
COMMENT ON COLUMN ais.kind.code IS $COMM$Поисковый внешний код класса (для пользователя). Уникален в пределах суперкласса.$COMM$;
COMMENT ON COLUMN ais.kind.shortname IS $COMM$Краткое поисковое название для показа в LookUp.$COMM$;
COMMENT ON COLUMN ais.kind.fullname IS $COMM$Полное название  для отчетов, пояснений. Использование зависит от особенностей
суперкласса.$COMM$;
COMMENT ON COLUMN ais.kind.active IS $COMM$Признак активности объекта:
N - новый, не проверен администратором,
Y - проверен администратором,
NULL-считается архивным и сохраняется для декодировки ссылок из ранее созданных объектов, но недоступен для ссылок из вновь
создаваемых объектов и не показывается в LookUp.
При создании нового объекта: если задан NULL - устанавливается N, если задан X - устанавливается в NULL.
Удалять можно только архивный объект.$COMM$;
COMMENT ON COLUMN ais.kind.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.kind.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.kind.sqltext IS $COMM$Текст запроса для полуячения кодов вида аналитики. Если текста нет -  используется для
стандартный запрос по Dicti.
Текст должен быть в формате, понятном Woodу.$COMM$;


CREATE TABLE ais.kindacc (
    isn                              NUMERIC,
    kindgroupisn                     NUMERIC,
    code                             VARCHAR(20),
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(255),
    status                           VARCHAR(1) DEFAULT 'N',
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.kindacc IS $COMM$Аналитический субсчет$COMM$;
COMMENT ON COLUMN ais.kindacc.kindgroupisn IS $COMM$Ссылка на аналитическую группу (DICTI)$COMM$;
COMMENT ON COLUMN ais.kindacc.code IS $COMM$Поисковый внешний код (для пользователя). Уникален в пределах суперкласса.$COMM$;
COMMENT ON COLUMN ais.kindacc.shortname IS $COMM$Краткое поисковое название класса для показа в LookUp.$COMM$;
COMMENT ON COLUMN ais.kindacc.fullname IS $COMM$Полное название класса для отчетов, пояснений. Использование зависит от особенностей
суперкласса.$COMM$;
COMMENT ON COLUMN ais.kindacc.status IS $COMM$Признак активности объекта:
N - новый, не проверен администратором,
Y - проверен администратором,
NULL-считается архивным и сохраняется для декодировки ссылок из ранее созданных объектов, но недоступен для ссылок из вновь
создаваемых объектов и не показывается в LookUp.
При создании нового объекта: если задан NULL - устанавливается N, если задан X - устанавливается в NULL.
Удалять можно только архивный объект.$COMM$;
COMMENT ON COLUMN ais.kindacc.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;


CREATE TABLE ais.kindaccext (
    isn                              NUMERIC,
    filterstr                        VARCHAR(1000)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.kindaccext.filterstr IS $COMM$Строка фильтрации. Используется на клиенте для ускорения работы.$COMM$;


CREATE TABLE ais.kindaccset (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    kindaccisn                       NUMERIC,
    kindisn                          NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.kindaccset IS $COMM$Состав аналитического счета - набор  значений видов аналитики, определяющие аналитический
счет.
$COMM$;
COMMENT ON COLUMN ais.kindaccset.classisn IS $COMM$Значение вида аналитики внутри аналитического счета (DICTI)$COMM$;
COMMENT ON COLUMN ais.kindaccset.kindaccisn IS $COMM$Аналитический счет (KINDACC)$COMM$;
COMMENT ON COLUMN ais.kindaccset.kindisn IS $COMM$Ссылка на вид аналитики (KIND)$COMM$;
COMMENT ON COLUMN ais.kindaccset.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.kindaccset.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;


CREATE TABLE ais.kindperiod (
    isn                              NUMERIC,
    kindgroupisn                     NUMERIC,
    subaccisn                        NUMERIC,
    status                           VARCHAR(1) DEFAULT 'N',
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.kindperiod IS $COMM$Привязка аналитических групп к субсчетам. Привязка происходит по периодам.$COMM$;
COMMENT ON COLUMN ais.kindperiod.kindgroupisn IS $COMM$Машинный номер объекта.
Устанавливается по умолчанию равным SEQ_DICTI.nextval.
Совпадает с ISN соответствующего объекта, имеющего отдельную таблицу для хранения дополнительных полей.$COMM$;
COMMENT ON COLUMN ais.kindperiod.datebeg IS $COMM$Начало действия связи$COMM$;
COMMENT ON COLUMN ais.kindperiod.dateend IS $COMM$Окончание действия связи.
$COMM$;
COMMENT ON COLUMN ais.kindperiod.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.kindperiod.updatedby IS $COMM$Автор изменений$COMM$;


CREATE TABLE ais.kindvalue (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    subaccisn                        NUMERIC,
    kindisn                          NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    valuetype                        VARCHAR(1) DEFAULT 'Y',
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    childonly                        VARCHAR(1),
    childisn                         NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.kindvalue IS $COMM$Значения видов аналитики для счета
Позволяет определить для субсчета обязательные значения видов аналитики и значения по умолчанию.
Тип значения определяется полем ValueType$COMM$;
COMMENT ON COLUMN ais.kindvalue.classisn IS $COMM$Значение вида аналитики (ссылка на DICTI)$COMM$;
COMMENT ON COLUMN ais.kindvalue.subaccisn IS $COMM$Cсылка на план счетов$COMM$;
COMMENT ON COLUMN ais.kindvalue.kindisn IS $COMM$Вид аналитики$COMM$;
COMMENT ON COLUMN ais.kindvalue.datebeg IS $COMM$Начало действия связи$COMM$;
COMMENT ON COLUMN ais.kindvalue.dateend IS $COMM$Окончание действия связи.
$COMM$;
COMMENT ON COLUMN ais.kindvalue.valuetype IS $COMM$Тип значения
Y - обязательный
N - по умолчанию$COMM$;
COMMENT ON COLUMN ais.kindvalue.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;


CREATE TABLE ais.location (
    isn                              NUMERIC,
    area                             NUMERIC,
    workplaces                       NUMERIC,
    deptisn                          NUMERIC,
    cityisn                          NUMERIC,
    streetisn                        NUMERIC,
    house                            VARCHAR(20),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    issuedwp                         NUMERIC,
    dateend                          TIMESTAMP,
    technical                        VARCHAR(1) DEFAULT 'N'
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.location.issuedwp IS $COMM$Количество рабочих мест в помещении, закрепленных за подразделением в соответствии со штатным расписанием$COMM$;
COMMENT ON COLUMN ais.location.dateend IS $COMM$Срок освобождения помещения$COMM$;
COMMENT ON COLUMN ais.location.technical IS $COMM$Техническое помещение: Y - да, N - нет.$COMM$;


CREATE TABLE ais.mcodomdata (
    isn                              NUMERIC,
    carisn                           NUMERIC,
    vin                              VARCHAR(20),
    regnum                           VARCHAR(20),
    period                           TIMESTAMP,
    startvalue                       NUMERIC,
    endvalue                         NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    aisvin                           VARCHAR(20)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.mcodomdata IS $COMM$@created=05.06.2011 @createdby=Бородин А.Ю. @seq=SEQ_MCODOMDATA @purpose=Дневной пробег ТС$COMM$;
COMMENT ON COLUMN ais.mcodomdata.isn IS $COMM$@purpose=Уникальный идентификатор пробега$COMM$;
COMMENT ON COLUMN ais.mcodomdata.carisn IS $COMM$@fk=AIS.OBJCAR(RESTRICT) @purpose=Ссылка на ТС$COMM$;
COMMENT ON COLUMN ais.mcodomdata.vin IS $COMM$@purpose=Идентификационный номер ТС$COMM$;
COMMENT ON COLUMN ais.mcodomdata.regnum IS $COMM$@purpose=Государственный номер ТС$COMM$;
COMMENT ON COLUMN ais.mcodomdata.period IS $COMM$@purpose=Отчетный период (сутки)$COMM$;
COMMENT ON COLUMN ais.mcodomdata.startvalue IS $COMM$@purpose=Показания на начало периода$COMM$;
COMMENT ON COLUMN ais.mcodomdata.endvalue IS $COMM$@purpose=Показания на конец периода$COMM$;
COMMENT ON COLUMN ais.mcodomdata.created IS $COMM$@purpose=Дата и время создания записи$COMM$;
COMMENT ON COLUMN ais.mcodomdata.createdby IS $COMM$@purpose=Пользователь-создатель (партнер производивший импорт)$COMM$;
COMMENT ON COLUMN ais.mcodomdata.updated IS $COMM$@purpose=Дата и время обновления записи$COMM$;
COMMENT ON COLUMN ais.mcodomdata.updatedby IS $COMM$@purpose=Последний обновивший пользователь (партнер производивший импорт)$COMM$;
COMMENT ON COLUMN ais.mcodomdata.aisvin IS $COMM$@purpose=Идентификационный номер ТС с учетом замены русских букв$COMM$;


CREATE TABLE ais.mcstatedata (
    isn                              NUMERIC,
    carisn                           NUMERIC,
    vin                              VARCHAR(20),
    regnum                           VARCHAR(20),
    actiondate                       TIMESTAMP,
    actiontype                       NUMERIC,
    actionreason                     NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    opsname                          VARCHAR(100),
    aisvin                           VARCHAR(20)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.mcstatedata IS $COMM$@created=05.06.2011 @createdby=Бородин А.Ю. @seq=SEQ_MCSTATEDATA @purpose=Состояние установленной системы телемониторинга$COMM$;
COMMENT ON COLUMN ais.mcstatedata.isn IS $COMM$@purpose=Уникальный идентификатор состояния$COMM$;
COMMENT ON COLUMN ais.mcstatedata.carisn IS $COMM$@fk=AIS.OBJCAR(RESTRICT) @purpose=Ссылка на ТС$COMM$;
COMMENT ON COLUMN ais.mcstatedata.vin IS $COMM$@purpose=Идентификационный номер ТС$COMM$;
COMMENT ON COLUMN ais.mcstatedata.regnum IS $COMM$@purpose=Государственный номер автомобиля$COMM$;
COMMENT ON COLUMN ais.mcstatedata.actiondate IS $COMM$@purpose=Дата перехода в состояние$COMM$;
COMMENT ON COLUMN ais.mcstatedata.actiontype IS $COMM$@fk=AIS.DICTI(RESTRICT) @purpose=Состояние системы$COMM$;
COMMENT ON COLUMN ais.mcstatedata.actionreason IS $COMM$@fk=AIS.DICTI(RESTRICT) @purpose=Причину изменения состояния$COMM$;
COMMENT ON COLUMN ais.mcstatedata.created IS $COMM$@purpose=Дата и время создания записи$COMM$;
COMMENT ON COLUMN ais.mcstatedata.createdby IS $COMM$@purpose=Пользователь-создатель (партнер производивший импорт)$COMM$;
COMMENT ON COLUMN ais.mcstatedata.updated IS $COMM$@purpose=Дата и время обновления записи$COMM$;
COMMENT ON COLUMN ais.mcstatedata.updatedby IS $COMM$@purpose=Последний обновивший пользователь (партнер производивший импорт)$COMM$;
COMMENT ON COLUMN ais.mcstatedata.opsname IS $COMM$@purpose=Название ОПС от Партнера$COMM$;
COMMENT ON COLUMN ais.mcstatedata.aisvin IS $COMM$@purpose=Идентификационный номер ТС с учетом замены русских букв$COMM$;


CREATE TABLE ais.mctrackdata (
    isn                              NUMERIC,
    carisn                           NUMERIC,
    trackdate                        TIMESTAMP,
    regnum                           VARCHAR(20),
    vin                              VARCHAR(20),
    aisvin                           VARCHAR(20),
    area                             NUMERIC,
    subarea                          VARCHAR(2),
    travelled                        NUMERIC,
    travelledoverspeed               NUMERIC,
    movetime                         NUMERIC,
    movetimeoverspeed                NUMERIC,
    speedavg                         NUMERIC,
    speedmax                         NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.mctrackdata IS $COMM$@created=14.08.2012 @createdby=Бородин А.Ю. @seq=SEQ_TRACKDATA @purpose=Данные о маршруте движения ТС$COMM$;
COMMENT ON COLUMN ais.mctrackdata.isn IS $COMM$@purpose=Уникальный идентификатор маршрута$COMM$;
COMMENT ON COLUMN ais.mctrackdata.carisn IS $COMM$@fk=AIS.OBJCAR(RESTRICT) @purpose=Ссылка на ТС$COMM$;
COMMENT ON COLUMN ais.mctrackdata.trackdate IS $COMM$@purpose=Дата и время начала маршрута с точностью до часа$COMM$;
COMMENT ON COLUMN ais.mctrackdata.regnum IS $COMM$@purpose=Государственный номер автомобиля$COMM$;
COMMENT ON COLUMN ais.mctrackdata.vin IS $COMM$@purpose=Идентификационный номер ТС (от партнера)$COMM$;
COMMENT ON COLUMN ais.mctrackdata.aisvin IS $COMM$@purpose=Идентификационный номер ТС (реальный в АИС)$COMM$;
COMMENT ON COLUMN ais.mctrackdata.area IS $COMM$@fk=AIS.REGION(RESTRICT) @purpose=Место движения. Ссылка на регион РФ по справочнику$COMM$;
COMMENT ON COLUMN ais.mctrackdata.subarea IS $COMM$@purpose=Детализация местоположения 01-столица субъекта,02-населенный пункт кроме столицы субъекта,03-вне населенных пунктов$COMM$;
COMMENT ON COLUMN ais.mctrackdata.travelled IS $COMM$@purpose=Пройденное расстояние, Км$COMM$;
COMMENT ON COLUMN ais.mctrackdata.travelledoverspeed IS $COMM$@purpose=Пройденное расстояние с превышением скорости, Км$COMM$;
COMMENT ON COLUMN ais.mctrackdata.movetime IS $COMM$@purpose=Время в движении, час $COMM$;
COMMENT ON COLUMN ais.mctrackdata.movetimeoverspeed IS $COMM$@purpose=Время в движении с превышением скорости, час$COMM$;
COMMENT ON COLUMN ais.mctrackdata.speedavg IS $COMM$@purpose=Средняя скорость движения, км/час$COMM$;
COMMENT ON COLUMN ais.mctrackdata.speedmax IS $COMM$@purpose=Максимальная скорость движения, км/час$COMM$;
COMMENT ON COLUMN ais.mctrackdata.created IS $COMM$@purpose=Дата и время создания записи$COMM$;
COMMENT ON COLUMN ais.mctrackdata.createdby IS $COMM$@purpose=Пользователь-создатель$COMM$;
COMMENT ON COLUMN ais.mctrackdata.updated IS $COMM$@purpose=Дата и время обновления записи$COMM$;
COMMENT ON COLUMN ais.mctrackdata.updatedby IS $COMM$@purpose=Последний обновивший пользователь$COMM$;


CREATE TABLE ais.medappealexchange (
    appealisn                        NUMERIC,
    subjname                         VARCHAR(255),
    caller                           VARCHAR(255),
    subjtel                          VARCHAR(40),
    reason                           VARCHAR(1000),
    appealtypeisn                    NUMERIC,
    diagnosis                        VARCHAR(1000),
    locationisn                      NUMERIC,
    objremark                        VARCHAR(1000),
    callerremark                     VARCHAR(40),
    remark                           VARCHAR(1000),
    hospitalisn                      NUMERIC,
    servicedate                      TIMESTAMP,
    transportisn                     NUMERIC,
    transportremark                  VARCHAR(100),
    distanceisn                      NUMERIC,
    timearrival                      VARCHAR(10),
    timeofday                        VARCHAR(5),
    status                           VARCHAR(2),
    action                           VARCHAR(1),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    garletterisn                     NUMERIC,
    objisn                           NUMERIC,
    isn                              NUMERIC,
    appealparentisn                  NUMERIC,
    mkbisn                           NUMERIC,
    appealid                         VARCHAR(20)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.medappealexchange IS $COMM$Буферная таблица обмена информацией по обращению по медицине между АИС и медицинской базой ТИМ Ассистанс$COMM$;
COMMENT ON COLUMN ais.medappealexchange.caller IS $COMM$Кто вызывает (аттрибут обращения attrMedCaller)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.subjtel IS $COMM$Телефон вызывающего (QuePhone.PhoneNum)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.reason IS $COMM$Повод для обращения (QuePhone.Reason)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.appealtypeisn IS $COMM$Тип обращения (QuePhone.ClassISN)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.diagnosis IS $COMM$Диагноз (QuePhone.Diagnosis)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.locationisn IS $COMM$Где клиент (аттрибут обращения attrMedLocation)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.objremark IS $COMM$Примечания из раздела "Данные по застрахованному", должны содержать фактический адрес  (QuePhone.ObjRemark)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.callerremark IS $COMM$Примечания из раздела "Кто вызывает", могут содержать краткую информацию, выводимую в списке обращений (QuePhone.SubjName)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.remark IS $COMM$Примечания из раздела "Повод для обращения" (QuePhone.Remark)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.hospitalisn IS $COMM$ISN ЛПУ (QuePhone.SubjDeptISN)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.servicedate IS $COMM$Дата оказания услуги (QuePhone.DateBeg)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.transportisn IS $COMM$ISN транспортирующей компании (QuePhone.AgentISN)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.transportremark IS $COMM$Сотрудник исполнителя - принял вызов в огранизации-транспортировщике (аттрибут обращения attrMedAgentRemark)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.distanceisn IS $COMM$Дальность выезда бригады (аттрибут обращения attrMedDistance)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.timearrival IS $COMM$Время доезда в формате ЧЧ:ММ (аттрибут обращения attrMedTimeArrival)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.timeofday IS $COMM$Время суток выезда бригады по вызову (утро, день, вечер, ночь) (аттрибут обращения attrMedTimeofDay)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.status IS $COMM$Статус записи (IN-необработанная запись из АИС, IY-обработанная запись из АИС,TN-необработанная запись из базы ТИМ Ассистанс,TY-обработанная запись из базы ТИМ Ассистанс )$COMM$;
COMMENT ON COLUMN ais.medappealexchange.action IS $COMM$Признак действия (A-аннулирование)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.updated IS $COMM$Дата выгрузки$COMM$;
COMMENT ON COLUMN ais.medappealexchange.updatedby IS $COMM$Автор выгрузки$COMM$;
COMMENT ON COLUMN ais.medappealexchange.garletterisn IS $COMM$Ссылка на гарантийное письмо (Agreement.ISN)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.objisn IS $COMM$Ссылка на застрахованного (MedTimAppeal.ObjISN)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.isn IS $COMM$Идентификатор записи (нужет для ТИМа)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.appealparentisn IS $COMM$Предыдущее обращение (для случаев заведения нескольких обращений по одной заявке когда госп./конс. в нескольких ЛПУ)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.mkbisn IS $COMM$Ссылка на диагноз по МКБ10 (MedMKB10.ISN)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.appealid IS $COMM$Номер обращения (QuePhone.TaskID)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.appealisn IS $COMM$ISN Обращения по медицине (QuePhone.ISN)$COMM$;
COMMENT ON COLUMN ais.medappealexchange.subjname IS $COMM$Наименование ЛПУ или ФИО застрахованного, выбранные в разделе "Кто вызывает" $COMM$;


CREATE TABLE ais.medcondparam (
    isn                              NUMERIC,
    condisn                          NUMERIC,
    premiumsum                       NUMERIC,
    premiumsum100calc                NUMERIC,
    premiumsum100                    NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    groupverno                       NUMERIC,
    operprev                         NUMERIC,
    groupverkoef                     NUMERIC,
    calcmode                         VARCHAR(1),
    groupvernoprev                   NUMERIC,
    stopclaimsum                     NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.medcondparam IS $COMM$Дополнительные параметры, исп. при расчете премии в дог. ДМС$COMM$;
COMMENT ON COLUMN ais.medcondparam.stopclaimsum IS $COMM$Cумма убытков по застрахованному для правила возврата "Pro rata с учетом убытков", Попов В.О. 2012.07.19, задача 33042179203$COMM$;
COMMENT ON COLUMN ais.medcondparam.groupvernoprev IS $COMM$100%-Принятие на пред. операции,Попов В.О. 2010.12.21, задача 19068905903$COMM$;
COMMENT ON COLUMN ais.medcondparam.isn IS $COMM$Совпадает с AgrTariff.Isn$COMM$;
COMMENT ON COLUMN ais.medcondparam.condisn IS $COMM$Ссылка на AgrCond.Isn$COMM$;
COMMENT ON COLUMN ais.medcondparam.premiumsum IS $COMM$Рассчитанная по алгоритму премия$COMM$;
COMMENT ON COLUMN ais.medcondparam.premiumsum100calc IS $COMM$100%принятие - рассч. по алгоритму премия у предыд. условия$COMM$;
COMMENT ON COLUMN ais.medcondparam.premiumsum100 IS $COMM$100%принятие - премия у предыд. условия, которая в нем можно изменить вручную$COMM$;
COMMENT ON COLUMN ais.medcondparam.groupverno IS $COMM$100%принятие - старая версия плана, по которой нужно производить расчет$COMM$;


CREATE TABLE ais.medicappeal (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    subjectisn                       NUMERIC,
    agentisn                         NUMERIC,
    descriptrioncaller               VARCHAR(40),
    glreason                         VARCHAR(255),
    division                         NUMERIC,
    diagnosis                        VARCHAR(1000),
    diagnosiscode                    NUMERIC,
    cityisn                          NUMERIC,
    attendantdiagnosis               VARCHAR(1000),
    attendantdiagnosiscode           NUMERIC,
    value                            NUMERIC,
    doctor                           VARCHAR(255),
    doctorphone                      VARCHAR(40),
    dischargedate                    TIMESTAMP,
    duration                         NUMERIC,
    adddescription                   VARCHAR(1000),
    note                             VARCHAR(1000),
    criteria                         NUMERIC,
    updatedby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.medicappeal IS $COMM$@purpose=Информация по медицинским обращениям @createdby=Бондарев А.А. @noaiuds @SEQ=SEQ_QUEPHONE$COMM$;
COMMENT ON COLUMN ais.medicappeal.cityisn IS $COMM$@purpose=город обращения@fk=CITY(restrict)$COMM$;
COMMENT ON COLUMN ais.medicappeal.attendantdiagnosis IS $COMM$@purpose=Диагноз при поступлении сопутствующий$COMM$;
COMMENT ON COLUMN ais.medicappeal.attendantdiagnosiscode IS $COMM$@purpose=Код диагноза при поступлении$COMM$;
COMMENT ON COLUMN ais.medicappeal.value IS $COMM$@purpose=Стоимость госпитализации$COMM$;
COMMENT ON COLUMN ais.medicappeal.doctor IS $COMM$@purpose=Лечащий врач$COMM$;
COMMENT ON COLUMN ais.medicappeal.doctorphone IS $COMM$@purpose=Телефон (контактная информация) лечащего врача$COMM$;
COMMENT ON COLUMN ais.medicappeal.dischargedate IS $COMM$@purpose=Фактическая дата выписки$COMM$;
COMMENT ON COLUMN ais.medicappeal.duration IS $COMM$@purpose=Продолжительность лечения$COMM$;
COMMENT ON COLUMN ais.medicappeal.adddescription IS $COMM$@purpose=Дополнительная информация. Любые примечания по госпитализации.$COMM$;
COMMENT ON COLUMN ais.medicappeal.note IS $COMM$@purpose=Дополнительная информация (по оценке удовлетворённости)$COMM$;
COMMENT ON COLUMN ais.medicappeal.criteria IS $COMM$@purpose=Оценка качества$COMM$;
COMMENT ON COLUMN ais.medicappeal.updatedby IS $COMM$@purpose=Кто обновил@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.medicappeal.updated IS $COMM$@purpose=Когда обновлено@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.medicappeal.subjectisn IS $COMM$@purpose=ЛПУ@fk=SUBJECT_T(restrict)$COMM$;
COMMENT ON COLUMN ais.medicappeal.agentisn IS $COMM$@purpose= Компания транспортировщик@fk=SUBJECT_T(restrict)$COMM$;
COMMENT ON COLUMN ais.medicappeal.descriptrioncaller IS $COMM$@purpose=Примечание для раздела кто вызывает$COMM$;
COMMENT ON COLUMN ais.medicappeal.glreason IS $COMM$@purpose=Повод для гарантийного письма$COMM$;
COMMENT ON COLUMN ais.medicappeal.division IS $COMM$@purpose=Отделение ЛПУ$COMM$;
COMMENT ON COLUMN ais.medicappeal.diagnosis IS $COMM$@purpose=Диагноз$COMM$;
COMMENT ON COLUMN ais.medicappeal.diagnosiscode IS $COMM$@purpose= код диагноза@fk=MEDMKB10(restrict)$COMM$;
COMMENT ON COLUMN ais.medicappeal.isn IS $COMM$@purpose=ISN связь один к одному с QUEPHONE@fk=QUEPHONE(restrict)$COMM$;
COMMENT ON COLUMN ais.medicappeal.classisn IS $COMM$@purpose=Тип обращения$COMM$;


CREATE TABLE ais.medlistlog (
    isn                              NUMERIC,
    objisn                           NUMERIC,
    refagrisn                        NUMERIC,
    created                          TIMESTAMP,
    processed                        TIMESTAMP,
    processedby                      NUMERIC,
    subjisn                          NUMERIC,
    docclassisn                      NUMERIC,
    reportname                       VARCHAR(40),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    classisn                         NUMERIC,
    status                           VARCHAR(1),
    condisn                          NUMERIC,
    remark                           VARCHAR(1000),
    groupisn                         NUMERIC,
    rectype                          VARCHAR(1),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    parentisn                        NUMERIC,
    docimageisn                      NUMERIC,
    emailmessage                     VARCHAR(4000),
    riskisn                          NUMERIC,
    limitisn                         NUMERIC,
    groupverno                       NUMERIC,
    oldgroupisn                      NUMERIC,
    oldgroupverno                    NUMERIC,
    oldriskisn                       NUMERIC,
    oldlimitisn                      NUMERIC,
    addisn                           NUMERIC,
    parentlogisn                     NUMERIC,
    clientisn                        NUMERIC,
    setno                            NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.medlistlog IS $COMM$Log всех полученных списков по застрахованным. УМС. Савичев.$COMM$;
COMMENT ON COLUMN ais.medlistlog.setno IS $COMM$Номер набора застрахованных для разбивки по файлам по порядку, Попов В.О. 2013.08.30, задача 51004647303$COMM$;
COMMENT ON COLUMN ais.medlistlog.clientisn IS $COMM$ --CLIENTISN из Subject 14/06/2013 KOTOV/AS/EPAM 48314642903$COMM$;
COMMENT ON COLUMN ais.medlistlog.emailmessage IS $COMM$Доп. информация, отправленая со списками, Шмакова Т.Г. 29.06.2009$COMM$;
COMMENT ON COLUMN ais.medlistlog.condisn IS $COMM$Попов В.О. 30.09.2006 ссылка на условие страхования для ЛПУ по прикреплению$COMM$;
COMMENT ON COLUMN ais.medlistlog.remark IS $COMM$Комментарий по истории изменения, Попов В.О. 2008.12.10, задача 4158823903$COMM$;
COMMENT ON COLUMN ais.medlistlog.groupisn IS $COMM$План страхования договора ДМС, Попов В.О. 2008.12.10, задача 4158823903$COMM$;
COMMENT ON COLUMN ais.medlistlog.rectype IS $COMM$Тип записи, I-добавление, U-обновление, Попов В.О. 2008.01.17, задача 7981689203$COMM$;
COMMENT ON COLUMN ais.medlistlog.parentisn IS $COMM$Ссылка на родительскую строку, Попов В.О. 2009.01.22, задача 7981689203$COMM$;
COMMENT ON COLUMN ais.medlistlog.docimageisn IS $COMM$Ссылка на файл в комплекте документов Шмакова Т.Г. 25.05.2009$COMM$;
COMMENT ON COLUMN ais.medlistlog.classisn IS $COMM$тип строки (узел справочника "тип строк MedListLog")$COMM$;
COMMENT ON COLUMN ais.medlistlog.status IS $COMM$A-архивный$COMM$;


CREATE TABLE ais.medsequiplist (
    isn                              NUMERIC,
    passportisn                      NUMERIC,
    equipisn                         NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.medsequiplist IS $COMM$ТПВ - Оснащенность, Попов В.О. 2011.07.21, задача 20038342603$COMM$;
COMMENT ON COLUMN ais.medsequiplist.passportisn IS $COMM$Формуляр ТПВ$COMM$;
COMMENT ON COLUMN ais.medsequiplist.equipisn IS $COMM$Оснащенность$COMM$;


CREATE TABLE ais.medspassport (
    isn                              NUMERIC,
    terrgroupisn                     NUMERIC,
    medcareisn                       NUMERIC,
    ageisn                           NUMERIC,
    daterate                         TIMESTAMP,
    datesync                         TIMESTAMP,
    datesyncby                       NUMERIC,
    active                           VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.medspassport IS $COMM$ТПВ-Формуляр, Попов В.О. 2011.07.21, задача 20038342603$COMM$;
COMMENT ON COLUMN ais.medspassport.terrgroupisn IS $COMM$Территориальная группа$COMM$;
COMMENT ON COLUMN ais.medspassport.medcareisn IS $COMM$Вид мед. помощи$COMM$;
COMMENT ON COLUMN ais.medspassport.ageisn IS $COMM$Возраст обслуживания$COMM$;
COMMENT ON COLUMN ais.medspassport.daterate IS $COMM$Дата рейтинга$COMM$;
COMMENT ON COLUMN ais.medspassport.datesync IS $COMM$Дата синхронизации$COMM$;
COMMENT ON COLUMN ais.medspassport.datesyncby IS $COMM$Автор синхронизации$COMM$;
COMMENT ON COLUMN ais.medspassport.active IS $COMM$Признак активности: Y - действующий, null-архивный$COMM$;


CREATE TABLE ais.medsprior (
    isn                              NUMERIC,
    passportisn                      NUMERIC,
    subjisn                          NUMERIC,
    addrisn                          NUMERIC,
    equipisn                         NUMERIC,
    classcode                        VARCHAR(10),
    priority                         NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    equipremark                      VARCHAR(4000)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.medsprior IS $COMM$ТПВ - Приоритеты, Попов В.О. 2011.07.21, задача 20038342603$COMM$;
COMMENT ON COLUMN ais.medsprior.passportisn IS $COMM$Формуляр ТПВ$COMM$;
COMMENT ON COLUMN ais.medsprior.subjisn IS $COMM$ЛПУ$COMM$;
COMMENT ON COLUMN ais.medsprior.addrisn IS $COMM$Адрес$COMM$;
COMMENT ON COLUMN ais.medsprior.equipisn IS $COMM$Оснащенность$COMM$;
COMMENT ON COLUMN ais.medsprior.classcode IS $COMM$Режим отображения:Null-нормальный, белый цвет;K – заданы “Койки”, оранжевый цвет$COMM$;
COMMENT ON COLUMN ais.medsprior.priority IS $COMM$Приоритет$COMM$;
COMMENT ON COLUMN ais.medsprior.equipremark IS $COMM$Комментарий оснащенности$COMM$;


CREATE TABLE ais.medssubjagrlist (
    isn                              NUMERIC,
    passportisn                      NUMERIC,
    subjlistisn                      NUMERIC,
    agrisn                           NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    tariffisn                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.medssubjagrlist IS $COMM$ТПВ - Список хоз. договоров, Попов В.О. 2011.07.21, задача 20038342603$COMM$;
COMMENT ON COLUMN ais.medssubjagrlist.tariffisn IS $COMM$Тарифная группа$COMM$;
COMMENT ON COLUMN ais.medssubjagrlist.passportisn IS $COMM$Формуляр ТПВ$COMM$;
COMMENT ON COLUMN ais.medssubjagrlist.subjlistisn IS $COMM$Ссылка на список ЛПУ$COMM$;
COMMENT ON COLUMN ais.medssubjagrlist.agrisn IS $COMM$Хоз. договор$COMM$;


CREATE TABLE ais.medssubjlist (
    isn                              NUMERIC,
    passportisn                      NUMERIC,
    subjisn                          NUMERIC,
    addrisn                          NUMERIC,
    priceisn                         NUMERIC,
    total                            NUMERIC,
    average                          NUMERIC,
    rating                           NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    subjremark                       VARCHAR(4000),
    nottooffer                       VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.medssubjlist IS $COMM$ТПВ - Список ЛПУ, Попов В.О. 2011.07.21, задача 20038342603$COMM$;
COMMENT ON COLUMN ais.medssubjlist.nottooffer IS $COMM$Не для предложения из КК$COMM$;
COMMENT ON COLUMN ais.medssubjlist.passportisn IS $COMM$Формуляр ТПВ$COMM$;
COMMENT ON COLUMN ais.medssubjlist.subjisn IS $COMM$ЛПУ$COMM$;
COMMENT ON COLUMN ais.medssubjlist.addrisn IS $COMM$Хоз. договор$COMM$;
COMMENT ON COLUMN ais.medssubjlist.priceisn IS $COMM$Ценовая группа$COMM$;
COMMENT ON COLUMN ais.medssubjlist.total IS $COMM$Общая приоритетность$COMM$;
COMMENT ON COLUMN ais.medssubjlist.average IS $COMM$Средний балл$COMM$;
COMMENT ON COLUMN ais.medssubjlist.rating IS $COMM$Балл по рейтингу$COMM$;


CREATE TABLE ais.nsso_msgxml (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    classisn                         NUMERIC,
    msg_id                           VARCHAR(255),
    agrisn                           NUMERIC,
    status                           VARCHAR(2),
    resultstatus                     VARCHAR(3),
    xmlblock                         TEXT,
    msg_date                         TIMESTAMP,
    xisn                             NUMERIC,
    hisn                             NUMERIC,
    refisn1                          NUMERIC,
    refisn2                          NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    remark                           VARCHAR(500),
    msg_type                         VARCHAR(3)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.nsso_msgxml.isn IS $COMM$pk$COMM$;
COMMENT ON COLUMN ais.nsso_msgxml.classisn IS $COMM$select * from dicti where parentisn = 3537614603$COMM$;
COMMENT ON COLUMN ais.nsso_msgxml.status IS $COMM$"-1":создан;"ГО"-отправлен; "ОШ" - ошибка; "ОО"- ошибка обработана$COMM$;
COMMENT ON COLUMN ais.nsso_msgxml.resultstatus IS $COMM$ответ НССО$COMM$;
COMMENT ON COLUMN ais.nsso_msgxml.xmlblock IS $COMM$текст xml$COMM$;
COMMENT ON COLUMN ais.nsso_msgxml.msg_date IS $COMM$дата сообщения$COMM$;
COMMENT ON COLUMN ais.nsso_msgxml.xisn IS $COMM$ссылка на Bso_agridx.isn$COMM$;
COMMENT ON COLUMN ais.nsso_msgxml.hisn IS $COMM$ссылка на Bso_Agridhead.isn$COMM$;
COMMENT ON COLUMN ais.nsso_msgxml.refisn1 IS $COMM$зарезервировано$COMM$;
COMMENT ON COLUMN ais.nsso_msgxml.refisn2 IS $COMM$зарезервировано$COMM$;
COMMENT ON COLUMN ais.nsso_msgxml.remark IS $COMM$примечание$COMM$;
COMMENT ON COLUMN ais.nsso_msgxml.msg_type IS $COMM$IN - входящие; OUT- исходящие$COMM$;


CREATE TABLE ais.obj_attrib (
    isn                              NUMERIC,
    objisn                           NUMERIC,
    discr                            VARCHAR(1),
    classisn                         NUMERIC,
    val                              VARCHAR(4000),
    valn                             NUMERIC,
    vald                             TIMESTAMP,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    objisn2                          NUMERIC,
    parentisn                        NUMERIC,
    remark                           VARCHAR(4000),
--WARNING: In table ais.obj_attrib column exclude matches Greenplum keyword. Corrected to exclude_.
    exclude_                         VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.obj_attrib IS $COMM$Таблица произвольных атрибутов объекта ATTRIB.
Описание атрибута содержит ссылку на объект, тип атрибута, значение атрибута, период действия атрибута.
Изменение varchar-значения тарибута наследуются на типизованные в соответсвии с типом.
Для аттрибутов, содержащих значения справочника, корень дерева справочника находится в поле FILTERISN атрибута.
$COMM$;
COMMENT ON COLUMN ais.obj_attrib.parentisn IS $COMM$Ссылка на головной атрибут (Угринович 01.11.06)$COMM$;
COMMENT ON COLUMN ais.obj_attrib.remark IS $COMM$Комментарий (Угринович 01.11.06)$COMM$;
COMMENT ON COLUMN ais.obj_attrib.exclude_ IS $COMM$Флаг "Кроме"  (Угринович 01.11.06)$COMM$;
COMMENT ON COLUMN ais.obj_attrib.objisn2 IS $COMM$Вторичный объект, от которого зависит значение атрибута (Smirnov 24/04/06)$COMM$;
COMMENT ON COLUMN ais.obj_attrib.datebeg IS $COMM$Период действия атрибута (начало)$COMM$;
COMMENT ON COLUMN ais.obj_attrib.dateend IS $COMM$Период действия атрибута (конец)$COMM$;
COMMENT ON COLUMN ais.obj_attrib.updated IS $COMM$Время изменения$COMM$;
COMMENT ON COLUMN ais.obj_attrib.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.obj_attrib.isn IS $COMM$SEQ_obj_attrib$COMM$;
COMMENT ON COLUMN ais.obj_attrib.objisn IS $COMM$FK(<Таблица>) Ссылка на какой-либо объект$COMM$;
COMMENT ON COLUMN ais.obj_attrib.discr IS $COMM$Дискриминатор объекта: Q-Queue A-Agreement D-Dicti L-AgrClaim C-Subject M-QuePhone$COMM$;
COMMENT ON COLUMN ais.obj_attrib.classisn IS $COMM$FK(DICTI) Тип атрибута: "Системные кодификаторы" - > "Атрибуты"$COMM$;
COMMENT ON COLUMN ais.obj_attrib.val IS $COMM$Значене атрибута (нетипизировнное)$COMM$;
COMMENT ON COLUMN ais.obj_attrib.valn IS $COMM$Значение атрибута (число)$COMM$;
COMMENT ON COLUMN ais.obj_attrib.vald IS $COMM$Значение атрибута (дата)$COMM$;


CREATE TABLE ais.objagr (
    isn                              NUMERIC,
    superclassisn                    NUMERIC,
    classisn                         NUMERIC,
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(255),
    remark                           VARCHAR(1000),
    countryisn                       NUMERIC,
    constructed                      TIMESTAMP,
    active                           VARCHAR(1),
    parentisn                        NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC(38),
    updatedby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    synisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.objagr IS $COMM$Заголовок физического объекта. Специфические параметра объектов хранятся в соответсвующих
детальных таблицах. Дискриминатором является SuperClassISN, автоматически определяемому по ClassISN. Старые наименования объекта
при необходимости сохраняются в OBJNAME приложением.$COMM$;
COMMENT ON COLUMN ais.objagr.isn IS $COMM$Машинный номер, SEQ_OBJAGR.nextval$COMM$;
COMMENT ON COLUMN ais.objagr.superclassisn IS $COMM$FK(DICTI). Ссылка на суперкласс физического объекта, является дискриминатором
детальной таблицы характеристик объекта$COMM$;
COMMENT ON COLUMN ais.objagr.classisn IS $COMM$FK(DICTI). Ссылка на класс физического объекта$COMM$;
COMMENT ON COLUMN ais.objagr.shortname IS $COMM$Краткое поисковое название$COMM$;
COMMENT ON COLUMN ais.objagr.fullname IS $COMM$Полное название, лат.эквивалент$COMM$;
COMMENT ON COLUMN ais.objagr.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.objagr.countryisn IS $COMM$FK(COUNTRY). Указатель страны принадлежности$COMM$;
COMMENT ON COLUMN ais.objagr.constructed IS $COMM$Дата постройки$COMM$;
COMMENT ON COLUMN ais.objagr.active IS $COMM$Признак активности объекта:
N - новый, не проверен администратором,
Y - проверен администратором,
NULL-считается архивным и сохраняется для декодировки ссылок из ранее созданных объектов, но недоступен для ссылок из вновь
создаваемых объектов и не показывается в LookUp.
При создании нового объекта: если задан NULL - устанавливается N, если задан X - устанавливается в NULL.
Удалять можно только архивный объект.$COMM$;
COMMENT ON COLUMN ais.objagr.parentisn IS $COMM$FK(OBJAGR). Историческая ссылка на предыдущее состояние объекта. При изменении
ParentISN объект автоматически исключается из старого списка (если таковой был) и вставляется в новый (если таковой есть).$COMM$;
COMMENT ON COLUMN ais.objagr.created IS $COMM$Дата создания. Регистрируется автоматически. Может быть задана клиентом при
регистрации задним числом.$COMM$;
COMMENT ON COLUMN ais.objagr.createdby IS $COMM$Автор создания, устанавливается автоматически равным ISN активного пользователя
(init.UserISN).$COMM$;
COMMENT ON COLUMN ais.objagr.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.objagr.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.objagr.synisn IS $COMM$FK(OBJAGR) ссылка на синоним$COMM$;


CREATE TABLE ais.objaircraft (
    isn                              NUMERIC,
    ownerisn                         NUMERIC,
    manufisn                         NUMERIC,
    manufid                          VARCHAR(20),
    maxweight                        NUMERIC,
    maxseats                         NUMERIC(38),
    prefix                           VARCHAR(2),
    regno                            VARCHAR(20),
    datereg                          TIMESTAMP,
    daterepair                       TIMESTAMP,
    international                    VARCHAR(1),
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    parentisn                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.objaircraft IS $COMM$Физические характеристики воздушного транспортного средства$COMM$;
COMMENT ON COLUMN ais.objaircraft.isn IS $COMM$FK(OBJAGR). Указатель на заголовок физического объекта$COMM$;
COMMENT ON COLUMN ais.objaircraft.ownerisn IS $COMM$FK(SUBJECT). Указатель авиакомпании-владельца$COMM$;
COMMENT ON COLUMN ais.objaircraft.manufisn IS $COMM$FK(SUBJECT). Указатель завода-изготовителя$COMM$;
COMMENT ON COLUMN ais.objaircraft.manufid IS $COMM$Заводской номер$COMM$;
COMMENT ON COLUMN ais.objaircraft.maxweight IS $COMM$Максимальный взлетный вес (т.)$COMM$;
COMMENT ON COLUMN ais.objaircraft.maxseats IS $COMM$Число посадочных мест$COMM$;
COMMENT ON COLUMN ais.objaircraft.prefix IS $COMM$Префикс страны у бортового номера$COMM$;
COMMENT ON COLUMN ais.objaircraft.regno IS $COMM$Номер регистрационного свидетельства$COMM$;
COMMENT ON COLUMN ais.objaircraft.datereg IS $COMM$Дата регистрационного свидетельства$COMM$;
COMMENT ON COLUMN ais.objaircraft.daterepair IS $COMM$Дата капремонта$COMM$;
COMMENT ON COLUMN ais.objaircraft.international IS $COMM$Наличие международного сертификата: Y$COMM$;
COMMENT ON COLUMN ais.objaircraft.remark IS $COMM$Отметка о списании$COMM$;
COMMENT ON COLUMN ais.objaircraft.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.objaircraft.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.objcar (
    isn                              NUMERIC,
    ownershipisn                     NUMERIC,
    customisn                        NUMERIC,
    standisn                         NUMERIC,
    tarifisn                         NUMERIC,
    colorisn                         NUMERIC,
    currisn                          NUMERIC,
    origprice                        NUMERIC(20,2),
    addprice                         NUMERIC(20,2),
    actualprice                      NUMERIC(20,2),
    risklevel                        NUMERIC(38),
    chassisid                        VARCHAR(20),
    bodyid                           VARCHAR(20),
    vin                              VARCHAR(20),
    enginetype                       VARCHAR(1) DEFAULT 'P',
    enginepowerkw                    NUMERIC(38),
    enginepowerhp                    NUMERIC,
    engineid                         VARCHAR(30),
    enginevolume                     NUMERIC(38),
    doors                            NUMERIC,
    seats                            NUMERIC,
    maxload                          NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    kilometres                       NUMERIC,
    keynum                           NUMERIC,
    owner                            VARCHAR(255),
    modelname                        VARCHAR(40),
    tarifno                          NUMERIC(38),
    makeisn                          NUMERIC,
    modification                     VARCHAR(40),
    tariffgroupisn                   NUMERIC,
    vinr                             VARCHAR(20),
    steeringside                     VARCHAR(1),
    protectionisn                    NUMERIC,
    daterun                          TIMESTAMP,
    grossweigh                       NUMERIC,
    categoryisn                      NUMERIC,
    bodymodelisn                     NUMERIC,
    bodytypeisn                      NUMERIC,
    transmission                     VARCHAR(1),
    netweight                        NUMERIC,
    warrantydate                     TIMESTAMP,
    warrantyperiod                   NUMERIC,
    warrantykilometrage              NUMERIC,
    seatscount                       NUMERIC,
    passagevolume                    NUMERIC,
    modelyear                        VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.objcar IS $COMM$Классификатор моделей автотранспорта. Иерархия тип-марка-модель-модификация (тип кузова)
является частью классификатора объектов страхования в словаре DICTI$COMM$;
COMMENT ON COLUMN ais.objcar.seatscount IS $COMM$Количество посадочных мест$COMM$;
COMMENT ON COLUMN ais.objcar.netweight IS $COMM$Масса ТС без нагрузки (т). -- Yunin V.A. 10/05/07$COMM$;
COMMENT ON COLUMN ais.objcar.passagevolume IS $COMM$Пассажировместимость$COMM$;
COMMENT ON COLUMN ais.objcar.modelyear IS $COMM$признак "Модельный год ТС превышает год выпуска ТС" -- Sobolev A.V. 03/09/13$COMM$;
COMMENT ON COLUMN ais.objcar.bodymodelisn IS $COMM$FK(Dicti) Наименование/модель кузова ТС - Марин А.В. 04.04.2007$COMM$;
COMMENT ON COLUMN ais.objcar.daterun IS $COMM$Дата начала эксплуатации ТС  -- Yunin V.A. 07/12/05$COMM$;
COMMENT ON COLUMN ais.objcar.warrantydate IS $COMM$Дата начала заводской гарантии -- Yunin V.A. 10/12/08$COMM$;
COMMENT ON COLUMN ais.objcar.grossweigh IS $COMM$Максимально разрешенная масса (т.)  -- Yunin V.A. 15/12/05$COMM$;
COMMENT ON COLUMN ais.objcar.categoryisn IS $COMM$FK(DICTI) Категория ТС  -- Yunin V.A. 15/12/05$COMM$;
COMMENT ON COLUMN ais.objcar.bodytypeisn IS $COMM$FK(Dicti) Тип кузова ТС - Марин А.В. 04.04.2007$COMM$;
COMMENT ON COLUMN ais.objcar.transmission IS $COMM$Тип КПП: "А" - автоматическая, "Р" - ручная - Марин А.В. 04.04.2007$COMM$;
COMMENT ON COLUMN ais.objcar.protectionisn IS $COMM$FK(Dicti) Класс защиты. Yunin V.A. 11/05/05$COMM$;
COMMENT ON COLUMN ais.objcar.isn IS $COMM$FK(OBJAGR). Указатель заголовка физического объекта$COMM$;
COMMENT ON COLUMN ais.objcar.ownershipisn IS $COMM$FK(DICTI). Указатель права владения: собственность, доверенность, аренда,
лизинг$COMM$;
COMMENT ON COLUMN ais.objcar.standisn IS $COMM$FK(DICTI). Указатель способа хранения: охраняемая стоянка, гараж$COMM$;
COMMENT ON COLUMN ais.objcar.tarifisn IS $COMM$FK(CARPRICE). Указатель тарифной записи$COMM$;
COMMENT ON COLUMN ais.objcar.colorisn IS $COMM$FK(DICTI). Указатель цвета автотранспорта$COMM$;
COMMENT ON COLUMN ais.objcar.currisn IS $COMM$FK(CURRENCY). Указатель валюты стоимости автотранспорта$COMM$;
COMMENT ON COLUMN ais.objcar.origprice IS $COMM$Первоначальная стоимость (цена)$COMM$;
COMMENT ON COLUMN ais.objcar.addprice IS $COMM$НЕ ИСПОЛЬЗУЕТСЯ. Дополнительная стоимость (благодаря модификациям, ремонту)$COMM$;
COMMENT ON COLUMN ais.objcar.actualprice IS $COMM$НЕ ИСПОЛЬЗУЕТСЯ (перенесено в  AgrObject.Cost). Действительная стоимость с учетом
износа$COMM$;
COMMENT ON COLUMN ais.objcar.risklevel IS $COMM$Уровень риска$COMM$;
COMMENT ON COLUMN ais.objcar.chassisid IS $COMM$Номер шасси$COMM$;
COMMENT ON COLUMN ais.objcar.bodyid IS $COMM$Номер кузова$COMM$;
COMMENT ON COLUMN ais.objcar.vin IS $COMM$Международный идентификационный код$COMM$;
COMMENT ON COLUMN ais.objcar.enginetype IS $COMM$Тип двигателя: D-дизель, P-бензиновый$COMM$;
COMMENT ON COLUMN ais.objcar.enginepowerkw IS $COMM$Мощность двигателя, кВт$COMM$;
COMMENT ON COLUMN ais.objcar.enginepowerhp IS $COMM$Мощность двигателя, л.с.$COMM$;
COMMENT ON COLUMN ais.objcar.engineid IS $COMM$Номер двигателя$COMM$;
COMMENT ON COLUMN ais.objcar.enginevolume IS $COMM$Объем двигателя, куб.см.$COMM$;
COMMENT ON COLUMN ais.objcar.doors IS $COMM$Количество дверей$COMM$;
COMMENT ON COLUMN ais.objcar.seats IS $COMM$Число посадочных мест$COMM$;
COMMENT ON COLUMN ais.objcar.maxload IS $COMM$Грузоподъемность, т.$COMM$;
COMMENT ON COLUMN ais.objcar.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.objcar.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.objcar.kilometres IS $COMM$НЕ ИСПОЛЬЗУЕТСЯ (перенесено в  AgrObjMotor). Пробег в км$COMM$;
COMMENT ON COLUMN ais.objcar.keynum IS $COMM$Количество комплектов ключей$COMM$;
COMMENT ON COLUMN ais.objcar.owner IS $COMM$Собственник транспортного средства (может отличаться от страхователя)$COMM$;
COMMENT ON COLUMN ais.objcar.modelname IS $COMM$Наименование модели, если формальная ссылка (TariffISN) не задана$COMM$;
COMMENT ON COLUMN ais.objcar.tarifno IS $COMM$Тарифный разряд$COMM$;
COMMENT ON COLUMN ais.objcar.makeisn IS $COMM$FK(DICTI). Сылка на марку автомобиля$COMM$;
COMMENT ON COLUMN ais.objcar.tariffgroupisn IS $COMM$FK(DICTI). Тарифная группа, по которой определяется базовый тариф$COMM$;
COMMENT ON COLUMN ais.objcar.vinr IS $COMM$VIN задом-наперед для поиска$COMM$;
COMMENT ON COLUMN ais.objcar.steeringside IS $COMM$Расположение руля: L-слева, R-справа$COMM$;
COMMENT ON COLUMN ais.objcar.warrantyperiod IS $COMM$Гарантийный период (в месяцах) (Yunin V.A. 13/12/10)$COMM$;
COMMENT ON COLUMN ais.objcar.warrantykilometrage IS $COMM$Гарантийный пробег (в км.) (Yunin V.A. 13/12/10)$COMM$;


CREATE TABLE ais.objcargo (
    isn                              NUMERIC,
    packclassisn                     NUMERIC,
    packnum                          NUMERIC(38),
    contclassisn                     NUMERIC,
    contnum                          NUMERIC(38),
    contid                           VARCHAR(255),
    nettoweight                      NUMERIC,
    bruttoweight                     NUMERIC,
    units                            NUMERIC,
    unitclassisn                     NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.objcargo IS $COMM$Детальное описание груза$COMM$;
COMMENT ON COLUMN ais.objcargo.isn IS $COMM$Машинный номер, уникальный в рамках договора: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.objcargo.packclassisn IS $COMM$FK(DICTI). Тип упаковки$COMM$;
COMMENT ON COLUMN ais.objcargo.packnum IS $COMM$Количество упаковок$COMM$;
COMMENT ON COLUMN ais.objcargo.contclassisn IS $COMM$FK(DICTI). Тип перевозочного средства, типоразмер контейнера$COMM$;
COMMENT ON COLUMN ais.objcargo.contnum IS $COMM$Количество контейнеров$COMM$;
COMMENT ON COLUMN ais.objcargo.contid IS $COMM$Номера контейнеров$COMM$;
COMMENT ON COLUMN ais.objcargo.nettoweight IS $COMM$Вес нетто в кг$COMM$;
COMMENT ON COLUMN ais.objcargo.bruttoweight IS $COMM$Вес брутто в кг$COMM$;
COMMENT ON COLUMN ais.objcargo.units IS $COMM$Количество в заданных единицах измерения$COMM$;
COMMENT ON COLUMN ais.objcargo.unitclassisn IS $COMM$FK(DICTI). Единица измерения для [UNITS]$COMM$;
COMMENT ON COLUMN ais.objcargo.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.objcargo.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.objplaceaddr (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    objisn                           NUMERIC,
    objclassisn                      NUMERIC,
    addrbaseisn                      NUMERIC,
    place                            VARCHAR(1000),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.objplaceaddr IS $COMM$Единая таблица адресов.$COMM$;
COMMENT ON COLUMN ais.objplaceaddr.isn IS $COMM$PK$COMM$;
COMMENT ON COLUMN ais.objplaceaddr.classisn IS $COMM$(FK DICTI) тип адреса$COMM$;
COMMENT ON COLUMN ais.objplaceaddr.objisn IS $COMM$FK(<Таблица>) Ссылка на объект$COMM$;
COMMENT ON COLUMN ais.objplaceaddr.objclassisn IS $COMM$(FK DICTI) тип объекта$COMM$;
COMMENT ON COLUMN ais.objplaceaddr.addrbaseisn IS $COMM$(FK ADDRBASE) ссылка на справочник адресов$COMM$;
COMMENT ON COLUMN ais.objplaceaddr.place IS $COMM$Уточнение места$COMM$;
COMMENT ON COLUMN ais.objplaceaddr.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.objplaceaddr.updatedby IS $COMM$FK(Subject) Автор изменения$COMM$;
COMMENT ON COLUMN ais.objplaceaddr.created IS $COMM$Дата создания$COMM$;
COMMENT ON COLUMN ais.objplaceaddr.createdby IS $COMM$FK(Subject) Создатель$COMM$;


CREATE TABLE ais.objship (
    isn                              NUMERIC,
    managerisn                       NUMERIC,
    ownerisn                         NUMERIC,
    lr_no                            VARCHAR(7),
    class_1                          VARCHAR(2),
    class_2                          VARCHAR(2),
    length                           NUMERIC(6,3),
    length_indicator                 VARCHAR(3),
    breadth                          NUMERIC(6,3),
    breadth_indicator                VARCHAR(3),
    gross_tonnage                    BIGINT,
    net_tonnage                      BIGINT,
    deadweight                       BIGINT,
    ice_class                        VARCHAR(2),
    ice_strengthend                  VARCHAR(1),
    tonnage_system                   VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    classifierisn                    NUMERIC,
    regno                            VARCHAR(20),
    engine                           VARCHAR(255),
    typeisn                          NUMERIC,
    passenger_count                  INT,
    container_count                  BIGINT
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.objship IS $COMM$Физические характеристики судна$COMM$;
COMMENT ON COLUMN ais.objship.classifierisn IS $COMM$Классификационное общество$COMM$;
COMMENT ON COLUMN ais.objship.typeisn IS $COMM$тип судна$COMM$;
COMMENT ON COLUMN ais.objship.passenger_count IS $COMM$количество пассажиров$COMM$;
COMMENT ON COLUMN ais.objship.container_count IS $COMM$контейнеровместимость$COMM$;
COMMENT ON COLUMN ais.objship.isn IS $COMM$Машинный номер записи: SEQ_OBJSHIP.nextval$COMM$;
COMMENT ON COLUMN ais.objship.managerisn IS $COMM$FK(OBJOWNER). Ссылка на менеджера судна$COMM$;
COMMENT ON COLUMN ais.objship.ownerisn IS $COMM$FK(OBJOWNER). Ссылка на владельца судна$COMM$;
COMMENT ON COLUMN ais.objship.lr_no IS $COMM$Регистрационный номер во внешнем справочнике (LLOID)$COMM$;
COMMENT ON COLUMN ais.objship.gross_tonnage IS $COMM$Водоизмещение$COMM$;
COMMENT ON COLUMN ais.objship.net_tonnage IS $COMM$Налогооблагаемая грузоподъемность$COMM$;
COMMENT ON COLUMN ais.objship.deadweight IS $COMM$Дидвейт$COMM$;
COMMENT ON COLUMN ais.objship.ice_class IS $COMM$Ледовый класс$COMM$;
COMMENT ON COLUMN ais.objship.ice_strengthend IS $COMM$Класс ледовой защиты$COMM$;
COMMENT ON COLUMN ais.objship.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.objship.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.params_case (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    paramname                        VARCHAR(100),
    case_val                         VARCHAR(100),
    from_sql                         VARCHAR(4000),
    where_sql                        VARCHAR(4000),
    init_sql                         VARCHAR(4000),
    declare_sql                      VARCHAR(4000),
    ord                              NUMERIC,
    noeffect                         NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE ais.params_hint (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    listparams                       VARCHAR(255),
    hint                             VARCHAR(255),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE ais.paycard (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    active                           VARCHAR(1),
    id                               VARCHAR(20),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    subjisn                          NUMERIC,
    subaccisn                        NUMERIC,
    cardno                           VARCHAR(40),
    securityno                       VARCHAR(40),
    paysystemisn                     NUMERIC,
    cardproductisn                   NUMERIC,
    overdraft                        NUMERIC,
    subjcheckdate                    TIMESTAMP,
    subjapproved                     VARCHAR(1) DEFAULT 'N',
    cardapproved                     VARCHAR(1) DEFAULT 'N',
    paymentsum                       NUMERIC,
    currisn                          NUMERIC,
    agrisn                           NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    bankisn                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.paycard IS $COMM$Сущность "платежная карта". Опеределяет банковскую карту выданную клиенту ИГС. Может являться как страховой картой выданной клиету при заключении договора страхования так и обычной банковской картой сотрудника или клиента ИГС. Страховая карта имеет связь с договором страхования при заключении которого она выдана$COMM$;
COMMENT ON COLUMN ais.paycard.isn IS $COMM$Превичный ключ. SEQ_PAYCARD$COMM$;
COMMENT ON COLUMN ais.paycard.classisn IS $COMM$FK(DICTI) Тип карты$COMM$;
COMMENT ON COLUMN ais.paycard.active IS $COMM$Статус карты. Y - активна, NULL - не используется$COMM$;
COMMENT ON COLUMN ais.paycard.id IS $COMM$Идентификатор карты в информационной системе банка$COMM$;
COMMENT ON COLUMN ais.paycard.datebeg IS $COMM$Дата начала действия карты$COMM$;
COMMENT ON COLUMN ais.paycard.dateend IS $COMM$Дата окончания действия карты$COMM$;
COMMENT ON COLUMN ais.paycard.subjisn IS $COMM$FK(SUBACC) Ссылка на клиента$COMM$;
COMMENT ON COLUMN ais.paycard.subaccisn IS $COMM$FK(SUBACC) Ссылка на счет$COMM$;
COMMENT ON COLUMN ais.paycard.cardno IS $COMM$Номер карты$COMM$;
COMMENT ON COLUMN ais.paycard.securityno IS $COMM$Код безопасности (3 цифры)$COMM$;
COMMENT ON COLUMN ais.paycard.paysystemisn IS $COMM$FK(DICTI) Платежная система$COMM$;
COMMENT ON COLUMN ais.paycard.cardproductisn IS $COMM$FK(DICTI) Банковский продукт$COMM$;
COMMENT ON COLUMN ais.paycard.overdraft IS $COMM$Лимит овердрафта по карте$COMM$;
COMMENT ON COLUMN ais.paycard.subjcheckdate IS $COMM$Дата проверки данных клиента$COMM$;
COMMENT ON COLUMN ais.paycard.subjapproved IS $COMM$Флаг подтверждения проверки данных клиента$COMM$;
COMMENT ON COLUMN ais.paycard.cardapproved IS $COMM$Флаг подтверждения проверки карты по реестру банка$COMM$;
COMMENT ON COLUMN ais.paycard.paymentsum IS $COMM$Сумма платежа при оплате договора страхования в счет офердрафта$COMM$;
COMMENT ON COLUMN ais.paycard.currisn IS $COMM$FK(CURRENCY). Ссылка на валюту карты.$COMM$;
COMMENT ON COLUMN ais.paycard.agrisn IS $COMM$FK(AGREEMNT). Ссылка на договор страхования при покупке которого выдана карта.$COMM$;
COMMENT ON COLUMN ais.paycard.created IS $COMM$Дата создания$COMM$;
COMMENT ON COLUMN ais.paycard.createdby IS $COMM$FK(SUBJECT) Пользователь создатель$COMM$;
COMMENT ON COLUMN ais.paycard.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.paycard.updatedby IS $COMM$FK(SUBJECT) Автор изменения$COMM$;
COMMENT ON COLUMN ais.paycard.bankisn IS $COMM$FK(SUBBANK) Банк выпустивший карту$COMM$;


CREATE TABLE ais.paysysteminfo (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    trnid                            VARCHAR(255),
    objisn                           NUMERIC,
    formisn                          NUMERIC,
    datepay                          TIMESTAMP,
    payinfo                          VARCHAR(4000),
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    paystatusid                      VARCHAR(1),
    aisinfo                          VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.paysysteminfo IS $COMM$ Таблица для хранения данных поступающих из платежной системы при оплате услуг ИГС через интернет, терминалы или сторонними партнерами. Угринович А.Н. 28.12.2011$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.aisinfo IS $COMM$Любая информация в AIS, связанная с платежом$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.paystatusid IS $COMM$Состояние платежа: Н - не подтверждён, П - подтверждён, А - аннулирован/отменён$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.isn IS $COMM$Машинный номер: SEQ_PAYSYSTEMINFO.nextval$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.classisn IS $COMM$(FK DICTI) Платежная система$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.trnid IS $COMM$ Идентификатор транзакциии в платежной системе$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.objisn IS $COMM$Ссылка на связанный с транзакцией объект в АИС$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.formisn IS $COMM$(FK DICTI) тип связанного объекта в АИС$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.datepay IS $COMM$Дата оплаты услуги$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.payinfo IS $COMM$Полная информация о транзакции$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.created IS $COMM$Дата создания поддерживается автоматически$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.createdby IS $COMM$(FK SUBJECT) Создатель поддерживается автоматически$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.updated IS $COMM$Дата изменения поддерживается автоматически$COMM$;
COMMENT ON COLUMN ais.paysysteminfo.updatedby IS $COMM$(FK SUBJECT) Автор изменения поддерживается автоматически$COMM$;


CREATE TABLE ais.presale (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    classisn                         NUMERIC,
    ruleisn                          NUMERIC,
    status                           VARCHAR(1),
    duedate                          TIMESTAMP,
    id                               VARCHAR(20),
    subjisn                          NUMERIC,
    sourceisn                        NUMERIC,
    emplisn                          NUMERIC,
    deptisn                          NUMERIC,
    premiumsum                       NUMERIC,
    currisn                          NUMERIC,
    agrcnt                           NUMERIC,
    objcnt                           NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    objpremiumsum                    NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    agentisn                         NUMERIC,
    remark                           VARCHAR(4000),
    rejectisn                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.presale IS $COMM$Сущность "предпродажа". Опеределяет контакт с клиетом для заключения одного или нескольких договоров страхования.
Содержит сведения о планируемом кол-ве договоров и плнируемой сумме премии$COMM$;
COMMENT ON COLUMN ais.presale.datebeg IS $COMM$Предполагаемая дата начала страхования$COMM$;
COMMENT ON COLUMN ais.presale.rejectisn IS $COMM$FK(DICTI) Причина отказа$COMM$;
COMMENT ON COLUMN ais.presale.objpremiumsum IS $COMM$Планируемая сумма премии по одному объекту$COMM$;
COMMENT ON COLUMN ais.presale.isn IS $COMM$Превичный ключ. SEQ_PRESALE$COMM$;
COMMENT ON COLUMN ais.presale.parentisn IS $COMM$Иерархическая связка$COMM$;
COMMENT ON COLUMN ais.presale.classisn IS $COMM$FK(DICTI) Вид предпродажи$COMM$;
COMMENT ON COLUMN ais.presale.ruleisn IS $COMM$FK(DICTI) Страховой продукт/правило страхования$COMM$;
COMMENT ON COLUMN ais.presale.status IS $COMM$Статус$COMM$;
COMMENT ON COLUMN ais.presale.duedate IS $COMM$Срок выполнения$COMM$;
COMMENT ON COLUMN ais.presale.id IS $COMM$Номер. По умолчанию равен ISN$COMM$;
COMMENT ON COLUMN ais.presale.subjisn IS $COMM$FK(SUBJECT) Ссылка на клиента$COMM$;
COMMENT ON COLUMN ais.presale.sourceisn IS $COMM$FK(DICTI) Источник$COMM$;
COMMENT ON COLUMN ais.presale.emplisn IS $COMM$FK(SUBJECT) Куратор$COMM$;
COMMENT ON COLUMN ais.presale.deptisn IS $COMM$FK(SUBDEPT) Подразделение куратора$COMM$;
COMMENT ON COLUMN ais.presale.premiumsum IS $COMM$Планируемая премия$COMM$;
COMMENT ON COLUMN ais.presale.currisn IS $COMM$FK(CURRENCY). Ссылка на валюту премии.$COMM$;
COMMENT ON COLUMN ais.presale.agrcnt IS $COMM$Планируемое количество договоров$COMM$;
COMMENT ON COLUMN ais.presale.objcnt IS $COMM$Планируемое количество объектов$COMM$;
COMMENT ON COLUMN ais.presale.created IS $COMM$Дата создания$COMM$;
COMMENT ON COLUMN ais.presale.createdby IS $COMM$FK(SUBJECT) Пользователь создатель$COMM$;
COMMENT ON COLUMN ais.presale.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.presale.updatedby IS $COMM$FK(SUBJECT) Автор изменения$COMM$;
COMMENT ON COLUMN ais.presale.dateend IS $COMM$Предполагаемая дата окончания страхования$COMM$;
COMMENT ON COLUMN ais.presale.agentisn IS $COMM$FK(Subject) Агент/посредник$COMM$;
COMMENT ON COLUMN ais.presale.remark IS $COMM$Примечание$COMM$;


CREATE TABLE ais.qtask (
    isn                              NUMERIC,
    title                            VARCHAR(100),
    description                      VARCHAR(4000),
    discr                            VARCHAR(1) DEFAULT 'E',
    priorityisn                      NUMERIC,
    submitterisn                     NUMERIC,
    managerisn                       NUMERIC,
    resolutionisn                    NUMERIC,
    activityisn                      NUMERIC,
    projectisn                       NUMERIC,
    fieldisn                         NUMERIC,
    moduleisn                        NUMERIC,
    ffunction                        VARCHAR(40),
    mediaisn                         NUMERIC,
    resolvedate                      TIMESTAMP,
    period                           TIMESTAMP,
    parentisn                        NUMERIC,
    origrequest                      VARCHAR(4000),
    updversion                       NUMERIC,
    estimatedhours                   NUMERIC,
    spendedhours                     NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    daterequest                      TIMESTAMP,
    subclassisn                      NUMERIC,
    locationisn                      NUMERIC,
    status                           VARCHAR(1),
    managerdeptisn                   NUMERIC,
    submitterdeptisn                 NUMERIC,
    rating                           NUMERIC,
    ratingreasonisn                  NUMERIC,
    duebegdate                       TIMESTAMP
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.qtask IS $COMM$Таблица для регистрации информации о задачах. (Юнин В.А. 29.09.2003)$COMM$;
COMMENT ON COLUMN ais.qtask.rating IS $COMM$Приоритет задачи от 1 до 100 (Угринович А.Н. 20.03.09)$COMM$;
COMMENT ON COLUMN ais.qtask.ratingreasonisn IS $COMM$(FK)DICTI Основание установки приоритета (Угринович А.Н. 20.03.09)$COMM$;
COMMENT ON COLUMN ais.qtask.subclassisn IS $COMM$(FK DICTI) Классификация -- 14.07.05 SR$COMM$;
COMMENT ON COLUMN ais.qtask.locationisn IS $COMM$(FK DICTI) Указатель местонахождения заказчика задачи (территория, кабинет) -- 14.07.05 SR$COMM$;
COMMENT ON COLUMN ais.qtask.status IS $COMM$Значение наследуется из Queue.Status для ускорения запросов. AL 30/06/05$COMM$;
COMMENT ON COLUMN ais.qtask.managerdeptisn IS $COMM$(FK SubDept) Подразделение исполнителя (Угринович А.Н. 21.02.06)$COMM$;
COMMENT ON COLUMN ais.qtask.submitterdeptisn IS $COMM$(FK SubDept) Подразделение заказчика (Угринович А.Н. 21.02.06)$COMM$;
COMMENT ON COLUMN ais.qtask.isn IS $COMM$Уникальный машинный ISN по SEQ_QUEUE.nextval$COMM$;
COMMENT ON COLUMN ais.qtask.title IS $COMM$Заголовок. Краткое описание задачи (проблемы или требуемой доработки) для отображения в отборе.$COMM$;
COMMENT ON COLUMN ais.qtask.description IS $COMM$Описание. Подробное описание задачи (проблемы или требуемой доработки).$COMM$;
COMMENT ON COLUMN ais.qtask.discr IS $COMM$Тип: E-ошибка, W-доработка, Q-запрос, D-проект, O-записка$COMM$;
COMMENT ON COLUMN ais.qtask.priorityisn IS $COMM$(FK DICTI) Важность. Значения: низкая L, средняя M, высокая H, критическая C.$COMM$;
COMMENT ON COLUMN ais.qtask.submitterisn IS $COMM$(FK Subject) Пользователь (заказчик). Пользователь или другой сотрудник, направивший заявку на доработку или исправление.$COMM$;
COMMENT ON COLUMN ais.qtask.managerisn IS $COMM$(FK Subject) Куратор. Руководитель, курирующий выполнение задачи.$COMM$;
COMMENT ON COLUMN ais.qtask.resolutionisn IS $COMM$(FK DICTI) Решение. Значения: для ошибки: исправлена, не воспроизводится, не подлежит исправлению, отложена, введена повторно; для доработки: реализована, отклонена, введена повторно$COMM$;
COMMENT ON COLUMN ais.qtask.activityisn IS $COMM$(FK DICTI) Тип работ. Разработка, тестирование, документирование и т.п.$COMM$;
COMMENT ON COLUMN ais.qtask.projectisn IS $COMM$(FK DICTI) Проект.$COMM$;
COMMENT ON COLUMN ais.qtask.fieldisn IS $COMM$(FK DICTI) Предметная область.$COMM$;
COMMENT ON COLUMN ais.qtask.moduleisn IS $COMM$(FK DICTI) Модуль.$COMM$;
COMMENT ON COLUMN ais.qtask.ffunction IS $COMM$Форма/функция$COMM$;
COMMENT ON COLUMN ais.qtask.mediaisn IS $COMM$(FK DICTI) Способ доставки. Способ доставки исходной заявки, значения: устно, письменно, телефон, почта.$COMM$;
COMMENT ON COLUMN ais.qtask.resolvedate IS $COMM$Дата выполнения.$COMM$;
COMMENT ON COLUMN ais.qtask.period IS $COMM$Требуемый срок$COMM$;
COMMENT ON COLUMN ais.qtask.parentisn IS $COMM$(FK QTask) Головная задача. Обобщающая задача, для которой данная является подзадачей.$COMM$;
COMMENT ON COLUMN ais.qtask.origrequest IS $COMM$Оригинальный текст письма$COMM$;
COMMENT ON COLUMN ais.qtask.updversion IS $COMM$Для недопущения коллизий при Update,
при одновременном использовании записи$COMM$;
COMMENT ON COLUMN ais.qtask.estimatedhours IS $COMM$Планируемое время (в часах)$COMM$;
COMMENT ON COLUMN ais.qtask.spendedhours IS $COMM$Затраченное время (в часах)$COMM$;
COMMENT ON COLUMN ais.qtask.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.qtask.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.qtask.daterequest IS $COMM$Дата заявки Yunin V.A. 29.12.03$COMM$;


CREATE TABLE ais.qtaskxobj (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    objisn                           NUMERIC,
    taskisn                          NUMERIC,
    formisn                          NUMERIC,
    id                               VARCHAR(50),
    detailobjisn                     NUMERIC,
    active                           VARCHAR(1),
    remark                           VARCHAR(2000),
    created                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.qtaskxobj IS $COMM$Таблица связей задач и бизнес объектов. Используется при регистрации бизнес задач и задач Workflow.$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.classisn IS $COMM$FK(DICTI) тип связи$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.objisn IS $COMM$FK(произвольная таблица) связанный с задачей объект$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.taskisn IS $COMM$FK(QUEUE) связанная задача$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.formisn IS $COMM$FK(DICTI) форма просмотра связанного объекта$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.id IS $COMM$номер/название объекта$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.detailobjisn IS $COMM$FK(произвольная таблица) детализация объекта: аддендум, претензия$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.active IS $COMM$Активность связи. Y - выверенная, N - невыверенная, NULL - не активная$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.created IS $COMM$Дата создания$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.createdby IS $COMM$FK(SUBJECT) Пользователь создатель$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.qtaskxobj.updatedby IS $COMM$FK(SUBJECT) Автор изменения$COMM$;


CREATE TABLE ais.quedecode (
    isn                              NUMERIC,
    queisn                           NUMERIC,
    objisn                           NUMERIC,
    objparam1                        NUMERIC,
    objparam2                        NUMERIC,
    refisn                           NUMERIC,
    refparam1                        NUMERIC,
    refparam2                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    daterate                         TIMESTAMP,
    rate                             NUMERIC,
    headisn                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.quedecode IS $COMM$Расшифровка задания в очереди$COMM$;
COMMENT ON COLUMN ais.quedecode.queisn IS $COMM$FK (Queue) Ссылка на очередь, к которой относится эта запись$COMM$;
COMMENT ON COLUMN ais.quedecode.objisn IS $COMM$Ссылка на обрабатываемый объект. Наследуется из queue.objisn$COMM$;
COMMENT ON COLUMN ais.quedecode.objparam1 IS $COMM$Параметр объекта обработки. Интерпретируется в зависимости от задачи$COMM$;
COMMENT ON COLUMN ais.quedecode.objparam2 IS $COMM$Параметр объекта обработки. Интерпретируется в зависимости от задачи$COMM$;
COMMENT ON COLUMN ais.quedecode.refisn IS $COMM$Ссылка на объект, с которым связывается обрабатываемый объект$COMM$;
COMMENT ON COLUMN ais.quedecode.refparam1 IS $COMM$Параметр для связанного объекта. Интерпретируется в зависимости от задачи$COMM$;
COMMENT ON COLUMN ais.quedecode.refparam2 IS $COMM$Параметр для связанного объекта. Интерпретируется в зависимости от задачи$COMM$;
COMMENT ON COLUMN ais.quedecode.updated IS $COMM$Время последнего изменения$COMM$;
COMMENT ON COLUMN ais.quedecode.updatedby IS $COMM$Автор последнего изменения$COMM$;
COMMENT ON COLUMN ais.quedecode.daterate IS $COMM$Дата, на которую определяется курс пересчета параметра связанного объекта (сумма оригинального начисления) в параметр объекта обработки (сумма покрытия). При этом считается, что REFPARAM2 и OBJPARAM2 - указатели валют соответствующих сумм$COMM$;
COMMENT ON COLUMN ais.quedecode.rate IS $COMM$Курс пересчета первого параметра связанного объекта REFPARAM1 в первый параметр объекта обработки OBJPARAM1$COMM$;
COMMENT ON COLUMN ais.quedecode.headisn IS $COMM$FK(BUHHEAD). Ссылка на проводку частичного сальдо при неполной балансовой квитовке$COMM$;


CREATE TABLE ais.quephone (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    deptisn                          NUMERIC,
    classisn                         NUMERIC,
    agentisn                         NUMERIC,
    subjisn                          NUMERIC,
    subjname                         VARCHAR(40),
    agrisn                           NUMERIC,
    agrid                            VARCHAR(20),
    claimisn                         NUMERIC,
    claimid                          VARCHAR(20),
    dateloss                         TIMESTAMP,
    objisn                           NUMERIC,
    objid                            VARCHAR(40),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    info                             NUMERIC,
    active                           VARCHAR(1),
    remark                           VARCHAR(1000),
    created                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    subjdeptisn                      NUMERIC,
    taskname                         VARCHAR(255),
    taskclassisn                     NUMERIC,
    taskid                           VARCHAR(20),
    emplisn                          NUMERIC,
    refagrisn                        NUMERIC,
    reason                           VARCHAR(1000),
    diagnosis                        VARCHAR(1000),
    objremark                        VARCHAR(1000),
    phonecode                        VARCHAR(10),
    phonenum                         VARCHAR(40),
    addrisn                          NUMERIC,
    adchannelisn                     NUMERIC,
    diagnosiscode                    NUMERIC,
    adchannelisn2                    NUMERIC,
    adchannelisn3                    NUMERIC,
    servaddrisn                      NUMERIC,
    fid                              VARCHAR(64),
    lpudeptno                        VARCHAR(60),
    agentid                          VARCHAR(64)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.quephone IS $COMM$Телефонные звонки (CALL-центр)$COMM$;
COMMENT ON COLUMN ais.quephone.adchannelisn IS $COMM$Каналы рекламы (Dicti) Кудинова Е.Р. 24/03/2006$COMM$;
COMMENT ON COLUMN ais.quephone.fid IS $COMM$Ссылка на уникальный идентификатор вызова из Genesys$COMM$;
COMMENT ON COLUMN ais.quephone.diagnosiscode IS $COMM$FK(MEDMKB10) Код диагноза по МКБ10 Кудинова Е.Р.30.10.06$COMM$;
COMMENT ON COLUMN ais.quephone.lpudeptno IS $COMM$Номер отделения ЛПУ$COMM$;
COMMENT ON COLUMN ais.quephone.agentid IS $COMM$Идентификатор AgentID из Genesys$COMM$;
COMMENT ON COLUMN ais.quephone.adchannelisn2 IS $COMM$Второй канал  рекламы (Dicti) Кудинова Е.Р. 18.04.07$COMM$;
COMMENT ON COLUMN ais.quephone.adchannelisn3 IS $COMM$Третий канал  рекламы (Dicti) Кудинова Е.Р. 18.04.07$COMM$;
COMMENT ON COLUMN ais.quephone.servaddrisn IS $COMM$FK(DICTI) - Город услуги, Шмакова Т.Г. 24.05.2010$COMM$;
COMMENT ON COLUMN ais.quephone.addrisn IS $COMM$FK(DICTI) - Город обращения КЦ. Попов В.О. 02/02/2005$COMM$;
COMMENT ON COLUMN ais.quephone.isn IS $COMM$Машинный номер, SEQ_QUEPHONE.nextval$COMM$;
COMMENT ON COLUMN ais.quephone.parentisn IS $COMM$FK(QUEPHONE) указатель предыдущего звонка$COMM$;
COMMENT ON COLUMN ais.quephone.deptisn IS $COMM$FK(SUBDEPT) указатель подразделения куратора$COMM$;
COMMENT ON COLUMN ais.quephone.classisn IS $COMM$FK(DICTI) Класс звонка$COMM$;
COMMENT ON COLUMN ais.quephone.agentisn IS $COMM$FK(SUBJECT) Указатель агента по урегулированию убытка (сюрвейер)$COMM$;
COMMENT ON COLUMN ais.quephone.subjisn IS $COMM$FK(SUBJECT) Указатель страхователя$COMM$;
COMMENT ON COLUMN ais.quephone.subjname IS $COMM$Контактное лицо (страхователь или его представитель), телефон$COMM$;
COMMENT ON COLUMN ais.quephone.agrisn IS $COMM$FK(AGREEMENT) Указатель договора$COMM$;
COMMENT ON COLUMN ais.quephone.agrid IS $COMM$Номер договора, автоматически проставляется по AgrISN$COMM$;
COMMENT ON COLUMN ais.quephone.claimisn IS $COMM$FK(AGRCLAIM) указатель убытка$COMM$;
COMMENT ON COLUMN ais.quephone.claimid IS $COMM$Номер убытка, автоматически проставляется по ClaimISN$COMM$;
COMMENT ON COLUMN ais.quephone.dateloss IS $COMM$Дата убытка (возникновения страхового случая)$COMM$;
COMMENT ON COLUMN ais.quephone.objisn IS $COMM$FK(OBJAGR) Указатель физического объекта$COMM$;
COMMENT ON COLUMN ais.quephone.objid IS $COMM$Идентификатор объекта, автоматически проставляется по ObjISN$COMM$;
COMMENT ON COLUMN ais.quephone.datebeg IS $COMM$Дата и время начала разговора$COMM$;
COMMENT ON COLUMN ais.quephone.dateend IS $COMM$Дата и время окончания разговора$COMM$;
COMMENT ON COLUMN ais.quephone.info IS $COMM$Битовая маска для отметки вида информации сообщенной звонившему: 1-Инф.о правилах заполнении заявления, 2-Запрос в ГИБДД на ф.2,4-ф.2 получена страхователем, 8-договоренность об Осмотре, 16-выдана смета$COMM$;
COMMENT ON COLUMN ais.quephone.active IS $COMM$Статус звонка:
N - текущий
Y - в работе
NULL-обработан$COMM$;
COMMENT ON COLUMN ais.quephone.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.quephone.created IS $COMM$Дата регистрации$COMM$;
COMMENT ON COLUMN ais.quephone.createdby IS $COMM$Создатель-оператор$COMM$;
COMMENT ON COLUMN ais.quephone.updated IS $COMM$Время последнего изменения$COMM$;
COMMENT ON COLUMN ais.quephone.updatedby IS $COMM$Автор последнего изменения$COMM$;
COMMENT ON COLUMN ais.quephone.subjdeptisn IS $COMM$Подразделение источника$COMM$;
COMMENT ON COLUMN ais.quephone.taskname IS $COMM$Наименование задачи$COMM$;
COMMENT ON COLUMN ais.quephone.taskclassisn IS $COMM$Классификатор задачи$COMM$;
COMMENT ON COLUMN ais.quephone.taskid IS $COMM$Номер служебной записки$COMM$;
COMMENT ON COLUMN ais.quephone.emplisn IS $COMM$Получатель$COMM$;
COMMENT ON COLUMN ais.quephone.refagrisn IS $COMM$FK(AGREEMENT) Указатель на хоз. договор. Yunin V.A. 25/05/04$COMM$;
COMMENT ON COLUMN ais.quephone.reason IS $COMM$Повод для обращения (причина). Yunin V.A. 25/05/04$COMM$;
COMMENT ON COLUMN ais.quephone.diagnosis IS $COMM$Диагноз. Yunin V.A. 25/05/04$COMM$;
COMMENT ON COLUMN ais.quephone.objremark IS $COMM$Примечание. Yunin V.A. 16/06/04$COMM$;
COMMENT ON COLUMN ais.quephone.phonecode IS $COMM$Код телефонного номера. SAG 02/08/2004 $COMM$;
COMMENT ON COLUMN ais.quephone.phonenum IS $COMM$Телефонный номер. SAG 02/08/2004 $COMM$;


CREATE TABLE ais.quephone_ext (
    isn                              NUMERIC,
    quiephoneisn                     NUMERIC,
    type_ext                         NUMERIC,
    servicename                      VARCHAR(255),
    note                             VARCHAR(1000),
    summa                            NUMERIC,
    value                            NUMERIC,
    datebeg                          TIMESTAMP,
    service                          NUMERIC,
    control                          NUMERIC,
    userisn                          NUMERIC,
    updatedby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    result                           NUMERIC,
    criteria                         NUMERIC,
    result_name                      VARCHAR(255),
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    expert_note                      VARCHAR(1000)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.quephone_ext IS $COMM$@purpose=расширение таблицы QuePhone@createdby=Бондарев А.А.@NOAIUDS @SEQ=seq_quephone_ext$COMM$;
COMMENT ON COLUMN ais.quephone_ext.isn IS $COMM$@purpose=Идентификатор@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.quiephoneisn IS $COMM$@purpose=Ссылка на родительскую таблицу@fk=QUEPHONE(restrict)@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.type_ext IS $COMM$@purpose=Тип записи (1-согласование услуг, 2-контроль госпитализаций, 3-снижение выплат, 4-удовлетворённость клиента)@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.servicename IS $COMM$@purpose=Наименование услуги@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.note IS $COMM$@purpose=Суть наршения/причина отказа@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.summa IS $COMM$@purpose=Сумма@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.value IS $COMM$@purpose=Значение критерия@fk=DICTI(restrict)@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.datebeg IS $COMM$@purpose=Дата@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.service IS $COMM$@purpose=Услуга@fk=DICTI(restrict)@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.control IS $COMM$@purpose=Вид контроля@fk=DICTI(restrict)@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.userisn IS $COMM$@purpose=Куратор, либо врач-эксперт@fk=DICTI(restrict)@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.updatedby IS $COMM$@purpose=Кто обновил@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.updated IS $COMM$@purpose=Когда обновлено@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.result IS $COMM$@purpose=Результат@fk=DICTI(restrict)@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.criteria IS $COMM$@purpose=Критерий@fk=DICTI(restrict)@createdby=Бондарев А.А.$COMM$;
COMMENT ON COLUMN ais.quephone_ext.result_name IS $COMM$@purpose=Наименование результата@createdby=Бондарев А.А.$COMM$;


CREATE TABLE ais.queue (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    senderisn                        NUMERIC,
    receiverisn                      NUMERIC,
    formisn                          NUMERIC,
    processor                        VARCHAR(32),
    objisn                           NUMERIC,
    id                               VARCHAR(20),
    request                          VARCHAR(255),
    reply                            VARCHAR(1),
    message                          VARCHAR(1000),
    status                           VARCHAR(1) DEFAULT 'W',
    datesend                         TIMESTAMP,
    datereceive                      TIMESTAMP,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    objisn2                          NUMERIC,
    replyisn                         NUMERIC,
    replymsg                         VARCHAR(255),
    dateexp                          TIMESTAMP,
    parentisn                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.queue.replyisn IS $COMM$FK(DICTI). Yunin V.A. 26/11/04$COMM$;
COMMENT ON COLUMN ais.queue.replymsg IS $COMM$Yunin V.A. 26/11/04$COMM$;
COMMENT ON COLUMN ais.queue.parentisn IS $COMM$Ссылка на первичное сообщение (Smirnov 13/01/06)$COMM$;
COMMENT ON COLUMN ais.queue.isn IS $COMM$Уникальный машинный ISN по SEQ_QUEUE.nextval$COMM$;
COMMENT ON COLUMN ais.queue.classisn IS $COMM$FK(DICTI,ОЧЕРЕДЬ). Тип очереди из соответствующего кодификатора в словаре.$COMM$;
COMMENT ON COLUMN ais.queue.senderisn IS $COMM$FK(DICTI). Ссылка на отправителя (сотрудник, подразделение, абстрактный процесс)$COMM$;
COMMENT ON COLUMN ais.queue.receiverisn IS $COMM$FK(DICTI). Ссылка на получателя (сотрудник, подразделение, абстрактный процесс)$COMM$;
COMMENT ON COLUMN ais.queue.formisn IS $COMM$FK(DICTI). Указатель типа формы для просмотра запроса$COMM$;
COMMENT ON COLUMN ais.queue.processor IS $COMM$Программа - обработчик запроса.$COMM$;
COMMENT ON COLUMN ais.queue.objisn IS $COMM$FK. Указатель объекта, поставленного в очередь: ISN записи одной из таблий БД. Какая это
таблица конкретно, знают программа-отправитель и программа-обработчик.$COMM$;
COMMENT ON COLUMN ais.queue.id IS $COMM$Внешний номер документа$COMM$;
COMMENT ON COLUMN ais.queue.request IS $COMM$Формализованный запрос в синтаксисе, зависящем от обработчика$COMM$;
COMMENT ON COLUMN ais.queue.reply IS $COMM$Индикатор ответа: Y-нужна квитанция. В этом случае после обработки запроса он
перенаправляется отправителю со статусом 'Y' (Отправитель и получатель меняются местами).$COMM$;
COMMENT ON COLUMN ais.queue.message IS $COMM$Текстовое сообщение адресату. Здесь ведется переписка по данному запросу.$COMM$;
COMMENT ON COLUMN ais.queue.status IS $COMM$Статус запроса: W-ожидание обработки, P-занят, O-в работе, Y-обработан$COMM$;
COMMENT ON COLUMN ais.queue.datesend IS $COMM$Дата отправления запроса, заполняется автоматически.$COMM$;
COMMENT ON COLUMN ais.queue.datereceive IS $COMM$Желаемая (контрольная ) дата обработки запроса, заполняется автоматически реальной
датой обработки.$COMM$;
COMMENT ON COLUMN ais.queue.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.queue.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.queue.objisn2 IS $COMM$FK.Указатель зависимого от OBJISN подобъекта (типа связки
AGREEMENT->DOCSUM,AGREEMENT->DOCS)$COMM$;


CREATE TABLE ais.reacc100 (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    id                               VARCHAR(20),
    name                             VARCHAR(255),
    dateacc                          TIMESTAMP,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    created                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    status                           VARCHAR(1) DEFAULT 'N',
    currisn                          NUMERIC,
    deptisn                          NUMERIC,
    reacctype                        VARCHAR(1),
    parentisn                        NUMERIC,
    docdate                          TIMESTAMP
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.reacc100 IS $COMM$Таблица хранит основные параметры 100% счета, выпускаемого при начислениях по исходящему
перестрахованию$COMM$;
COMMENT ON COLUMN ais.reacc100.isn IS $COMM$Машинный номер объекта. Устанавливается по умолчанию равным SEQ_REACC100.nextval$COMM$;
COMMENT ON COLUMN ais.reacc100.agrisn IS $COMM$FK(AGREEMENT). Cсылка на соответствующий перестраховочный договор.$COMM$;
COMMENT ON COLUMN ais.reacc100.id IS $COMM$Учетный номер/индекс начисления (100% счета) - уникальный для всего исходящего
перестрахования$COMM$;
COMMENT ON COLUMN ais.reacc100.name IS $COMM$Название начисления (напр. "Счет за 1ый квартал 1997 года")$COMM$;
COMMENT ON COLUMN ais.reacc100.dateacc IS $COMM$Дата начисления - дата расчета 100% счета и размещения по участникам$COMM$;
COMMENT ON COLUMN ais.reacc100.datebeg IS $COMM$Начальная дата периода, за который делается начисление (для регулярных начислений)$COMM$;
COMMENT ON COLUMN ais.reacc100.dateend IS $COMM$Конечная дата периода, за который делается начисление$COMM$;
COMMENT ON COLUMN ais.reacc100.created IS $COMM$Дата создания. Регистрируется автоматически$COMM$;
COMMENT ON COLUMN ais.reacc100.createdby IS $COMM$Автор создания объекта. Устанавливается автоматически равным ISN активного
пользователя (init.UserISN) при создании.$COMM$;
COMMENT ON COLUMN ais.reacc100.updated IS $COMM$Дата создания или последнего изменения объекта. Устанавливается автоматически равной
SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.reacc100.updatedby IS $COMM$Автор создания или последнего изменения объекта. Устанавливается автоматически
равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.reacc100.status IS $COMM$Флаг, определяющий состояние начисления :
N (new) - начисление в стадии создания, расчета, размещения
Y (yes)- начисление подтверждено - закрыто для дальнейшей модификации
X - (eХit) начисление аннулировано
A - (Архив) архивная запись$COMM$;
COMMENT ON COLUMN ais.reacc100.currisn IS $COMM$FK(CURRENCY). Указатель валюты начисления.$COMM$;
COMMENT ON COLUMN ais.reacc100.deptisn IS $COMM$FK(SUBDEPT). Указатель подразделения-владельца.$COMM$;
COMMENT ON COLUMN ais.reacc100.reacctype IS $COMM$Тип начисления-размещения:
R-регулярное начисление (по условиям договора)
S-долевое размещение 100-процентных сумм
O-персональное размещение 100-процентной суммы
P-расчет тантьемы
T-налог на доход иностранных юр.лиц U- сторно$COMM$;
COMMENT ON COLUMN ais.reacc100.parentisn IS $COMM$FK(REACC100). Ссылка на перестраховочное начисление, ставшего основанием для
автоматической генерации данного
начисления (как, например, в случае генерации начисления на налог с дохода иностранных
юридических лиц при платеже участнику)$COMM$;


CREATE TABLE ais.reaccsum (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    classisn2                        NUMERIC,
    reaccisn                         NUMERIC,
    agrisn                           NUMERIC,
    sectisn                          NUMERIC,
    condisn                          NUMERIC,
    subjisn                          NUMERIC,
    subaccisn                        NUMERIC,
    amount                           NUMERIC(20,2),
    amount100                        NUMERIC(20,2),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    currisn                          NUMERIC,
    parentisn                        NUMERIC,
    sumisn                           NUMERIC,
    docdate                          TIMESTAMP
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.reaccsum IS $COMM$Суммы статей начисления по исходящему перестрахованию - расшифровка 100 счета$COMM$;
COMMENT ON COLUMN ais.reaccsum.reaccisn IS $COMM$FK(REACC100). Указатель на начисление (заголовок 100 счета), к которому относится
объект.$COMM$;
COMMENT ON COLUMN ais.reaccsum.agrisn IS $COMM$FK(AGREEMENT). Cсылка на соответствующий перестраховочный договор.$COMM$;
COMMENT ON COLUMN ais.reaccsum.sectisn IS $COMM$FK(RESECTION). Указатель на секцию перестраховочного договора.$COMM$;
COMMENT ON COLUMN ais.reaccsum.condisn IS $COMM$FK(RECOND). Указатель на условия перестраховочной секции.$COMM$;
COMMENT ON COLUMN ais.reaccsum.subaccisn IS $COMM$FK(DICTI). Указатель бухгалтерского счета$COMM$;
COMMENT ON COLUMN ais.reaccsum.amount IS $COMM$Сумма в валюте начисления$COMM$;
COMMENT ON COLUMN ais.reaccsum.amount100 IS $COMM$100 сумма в валюте начисления$COMM$;
COMMENT ON COLUMN ais.reaccsum.updated IS $COMM$Дата изменения, устанавливается автоматически.$COMM$;
COMMENT ON COLUMN ais.reaccsum.updatedby IS $COMM$Автор создания или последнего изменения объекта. Устанавливается автоматически
равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.reaccsum.currisn IS $COMM$FK(CURRENCY) Валюта начисления. По умолчанию - должна наследоваться из секции (RESECTION)$COMM$;
COMMENT ON COLUMN ais.reaccsum.parentisn IS $COMM$FK(REACCSUM) Ссылка на сумму, определяющую данную. Например - налог с инюрлиц ссылается на премию$COMM$;
COMMENT ON COLUMN ais.reaccsum.sumisn IS $COMM$FK(DOCSUM) Ссылка на сумму документа (начисленную), которую сформировала данная$COMM$;
COMMENT ON COLUMN ais.reaccsum.isn IS $COMM$Машинный номер объекта. Устанавливается по умолчанию равным SEQ_REACCSUM.nextval$COMM$;
COMMENT ON COLUMN ais.reaccsum.classisn IS $COMM$FK(DICTI,СУММА). Тип суммы (как бухгалтерской операции для автоматической генерации
проводок).$COMM$;
COMMENT ON COLUMN ais.reaccsum.classisn2 IS $COMM$FK(DICTI). Указатель подкласса суммы. ClassISN - его родитель$COMM$;


CREATE TABLE ais.recond (
    isn                              NUMERIC,
    sectisn                          NUMERIC,
    agrisn                           NUMERIC,
    name                             VARCHAR(20),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    retention                        NUMERIC(9,6),
    prioritysum                      NUMERIC(20,2),
    limitsum                         NUMERIC(20,2),
    rate                             NUMERIC(9,6),
    depospremsum                     NUMERIC(20,2),
    reinstnum                        NUMERIC(38),
    reinstrate                       NUMERIC(9,6),
    reinstprorata                    VARCHAR(1),
    brokerage                        NUMERIC(9,6),
    recommiss                        NUMERIC(9,6),
    ovrcommiss                       NUMERIC(9,6),
    profitcommiss                    NUMERIC(9,6),
    bonus                            NUMERIC(9,6),
    deficityears                     NUMERIC(38),
    cashloss                         NUMERIC(20,2),
    premreserve                      NUMERIC(9,6),
    premrelperiod                    NUMERIC(38),
    lossreserve                      NUMERIC(9,6),
    lossrelperiod                    NUMERIC(38),
    preminterest                     NUMERIC(9,6),
    lossinterest                     NUMERIC(9,6),
    premportf                        NUMERIC(9,6),
    lossportf                        NUMERIC(9,6),
    premtype                         VARCHAR(1),
    epi                              NUMERIC(20,2),
    nextinvoicedate                  TIMESTAMP,
    paymentnum                       NUMERIC(38),
    franchsum                        NUMERIC(20,2),
    sharepc                          NUMERIC(9,6),
    taxrate                          NUMERIC(9,6),
    expences                         NUMERIC(9,6),
    rpm                              NUMERIC(9,6),
    fpm                              NUMERIC(9,6),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    profitexpense                    NUMERIC(9,6),
    remark                           VARCHAR(255),
    accountperiod                    VARCHAR(1),
    accountterm                      NUMERIC,
    noticeterm                       NUMERIC,
    losstypeisn                      NUMERIC,
    premiumtypeisn                   NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.recond.isn IS $COMM$Машинный номер: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.recond.sectisn IS $COMM$FK(RESECTION). Указатель секции договора перестрахования$COMM$;
COMMENT ON COLUMN ais.recond.agrisn IS $COMM$FK(AGREEMENT). Указатель формуляра договора перестрахования, наследуется из секции$COMM$;
COMMENT ON COLUMN ais.recond.name IS $COMM$Наименование условия$COMM$;
COMMENT ON COLUMN ais.recond.datebeg IS $COMM$Дата начала действия секции$COMM$;
COMMENT ON COLUMN ais.recond.dateend IS $COMM$Дата окончания действия секции$COMM$;
COMMENT ON COLUMN ais.recond.retention IS $COMM$ собственного удержания$COMM$;
COMMENT ON COLUMN ais.recond.prioritysum IS $COMM$Сумма приоритета (нижней границы лейера)$COMM$;
COMMENT ON COLUMN ais.recond.limitsum IS $COMM$Сумма лимита ответственности (емкость лейера)$COMM$;
COMMENT ON COLUMN ais.recond.rate IS $COMM$Ставка премии  в $COMM$;
COMMENT ON COLUMN ais.recond.depospremsum IS $COMM$Сумма депозитной премии$COMM$;
COMMENT ON COLUMN ais.recond.reinstnum IS $COMM$Число восстановлений: количество выплат премии цедентом перестраховщику, если общая
сумма убытков превышает лимит ответственности по данному лейеру$COMM$;
COMMENT ON COLUMN ais.recond.reinstrate IS $COMM$Ставка одного полного восстановления ответственности$COMM$;
COMMENT ON COLUMN ais.recond.reinstprorata IS $COMM$Индикатор рассчета суммы восстановления pro rata as to time$COMM$;
COMMENT ON COLUMN ais.recond.brokerage IS $COMM$Брокерская комиссия $COMM$;
COMMENT ON COLUMN ais.recond.recommiss IS $COMM$Перестраховочная комиссия с собственных договоров в пользу цедента с базовой
премии.$COMM$;
COMMENT ON COLUMN ais.recond.ovrcommiss IS $COMM$Комиссия с ретроцессии в пользу цедента с базовой премии. Обычно меньше, чем
перестраховочная комиссия.$COMM$;
COMMENT ON COLUMN ais.recond.profitcommiss IS $COMM$Тантьема - комиссия с прибыли перестраховщика в пользу цедента. Берется с нетто
премии после вычета всех расходов перестраховщика.$COMM$;
COMMENT ON COLUMN ais.recond.bonus IS $COMM$Бонус () - возврат премии цеденту при безубытосном прохождении договора$COMM$;
COMMENT ON COLUMN ais.recond.deficityears IS $COMM$Максимальное число лет переноса дефицита на следующий год при расчете тантьемы$COMM$;
COMMENT ON COLUMN ais.recond.cashloss IS $COMM$Кассовый убыток - сумма убытка, начиная с которой убыток оплачивается
перестраховщиком немедленно$COMM$;
COMMENT ON COLUMN ais.recond.premreserve IS $COMM$Резерв премии (), удерживаемый цедентом, на который начисляется  на депо премии$COMM$;
COMMENT ON COLUMN ais.recond.premrelperiod IS $COMM$Периодичность освобождения резерва премии (мес.)$COMM$;
COMMENT ON COLUMN ais.recond.lossreserve IS $COMM$Резерв убытков (), удерживаемый цедентом, на который начисляется  на депо
убытков$COMM$;
COMMENT ON COLUMN ais.recond.lossrelperiod IS $COMM$Периодичность освобождения резерва убытков (мес.)$COMM$;
COMMENT ON COLUMN ais.recond.preminterest IS $COMM$ на депо премии в пользу перестраховщика$COMM$;
COMMENT ON COLUMN ais.recond.lossinterest IS $COMM$ на депо убытков в пользу перестраховщика$COMM$;
COMMENT ON COLUMN ais.recond.premportf IS $COMM$Портфель премии в  от предыдущего года$COMM$;
COMMENT ON COLUMN ais.recond.lossportf IS $COMM$Портфель убытков в  от предыдущего года$COMM$;
COMMENT ON COLUMN ais.recond.premtype IS $COMM$Тип премии: F-flat (фиксированная), M-min.deposit (минимальная депозитная, с
перерасчетом в сторону увеличения по ставке перерасчета), D-deposit (депозитная, с перерасчетом в обе стороны)$COMM$;
COMMENT ON COLUMN ais.recond.epi IS $COMM$Предполагаемая премия - Estimated Premium Income$COMM$;
COMMENT ON COLUMN ais.recond.nextinvoicedate IS $COMM$Следующая дата выставления счетов$COMM$;
COMMENT ON COLUMN ais.recond.paymentnum IS $COMM$Количество платежей в рассрочку равными долями$COMM$;
COMMENT ON COLUMN ais.recond.franchsum IS $COMM$Франшиза$COMM$;
COMMENT ON COLUMN ais.recond.sharepc IS $COMM$Доля участия в $COMM$;
COMMENT ON COLUMN ais.recond.taxrate IS $COMM$Ставка налога ()$COMM$;
COMMENT ON COLUMN ais.recond.expences IS $COMM$Расходы на ведение дела$COMM$;
COMMENT ON COLUMN ais.recond.rpm IS $COMM$Резерв предупредительных мероприятий ()  (доля перестраховщиков)$COMM$;
COMMENT ON COLUMN ais.recond.fpm IS $COMM$Отчисления в фонд противопожарных мероприятий (доля перестраховщиков)$COMM$;
COMMENT ON COLUMN ais.recond.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.recond.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.recond.profitexpense IS $COMM$Норма накладных расходов, вычитаемых из прибыли при расчете тантьемы$COMM$;
COMMENT ON COLUMN ais.recond.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.recond.accountperiod IS $COMM$Периодичность выставления счетов M -помесячно, Q - поквартально, H-полгода, Y - год$COMM$;
COMMENT ON COLUMN ais.recond.accountterm IS $COMM$Срок выставления счета в днях по окончании расчетного периода$COMM$;
COMMENT ON COLUMN ais.recond.noticeterm IS $COMM$Срок оформления нотиса в днях до окончания периода действия договора (секции)$COMM$;


CREATE TABLE ais.regress (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    currisn                          NUMERIC,
    emplisn                          NUMERIC,
    deptisn                          NUMERIC,
    agentisn                         NUMERIC,
    faultisn                         NUMERIC,
    id                               VARCHAR(20),
    role                             VARCHAR(1),
    dateopen                         TIMESTAMP,
    dateclose                        TIMESTAMP,
    demandsum                        NUMERIC(20,2),
    acceptdate                       TIMESTAMP,
    acceptsum                        NUMERIC(20,2),
    actiondate                       TIMESTAMP,
    actionsum                        NUMERIC(20,2),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    defendantisn                     NUMERIC,
    sharepc                          NUMERIC(9,6),
    claimisn                         NUMERIC,
    stateisn                         NUMERIC,
    reasonisn                        NUMERIC,
    classisn                         NUMERIC,
    ruleisn                          NUMERIC,
    refundisn                        NUMERIC,
    closereasonisn                   NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.regress IS $COMM$Регресс$COMM$;
COMMENT ON COLUMN ais.regress.refundisn IS $COMM$Ссылка на претензию (Smirnov 24/04/09)$COMM$;
COMMENT ON COLUMN ais.regress.closereasonisn IS $COMM$FK(DICTI) Причина закрытия дела (Smirnov 16/06/09)$COMM$;
COMMENT ON COLUMN ais.regress.ruleisn IS $COMM$Бизнес (правило страхования, c.get('Rule')) (Smirnov 06/06/06)$COMM$;
COMMENT ON COLUMN ais.regress.classisn IS $COMM$FK(DICTI). Ссылка на класс документа.  -- 31.08.05 SR$COMM$;
COMMENT ON COLUMN ais.regress.agrisn IS $COMM$FK(AGREEMENT). Указатель договора$COMM$;
COMMENT ON COLUMN ais.regress.currisn IS $COMM$FK(CURRENCY). Указатель валюты регресса$COMM$;
COMMENT ON COLUMN ais.regress.emplisn IS $COMM$FK(SUBJECT). Указатель сотрудника - исполнителя$COMM$;
COMMENT ON COLUMN ais.regress.deptisn IS $COMM$FK(SUBDEPT). Указатель отдела сотрудника - исполнителя$COMM$;
COMMENT ON COLUMN ais.regress.agentisn IS $COMM$FK(SUBJECT). Указатель регрессного агента$COMM$;
COMMENT ON COLUMN ais.regress.faultisn IS $COMM$FK(SUBJECT). Указатель виновного лица$COMM$;
COMMENT ON COLUMN ais.regress.id IS $COMM$Номер регресса, обычно совпадает с номером убытка$COMM$;
COMMENT ON COLUMN ais.regress.role IS $COMM$Роль ИГС по отношению к регрессу: D-истец, R-ответчик$COMM$;
COMMENT ON COLUMN ais.regress.dateopen IS $COMM$Дата открытия дела$COMM$;
COMMENT ON COLUMN ais.regress.dateclose IS $COMM$Дата закрытия дела$COMM$;
COMMENT ON COLUMN ais.regress.demandsum IS $COMM$Сумма претензии$COMM$;
COMMENT ON COLUMN ais.regress.acceptdate IS $COMM$Дата урегулирования$COMM$;
COMMENT ON COLUMN ais.regress.acceptsum IS $COMM$Сумма урегулирования$COMM$;
COMMENT ON COLUMN ais.regress.actiondate IS $COMM$Дата иска$COMM$;
COMMENT ON COLUMN ais.regress.actionsum IS $COMM$Сумма иска$COMM$;
COMMENT ON COLUMN ais.regress.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.regress.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.regress.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.regress.defendantisn IS $COMM$FK(SUBJECT). Ответчик в суде$COMM$;
COMMENT ON COLUMN ais.regress.sharepc IS $COMM$Доля Ингосстраха ( в общей сумме иска)$COMM$;
COMMENT ON COLUMN ais.regress.claimisn IS $COMM$FK(AGRCLAIM). Указатель паспорта убытка$COMM$;
COMMENT ON COLUMN ais.regress.stateisn IS $COMM$FK( DICTI) передано в суд, получены деньги и т.д.$COMM$;
COMMENT ON COLUMN ais.regress.reasonisn IS $COMM$FK(DICTI) Причина регресса$COMM$;


CREATE TABLE ais.regressitem_t (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    roleisn                          NUMERIC,
    courtisn                         NUMERIC,
    regressisn                       NUMERIC,
    currisn                          NUMERIC,
    dateevent                        TIMESTAMP,
    id                               VARCHAR(20),
    amount                           NUMERIC(20,2),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    datesend                         TIMESTAMP,
    amount2                          NUMERIC(20,2),
    extid                            VARCHAR(30),
    amountmd                         NUMERIC(20,2),
    caseid                           VARCHAR(30),
    amountd                          NUMERIC(20,2),
    amountrepr                       NUMERIC(20,2),
    amountdelay                      NUMERIC(20,2),
    amountother                      NUMERIC(20,2),
    status                           VARCHAR(1),
    previsn                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.regressitem_t.amountrepr IS $COMM$Представительские расходы (Smirnov 30/04/10)$COMM$;
COMMENT ON COLUMN ais.regressitem_t.amountdelay IS $COMM$Сумма процента просрочки (Smirnov 30/04/10)$COMM$;
COMMENT ON COLUMN ais.regressitem_t.amountother IS $COMM$Прочие суммы (Smirnov 30/04/10)$COMM$;
COMMENT ON COLUMN ais.regressitem_t.previsn IS $COMM$FK(Regressitem_t.isn) Ссылка на предшествующий документ. Michurin 29.09.2011$COMM$;
COMMENT ON COLUMN ais.regressitem_t.amountmd IS $COMM$Сумма морального вреда (Smirnov 04/09/07)$COMM$;
COMMENT ON COLUMN ais.regressitem_t.amountd IS $COMM$Сумма вреда (Smirnov 30/04/10)$COMM$;
COMMENT ON COLUMN ais.regressitem_t.caseid IS $COMM$Номер судебного дела (Smirnov 16/06/09)$COMM$;
COMMENT ON COLUMN ais.regressitem_t.isn IS $COMM$Машинный номер: SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.regressitem_t.classisn IS $COMM$Машинный номер объекта.
Устанавливается по умолчанию равным SEQ_DICTI.nextval.
Совпадает с ISN соответствующего объекта, имеющего отдельную таблицу для хранения дополнительных полей.$COMM$;
COMMENT ON COLUMN ais.regressitem_t.roleisn IS $COMM$Машинный номер объекта.
Устанавливается по умолчанию равным SEQ_DICTI.nextval.
Совпадает с ISN соответствующего объекта, имеющего отдельную таблицу для хранения дополнительных полей.$COMM$;
COMMENT ON COLUMN ais.regressitem_t.courtisn IS $COMM$FK(DICTI,СУБЪЕКТ). Машинный номер объекта, совпадает с ISN соответствующей
записи в словаре DICTI.$COMM$;
COMMENT ON COLUMN ais.regressitem_t.currisn IS $COMM$FK(DICTI,ВАЛЮТА). Машинный номер объекта, совпадает с ISN соответствующей записи
в словаре DICTI.$COMM$;
COMMENT ON COLUMN ais.regressitem_t.dateevent IS $COMM$Дата составления документа (иска)$COMM$;
COMMENT ON COLUMN ais.regressitem_t.id IS $COMM$Юридический номер документа$COMM$;
COMMENT ON COLUMN ais.regressitem_t.amount IS $COMM$Сумма иска(пошлины ...)$COMM$;
COMMENT ON COLUMN ais.regressitem_t.datesend IS $COMM$Дата отправки документа получателю ( иска в суд)$COMM$;
COMMENT ON COLUMN ais.regressitem_t.amount2 IS $COMM$Сумма компенсации госпошлины$COMM$;
COMMENT ON COLUMN ais.regressitem_t.extid IS $COMM$Номер исполнительного листа$COMM$;


CREATE TABLE ais.regressitemrole (
    isn                              NUMERIC,
    itemisn                          NUMERIC,
    roleisn                          NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE ais.regressrole (
    isn                              NUMERIC,
    regressisn                       NUMERIC,
    groupno                          NUMERIC,
    classisn                         NUMERIC,
    subjisn                          NUMERIC,
    subjname                         VARCHAR(255),
    sharepc                          NUMERIC(9,6),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    policyid                         VARCHAR(20)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.regressrole.policyid IS $COMM$Полис ОСАГО ответчика (Smirnov 14/08/06, задача 2349715403)$COMM$;
COMMENT ON COLUMN ais.regressrole.datebeg IS $COMM$Дата начала стадии, в которой задействован участник делопроизводства$COMM$;
COMMENT ON COLUMN ais.regressrole.dateend IS $COMM$Дата окончания стадии, в которой задействован участник делопроизводства$COMM$;
COMMENT ON COLUMN ais.regressrole.classisn IS $COMM$Тип лица - ответчик и т.д.$COMM$;
COMMENT ON COLUMN ais.regressrole.sharepc IS $COMM$Какая-то доля (обычно агентское вознаграждение)$COMM$;


CREATE TABLE ais.rep_fd_body (
    isn                              NUMERIC,
    structisn                        NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    amountrub                        NUMERIC,
    amountusd                        NUMERIC,
    filterisn                        NUMERIC,
    filterisntwo                     NUMERIC,
    subkindisn                       NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    status                           VARCHAR(1),
    flg                              VARCHAR(1),
    classisn                         NUMERIC,
    provcount                        NUMERIC,
    amountrubcurr                    NUMERIC,
    filterisn3                       NUMERIC,
    comissisn                        NUMERIC,
    bodyisn                          NUMERIC,
    agrisn                           NUMERIC,
    reprisn                          NUMERIC,
    bizisn                           NUMERIC,
    ruleisn                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.rep_fd_body IS $COMM$Есаулов, Новоселова. Тело (и мясо) отчета "по бизнесу" ФД. Подготовленные по заданным алгоритмам данные этого отчета$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.isn IS $COMM$PK$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.structisn IS $COMM$FK (REP_FD_STRUCTURE). Указатель на строку отчета$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.datebeg IS $COMM$Начало периода, к которому относятся данные$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.dateend IS $COMM$Конец периода, к которому относятся данные$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.amountrub IS $COMM$Сумма (руб.)$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.amountusd IS $COMM$Сумма (USD)$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.filterisn IS $COMM$FK - ISN объекта фильтра, по которому формируется сумма$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.filterisntwo IS $COMM$FK - ISN объекта дополнительного фильтра, по которому формируется сумма (например, учетное подразделение)$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.subkindisn IS $COMM$FK(KindAcc) - ссылка на аналитический счет$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.updated IS $COMM$Дата изменения (формирования отчета)$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.updatedby IS $COMM$Автор изменений (формирования отчета)$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.status IS $COMM$Статус загрузки (по умолчанию 'W' - черновая загрузка; 'Y' - чистовая загрузка)$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.flg IS $COMM$Специальный флаг для технологических отметок$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.classisn IS $COMM$вид отчета$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.provcount IS $COMM$количество проводок в сумме$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.amountrubcurr IS $COMM$сумма в рублях (проводки в ин. валюте переводятся в рубли по курсу на дату)$COMM$;
COMMENT ON COLUMN ais.rep_fd_body.filterisn3 IS $COMM$FK - ISN объекта дополнительного фильтра, по которому формируется сумма$COMM$;


CREATE TABLE ais.rep_fd_status (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    status                           VARCHAR(1),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.rep_fd_status IS $COMM$Таблица для хранения статуса загрузки отчета$COMM$;
COMMENT ON COLUMN ais.rep_fd_status.classisn IS $COMM$Тип отчета$COMM$;
COMMENT ON COLUMN ais.rep_fd_status.datebeg IS $COMM$Дата начала периода расчета$COMM$;
COMMENT ON COLUMN ais.rep_fd_status.dateend IS $COMM$Дата окончания периода расчета$COMM$;
COMMENT ON COLUMN ais.rep_fd_status.status IS $COMM$Статус загрузки (W - черновая загрузка, Y - чистовая загрузка)$COMM$;
COMMENT ON COLUMN ais.rep_fd_status.updated IS $COMM$дата расчета$COMM$;
COMMENT ON COLUMN ais.rep_fd_status.updatedby IS $COMM$кем расчитано$COMM$;


CREATE TABLE ais.rep_fd_struct_filter (
    isn                              NUMERIC,
    seq                              NUMERIC,
    nn                               NUMERIC,
    name                             VARCHAR(500),
    checktotal                       NUMERIC(38),
    classisn                         NUMERIC,
    filterisn                        NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    structisn                        NUMERIC,
    ostprice                         NUMERIC,
    k1                               NUMERIC,
    emplcount                        NUMERIC,
    k2                               NUMERIC,
    koef                             NUMERIC,
    checkembed                       NUMERIC,
    checkvariant                     NUMERIC,
    pp                               VARCHAR(10),
    filterparentisn                  NUMERIC,
    parentisn                        NUMERIC,
    updatedby                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    filterprevisn                    NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.rep_fd_struct_filter IS $COMM$Структура фильтра, в разбивке по которому суммы выводятся в отчет$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.datebeg IS $COMM$Дата начала участия фильтра в отчете$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.dateend IS $COMM$Дата окончания участия фильтра в отчете$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.structisn IS $COMM$FK (Rep_fd_structure) Ссылка на строку отчета (при необходимости, например в отчете о страх.премии по филиалам на базе дочек)$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.ostprice IS $COMM$Остаточная ст-ть (для расчета налога на прибыль )$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.k1 IS $COMM$Удельный вес филиала в общей остаточной ст-ти (для расчета налога на прибыль)$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.seq IS $COMM$последовательность вывода в отчет$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.nn IS $COMM$порядковый номер$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.name IS $COMM$название$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.checktotal IS $COMM$Признак участия в итогах (1 - не участвует в итогах)$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.classisn IS $COMM$FK(Dicti) Ссылка на вид отчета$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.filterisn IS $COMM$FK - ссылка на объект фильтра (например, на subdept)$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.emplcount IS $COMM$Среднесписочная численность (для расчета налога на прибыль)$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.k2 IS $COMM$Удельный вес численности  (для расчета налога на прибыль)$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.koef IS $COMM$Средний удельный вес численности - коэффициент расчета  (для расчета налога на прибыль)$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.checkembed IS $COMM$Признак участия вложенности: 0 - все вложенные филиалы, 1 - без вложенных филиалов, 2 - все вложенные, кроме дочек$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.checkvariant IS $COMM$номер варианта структуры отчета (для разных структур отчетов, действующих одновременно. Отчеты по одному хранилищу)$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.pp IS $COMM$строковый порядковый номер$COMM$;
COMMENT ON COLUMN ais.rep_fd_struct_filter.updatedby IS $COMM$@purpose=FK(SUBHUMAN). Автор изменения$COMM$;


CREATE TABLE ais.resection (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    id                               VARCHAR(20),
    secttype                         VARCHAR(2),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    rate                             NUMERIC(9,6),
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    classisn                         NUMERIC,
    currisn                          NUMERIC,
    premiumtype                      VARCHAR(2),
    limiteverymode                   VARCHAR(1),
    name                             VARCHAR(255),
    optionalcode                     VARCHAR(1),
    orderno                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.resection IS $COMM$Секция договора перестрахования. Один договор может содержать несколько секций, отличающихся
условиями, напимер: XL/SP, резиденты/нерезиденты, самолеты Аэрофлота.$COMM$;
COMMENT ON COLUMN ais.resection.isn IS $COMM$Машинный номер секции, SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.resection.agrisn IS $COMM$FK(AGREEMENT). Указатель договора перестрахования$COMM$;
COMMENT ON COLUMN ais.resection.id IS $COMM$Идентификатор секции: A,B,C...$COMM$;
COMMENT ON COLUMN ais.resection.secttype IS $COMM$Тип секции: XL-excess of lost, SP-surplus, QS-quota share, SL-stop loss$COMM$;
COMMENT ON COLUMN ais.resection.datebeg IS $COMM$Дата начала действия секции$COMM$;
COMMENT ON COLUMN ais.resection.dateend IS $COMM$Дата окончания действия секции$COMM$;
COMMENT ON COLUMN ais.resection.rate IS $COMM$Ставка премии  в процентах$COMM$;
COMMENT ON COLUMN ais.resection.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.resection.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.resection.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.resection.classisn IS $COMM$FK(DICTI). Указатель вида страхования.$COMM$;
COMMENT ON COLUMN ais.resection.currisn IS $COMM$FK(CURRENCY). Указатель валюты секции.$COMM$;
COMMENT ON COLUMN ais.resection.premiumtype IS $COMM$Тип премии: [Gross][Net]$COMM$;
COMMENT ON COLUMN ais.resection.limiteverymode IS $COMM$Способ расчета лимита ответственности:
Y-по лимиту по каждому (LimitEverySum)
null-по полному лимиту (LimitSum)$COMM$;
COMMENT ON COLUMN ais.resection.name IS $COMM$Название секции в договоре$COMM$;
COMMENT ON COLUMN ais.resection.optionalcode IS $COMM$дополнительные условия договора$COMM$;


CREATE TABLE ais.resubjperiod (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    sectisn                          NUMERIC,
    condisn                          NUMERIC,
    agrisn                           NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    sharepc                          NUMERIC(9,6),
    recommiss                        NUMERIC(9,6),
    ovrcommiss                       NUMERIC(9,6),
    profitcommiss                    NUMERIC(9,6),
    cashloss                         NUMERIC(20,2),
    depospremsum                     NUMERIC(20,2),
    bonus                            NUMERIC(9,6),
    deficityears                     NUMERIC(38),
    premreserve                      NUMERIC(9,6),
    premrelperiod                    NUMERIC(38),
    lossreserve                      NUMERIC(9,6),
    lossrelperiod                    NUMERIC(38),
    preminterest                     NUMERIC(9,6),
    lossinterest                     NUMERIC(9,6),
    premportf                        NUMERIC(9,6),
    lossportf                        NUMERIC(9,6),
    premtype                         VARCHAR(1),
    nextinvoicedate                  TIMESTAMP,
    paymentnum                       NUMERIC(38),
    franchsum                        NUMERIC(20,2),
    taxrate                          NUMERIC(9,6),
    expences                         NUMERIC(9,6),
    rpm                              NUMERIC(9,6),
    fpm                              NUMERIC(9,6),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    id                               VARCHAR(20),
    profitexpense                    NUMERIC(9,6),
    parentisn                        NUMERIC,
    brokerage                        NUMERIC(9,6),
    datesign                         TIMESTAMP,
    subscription                     VARCHAR(1),
    dateswitch                       TIMESTAMP,
    dateentry                        TIMESTAMP
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.resubjperiod IS $COMM$Участники договора перестрахования с долями участия. Могут иметь иерархию при работе
перестраховщиков через брокера.$COMM$;
COMMENT ON COLUMN ais.resubjperiod.dateswitch IS $COMM$Дата коммутации. Дата с которой прекращаются все отношения с перестраховщиком, включая урегулирование убытков$COMM$;
COMMENT ON COLUMN ais.resubjperiod.dateentry IS $COMM$Дата входа участников договоров исходящего перестрахования при расчете страховых резервов$COMM$;
COMMENT ON COLUMN ais.resubjperiod.subscription IS $COMM$Признак получения подписного листа, Y - получен, N - не получен$COMM$;
COMMENT ON COLUMN ais.resubjperiod.datesign IS $COMM$Дата подписания документа$COMM$;
COMMENT ON COLUMN ais.resubjperiod.isn IS $COMM$Машинный номер, SEQ_AGREEMENT.nextval$COMM$;
COMMENT ON COLUMN ais.resubjperiod.subjisn IS $COMM$FK(SUBJECT). Указатель участника договора перестрахования$COMM$;
COMMENT ON COLUMN ais.resubjperiod.sectisn IS $COMM$FK(RESECTION). Указатель секции договора перестрахования, наследуется из
условия$COMM$;
COMMENT ON COLUMN ais.resubjperiod.condisn IS $COMM$FK(RECOND). Указатель условия периода договора перестрахования$COMM$;
COMMENT ON COLUMN ais.resubjperiod.agrisn IS $COMM$FK(AGREEMENT). Указатель формуляра договора перестрахования, наследуется из
условия$COMM$;
COMMENT ON COLUMN ais.resubjperiod.datebeg IS $COMM$Дата начала$COMM$;
COMMENT ON COLUMN ais.resubjperiod.dateend IS $COMM$Дата окончания$COMM$;
COMMENT ON COLUMN ais.resubjperiod.sharepc IS $COMM$Доля участия в %$COMM$;
COMMENT ON COLUMN ais.resubjperiod.recommiss IS $COMM$Перестраховочная комиссия с собственных договоров в пользу цедента с базовой
премии.$COMM$;
COMMENT ON COLUMN ais.resubjperiod.ovrcommiss IS $COMM$Комиссия с ретроцессии в пользу цедента с базовой премии. Обычно меньше, чем
перестраховочная комиссия.$COMM$;
COMMENT ON COLUMN ais.resubjperiod.profitcommiss IS $COMM$Тантьема - комиссия с прибыли перестраховщика в пользу цедента. Берется с
нетто премии после вычета всех расходов перестраховщика.$COMM$;
COMMENT ON COLUMN ais.resubjperiod.cashloss IS $COMM$Кассовый убыток - сумма убытка, начиная с которой убыток оплачивается
перестраховщиком немедленно$COMM$;
COMMENT ON COLUMN ais.resubjperiod.depospremsum IS $COMM$Сумма депозитной премии$COMM$;
COMMENT ON COLUMN ais.resubjperiod.bonus IS $COMM$Бонус (%) - возврат премии цеденту при безубытосном прохождении договора$COMM$;
COMMENT ON COLUMN ais.resubjperiod.deficityears IS $COMM$Максимальное число лет переноса дефицита на следующий год при расчете
тантьемы$COMM$;
COMMENT ON COLUMN ais.resubjperiod.premreserve IS $COMM$Резерв премии (%), удерживаемый цедентом, на который начисляется % на депо
премии$COMM$;
COMMENT ON COLUMN ais.resubjperiod.premrelperiod IS $COMM$Периодичность освобождения резерва премии (мес.)$COMM$;
COMMENT ON COLUMN ais.resubjperiod.lossreserve IS $COMM$Резерв убытков (%), удерживаемый цедентом, на который начисляется % на депо
убытков$COMM$;
COMMENT ON COLUMN ais.resubjperiod.lossrelperiod IS $COMM$Периодичность освобождения резерва убытков (мес.)$COMM$;
COMMENT ON COLUMN ais.resubjperiod.preminterest IS $COMM$% на депо премии в пользу перестраховщика$COMM$;
COMMENT ON COLUMN ais.resubjperiod.lossinterest IS $COMM$% на депо убытков в пользу перестраховщика$COMM$;
COMMENT ON COLUMN ais.resubjperiod.premportf IS $COMM$Портфель премии в % от предыдущего года$COMM$;
COMMENT ON COLUMN ais.resubjperiod.lossportf IS $COMM$Портфель убытков в % от предыдущего года$COMM$;
COMMENT ON COLUMN ais.resubjperiod.premtype IS $COMM$Тип премии: F-flat (фиксированная), M-min.deposit (минимальная депозитная, с
перерасчетом в сторону увеличения по ставке перерасчета), D-deposit (депозитная, с перерасчетом в обе стороны)$COMM$;
COMMENT ON COLUMN ais.resubjperiod.nextinvoicedate IS $COMM$Следующая дата выставления счетов$COMM$;
COMMENT ON COLUMN ais.resubjperiod.paymentnum IS $COMM$Количество платежей в рассрочку равными долями$COMM$;
COMMENT ON COLUMN ais.resubjperiod.franchsum IS $COMM$Франшиза$COMM$;
COMMENT ON COLUMN ais.resubjperiod.taxrate IS $COMM$Ставка налога (%)$COMM$;
COMMENT ON COLUMN ais.resubjperiod.expences IS $COMM$Расходы на ведение дела$COMM$;
COMMENT ON COLUMN ais.resubjperiod.rpm IS $COMM$Резерв предупредительных мероприятий (%)  (доля перестраховщиков)$COMM$;
COMMENT ON COLUMN ais.resubjperiod.fpm IS $COMM$Отчисления в фонд противопожарных мероприятий (доля перестраховщиков)$COMM$;
COMMENT ON COLUMN ais.resubjperiod.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.resubjperiod.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.resubjperiod.id IS $COMM$Номер документа$COMM$;
COMMENT ON COLUMN ais.resubjperiod.profitexpense IS $COMM$Норма накладных расходов, вычитаемых из прибыли при расчете тантьемы$COMM$;
COMMENT ON COLUMN ais.resubjperiod.parentisn IS $COMM$FK(RESUBJPERIOD). Указатель ведущего участника: при перестраховании через
брокера можно перечислять его перестраховщиков$COMM$;
COMMENT ON COLUMN ais.resubjperiod.brokerage IS $COMM$Ставка брокерской комиссии$COMM$;


CREATE TABLE ais.rptform (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    deptisn                          NUMERIC,
    parentisn                        NUMERIC,
    rowparentisn                     NUMERIC,
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(255),
    sqlpattern                       VARCHAR(4000),
    imagefile                        VARCHAR(128),
    codeshift                        NUMERIC(38),
    timing                           NUMERIC,
    discr                            VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    emplisn                          NUMERIC,
    vctemplate                       TEXT,
    formdiscr                        VARCHAR(12),
    repdescr                         VARCHAR(2000),
    template                         BYTEA,
    formisn                          NUMERIC,
    async                            SMALLINT,
    flexclassisn                     NUMERIC,
    xml                              TEXT
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.rptform IS $COMM$Иерархический справочник отчетных форм.
Потомки наследуют реквизиты родителей (файл образа, SQL-фильтр, строки и колонки).
На печать выдаются отчеты, имеющие дискриминатор Y.$COMM$;
COMMENT ON COLUMN ais.rptform.template IS $COMM$Заголовок отчетной формы, содержщий всю необходимую для создания отчета информацию
в формате электронной таблицы, хранится в BLOB, поле VCTemplate больше не используется (Романов К. 29-10-04)$COMM$;
COMMENT ON COLUMN ais.rptform.async IS $COMM$Признак асинхронного исполнения отчета$COMM$;
COMMENT ON COLUMN ais.rptform.formisn IS $COMM$Тип формы из какой вызывать$COMM$;
COMMENT ON COLUMN ais.rptform.isn IS $COMM$FK(DICTI). Уникальный машинный номер SEQ_DICTI.nextval$COMM$;
COMMENT ON COLUMN ais.rptform.classisn IS $COMM$FK(DICTI). Класс строки формы, ограничивает множество ClassISN для строк формы
потомками заданного узла.$COMM$;
COMMENT ON COLUMN ais.rptform.deptisn IS $COMM$FK(SUBDEPT). Подразделение, к которому относится отчет.$COMM$;
COMMENT ON COLUMN ais.rptform.parentisn IS $COMM$FK(RPTFORM). Указатель родителькой записи, из которой наследуются колонки отчета и
другие реквизиты.$COMM$;
COMMENT ON COLUMN ais.rptform.rowparentisn IS $COMM$FK(RPTFORM). Указатель родителькой записи, из которой наследуются строки
отчета.$COMM$;
COMMENT ON COLUMN ais.rptform.shortname IS $COMM$Краткое наименование формы: Форма 1. Помещается в правый верхний угол отчета.$COMM$;
COMMENT ON COLUMN ais.rptform.fullname IS $COMM$Полное наименование формы, помещается в заголовок.$COMM$;
COMMENT ON COLUMN ais.rptform.sqlpattern IS $COMM$SQL-шаблон, задающий условие отбора записей в отчет: where ...
Подставляется от потомка к родителю:
родитель: FROM DOCSUM s, DICTI d WHERE % ORDER BY d.Code
потомок:  s.Amount>0 AND d.ISN=123
результат: FROM DOCSUM s, DICTI d WHERE s.Amount>0 AND d.ISN=123 ORDER BY d.Code$COMM$;
COMMENT ON COLUMN ais.rptform.imagefile IS $COMM$Файл-образ отчетной формы в формате электронной таблицы *.vts$COMM$;
COMMENT ON COLUMN ais.rptform.codeshift IS $COMM$Сдвиг нумерации наследуемых строк отчета при сквозной нумерации в рамках группы
отчетов.
Если null - берется исходная нумерация.$COMM$;
COMMENT ON COLUMN ais.rptform.timing IS $COMM$Длительность запроса в сек.$COMM$;
COMMENT ON COLUMN ais.rptform.discr IS $COMM$Дискриминатор: U-форма для управлений, O- для отделов$COMM$;
COMMENT ON COLUMN ais.rptform.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.rptform.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.rptform.emplisn IS $COMM$FK(SUBHUMAN). Владелец отчета (для отчетов, созданных пользователем)$COMM$;
COMMENT ON COLUMN ais.rptform.vctemplate IS $COMM$Заголовок отчетной формы, содержщий всю необходимую для создания отчета информацию
в формате электронной таблицы$COMM$;
COMMENT ON COLUMN ais.rptform.repdescr IS $COMM$Описание отчета$COMM$;


CREATE TABLE ais.rulbranch (
    riskisn                          NUMERIC,
    objisn                           NUMERIC,
    branchisn                        NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    parentriskisn                    NUMERIC,
    parentobjisn                     NUMERIC
)
DISTRIBUTED BY (riskisn,objisn);

COMMENT ON TABLE ais.rulbranch IS $COMM$Задает отношение объект-риск-вид страхования по классификации Росстрахнадзора для
группировки условий страхования$COMM$;
COMMENT ON COLUMN ais.rulbranch.riskisn IS $COMM$FK(DICTI). Указатель класса риска$COMM$;
COMMENT ON COLUMN ais.rulbranch.objisn IS $COMM$FK(DICTI). Указатель класса объекта$COMM$;
COMMENT ON COLUMN ais.rulbranch.branchisn IS $COMM$FK(DICTI). Указатель вида страхования$COMM$;
COMMENT ON COLUMN ais.rulbranch.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.rulbranch.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.rulbranch.parentriskisn IS $COMM$FK(DICTI). Указатель родительского класса риска, поддерживается
автоматически$COMM$;
COMMENT ON COLUMN ais.rulbranch.parentobjisn IS $COMM$FK(DICTI). Указатель родительского класса объекта, поддерживается
автоматически$COMM$;


CREATE TABLE ais.rultariff (
    isn                              NUMERIC DEFAULT NULL,
    tariffisn                        NUMERIC,
    basetariff                       NUMERIC(26,12),
    adjustment                       NUMERIC(9,6),
    discount                         NUMERIC(9,6),
    tariff                           NUMERIC(26,12),
    franchtariff                     NUMERIC(9,6),
    franchsum                        NUMERIC(20,2),
    franchmaxsum                     NUMERIC(20,2),
    roundm                           NUMERIC(38),
    objclassisn                      NUMERIC,
    riskclassisn                     NUMERIC,
    x1                               NUMERIC,
    x2                               NUMERIC,
    x3                               NUMERIC,
    x4                               NUMERIC,
    x5                               NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    limclassisn                      NUMERIC,
    franchtype                       VARCHAR(1),
    currisn                          NUMERIC,
    subjisn                          NUMERIC,
    taskisn                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.rultariff.taskisn IS $COMM$FK(QTASK). Указатель на задачу. Kudzoev Z.F. 15/01/2007$COMM$;
COMMENT ON COLUMN ais.rultariff.isn IS $COMM$Машинный номер записи: SEQ_RULTARIFF.nextval$COMM$;
COMMENT ON COLUMN ais.rultariff.tariffisn IS $COMM$FK(DICTI). Указатель типа тарифа, определяемого по виду страхования и страховому
продукту$COMM$;
COMMENT ON COLUMN ais.rultariff.basetariff IS $COMM$Базовая тарифная ставка в %%$COMM$;
COMMENT ON COLUMN ais.rultariff.adjustment IS $COMM$Поправочный коэффициент, на который умножается базовый тариф$COMM$;
COMMENT ON COLUMN ais.rultariff.discount IS $COMM$Скидка/надбавка в %%$COMM$;
COMMENT ON COLUMN ais.rultariff.tariff IS $COMM$Тарифная ставка в %%, по умолчанию: BaseTariff * Adjustment * (1+Discount/100), но
может устанавливаться вручную.$COMM$;
COMMENT ON COLUMN ais.rultariff.franchtariff IS $COMM$Ставка франшизы в %%$COMM$;
COMMENT ON COLUMN ais.rultariff.franchsum IS $COMM$Сумма франшизы абсолютная или минимальная, если задана максимальная$COMM$;
COMMENT ON COLUMN ais.rultariff.franchmaxsum IS $COMM$Максимальная франшиза$COMM$;
COMMENT ON COLUMN ais.rultariff.roundm IS $COMM$Масштаб округления тарифа, соответствует параметру m ф-ии ROUND(n,m)$COMM$;
COMMENT ON COLUMN ais.rultariff.objclassisn IS $COMM$FK(DICTI). Указатель класса объекта, одного из разрешенных в правиле
страхования$COMM$;
COMMENT ON COLUMN ais.rultariff.riskclassisn IS $COMM$FK(DICTI). Указатель класса риска, одного из разрешенных в правиле
страхования$COMM$;
COMMENT ON COLUMN ais.rultariff.x1 IS $COMM$Параметры тарифов. Интерпретация зависит от типа тарифа$COMM$;
COMMENT ON COLUMN ais.rultariff.datebeg IS $COMM$Дата начала действия$COMM$;
COMMENT ON COLUMN ais.rultariff.dateend IS $COMM$Дата окончания действия$COMM$;
COMMENT ON COLUMN ais.rultariff.remark IS $COMM$Наименование тарифа или примечание относительно особенностей использования данного
тарифа$COMM$;
COMMENT ON COLUMN ais.rultariff.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.rultariff.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.rultariff.limclassisn IS $COMM$FK(DICTI). Указатель класса ограничения$COMM$;
COMMENT ON COLUMN ais.rultariff.franchtype IS $COMM$Тип франшизы: У-условная, Б-безусловная$COMM$;
COMMENT ON COLUMN ais.rultariff.currisn IS $COMM$FK(DICTI). Указатель валюты тарифа, должен быть задан для абсолютного тарифа$COMM$;
COMMENT ON COLUMN ais.rultariff.subjisn IS $COMM$FK(SUBJECT). Указатель на филиал. Yunin V.A. 30/09/04$COMM$;


CREATE TABLE ais.smsitem (
    isn                              NUMERIC,
    messageisn                       NUMERIC,
    subjisn                          NUMERIC,
    phone                            VARCHAR(50),
    response                         TEXT,
    status                           VARCHAR(1),
    code                             NUMERIC,
    date2sql                         TIMESTAMP,
    datesend                         TIMESTAMP,
    datefromsql                      TIMESTAMP,
    createdby                        NUMERIC,
    created                          TIMESTAMP,
    phoneisn                         NUMERIC,
    item_messagetext                 VARCHAR(2000),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    objisn                           NUMERIC,
    objdiscr                         VARCHAR(1),
    objid                            VARCHAR(100)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.smsitem IS $COMM$Сообщение для субъекта$COMM$;
COMMENT ON COLUMN ais.smsitem.objisn IS $COMM$Связанный с СМС объект - ISN (Smirnov 25/10/12)$COMM$;
COMMENT ON COLUMN ais.smsitem.objdiscr IS $COMM$Связанный с СМС объект - тип объекта (Smirnov 25/10/12)$COMM$;
COMMENT ON COLUMN ais.smsitem.objid IS $COMM$Связанный с СМС объект - ID объекта (Smirnov 25/10/12)$COMM$;
COMMENT ON COLUMN ais.smsitem.response IS $COMM$Ответ в xml-формате из службы отсылки сообщений http://api.mobvision.ru/send/$COMM$;
COMMENT ON COLUMN ais.smsitem.status IS $COMM$R-готово к передаче, S-передано в сервис отсылки, W-отослано успешно, E-ошибка пересылки$COMM$;
COMMENT ON COLUMN ais.smsitem.code IS $COMM$<response><code> в ответе (из поля Response)$COMM$;
COMMENT ON COLUMN ais.smsitem.date2sql IS $COMM$Дата и время передачи в сервис отсылки sms$COMM$;
COMMENT ON COLUMN ais.smsitem.datesend IS $COMM$Дата и время отсылки$COMM$;
COMMENT ON COLUMN ais.smsitem.datefromsql IS $COMM$Дата и время получения ответа из сервиса отсылки sms$COMM$;
COMMENT ON COLUMN ais.smsitem.phoneisn IS $COMM$Ссылка на SubPhone$COMM$;


CREATE TABLE ais.smsmessages (
    isn                              NUMERIC,
    messagetext                      VARCHAR(2000),
    date2sql                         TIMESTAMP,
    createdby                        NUMERIC,
    created                          TIMESTAMP,
    datestart                        TIMESTAMP,
    dateend                          TIMESTAMP,
    isadvert                         VARCHAR(1),
    providerisn                      NUMERIC,
    classisn                         NUMERIC,
    status                           VARCHAR(1),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    emplisn                          NUMERIC,
    deptisn                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.smsmessages IS $COMM$Таблица с текстом Sms-сообщений$COMM$;
COMMENT ON COLUMN ais.smsmessages.emplisn IS $COMM$Сотрудник, от имени которого сформировано сообщение (может устанавливаться явно ночным роботом)$COMM$;
COMMENT ON COLUMN ais.smsmessages.deptisn IS $COMM$Подразделение, от имени которого сформировано сообщение (может устанавливаться явно ночным роботом)$COMM$;
COMMENT ON COLUMN ais.smsmessages.messagetext IS $COMM$Текст сообщения$COMM$;
COMMENT ON COLUMN ais.smsmessages.date2sql IS $COMM$Дата и время передачи в сервис отсылки sms$COMM$;
COMMENT ON COLUMN ais.smsmessages.datestart IS $COMM$Дата начала рассылки$COMM$;
COMMENT ON COLUMN ais.smsmessages.dateend IS $COMM$Дата окончания рассылки$COMM$;
COMMENT ON COLUMN ais.smsmessages.isadvert IS $COMM$Рекламная рассылка (Y/N)$COMM$;


CREATE TABLE ais.srchlist_cur (
    isn                              NUMERIC,
    formisn                          NUMERIC,
    resultcur                        VARCHAR(4000),
    foldercur                        VARCHAR(4000),
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.srchlist_cur.formisn IS $COMM$PK (DICTI)(seq_dicti.nextval) идентификатор формы отбора $COMM$;
COMMENT ON COLUMN ais.srchlist_cur.resultcur IS $COMM$запрос для отображения результата$COMM$;
COMMENT ON COLUMN ais.srchlist_cur.foldercur IS $COMM$запрос для отображения личной папки$COMM$;
COMMENT ON COLUMN ais.srchlist_cur.created IS $COMM$Время создания$COMM$;
COMMENT ON COLUMN ais.srchlist_cur.createdby IS $COMM$Автор создания$COMM$;
COMMENT ON COLUMN ais.srchlist_cur.updated IS $COMM$Время изменения$COMM$;
COMMENT ON COLUMN ais.srchlist_cur.updatedby IS $COMM$Автор изменений$COMM$;


CREATE TABLE ais.stotariff_t (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    stoisn                           NUMERIC,
    makeisn                          NUMERIC,
    nch                              NUMERIC,
    nchcurrisn                       NUMERIC,
    kzch                             NUMERIC,
    lkm                              NUMERIC,
    lkmakr                           NUMERIC,
    lkmperl                          NUMERIC,
    lkmmet                           NUMERIC,
    lkmcurrisn                       NUMERIC,
    discont                          NUMERIC,
    disconttypeisn                   NUMERIC,
    storepartisn                     NUMERIC,
    tariffpriority                   VARCHAR(1),
    pricedynamics                    NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    remark                           VARCHAR(255),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE ais.subacc (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    bankisn                          NUMERIC,
    currisn                          NUMERIC,
    id                               VARCHAR(40),
    name                             VARCHAR(255),
    discr                            VARCHAR(1) DEFAULT 'J',
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    corrwith                         VARCHAR(255),
    clientacc                        VARCHAR(40),
    active                           VARCHAR(1),
    oldid                            VARCHAR(20),
    classisn                         NUMERIC DEFAULT 13447316,
    corraccisn                       NUMERIC,
    status                           VARCHAR(1),
    ownerisn                         NUMERIC,
    createdby                        NUMERIC,
    branchaccisn                     NUMERIC,
    type_rec_name                    VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subacc IS $COMM$Банковский счет субъекта.
Предназначен для фиксации платежных реквизитов субъекта взаиморасчетов.
Может использоваться для поиска и идентификации субъекта.$COMM$;
COMMENT ON COLUMN ais.subacc.isn IS $COMM$FK(DICTI,SUBACC). Машинный номер объекта, совпадает с ISN соответствующей записи в словаре
DICTI. В DICTI дублируются только счета ИГС !$COMM$;
COMMENT ON COLUMN ais.subacc.subjisn IS $COMM$FK(SUBJECT/SUBBANK) в зависимости от Discr. Субъект/банк - владелец банковского
счета/корсчета.

В одном банке у одного субъекта может быть несколько счетов, например в разных валютах.$COMM$;
COMMENT ON COLUMN ais.subacc.bankisn IS $COMM$FK(SUBBANK). Банк, в котором открыт счет. Для расч.счета клиента обязателен. Для
корсчета банка может отсутствовать, тогда необходимо задать банк-корреспондент текстом [CorrWith]$COMM$;
COMMENT ON COLUMN ais.subacc.currisn IS $COMM$FK(DICTI,ВАЛЮТА). Валюта банковского счета.
Если не задана, предполагается, что это мультивалютный счет.
Таким образом, для рублевого счета (точнее, счета в локальной валюте) валюта должна быть задана !$COMM$;
COMMENT ON COLUMN ais.subacc.id IS $COMM$Номер (код) банковского счета, указываемый в платежных документах.$COMM$;
COMMENT ON COLUMN ais.subacc.name IS $COMM$Название счета или примечание касающееся назначения счета и условий его использования.$COMM$;
COMMENT ON COLUMN ais.subacc.discr IS $COMM$Дискриминатор записи (SubjISN): J-расчетный счет SUBJECT, B-корсчет SUBBANK$COMM$;
COMMENT ON COLUMN ais.subacc.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.subacc.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.subacc.corrwith IS $COMM$Банк-корреспондент: название, адрес/город, страна, SWIFT. Задается для корсчетов
банков, если не используется формальная ссылка [BankISN].$COMM$;
COMMENT ON COLUMN ais.subacc.clientacc IS $COMM$Лицевой счет клиента$COMM$;
COMMENT ON COLUMN ais.subacc.active IS $COMM$Признак активности объекта:
N - новый, не проверен администратором,
Y - проверен администратором,
NULL-считается архивным и сохраняется для декодировки ссылок из ранее созданных объектов, но недоступен для ссылок из вновь
создаваемых объектов и не показывается в LookUp.
При создании нового объекта: если задан NULL - устанавливается N, если задан X - устанавливается в NULL.
Удалять можно только архивный объект.$COMM$;
COMMENT ON COLUMN ais.subacc.oldid IS $COMM$Старый номер счета (до конца 1997г. - новый номер)$COMM$;
COMMENT ON COLUMN ais.subacc.classisn IS $COMM$FK(DICTI). Указатель назначения банковского счета: расчетный, депозитный ...$COMM$;
COMMENT ON COLUMN ais.subacc.corraccisn IS $COMM$FK(SUBACC). Указатель счета банка-корреспондента$COMM$;
COMMENT ON COLUMN ais.subacc.status IS $COMM$для выбора счета, используемого по умолчанию$COMM$;
COMMENT ON COLUMN ais.subacc.ownerisn IS $COMM$FK(Subject) кому фактически принадлежит счет - для платежных реквизитов$COMM$;
COMMENT ON COLUMN ais.subacc.createdby IS $COMM$Создатель$COMM$;
COMMENT ON COLUMN ais.subacc.branchaccisn IS $COMM$FK(SUBACC, SUBJECT) Указатель отделения банка или счета в посреднике$COMM$;


CREATE TABLE ais.subaccnum (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    objisn                           NUMERIC,
    objisnlist                       NUMERIC,
    code                             VARCHAR(20),
    id                               VARCHAR(20),
    n_children                       NUMERIC,
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(255),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    datemod                          TIMESTAMP,
    active                           VARCHAR(1) DEFAULT 'N',
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    s_type                           VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subaccnum IS $COMM$Список лицевых счетов$COMM$;
COMMENT ON COLUMN ais.subaccnum.s_type IS $COMM$Тип лицевого счета И-инкассирует Н-не инкассирует премию$COMM$;
COMMENT ON COLUMN ais.subaccnum.objisn IS $COMM$Ссылка на объект учета Subject.isn$COMM$;
COMMENT ON COLUMN ais.subaccnum.objisnlist IS $COMM$Ссылка на список объектов учета ObjisnList.Isn$COMM$;
COMMENT ON COLUMN ais.subaccnum.code IS $COMM$Код учета$COMM$;
COMMENT ON COLUMN ais.subaccnum.id IS $COMM$Полный номер лицевого счета$COMM$;
COMMENT ON COLUMN ais.subaccnum.shortname IS $COMM$Краткое наименование$COMM$;
COMMENT ON COLUMN ais.subaccnum.fullname IS $COMM$Полное наименование$COMM$;
COMMENT ON COLUMN ais.subaccnum.datebeg IS $COMM$Дата начала использования лицевого счета$COMM$;
COMMENT ON COLUMN ais.subaccnum.dateend IS $COMM$Дата окончания использования лицевого счета$COMM$;
COMMENT ON COLUMN ais.subaccnum.datemod IS $COMM$Дата с которой разрешена модификация лицевого счета$COMM$;
COMMENT ON COLUMN ais.subaccnum.active IS $COMM$Признак активности N - новый, Y -активный, S - архивный, null - не существующий$COMM$;
COMMENT ON COLUMN ais.subaccnum.updated IS $COMM$Дата  изменения$COMM$;
COMMENT ON COLUMN ais.subaccnum.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.subaddr_t (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    classisn                         NUMERIC,
    countryisn                       NUMERIC,
    postcode                         VARCHAR(10),
    cityisn                          NUMERIC,
    address                          VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    code                             VARCHAR(10),
    streetisn                        NUMERIC,
    street                           VARCHAR(255),
    house                            VARCHAR(20),
    building                         VARCHAR(20),
    flat                             VARCHAR(20),
    district                         VARCHAR(255),
    addresslat                       VARCHAR(255),
    active                           VARCHAR(1),
    securitylevel                    NUMERIC,
    securitystr                      VARCHAR(255),
    areaisn                          NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subaddr_t IS $COMM$Юридический адрес фирмы, адрес офиса, домашний адрес физического лица, местонахождение объекта
страхования.$COMM$;
COMMENT ON COLUMN ais.subaddr_t.securitystr IS $COMM$Строка с ролями для ограничения возможности просмотра - Угринович А.Н. 06.04.06$COMM$;
COMMENT ON COLUMN ais.subaddr_t.areaisn IS $COMM$FK(DICTI, ОКРУГ). Округ/район$COMM$;
COMMENT ON COLUMN ais.subaddr_t.isn IS $COMM$Машинный номер объекта. Устанавливается по умолчанию равным SEQ_SUBADDR.nextval$COMM$;
COMMENT ON COLUMN ais.subaddr_t.subjisn IS $COMM$FK(SUBJECT or AGROBJECT). Субъект или объект страхования, находящийся по данному
адресу.$COMM$;
COMMENT ON COLUMN ais.subaddr_t.classisn IS $COMM$FK(DICTI,АДРЕС). Дискриминатор адреса. Определяет класс адреса субъекта:
юридический, домашний адрес, местоположение объекта страхования.$COMM$;
COMMENT ON COLUMN ais.subaddr_t.countryisn IS $COMM$FK(COUNTRY). Указатель страны$COMM$;
COMMENT ON COLUMN ais.subaddr_t.postcode IS $COMM$Почтовый индекс, ZIP-код.$COMM$;
COMMENT ON COLUMN ais.subaddr_t.cityisn IS $COMM$FK(CITY).Указатель населенного пункта$COMM$;
COMMENT ON COLUMN ais.subaddr_t.address IS $COMM$Почтовый адрес (текст) русский или латинский, если русский не нужен.$COMM$;
COMMENT ON COLUMN ais.subaddr_t.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.subaddr_t.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.subaddr_t.code IS $COMM$Условный код территории для идентификации полисов$COMM$;
COMMENT ON COLUMN ais.subaddr_t.streetisn IS $COMM$FK(STREET). Указатель улицы$COMM$;
COMMENT ON COLUMN ais.subaddr_t.street IS $COMM$Название улицы: Ср.Тишинский пер.$COMM$;
COMMENT ON COLUMN ais.subaddr_t.house IS $COMM$Номер дома: 11а/2б$COMM$;
COMMENT ON COLUMN ais.subaddr_t.building IS $COMM$Номер корпуса$COMM$;
COMMENT ON COLUMN ais.subaddr_t.flat IS $COMM$Номер квартиры$COMM$;
COMMENT ON COLUMN ais.subaddr_t.district IS $COMM$Административный район$COMM$;
COMMENT ON COLUMN ais.subaddr_t.addresslat IS $COMM$Латинский адрес для случаев, когда нужен и русский и латинский.
В случаях, когда нужен латинский адрес, если он не задан, АИС использует транслитерацию поля ADDRESS. Если задается только
латинский адрес, он должен быть задан в ADDRESS.$COMM$;


CREATE TABLE ais.subbank (
    isn                              NUMERIC,
    countryisn                       NUMERIC,
    regionisn                        NUMERIC,
    pznisn                           NUMERIC,
    uerisn                           NUMERIC,
    vkey                             VARCHAR(8),
    vkeydel                          VARCHAR(8),
    id                               NUMERIC(38),
    shortname                        VARCHAR(255),
    namelat                          VARCHAR(255),
    fullname                         VARCHAR(255),
    searchname                       VARCHAR(80),
    address                          VARCHAR(255),
    addresslat                       VARCHAR(255),
    corracc                          VARCHAR(20),
    oldcorracc                       VARCHAR(20),
    rkc                              INT,
    mfo                              INT,
    oldmfo                           INT,
    city                             VARCHAR(40),
    citylat                          VARCHAR(40),
    postcode                         VARCHAR(10),
    phone                            VARCHAR(30),
    fax                              VARCHAR(30),
    telex                            VARCHAR(20),
    telegraph                        VARCHAR(60),
    email                            VARCHAR(60),
    swift                            VARCHAR(11),
    real                             VARCHAR(4),
    srok                             VARCHAR(2),
    regn                             VARCHAR(9),
    okpo                             VARCHAR(8),
    p                                VARCHAR(1),
    cks                              VARCHAR(6),
    active                           VARCHAR(1),
    dt_izm                           TIMESTAMP,
    datedel                          TIMESTAMP,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    chkflag                          VARCHAR(1),
    created                          TIMESTAMP DEFAULT current_timestamp,
    createdby                        NUMERIC(38),
    synisn                           NUMERIC,
    parentisn                        NUMERIC,
    subjname                         VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subbank IS $COMM$Банк. Содержит реквизиты, необходимые для формирования платежных документов (поручений и
заявлений на перевод).
Для внутренних расчетов необходимы:
название, город и (код участника, МФО для банка-участника прямых расчетов) или (корсчет с указанием банка-корреспондента (обычно
- РКЦ) - участника прямых расчетов).
Для внешних расчетов необходимы: название лат. и адрес лат.и возможно, банк-корреспондент.$COMM$;
COMMENT ON COLUMN ais.subbank.subjname IS $COMM$наименование банка как юр лица (для использования в SUBJECT.FullName)$COMM$;
COMMENT ON COLUMN ais.subbank.parentisn IS $COMM$FK(SUBBANK). Указатель иерархии банков$COMM$;
COMMENT ON COLUMN ais.subbank.isn IS $COMM$FK(DICTI,SUBBANK). Машинный номер объекта, совпадает с ISN соответствующей записи в
словаре DICTI.$COMM$;
COMMENT ON COLUMN ais.subbank.countryisn IS $COMM$FK(COUNTRY). Указатель страны банка$COMM$;
COMMENT ON COLUMN ais.subbank.regionisn IS $COMM$FK(REGION). Указатель территории  по  классификатору Система обозначений объектов
административно-территориального деления,  а также населенных пунктов
(СОАТО)$COMM$;
COMMENT ON COLUMN ais.subbank.pznisn IS $COMM$FK(DICTI). Указатель типа кредитного учреждения - стандартного сокращения при
формировании расчетно-денежных документов и отражает классификацию участников расчетов по уже сложившимся типам :  РКЦ,  ГРКЦ,
кредитная организация, филиал кредитной организации и т.д.$COMM$;
COMMENT ON COLUMN ais.subbank.uerisn IS $COMM$FK(DICTI). Указатель типа участников системы ЭР (РКЦ, ГРКЦ). DICTI.Code:
0 - не участвует в ЭР
1 - участвует в межрегиональных ЭР
2 - участвует во внутрирегиональных ЭР
3 - участвует в межрегиональных и внутрирегиональных ЭР.
Для пользователей  системы  ЭР (кредитных  организаций,  филиалов кредитных   организаций, обслуживаемых на  РКЦ,  ГРКЦ,
являющихся участниками ЭР) поле UER может принимать следующие значения:
0 - не является пользователем ЭР
1 - является пользователем только межрегиональных ЭР
2 - является пользователем только внутрирегиональных ЭР
3 - является пользователем межрегиональных и внутрирегиональных ЭР$COMM$;
COMMENT ON COLUMN ais.subbank.vkey IS $COMM$Внешний машинный номер по справочнику банков МЦИ$COMM$;
COMMENT ON COLUMN ais.subbank.vkeydel IS $COMM$Внешний машинный номер (VKEY) правопреемника при отзыве лицензии$COMM$;
COMMENT ON COLUMN ais.subbank.id IS $COMM$Внешний машинный идентификатор$COMM$;
COMMENT ON COLUMN ais.subbank.shortname IS $COMM$Русское платежное название банка для внутренних расчетов.$COMM$;
COMMENT ON COLUMN ais.subbank.namelat IS $COMM$Латинское платежное название банка для внешних рачсетов.$COMM$;
COMMENT ON COLUMN ais.subbank.fullname IS $COMM$Полное наименование юридического лица$COMM$;
COMMENT ON COLUMN ais.subbank.searchname IS $COMM$Краткое наименование участника расчетов образуется из платежного
наименования  и служит для быстрого поиска участника расчетов.  Если в
полном наименовании присутствует название,  заключенное в кавычки,  то
краткому  наименованию присваивается содержимое кавычек,  ограниченное
18 символами.  Если в платежном наименовании кавычек нет,  то краткому
наименованию присваивается значение первых 18 символов.$COMM$;
COMMENT ON COLUMN ais.subbank.address IS $COMM$Русский адрес банка для внутренних расчетов и заявлений на перевод.$COMM$;
COMMENT ON COLUMN ais.subbank.addresslat IS $COMM$Латинский адрес банка для внешних расчетов.$COMM$;
COMMENT ON COLUMN ais.subbank.corracc IS $COMM$Корсчет банка в РКЦ для внутренних расчетов.$COMM$;
COMMENT ON COLUMN ais.subbank.oldcorracc IS $COMM$Старый номер корсчета (до конца 1997г. - новый номер)$COMM$;
COMMENT ON COLUMN ais.subbank.rkc IS $COMM$БИК РКЦ$COMM$;
COMMENT ON COLUMN ais.subbank.mfo IS $COMM$БИК - банковский идентификационный код$COMM$;
COMMENT ON COLUMN ais.subbank.oldmfo IS $COMM$Старый 6-значный МФО$COMM$;
COMMENT ON COLUMN ais.subbank.city IS $COMM$Город банка для внутренних расчетов.$COMM$;
COMMENT ON COLUMN ais.subbank.postcode IS $COMM$Почтовый индекс, ZIP-код$COMM$;
COMMENT ON COLUMN ais.subbank.phone IS $COMM$Телефон$COMM$;
COMMENT ON COLUMN ais.subbank.fax IS $COMM$Факс$COMM$;
COMMENT ON COLUMN ais.subbank.telex IS $COMM$Телекс$COMM$;
COMMENT ON COLUMN ais.subbank.telegraph IS $COMM$Телеграфный код$COMM$;
COMMENT ON COLUMN ais.subbank.email IS $COMM$E-mail$COMM$;
COMMENT ON COLUMN ais.subbank.swift IS $COMM$SWIFT$COMM$;
COMMENT ON COLUMN ais.subbank.real IS $COMM$Признак ограничения участия в межбанковских расчетах может принимать следующие значения:
1) Не  заполнен  -  кредитная организация не  имеет ограничений  на проведение межбанковских расчетов
2) ОТЗВ -  для  кредитных  организаций проставляется   на  основании  риказа  Банка  России  об  отзыве лицензии  на совершение
 банковских операций у кредитной организации
3) ИСКЛ  - для  РКЦ  проставляется  на основании выписки из протокола заседания Совета директоров Банка России  о  закрытии
расчетно-кассового центра
4) БЛОК  -   проставляют  на  основании приказа  Банка  России о приостановлении расчетов с кредитной организацией$COMM$;
COMMENT ON COLUMN ais.subbank.srok IS $COMM$Фактический   срок  экспедирования
расчетно-денежных документов от кредитных организаций до обслуживающих
РКЦ и от РКЦ до ГРКЦ, с учетом времени на их обработку (в днях).$COMM$;
COMMENT ON COLUMN ais.subbank.regn IS $COMM$Регистрационный   номер   кредитной организации  (филиала).  Для  филиалов кредитной
организации регистрационный номер   указывается через /, после указания  регистрационного номера головной кредитной организации.
Также возможны следующие разновидности указания регистрационного номера в ЭБД Справочника БИК РФ:
- в последней позиции указан знак вопроса,  следовательно  в  базе данных дублируются регистрационные номера или для филиала
отсутствует головная кредитная организация с указанным регистрационным  номером. Знак вопроса будет снят после уточнения
регистрационного номера; если   для   филиала   указан  только  номер  головной кредитной организации и /, следовательно
регистрационный номер головного банка известен, а номер филиала уточняется$COMM$;
COMMENT ON COLUMN ais.subbank.okpo IS $COMM$Код по Общероссийскому классификатору организаций  и  предприятий
(ОКПО),  присвоенный  при  регистрации  участника  расчетов в органах
статистики$COMM$;
COMMENT ON COLUMN ais.subbank.p IS $COMM$Как правило, поле P пустое. Если же в поле указан знак +, то при выводе  на  экран или
печать данных по участнику расчетов признак типа игнорируется.
Пример: PZN=26, NAMEP=АГРОПРОМБАНК РЕГИОНАЛЬНЫЙ ФИЛИАЛ
P=пусто: На экран(печать) будет выдана строка: АПБ АГРОПРОМБАНК РЕГИОНАЛЬНЫЙ ФИЛИАЛ
P=+: На экран(печать) будет выдана строка: АГРОПРОМБАНК РЕГИОНАЛЬНЫЙ ФИЛИАЛ$COMM$;
COMMENT ON COLUMN ais.subbank.cks IS $COMM$Номер установки центра коммутации сообщений$COMM$;
COMMENT ON COLUMN ais.subbank.active IS $COMM$Признак активности объекта:
N - новый, не проверен администратором,
Y - проверен администратором,
NULL-считается архивным и сохраняется для декодировки ссылок из ранее созданных объектов, но недоступен для ссылок из вновь
создаваемых объектов и не показывается в LookUp.
При создании нового объекта: если задан NULL - устанавливается N, если задан X - устанавливается в NULL.
Удалять можно только архивный объект.$COMM$;
COMMENT ON COLUMN ais.subbank.dt_izm IS $COMM$Содержит дату  последнего  изменения  информации  (реквизитов)  по
конкретному  участнику  расчетов  или дату внесения в ЭБД Справочника
БИК РФ.  Изначально для всех участников расчетов установлено значение
поля 25/01/94 - дата введения в эксплуатацию ПК Справочник БИК$COMM$;
COMMENT ON COLUMN ais.subbank.datedel IS $COMM$Дата отзыва лицензии$COMM$;
COMMENT ON COLUMN ais.subbank.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.subbank.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.subbank.chkflag IS $COMM$Признак контроля 20-разрядных корсчетов и банковских счетов клиентов. Устанавливается
автоматически для банков-резидентов (страна - Россия)$COMM$;
COMMENT ON COLUMN ais.subbank.created IS $COMM$Дата создания записи, трактуется как дата начала сотрудничества$COMM$;
COMMENT ON COLUMN ais.subbank.createdby IS $COMM$Создатель$COMM$;
COMMENT ON COLUMN ais.subbank.synisn IS $COMM$FK(SUBBANK). Указатель синонима, по умолчанию устанавливается на себя$COMM$;


CREATE TABLE ais.subdept_t (
    isn                              NUMERIC,
    firmisn                          NUMERIC,
    id                               NUMERIC(38),
    parentisn                        NUMERIC,
    n_children                       NUMERIC(38),
    addrisn                          NUMERIC,
    code                             VARCHAR(10),
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(255),
    active                           VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    docid                            NUMERIC,
    authisn                          NUMERIC,
    fcode                            VARCHAR(10),
    abbreviation                     VARCHAR(10),
    admid                            VARCHAR(20),
    datebeg                          TIMESTAMP,
    orderendisn                      NUMERIC,
    orderbegisn                      NUMERIC,
    dateend                          TIMESTAMP,
    classisn                         NUMERIC,
    orgparentisn                     NUMERIC,
    bpid                             VARCHAR(20),
    buhcode                          VARCHAR(30),
    securitystr                      VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subdept_t IS $COMM$Подразделение (сектор, отдел, управление) компании и филиалов.
DICTI.Code - числовая кодировка подразделений Ингосстраха.
DICTI.ShortName - краткое название: упр.страхования от огня.
DICTI.FullName - официальное название: Управление страхования от огня и сопутствующих рисков.$COMM$;
COMMENT ON COLUMN ais.subdept_t.bpid IS $COMM$Код подразделения в сводном отчете бизнес-плана (AL 5/10/05)$COMM$;
COMMENT ON COLUMN ais.subdept_t.isn IS $COMM$Машинный номер объекта.
Устанавливается по умолчанию равным SEQ_DICTI.nextval.
Совпадает с ISN соответствующего объекта, имеющего отдельную таблицу для хранения дополнительных полей.$COMM$;
COMMENT ON COLUMN ais.subdept_t.firmisn IS $COMM$FK(SUBJECT). Юридическое лицо, в состав которого входит подразделение.$COMM$;
COMMENT ON COLUMN ais.subdept_t.id IS $COMM$Числовой идентификатор подразделения в проводках, используется УБУиО и ЭУ$COMM$;
COMMENT ON COLUMN ais.subdept_t.parentisn IS $COMM$FK(SUBJECT). Указатель головного подразделения$COMM$;
COMMENT ON COLUMN ais.subdept_t.addrisn IS $COMM$FK(SUBADDR). Адрес подразделения.
Должен быть одним из адресов юридического лица, к которому относится данное подразделение.$COMM$;
COMMENT ON COLUMN ais.subdept_t.code IS $COMM$Префикс подразделения, используемый при формировании номеров счетов по косвенным
рискам$COMM$;
COMMENT ON COLUMN ais.subdept_t.shortname IS $COMM$Сокращенное наименование$COMM$;
COMMENT ON COLUMN ais.subdept_t.fullname IS $COMM$Полное наименование$COMM$;
COMMENT ON COLUMN ais.subdept_t.active IS $COMM$Индикатор активности. Для подразделений значение поля = Y означает, что данное подразделение является группирующим (при построении отчетности все вложенные подразделения сворачиваются)$COMM$;
COMMENT ON COLUMN ais.subdept_t.updated IS $COMM$Дата изменения.$COMM$;
COMMENT ON COLUMN ais.subdept_t.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.subdept_t.docid IS $COMM$Префикс подразделения, используемый при формировании номеров счетов по прямому
страхованию$COMM$;
COMMENT ON COLUMN ais.subdept_t.authisn IS $COMM$FK(SUBHUMAN). Ссылка на сотрудника, подписывающего платежные документы в
подразделении$COMM$;
COMMENT ON COLUMN ais.subdept_t.fcode IS $COMM$Код подразделения для полисов$COMM$;
COMMENT ON COLUMN ais.subdept_t.abbreviation IS $COMM$Аббревиатура подразделения: УСОСР, УСКАР$COMM$;
COMMENT ON COLUMN ais.subdept_t.admid IS $COMM$Делопроизводственный индекс подразделения (хоздоговоры в ПУ, тел.справочник):
[департамент]-управление-отдел/сектор$COMM$;
COMMENT ON COLUMN ais.subdept_t.datebeg IS $COMM$Дата открытия подразделения$COMM$;
COMMENT ON COLUMN ais.subdept_t.orderendisn IS $COMM$FK(SUBORDER). Указатель приказа на закрытие подразделения$COMM$;
COMMENT ON COLUMN ais.subdept_t.orderbegisn IS $COMM$FK(SUBORDER). Указатель приказа на открытие подразделения$COMM$;
COMMENT ON COLUMN ais.subdept_t.dateend IS $COMM$Дата закрытия подразделения$COMM$;
COMMENT ON COLUMN ais.subdept_t.classisn IS $COMM$FK(DICTI). Указатель категории подразделения: функциональное, оперативное ...$COMM$;
COMMENT ON COLUMN ais.subdept_t.orgparentisn IS $COMM$Указатель головного подразделения по оргструктуре, может отличаться от
ParentISN$COMM$;


CREATE TABLE ais.subdoc (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    deptisn                          NUMERIC,
    emplisn                          NUMERIC,
    classisn                         NUMERIC,
    signed                           TIMESTAMP,
    no                               NUMERIC,
    id                               VARCHAR(20),
    extid                            VARCHAR(20),
    status                           VARCHAR(1),
    remark                           VARCHAR(4000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    name                             VARCHAR(255),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    docser                           VARCHAR(10)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subdoc IS $COMM$Для регистрации входящих или исходящих документов$COMM$;
COMMENT ON COLUMN ais.subdoc.docser IS $COMM$Серия документа$COMM$;
COMMENT ON COLUMN ais.subdoc.isn IS $COMM$Машинный номер: SEQ_SUBDOC.nextval$COMM$;
COMMENT ON COLUMN ais.subdoc.subjisn IS $COMM$FK(SUBJECT). Указатель контрагента: получателя или источника документа$COMM$;
COMMENT ON COLUMN ais.subdoc.deptisn IS $COMM$FK(SUBDEPT). Указатель подразделения-куратора документа$COMM$;
COMMENT ON COLUMN ais.subdoc.emplisn IS $COMM$FK(SUBHUMAN). Указатель сотрудника-куратора документа$COMM$;
COMMENT ON COLUMN ais.subdoc.classisn IS $COMM$FK(DICTI). Тип документа, определяет нумератор$COMM$;
COMMENT ON COLUMN ais.subdoc.signed IS $COMM$Дата документа$COMM$;
COMMENT ON COLUMN ais.subdoc.no IS $COMM$Внутренний номер документа: порядковый номер в рамках нумератора типа документа$COMM$;
COMMENT ON COLUMN ais.subdoc.id IS $COMM$Идентификатор документа (наш референс), формируется автоматически по номеру и макету,
заданному в нумераторе$COMM$;
COMMENT ON COLUMN ais.subdoc.extid IS $COMM$Референс контрагента$COMM$;
COMMENT ON COLUMN ais.subdoc.status IS $COMM$Статус документа: N-в работе, Y-выпущен, null-аннулирован$COMM$;
COMMENT ON COLUMN ais.subdoc.remark IS $COMM$Аннотация документа$COMM$;
COMMENT ON COLUMN ais.subdoc.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.subdoc.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.subduty (
    isn                              NUMERIC,
    rank                             NUMERIC(38),
    code                             VARCHAR(10),
    shortname                        VARCHAR(40),
    active                           VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    orderbegisn                      NUMERIC,
    typeisn                          NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    orderendisn                      NUMERIC,
    classisn                         NUMERIC,
    fullname                         VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subduty IS $COMM$Кодификатор должностей сотрудников.$COMM$;
COMMENT ON COLUMN ais.subduty.fullname IS $COMM$Полное наименование должности$COMM$;
COMMENT ON COLUMN ais.subduty.isn IS $COMM$Машинный номер объекта.
Устанавливается по умолчанию равным SEQ_DICTI.nextval.
Совпадает с ISN соответствующего объекта, имеющего отдельную таблицу для хранения дополнительных полей.$COMM$;
COMMENT ON COLUMN ais.subduty.rank IS $COMM$Ранг должности. Задает порядок должностей как в телефонном справочнике.
0 - самая высокая должность (Президент компании).$COMM$;
COMMENT ON COLUMN ais.subduty.orderbegisn IS $COMM$FK(SUBORDER). Указатель приказа об открытии должности$COMM$;
COMMENT ON COLUMN ais.subduty.typeisn IS $COMM$FK(DICTI). Указатель типа должности$COMM$;
COMMENT ON COLUMN ais.subduty.datebeg IS $COMM$Дата открытия должности$COMM$;
COMMENT ON COLUMN ais.subduty.dateend IS $COMM$Дата закрытия должности$COMM$;
COMMENT ON COLUMN ais.subduty.orderendisn IS $COMM$FK(SUBORDER). Указатель приказа о закрытии должности$COMM$;
COMMENT ON COLUMN ais.subduty.classisn IS $COMM$FK(DICTI). Указатель категории должности: руковолитель, специалист...$COMM$;


CREATE TABLE ais.subhuman_t (
    isn                              NUMERIC,
    firmisn                          NUMERIC,
    deptisn                          NUMERIC,
    dutyisn                          NUMERIC,
    tabno                            NUMERIC(38),
    room                             VARCHAR(10),
    userpassword                     VARCHAR(8)VARCHAR(8),
    sex                              VARCHAR(1),
    docser                           VARCHAR(10),
    docclassisn                      NUMERIC,
    docno                            VARCHAR(20),
    docdate                          TIMESTAMP,
    docissuedby                      VARCHAR(255),
    birthday                         TIMESTAMP,
    ssn                              VARCHAR(20),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    locationisn                      NUMERIC,
    ikey                             VARCHAR(20),
    securitylevel                    NUMERIC,
    securitystr                      VARCHAR(255),
    cardkey                          VARCHAR(20),
    docdateend                       TIMESTAMP,
    docissuedcode                    VARCHAR(20)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subhuman_t IS $COMM$Физическое лицо, являющееся субъектом взаимоотношений:
клиент, агент, сотрудник компании или контактное лицо.
DICTI.Code - табельный номер сотрудника Ингосстраха.
DICTI.ShortName - фамилия и инициалы: T.J.Shriber, Иванов И.И. Для пользователей системы используется как имя пользователя.
DICTI.FullName - полные Ф.И.О.: ИВАНОВ Иван Иванович, SHRIBER, Tom John.$COMM$;
COMMENT ON COLUMN ais.subhuman_t.docdateend IS $COMM$Дата окончания действия документа$COMM$;
COMMENT ON COLUMN ais.subhuman_t.docissuedcode IS $COMM$Код подразделения выдавшего документ$COMM$;
COMMENT ON COLUMN ais.subhuman_t.cardkey IS $COMM$Номер карточки сотрудника (Em-marine).$COMM$;
COMMENT ON COLUMN ais.subhuman_t.securitystr IS $COMM$Строка с ролями для ограничения возможности просмотра - Угринович А.Н. 15.05.07$COMM$;
COMMENT ON COLUMN ais.subhuman_t.isn IS $COMM$FK(DICTI,СУБЪЕКТ). Машинный номер объекта, совпадает с ISN соответствующей записи в
словаре DICTI.$COMM$;
COMMENT ON COLUMN ais.subhuman_t.firmisn IS $COMM$FK(SUBJECT). Фирма, в которой работает сотрудник.$COMM$;
COMMENT ON COLUMN ais.subhuman_t.deptisn IS $COMM$FK(SUBDEPT). Подразделение - рабочая группа, используется в оперативной
деятельности. Может отличаться от подразделения по штатному расписанию$COMM$;
COMMENT ON COLUMN ais.subhuman_t.dutyisn IS $COMM$FK(DICTI,ДОЛЖНОСТЬ). Должность сотрудника.$COMM$;
COMMENT ON COLUMN ais.subhuman_t.tabno IS $COMM$табельный номер сотрудника$COMM$;
COMMENT ON COLUMN ais.subhuman_t.room IS $COMM$Кабинет сотрудника.$COMM$;
COMMENT ON COLUMN ais.subhuman_t.userpassword IS $COMM$Пароль, если сотрудник является пользователем системы.
Хранится в закодированном виде.$COMM$;
COMMENT ON COLUMN ais.subhuman_t.sex IS $COMM$Пол физического лица: М-мужской, Ж-женский$COMM$;
COMMENT ON COLUMN ais.subhuman_t.docser IS $COMM$Серия документа$COMM$;
COMMENT ON COLUMN ais.subhuman_t.docclassisn IS $COMM$FK(DICTI). Тип документа$COMM$;
COMMENT ON COLUMN ais.subhuman_t.docno IS $COMM$Номер документа$COMM$;
COMMENT ON COLUMN ais.subhuman_t.docdate IS $COMM$Дата выдачи документа$COMM$;
COMMENT ON COLUMN ais.subhuman_t.docissuedby IS $COMM$Кем выдан документ$COMM$;
COMMENT ON COLUMN ais.subhuman_t.birthday IS $COMM$Дата рождения$COMM$;
COMMENT ON COLUMN ais.subhuman_t.ssn IS $COMM$Номер полиса социального страхования$COMM$;
COMMENT ON COLUMN ais.subhuman_t.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.subhuman_t.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.subhuman_t.locationisn IS $COMM$FK(DICTI). Указатель местонахождения сотрудника (территория, кабинет)$COMM$;


CREATE TABLE ais.subjbonus (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    objisn                           NUMERIC,
    agrisn                           NUMERIC,
    classisn                         NUMERIC,
    bonusisn                         NUMERIC,
    tariffval                        NUMERIC,
    datecalc                         TIMESTAMP,
    previsn                          NUMERIC,
    chgflg                           VARCHAR(1),
    remark                           VARCHAR(255),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    requestid                        NUMERIC,
    bonusisn2                        NUMERIC,
    addisn                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.subjbonus.requestid IS $COMM$ID запроса РСА$COMM$;
COMMENT ON COLUMN ais.subjbonus.bonusisn2 IS $COMM$FK(Dicti) Разряд БМ после проверки$COMM$;
COMMENT ON COLUMN ais.subjbonus.addisn IS $COMM$FK(Agreement) Ссылка на аддендум, в котором произошло изменение разряда БМ$COMM$;
COMMENT ON COLUMN ais.subjbonus.isn IS $COMM$SEQ_SubjBonus$COMM$;
COMMENT ON COLUMN ais.subjbonus.subjisn IS $COMM$FK(Subject) Ссылка на субъекта, которому присваивается персональный разряд БМ$COMM$;
COMMENT ON COLUMN ais.subjbonus.objisn IS $COMM$FK(ObjAgr) Ссылка на объект, в отношении которого устанавливается разряд БМ$COMM$;
COMMENT ON COLUMN ais.subjbonus.agrisn IS $COMM$FK(Agreement) Ссылка на договор, в котором произошло присвоение разряда БМ$COMM$;
COMMENT ON COLUMN ais.subjbonus.classisn IS $COMM$FK(Dicti) Класс БМ$COMM$;
COMMENT ON COLUMN ais.subjbonus.bonusisn IS $COMM$FK(Dicti) Разряд БМ$COMM$;
COMMENT ON COLUMN ais.subjbonus.tariffval IS $COMM$Коэффициент БМ$COMM$;
COMMENT ON COLUMN ais.subjbonus.datecalc IS $COMM$Дата расчета (установки) БМ$COMM$;
COMMENT ON COLUMN ais.subjbonus.previsn IS $COMM$FK(SubjBonus) Ссылка на исходный разряд БМ использованный для пересчета$COMM$;
COMMENT ON COLUMN ais.subjbonus.chgflg IS $COMM$Источник КБМ: АИС - A;
РСА - R;
Агентский сервис - B;
Сведения о страховании - M;
Заявление - S;$COMM$;
COMMENT ON COLUMN ais.subjbonus.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.subjbonus.updated IS $COMM$Дата изменения записи$COMM$;
COMMENT ON COLUMN ais.subjbonus.updatedby IS $COMM$Автор изменения записи$COMM$;


CREATE TABLE ais.subjclassext (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    classisnold                      NUMERIC,
    classisnnew                      NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subjclassext IS $COMM$Расширенная классификация клиента. Сохраняется при изменении класса клиента$COMM$;
COMMENT ON COLUMN ais.subjclassext.isn IS $COMM$Машинный номер: SEQ_DICX.nextval$COMM$;
COMMENT ON COLUMN ais.subjclassext.subjisn IS $COMM$FK(SUBJECT). Ссылка на клиента$COMM$;
COMMENT ON COLUMN ais.subjclassext.classisnold IS $COMM$FK(DICTI). Старое значение класса клиента$COMM$;
COMMENT ON COLUMN ais.subjclassext.classisnnew IS $COMM$FK(DICTI). Новое значение класса клиента$COMM$;
COMMENT ON COLUMN ais.subjclassext.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.subjclassext.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.subjdepthist (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    deptisn                          NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subjdepthist IS $COMM$Таблица для хранения истории изменения рабочих групп SUBHUMAN.deptisn во времени. Используется в основном для формирования отчетов по историческим данным.$COMM$;
COMMENT ON COLUMN ais.subjdepthist.isn IS $COMM$Превичный ключ. SEQ_subjdepthist$COMM$;
COMMENT ON COLUMN ais.subjdepthist.subjisn IS $COMM$FK(SUBJECT) Ссылка на субъекта$COMM$;
COMMENT ON COLUMN ais.subjdepthist.deptisn IS $COMM$FK(SUBDEPT) Рабочая группа, место работы$COMM$;
COMMENT ON COLUMN ais.subjdepthist.datebeg IS $COMM$Дата начала работы в подраздлеении$COMM$;
COMMENT ON COLUMN ais.subjdepthist.dateend IS $COMM$Дата окончания работы в подраздлеении$COMM$;
COMMENT ON COLUMN ais.subjdepthist.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.subjdepthist.updatedby IS $COMM$FK(SUBJECT) Автор изменения$COMM$;


CREATE TABLE ais.subject_t (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    roleclassisn                     NUMERIC,
    countryisn                       NUMERIC,
    branchisn                        NUMERIC,
    juridical                        VARCHAR(1) DEFAULT 'Y',
    resident                         VARCHAR(1) DEFAULT 'Y',
    vip                              VARCHAR(1),
    inn                              VARCHAR(15),
    id                               NUMERIC(38),
    fid                              NUMERIC(38),
    code                             VARCHAR(10),
    shortname                        VARCHAR(40),
    fullname                         VARCHAR(255),
    active                           VARCHAR(1),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    licenseno                        NUMERIC(38),
    licensedate                      TIMESTAMP,
    okpo                             NUMERIC(38),
    okohx                            VARCHAR(255),
    synisn                           NUMERIC,
    createdby                        NUMERIC,
    created                          TIMESTAMP DEFAULT current_timestamp,
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
    securitystr                      VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subject_t IS $COMM$Классификатор юридических и физических лицо, являющихся субъектами деятельности САО
"Ингосстрах":
клиенты, страховые компании, брокеры, агенты, филиалы, сотрудники ...
DICTI.Code, DICTI.ShortName, DICTI.FullName - см.ЮРИДИЧЕСКОЕ ЛИЦО, ФИЗИЧЕСКОЕ ЛИЦО.$COMM$;
COMMENT ON COLUMN ais.subject_t.ogrn IS $COMM$Код ОГРН (Основной Государственный Регистрационный Номер )$COMM$;
COMMENT ON COLUMN ais.subject_t.okved IS $COMM$Код ОКВЭД$COMM$;
COMMENT ON COLUMN ais.subject_t.isn IS $COMM$FK(DICTI,СУБЪЕКТ). Машинный номер объекта, совпадает с ISN соответствующей записи в
словаре DICTI.$COMM$;
COMMENT ON COLUMN ais.subject_t.classisn IS $COMM$FK(DICTI,ТСУБЪЕКТ). Класс субъекта (сотрудник, клиент, страховая компания...)$COMM$;
COMMENT ON COLUMN ais.subject_t.roleclassisn IS $COMM$FK(DICTI,ДСУБЪЕКТ). Основной вид взаимоотношений с субъектом: страхователь,
агент, ... .
Определяет РОЛЬ СУБЪЕКТА в договоре по умолчанию, но не ограничивает ее.$COMM$;
COMMENT ON COLUMN ais.subject_t.countryisn IS $COMM$Страна регистрации юр.лица, гражданство физ.лица.
Страна и признак резидентности функционально зависимы:
для локальной страны автоматически устанавливается Resident='Y', в противном случае 'N',
для резидента (Resident='Y') страна устанавливается локальной автоматически.$COMM$;
COMMENT ON COLUMN ais.subject_t.branchisn IS $COMM$FK(Dicti). Указатель отрасли субъекта. DICTI.Code содержит код отрасли.$COMM$;
COMMENT ON COLUMN ais.subject_t.juridical IS $COMM$Признак юридического лица:
Y - юридическое лицо
N - физическое лицо$COMM$;
COMMENT ON COLUMN ais.subject_t.resident IS $COMM$Признак резидентности субъекта.
Определяется автоматически страной регистрации (гражданством) субъекта (см.СТРАНА СУБЪЕКТА):
Y - резидент,
N - нерезидент.
Значение Y автоматически определяет страну регистрации.$COMM$;
COMMENT ON COLUMN ais.subject_t.vip IS $COMM$Признак важности клиента:
Y - клиент особой важности, G - крупный клиент (объем премии свыше 50тыс$), L - постоянный клиент, null - обычный (разовый)
$COMM$;
COMMENT ON COLUMN ais.subject_t.inn IS $COMM$ИНН - идентификационный номер налогоплательщика$COMM$;
COMMENT ON COLUMN ais.subject_t.id IS $COMM$Машинный идентификатор АСУ ИГС KOD_CLN$COMM$;
COMMENT ON COLUMN ais.subject_t.fid IS $COMM$Локальный машинный идентификатор АСУ ИГС = 100 * KOD_CLOT + CODE_HOST.
Для новой записи устанавливается равным ISN.$COMM$;
COMMENT ON COLUMN ais.subject_t.code IS $COMM$Бухгалтерский код, устанавливается по машинному коду, если не задан$COMM$;
COMMENT ON COLUMN ais.subject_t.shortname IS $COMM$Краткое наименование$COMM$;
COMMENT ON COLUMN ais.subject_t.fullname IS $COMM$Полное юридическое русское или латинское (если русское в принципе отсутствует)
наименование$COMM$;
COMMENT ON COLUMN ais.subject_t.active IS $COMM$Индикатор активности: N-новый, Y-выверен, null-архив$COMM$;
COMMENT ON COLUMN ais.subject_t.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.subject_t.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.subject_t.licenseno IS $COMM$Номер лицензии/аттестата$COMM$;
COMMENT ON COLUMN ais.subject_t.licensedate IS $COMM$Дата выдачи лицензии/аттестата$COMM$;
COMMENT ON COLUMN ais.subject_t.okpo IS $COMM$Код ОКПО$COMM$;
COMMENT ON COLUMN ais.subject_t.okohx IS $COMM$Код ОКОНХ$COMM$;
COMMENT ON COLUMN ais.subject_t.synisn IS $COMM$FK(SUBJECT). Указатель синонима субъекта, по умолчанию устанавливается на себя$COMM$;
COMMENT ON COLUMN ais.subject_t.createdby IS $COMM$Создатель$COMM$;
COMMENT ON COLUMN ais.subject_t.created IS $COMM$Дата создания записи, трактуется как дата начала сотрудничества$COMM$;
COMMENT ON COLUMN ais.subject_t.profittaxflag IS $COMM$Флаг налога на доход иностранных юридических лиц: Y-взимается, null-нет$COMM$;
COMMENT ON COLUMN ais.subject_t.parentisn IS $COMM$FK(SUBJECT). Указатель головной фирмы$COMM$;
COMMENT ON COLUMN ais.subject_t.namelat IS $COMM$Полное юридическое латинское наименование. Задается, если есть русское$COMM$;
COMMENT ON COLUMN ais.subject_t.orgformisn IS $COMM$FK(DICTI). Укказатель организационно-правовой вормы (ООО,АО...)$COMM$;
COMMENT ON COLUMN ais.subject_t.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.subject_t.searchname IS $COMM$поисковое поле$COMM$;


CREATE TABLE ais.subjectx (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    subjisn1                         NUMERIC,
    subjisn2                         NUMERIC,
    active                           VARCHAR(1),
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    deptisn                          NUMERIC,
    remark                           VARCHAR(2000),
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    id                               VARCHAR(50)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subjectx IS $COMM$Таблица для регистрации связей/отношений между субъектами. Аналог DICX для связи субъектов.
Типы связей хранятся в DICTI, подразделяются на универсальные, связи Ю-Ю, связи Ф-Ф и связи Ф-Ю.
Контроль соответствия типов субъетов и отношний на триггерах таблицы.
Связь между субъектами может действовать определенное время ограниченное датами datebeg и dateend.
Связь может относиться к конкретному подразделению ИГС (поле DEPTISN). 
Например если два субъекта объединены связью "партнер", а в поле deptisn указан ДКС то это пактнер по комплексному страхованию.
Отношения всегда направлены от SUBJISN1 к SUBJISN2.
Угринович А.Н. 20.07.2007$COMM$;
COMMENT ON COLUMN ais.subjectx.id IS $COMM$Идентификатор(номер) связи$COMM$;
COMMENT ON COLUMN ais.subjectx.isn IS $COMM$Машинный номер: SEQ_DICX.nextval$COMM$;
COMMENT ON COLUMN ais.subjectx.classisn IS $COMM$(FK DICTI) тип отношения между субъектами$COMM$;
COMMENT ON COLUMN ais.subjectx.subjisn1 IS $COMM$(FK SUBJECT) начальный субъект отношения$COMM$;
COMMENT ON COLUMN ais.subjectx.subjisn2 IS $COMM$(FK SUBJECT) конечный субъект отношения$COMM$;
COMMENT ON COLUMN ais.subjectx.active IS $COMM$Признак активности отношения N-новый, Y-вывепенный, Null - архивный$COMM$;
COMMENT ON COLUMN ais.subjectx.datebeg IS $COMM$Начало действия отношения$COMM$;
COMMENT ON COLUMN ais.subjectx.dateend IS $COMM$Окончание действия отношения$COMM$;
COMMENT ON COLUMN ais.subjectx.deptisn IS $COMM$(FK SUBDEPT) подразделение ИГС имеющее отношение к связи$COMM$;
COMMENT ON COLUMN ais.subjectx.remark IS $COMM$комментарии$COMM$;
COMMENT ON COLUMN ais.subjectx.created IS $COMM$Дата создания поддерживается автоматически$COMM$;
COMMENT ON COLUMN ais.subjectx.createdby IS $COMM$(FK SUBJECT) Создатель поддерживается автоматически$COMM$;
COMMENT ON COLUMN ais.subjectx.updated IS $COMM$Дата изменения поддерживается автоматически$COMM$;
COMMENT ON COLUMN ais.subjectx.updatedby IS $COMM$(FK SUBJECT) Автор изменения поддерживается автоматически$COMM$;


CREATE TABLE ais.subowner (
    subjisn                          NUMERIC,
    deptisn                          NUMERIC,
    humanisn                         NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    opcode                           VARCHAR(1),
    isn                              NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subowner IS $COMM$Сотрудник, работающий с субъектом (клиентом) в соответствии с распределением участков в
подразделении.
Используется для ограничения поиска, для построения списка субъектов сотрудника или подразделения.
Обычно в подразделении участки работ делятся по клиентам.
При создании субъекта и при указании его в качестве субъекта договора он автоматически закрепляется за активным пользователем и
его подразделением.
Список может быть откорректирован и вручную.$COMM$;
COMMENT ON COLUMN ais.subowner.isn IS $COMM$PK (ISN)$COMM$;
COMMENT ON COLUMN ais.subowner.opcode IS $COMM$Доспустимая операция  (R - чтение, W - запись) по умолчанию null = W$COMM$;
COMMENT ON COLUMN ais.subowner.subjisn IS $COMM$FK(SUBJECT). Субъект, владельцем которого является сотрудник.$COMM$;
COMMENT ON COLUMN ais.subowner.deptisn IS $COMM$FK(SUBDEPT). Подразделение-владелец субъекта.$COMM$;
COMMENT ON COLUMN ais.subowner.humanisn IS $COMM$FK(SUBHUMAN). Сотрудник-владелец субъекта.$COMM$;
COMMENT ON COLUMN ais.subowner.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.subowner.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;


CREATE TABLE ais.subparm (
    isn                              NUMERIC,
    userisn                          NUMERIC,
    classisn                         NUMERIC,
    valc                             VARCHAR(255),
    valn                             NUMERIC,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subparm IS $COMM$Настроечные параметры пользователя$COMM$;
COMMENT ON COLUMN ais.subparm.isn IS $COMM$Машинный номер записи: SEQ_SUBPARM.nextval$COMM$;
COMMENT ON COLUMN ais.subparm.userisn IS $COMM$FK(SUBHUMAN). Ссылка на пользователя$COMM$;
COMMENT ON COLUMN ais.subparm.classisn IS $COMM$FK(DICTI). Тип параметра, определяющий его семантику при использовании в
приложении$COMM$;
COMMENT ON COLUMN ais.subparm.valc IS $COMM$Строковое значение$COMM$;
COMMENT ON COLUMN ais.subparm.valn IS $COMM$Числовое значение$COMM$;
COMMENT ON COLUMN ais.subparm.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.subparm.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;


CREATE TABLE ais.subphone_t (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    classisn                         NUMERIC,
    cityisn                          NUMERIC,
    phone                            VARCHAR(60),
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    contact                          VARCHAR(255),
    addrisn                          NUMERIC,
    active                           VARCHAR(1),
    securitylevel                    NUMERIC,
    securitystr                      VARCHAR(255),
    searchphone                      VARCHAR(60)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subphone_t IS $COMM$Средства связи с субъектом. Под средством связи понимается телефон, факс, телекс, телетайп,
электронная почта.$COMM$;
COMMENT ON COLUMN ais.subphone_t.searchphone IS $COMM$поле PHONE преобразованное для поиска(FINDEX_SUBPHONE) JZ 110706$COMM$;
COMMENT ON COLUMN ais.subphone_t.securitystr IS $COMM$Строка с ролями для ограничения возможности просмотра - Угринович А.Н. 06.04.06$COMM$;
COMMENT ON COLUMN ais.subphone_t.isn IS $COMM$Машинный номер объекта. Устанавливается по умолчанию равным SEQ_SUBPHONE.nextval$COMM$;
COMMENT ON COLUMN ais.subphone_t.subjisn IS $COMM$FK(DICTI,СУБЪЕКТ). Субъект средства связи.$COMM$;
COMMENT ON COLUMN ais.subphone_t.classisn IS $COMM$FK(DICTI,СВЯЗЬ). Тип средства связи: телефон, факс, телекс, E-Mail ...$COMM$;
COMMENT ON COLUMN ais.subphone_t.cityisn IS $COMM$FK(DICTI,ГОРОД). Город, телефонный код которого будет использоваться для
междугородней связи.$COMM$;
COMMENT ON COLUMN ais.subphone_t.phone IS $COMM$Номер телефона, факса, код телекса, адрес E-Mail без кода города.$COMM$;
COMMENT ON COLUMN ais.subphone_t.remark IS $COMM$Примечание, касающееся назначения или особенностей использования.$COMM$;
COMMENT ON COLUMN ais.subphone_t.updated IS $COMM$Дата создания или последнего изменения объекта.

Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.subphone_t.updatedby IS $COMM$Автор создания или последнего изменения объекта.

Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.subphone_t.contact IS $COMM$Контактное лицо$COMM$;
COMMENT ON COLUMN ais.subphone_t.addrisn IS $COMM$FK(SUBADDR)$COMM$;
COMMENT ON COLUMN ais.subphone_t.active IS $COMM$Статус записи: null-архив$COMM$;


CREATE TABLE ais.subrole (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    userisn                          NUMERIC,
    rank                             NUMERIC(38),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subrole IS $COMM$Роль пользователя. Предназначена для управления системными привилегиями.
Для физических лиц, являющихся пользователями системы, и должностей могут задаваться списки ролей с указанием ранга для каждой.
Роль определяет функциональную подсистему (С-страхование, П-перестрахование, Б-бухгалтерия,...), функции которой может выполнять
пользователь.
Функциональные подсистемы задают разбиение на множестве функций: каждой подсистеме соответствует некоторое подмножество функций,
не пересекающееся с другими подсистемами.
Функции подсистемы проранжированы по значимости (страхование: 0-просмотр полиса, 1-ввод полиса, начисление, 2-андеррайтинг).
Ранг функции определяет минимальный ранг соответствующей роли пользователя, при котором разрешено выполнение этой функции.
Так, для ввода полиса необходимо включить в список ролей пользователя роль страхователя с рангом не ниже 2, например: (С2,Б0).
При указании должности вновь зарегистрированного пользователя автоматически определяется список ролей, который может быть
откорректирован вручную.$COMM$;
COMMENT ON COLUMN ais.subrole.isn IS $COMM$Машинный номер записи: SEQ_SUBROLE.nextval$COMM$;
COMMENT ON COLUMN ais.subrole.classisn IS $COMM$FK(DICTI,РОЛЬ). Класс роли.$COMM$;
COMMENT ON COLUMN ais.subrole.userisn IS $COMM$FK(SUBHUMAN or SUBDUTY). Пользователь или должность, имеющие данную роль.$COMM$;
COMMENT ON COLUMN ais.subrole.rank IS $COMM$Ранг роли, определяющий уровень доступа, предоставляемый пользователю при выполнении
функций, соответствующих данной роли.
Не имеет отношения к рангу должности.
Увеличение ранга снижает права пользователя. Максимальный доступ дает ранг 0 (администратор).$COMM$;
COMMENT ON COLUMN ais.subrole.updated IS $COMM$Дата создания или последнего изменения объекта.
Устанавливается автоматически равной SYSDATE при создании и корректировке.$COMM$;
COMMENT ON COLUMN ais.subrole.updatedby IS $COMM$Автор создания или последнего изменения объекта.
Устанавливается автоматически равным ISN активного пользователя (init.UserISN) при создании и корректировке.$COMM$;


CREATE TABLE ais.subword (
    isn                              NUMERIC,
    parentisn                        NUMERIC DEFAULT 0,
    wordlat                          VARCHAR(40),
    wordrus                          VARCHAR(40),
    status                           VARCHAR(1),
    reptimes                         NUMERIC,
--WARNING: In table ais.subword column new matches Greenplum keyword. Corrected to new_.
    new_                             VARCHAR(1)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subword IS $COMM$Словарь содержит перечень слов, встречающихся в наименованиях юридических лиц. Автоматически
поддерживается при изменении SUBJECT.$COMM$;
COMMENT ON COLUMN ais.subword.isn IS $COMM$Машинный номер, SEQ_SUBWORD.nextval$COMM$;
COMMENT ON COLUMN ais.subword.parentisn IS $COMM$FK(SUBWORD). Указатель синонима данного слова$COMM$;
COMMENT ON COLUMN ais.subword.wordlat IS $COMM$Латинское написание слова$COMM$;
COMMENT ON COLUMN ais.subword.wordrus IS $COMM$Слово для показа пользователю$COMM$;
COMMENT ON COLUMN ais.subword.status IS $COMM$Статус: null-не обработано, 0-мусор, мешающий при поиске, 1-обработано, 2-в работе$COMM$;
COMMENT ON COLUMN ais.subword.reptimes IS $COMM$Счетчик использования слова в названиях субъектов$COMM$;
COMMENT ON COLUMN ais.subword.new_ IS $COMM$Признак нового использования$COMM$;


CREATE TABLE ais.subwordx (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    subjsyn                          NUMERIC,
    word                             VARCHAR(40),
    latin                            VARCHAR(40)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.subwordx IS $COMM$Таблица перекрестных ссылок - встречаемости слов в наименованиях юридических лиц. Заполняется
автоматически при изменениях SUBJECT.$COMM$;
COMMENT ON COLUMN ais.subwordx.isn IS $COMM$Машинный номер, SEQ_SUBWORDX.nextval$COMM$;
COMMENT ON COLUMN ais.subwordx.subjisn IS $COMM$FK(SUBJECT). Указатель субъекта, в названии которого используется данное слово$COMM$;
COMMENT ON COLUMN ais.subwordx.subjsyn IS $COMM$FK(SUBJECT). Указатель эталонного субъекта для SubjISN$COMM$;
COMMENT ON COLUMN ais.subwordx.word IS $COMM$Слово из названия субъекта$COMM$;
COMMENT ON COLUMN ais.subwordx.latin IS $COMM$Латинская транслитерация слова$COMM$;


CREATE TABLE ais.survey (
    isn                              NUMERIC,
    taskisn                          NUMERIC,
    objtypeisn                       NUMERIC,
    agrtypeisn                       NUMERIC,
    contactname                      VARCHAR(255),
    addressname                      VARCHAR(250),
    claimsum                         NUMERIC,
    eestimatesum                     NUMERIC,
    currisn                          NUMERIC,
    agrisn                           NUMERIC,
    descisn                          NUMERIC,
    refundisn                        NUMERIC,
    remark                           VARCHAR(1000),
    status                           VARCHAR(1) DEFAULT 'W',
    receiverisn                      NUMERIC,
    formisn                          NUMERIC,
    datereceive                      TIMESTAMP,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    objfullname                      VARCHAR(255),
    locationisn                      NUMERIC,
    partnerisn                       NUMERIC,
    regdate                          TIMESTAMP,
    estimatedhours                   NUMERIC,
    duration                         NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.survey IS $COMM$Таблица осмотров объектов страхования$COMM$;
COMMENT ON COLUMN ais.survey.isn IS $COMM$Машинный номер, уникальный в рамках коэф. СТО: SEQ_Survey.nextval$COMM$;
COMMENT ON COLUMN ais.survey.taskisn IS $COMM$FK(QTask).ссылка на задачу$COMM$;
COMMENT ON COLUMN ais.survey.objtypeisn IS $COMM$FK(Dicti) Тип объекта $COMM$;
COMMENT ON COLUMN ais.survey.agrtypeisn IS $COMM$FK(Dicti) Вид договора$COMM$;
COMMENT ON COLUMN ais.survey.contactname IS $COMM$Контактное лицо$COMM$;
COMMENT ON COLUMN ais.survey.addressname IS $COMM$Адрес $COMM$;
COMMENT ON COLUMN ais.survey.claimsum IS $COMM$Заявленная стоимость$COMM$;
COMMENT ON COLUMN ais.survey.eestimatesum IS $COMM$Оценочная стоимость$COMM$;
COMMENT ON COLUMN ais.survey.currisn IS $COMM$FK(Dicti) Валюта$COMM$;
COMMENT ON COLUMN ais.survey.agrisn IS $COMM$FK(AGREEMENT). Ссылка на договор$COMM$;
COMMENT ON COLUMN ais.survey.descisn IS $COMM$FK(OBJAGR). Указатель физического объекта$COMM$;
COMMENT ON COLUMN ais.survey.refundisn IS $COMM$FK(AGRREFUND).ссылка на претензию$COMM$;
COMMENT ON COLUMN ais.survey.remark IS $COMM$Замечание$COMM$;
COMMENT ON COLUMN ais.survey.status IS $COMM$Статус запроса: W- не обработан (оформление) , P-аннулирован, O-в работе, Y-обработан (закрыта)$COMM$;
COMMENT ON COLUMN ais.survey.receiverisn IS $COMM$FK(DICTI). Ссылка на получателя (сотрудник, подразделение, абстрактный процесс)$COMM$;
COMMENT ON COLUMN ais.survey.formisn IS $COMM$FK(DICTI). Указатель типа формы для просмотра запроса$COMM$;
COMMENT ON COLUMN ais.survey.datereceive IS $COMM$Желаемая (контрольная ) дата обработки запроса, заполняется автоматически реальной
датой обработки.$COMM$;
COMMENT ON COLUMN ais.survey.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.survey.updatedby IS $COMM$FK(Subject) Автор изменения$COMM$;
COMMENT ON COLUMN ais.survey.created IS $COMM$Дата Создания$COMM$;
COMMENT ON COLUMN ais.survey.estimatedhours IS $COMM$Планируемое время (в часах)$COMM$;
COMMENT ON COLUMN ais.survey.duration IS $COMM$Затраченное время в часах$COMM$;
COMMENT ON COLUMN ais.survey.createdby IS $COMM$FK(Subject) Создатель$COMM$;
COMMENT ON COLUMN ais.survey.objfullname IS $COMM$Наименование объекта$COMM$;
COMMENT ON COLUMN ais.survey.partnerisn IS $COMM$FK(DICTI) партнер, которому предполагается передать запрос на регрессный осмотр$COMM$;
COMMENT ON COLUMN ais.survey.regdate IS $COMM$Дата предстрахового осмотра недвижимости$COMM$;
COMMENT ON COLUMN ais.survey.locationisn IS $COMM$FK(STREET) Место смотра$COMM$;


CREATE TABLE ais.tk_duplist (
    isn                              NUMERIC,
    mark                             VARCHAR(1),
    fullname                         VARCHAR(255),
    created                          TIMESTAMP,
    changed                          TIMESTAMP,
    remark                           VARCHAR(255),
    totsum                           NUMERIC,
    datepay                          TIMESTAMP,
    userisn                          NUMERIC,
    estim                            NUMERIC,
    loaded                           TIMESTAMP DEFAULT current_timestamp,
    synisn                           NUMERIC,
    id                               VARCHAR(20),
    source                           VARCHAR(40)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.tk_duplist IS $COMM$Крылов: клиенты, загруженные из филиалов для дедупликации (АРМ КК) $COMM$;
COMMENT ON COLUMN ais.tk_duplist.synisn IS $COMM$синоним$COMM$;
COMMENT ON COLUMN ais.tk_duplist.id IS $COMM$идентификатор$COMM$;
COMMENT ON COLUMN ais.tk_duplist.source IS $COMM$источник$COMM$;
COMMENT ON COLUMN ais.tk_duplist.isn IS $COMM$FK: Subject$COMM$;
COMMENT ON COLUMN ais.tk_duplist.mark IS $COMM$отметка о р-те обработки$COMM$;
COMMENT ON COLUMN ais.tk_duplist.created IS $COMM$дата создания клиента$COMM$;
COMMENT ON COLUMN ais.tk_duplist.changed IS $COMM$дата изменения$COMM$;
COMMENT ON COLUMN ais.tk_duplist.remark IS $COMM$примечание пользователя$COMM$;
COMMENT ON COLUMN ais.tk_duplist.totsum IS $COMM$сумма на дату включения$COMM$;
COMMENT ON COLUMN ais.tk_duplist.datepay IS $COMM$дата включения Dup_Utils$COMM$;
COMMENT ON COLUMN ais.tk_duplist.userisn IS $COMM$кто отметил$COMM$;
COMMENT ON COLUMN ais.tk_duplist.estim IS $COMM$оценка$COMM$;
COMMENT ON COLUMN ais.tk_duplist.loaded IS $COMM$дата записи в таблицу$COMM$;


CREATE TABLE ais.tk_duplist_new (
    isn                              NUMERIC,
    mark                             VARCHAR(1),
    fullname                         VARCHAR(255),
    created                          TIMESTAMP DEFAULT current_timestamp,
    changed                          TIMESTAMP,
    remark                           VARCHAR(255),
    totsum                           NUMERIC,
    datepay                          TIMESTAMP,
    userisn                          NUMERIC,
    estim                            NUMERIC,
    loaded                           TIMESTAMP DEFAULT current_timestamp,
    synisn                           NUMERIC,
    id                               VARCHAR(20),
    source                           VARCHAR(40)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.tk_duplist_new IS $COMM$Крылов:  новые клиенты, введенные в центр.офисе и on-line  (АРМ КК) $COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.synisn IS $COMM$синоним$COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.id IS $COMM$идентификатор$COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.source IS $COMM$источник$COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.isn IS $COMM$FK: Subject$COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.mark IS $COMM$отметка$COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.fullname IS $COMM$клиент$COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.changed IS $COMM$дата изменения$COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.totsum IS $COMM$сумма на дату включения$COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.datepay IS $COMM$дата включения$COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.userisn IS $COMM$кто изменил$COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.estim IS $COMM$оценка$COMM$;
COMMENT ON COLUMN ais.tk_duplist_new.loaded IS $COMM$дата записи в таблицу$COMM$;


CREATE TABLE ais.tourmedbordero (
    isn                              NUMERIC,
    id                               VARCHAR(20),
    name                             VARCHAR(255),
    description                      VARCHAR(255),
    dateevent                        TIMESTAMP,
    flag                             VARCHAR(1),
    qualid                           NUMERIC,
    qualname                         NUMERIC,
    qnt                              SMALLINT,
    exlrow                           INT,
    agrid                            VARCHAR(20),
    objname                          VARCHAR(255),
    extid                            VARCHAR(20),
    claimsum                         NUMERIC(20,2),
    refundsum                        NUMERIC(20,2),
    status                           VARCHAR(1),
    remark                           VARCHAR(255),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    refundisn                        NUMERIC,
    claimisn                         NUMERIC,
    agrisn                           NUMERIC,
    objisn                           NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    subjisn                          NUMERIC,
    parentisn                        NUMERIC,
    objnom                           VARCHAR(20),
    claims                           NUMERIC(38),
    refunds                          NUMERIC(38),
    active                           VARCHAR(1),
    country                          VARCHAR(40),
    territory                        VARCHAR(40),
    sum1                             NUMERIC,
    sum2                             NUMERIC,
    curr1                            VARCHAR(4),
    curr2                            VARCHAR(4),
    descrus                          VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.tourmedbordero IS $COMM$начальное хранилище бордеро  мед.убытковтуристов (Крылов В.)$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.descrus IS $COMM$описание по-русски$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.id IS $COMM$ID из бордеро или имя бордеро$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.name IS $COMM$Застрахов. из бордеро$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.description IS $COMM$Описание убытка$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.dateevent IS $COMM$дата страхового случ.$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.flag IS $COMM$Y -пpивязано по ID и Name, N -1 найденная запись и что-то неточно соответсвует, M-то же, но неск.записей$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.qualid IS $COMM$Степень совпадения написания ID$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.qualname IS $COMM$Степень совпадения написания фамилий$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.qnt IS $COMM$Кол-во найденных вариантов$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.exlrow IS $COMM$N строки в исходном xls-файле$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.agrid IS $COMM$ID из найденного договора$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.objname IS $COMM$Имя застрах.из найденного договора$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.extid IS $COMM$внешний реф.$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.claimsum IS $COMM$убыток$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.status IS $COMM$A - привязано по номеру договора, B - привязано расш.номеру, C - - привязано по имени$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.remark IS $COMM$путь к файлу бордеро или список шаблонов для поиска$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.claimisn IS $COMM$убыток$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.agrisn IS $COMM$найденный договор$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.objisn IS $COMM$найденный объект$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.datebeg IS $COMM$гран.страхования$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.subjisn IS $COMM$заявитель$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.objnom IS $COMM$номер в списке застрахов.$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.claims IS $COMM$кол-во найденных клаймов$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.refunds IS $COMM$кол-во найденных убытков$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.country IS $COMM$страна$COMM$;
COMMENT ON COLUMN ais.tourmedbordero.territory IS $COMM$территория$COMM$;


CREATE TABLE ais.wf_action (
    eventclassisn                    NUMERIC,
    sqlaction                        VARCHAR(1999),
    active                           VARCHAR(1),
    remark                           VARCHAR(1999),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    isn                              NUMERIC,
    processisn                       NUMERIC,
    email                            VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN ais.wf_action.isn IS $COMM$(PK) SEQ_WFACTION$COMM$;
COMMENT ON COLUMN ais.wf_action.processisn IS $COMM$(FK)Dicti Подписчик для связки событие-действие$COMM$;
COMMENT ON COLUMN ais.wf_action.email IS $COMM$Адрес для рассылки ошибок в работе функции$COMM$;


CREATE TABLE ais.wrkorder (
    isn                              NUMERIC,
    motorisn                         NUMERIC,
    deptisn                          NUMERIC,
    ordererisn                       NUMERIC,
    routeisn                         NUMERIC,
    route                            VARCHAR(1000),
    passenger                        VARCHAR(255),
    datecheck                        TIMESTAMP,
    privateflg                       VARCHAR(1),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.wrkorder IS $COMM$Заявка на автотранспорт, расширение WRKTIME$COMM$;
COMMENT ON COLUMN ais.wrkorder.isn IS $COMM$FK(WRKTIME). Машинный номер  SEQ_WRKTIME.nextval$COMM$;
COMMENT ON COLUMN ais.wrkorder.motorisn IS $COMM$FK(DICTI). Указатель автомобиля$COMM$;
COMMENT ON COLUMN ais.wrkorder.deptisn IS $COMM$FK(SUBDEPT). Указатель подразделения заказчика$COMM$;
COMMENT ON COLUMN ais.wrkorder.ordererisn IS $COMM$FK(SUBHUMAN). Указатель заказчика$COMM$;
COMMENT ON COLUMN ais.wrkorder.routeisn IS $COMM$FK(DICTI). Указатель типового маршрута$COMM$;
COMMENT ON COLUMN ais.wrkorder.route IS $COMM$Маршрут. Пункты маршрута отделяются концом строки$COMM$;
COMMENT ON COLUMN ais.wrkorder.passenger IS $COMM$Пассажиры$COMM$;
COMMENT ON COLUMN ais.wrkorder.datecheck IS $COMM$Контрольное время прибытия в пункт посадки пассажиров$COMM$;
COMMENT ON COLUMN ais.wrkorder.privateflg IS $COMM$Индикатор личной поездки (Y), null-служебная$COMM$;
COMMENT ON COLUMN ais.wrkorder.remark IS $COMM$Примечание: назначение поездки$COMM$;
COMMENT ON COLUMN ais.wrkorder.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.wrkorder.updatedby IS $COMM$Автор изменения$COMM$;


CREATE TABLE ais.wrktime (
    isn                              NUMERIC,
    classisn                         NUMERIC,
    emplisn                          NUMERIC,
    prjisn                           NUMERIC,
    datework                         TIMESTAMP,
    duration                         NUMERIC,
    status                           VARCHAR(1),
    remark                           VARCHAR(1000),
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    discr                            VARCHAR(1),
    dateworkend                      TIMESTAMP,
    durationmax                      NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE ais.wrktime IS $COMM$Работа некоторого рода над указанным проектом, выполненная сотрудником за определенное время$COMM$;
COMMENT ON COLUMN ais.wrktime.isn IS $COMM$Машинный номер объекта. Устанавливается по умолчанию равным SEQ_WRKTIME.nextval$COMM$;
COMMENT ON COLUMN ais.wrktime.classisn IS $COMM$FK(DICTI). Указатель вида деятельности$COMM$;
COMMENT ON COLUMN ais.wrktime.emplisn IS $COMM$FK(SUBHUMAN). Указатель исполнителя$COMM$;
COMMENT ON COLUMN ais.wrktime.prjisn IS $COMM$FK(WRKPROJECT). Указатель проекта$COMM$;
COMMENT ON COLUMN ais.wrktime.datework IS $COMM$Дата произведения работы$COMM$;
COMMENT ON COLUMN ais.wrktime.duration IS $COMM$Продолжительность работы в часах$COMM$;
COMMENT ON COLUMN ais.wrktime.status IS $COMM$Статус записи: N-не завизирована, null-завизирована руководителем$COMM$;
COMMENT ON COLUMN ais.wrktime.remark IS $COMM$Примечание$COMM$;
COMMENT ON COLUMN ais.wrktime.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN ais.wrktime.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN ais.wrktime.discr IS $COMM$Тип W-запись учёта рабочего времени (Yunin V.A. 15.01.04)$COMM$;
COMMENT ON COLUMN ais.wrktime.dateworkend IS $COMM$Дата окончания Yunin V.A. 13/07/04$COMM$;
COMMENT ON COLUMN ais.wrktime.durationmax IS $COMM$Максимальная продолжительность. Yunin V.A. 13/07/04$COMM$;


CREATE TABLE aisadm.subuser (
    isn                              NUMERIC,
    userpassword                     VARCHAR(40)VARCHAR(40),
    status                           VARCHAR(1),
    dateend                          TIMESTAMP,
    updated                          TIMESTAMP DEFAULT current_timestamp,
    updatedby                        NUMERIC,
    netlogin                         VARCHAR(20),
    keyid                            VARCHAR(32),
    pwdchanged                       TIMESTAMP,
    retrycount                       NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE aisadm.subuser IS $COMM$Физическое лицо - пользователь$COMM$;
COMMENT ON COLUMN aisadm.subuser.pwdchanged IS $COMM$Дата последнего изменения пароля$COMM$;
COMMENT ON COLUMN aisadm.subuser.isn IS $COMM$FK(SUBHUMAN). Машинный номер физического лица$COMM$;
COMMENT ON COLUMN aisadm.subuser.userpassword IS $COMM$Пароль, если физлицо является пользователем системы.
Хранится в закодированном виде.$COMM$;
COMMENT ON COLUMN aisadm.subuser.status IS $COMM$Флаг разрешение работы: Y-работа разрешена до DATEEND, N-работа приостановлена до DATEEND, null-работа запрещена$COMM$;
COMMENT ON COLUMN aisadm.subuser.dateend IS $COMM$Дата окончания действия STATUS$COMM$;
COMMENT ON COLUMN aisadm.subuser.updated IS $COMM$Дата изменения$COMM$;
COMMENT ON COLUMN aisadm.subuser.updatedby IS $COMM$Автор изменения$COMM$;
COMMENT ON COLUMN aisadm.subuser.netlogin IS $COMM$Логин пользователя для входа в сеть,
имя почтового ящика, имя для выхода в интернет и т.д$COMM$;
COMMENT ON COLUMN aisadm.subuser.keyid IS $COMM$идентификатор аппаратного ключа$COMM$;


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


CREATE TABLE life.agenthist (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    agrisn                           NUMERIC,
    agentlevel                       NUMERIC,
    agentclass                       NUMERIC,
    jurstatus                        VARCHAR(1),
    begindate                        TIMESTAMP,
    enddate                          TIMESTAMP,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    commisionown                     NUMERIC(10,4),
    commisionover                    NUMERIC(10,4),
    overisn                          NUMERIC,
    agrtemplateisn                   NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE life.agenthist IS $COMM$Таблица расширяет SUBJECT для хранения дополнительных характеристик агента. Содержит историю взаимоотношений с агентом (изменение статуса в рамках агентских соглашений) $COMM$;
COMMENT ON COLUMN life.agenthist.subjisn IS $COMM$ISN агента (ссылка на SUBJECT)$COMM$;
COMMENT ON COLUMN life.agenthist.agrisn IS $COMM$Ссылка на агентское соглашение в рамках которого проходили изменения характеристик агента.$COMM$;
COMMENT ON COLUMN life.agenthist.agentlevel IS $COMM$Категория агента. Сссылка на DICTI. Категории хрянятся в ветке  "Страховая деятельность - Расчеты с агентами - Категория получателя - Физические лица" и  "Страховая деятельность - Расчеты с агентами - Категория получателя - Юридические лица$COMM$;
COMMENT ON COLUMN life.agenthist.agentclass IS $COMM$Классность получателя. Ссылка на DICTI ("Страховая деятельность - Расчеты с агентами - Класс получателя")$COMM$;
COMMENT ON COLUMN life.agenthist.jurstatus IS $COMM$Юридический статус агента.
Для физического лица может принимать значения:
П - ПБОЮЛ
Н - не ПБОЮЛ
Для юридического лица:
Ю - юр. лицо$COMM$;
COMMENT ON COLUMN life.agenthist.begindate IS $COMM$Дата начала действия характеристик агента.$COMM$;
COMMENT ON COLUMN life.agenthist.enddate IS $COMM$Дата окончания действия характеристик агента.$COMM$;
COMMENT ON COLUMN life.agenthist.updated IS $COMM$Дата изменения записи$COMM$;
COMMENT ON COLUMN life.agenthist.updatedby IS $COMM$Автор изменения записи$COMM$;


CREATE TABLE life.lifaccount (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    parentisn2                       NUMERIC,
    amount                           NUMERIC(23,5),
    amountagrcur                     NUMERIC(23,5),
    amountrub                        NUMERIC(23,5),
    sumdate                          TIMESTAMP,
    classisn                         NUMERIC,
    operationtype                    VARCHAR(1),
    docsumisn                        NUMERIC,
    agrisn                           NUMERIC,
    cagrisn                          NUMERIC,
    currisn                          NUMERIC,
    analyticvalue1                   VARCHAR(64),
    analyticvalue2                   VARCHAR(64),
    analyticvalue3                   VARCHAR(64),
    analyticvalue4                   VARCHAR(64),
    analyticvalue5                   VARCHAR(64),
    analyticvalue6                   VARCHAR(64),
    analyticvalue7                   VARCHAR(64),
    analyticvalue8                   VARCHAR(64),
    analyticvalue9                   VARCHAR(64),
    analyticvalue10                  VARCHAR(64),
    analyticvalue11                  VARCHAR(64),
    analyticvalue12                  VARCHAR(64),
    analyticvalue13                  VARCHAR(64),
    analyticvalue14                  VARCHAR(64),
    analyticvalue15                  VARCHAR(64),
    analyticisn1                     NUMERIC,
    analyticisn2                     NUMERIC,
    analyticisn3                     NUMERIC,
    analyticisn4                     NUMERIC,
    analyticisn5                     NUMERIC,
    analyticisn6                     NUMERIC,
    analyticisn7                     NUMERIC,
    analyticisn8                     NUMERIC,
    analyticisn9                     NUMERIC,
    analyticisn10                    NUMERIC,
    analyticisn11                    NUMERIC,
    analyticisn12                    NUMERIC,
    analyticisn13                    NUMERIC,
    analyticisn14                    NUMERIC,
    analyticisn15                    NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    commenttext                      VARCHAR(255)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE life.lifaccount IS $COMM$Отражает движение по лицевым счетам, связанным договором страхования жизни. $COMM$;
COMMENT ON COLUMN life.lifaccount.parentisn IS $COMM$Ссылка на логически связанную запись (сторнируемая запись, корпоративное начисление для сертификата).$COMM$;
COMMENT ON COLUMN life.lifaccount.amount IS $COMM$Cумма$COMM$;
COMMENT ON COLUMN life.lifaccount.amountagrcur IS $COMM$Сумма в валюте договора$COMM$;
COMMENT ON COLUMN life.lifaccount.amountrub IS $COMM$Сумма в рублях$COMM$;
COMMENT ON COLUMN life.lifaccount.sumdate IS $COMM$Дата операции$COMM$;
COMMENT ON COLUMN life.lifaccount.classisn IS $COMM$Ссылка на тип суммы первого уровня. Определяет тип лицевого счета$COMM$;
COMMENT ON COLUMN life.lifaccount.operationtype IS $COMM$Тип операции - начисление (K) или списание(D)$COMM$;
COMMENT ON COLUMN life.lifaccount.docsumisn IS $COMM$Ссылка на DocSum$COMM$;
COMMENT ON COLUMN life.lifaccount.agrisn IS $COMM$Ссылка на договор$COMM$;
COMMENT ON COLUMN life.lifaccount.cagrisn IS $COMM$Ссылка на корпоративный договор (ISN нулевого аддендума корпоративного договора) для операций сертификатов$COMM$;
COMMENT ON COLUMN life.lifaccount.currisn IS $COMM$Валюта$COMM$;
COMMENT ON COLUMN life.lifaccount.analyticvalue1 IS $COMM$Данное и все остальные аналогичные поля отведены под значения аналитических признаков.
Замечание. Первый аналитический признак - это всегда договор.$COMM$;
COMMENT ON COLUMN life.lifaccount.analyticisn1 IS $COMM$Содержит ИСН записи для AnalyticValue 1. Если описание аналитического признака ссылается на мета-описание простого параметра, то NULL$COMM$;


CREATE TABLE life.lifagrclaimext (
    isn                              NUMERIC,
    agrclaimisn                      NUMERIC,
    losseventisn                     NUMERIC,
    adjustmenteventisn               NUMERIC,
    claimtype                        NUMERIC(38),
    claimriskisn                     NUMERIC,
    insuredisn                       NUMERIC,
    regtype                          NUMERIC,
    reqsum                           NUMERIC(11,2),
    accsum                           NUMERIC(11,2),
    sertificateisn                   NUMERIC,
    releasestart                     TIMESTAMP,
    releaseend                       TIMESTAMP,
    docisn                           NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE life.lifagrclaimext IS $COMM$Расширяет стандартный паспорт убытка для описания деталей специфичных для убытков по договорам страхования жизни.$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.docisn IS $COMM$ISN общего счета-платежки к которому прикреплено начисление этого убытка$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.losseventisn IS $COMM$Ссылка на экземпляр события "Страховой случай",  соотвествующий убытку, в очереди событий соотвествующего аддендума.$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.adjustmenteventisn IS $COMM$Ссылка на экземпляр события "Урегулировать",  соотвествующий убытку, в очереди событий соотвествующего аддендума.$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.claimtype IS $COMM$0 - страховой случай
1 - выплата выкупной суммы
2 - выдача судды под залог выкупной суммы$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.claimriskisn IS $COMM$Тип риск, по которому произошел страховой случай. В страхованиии жизни любой страховой случай однозначно связан с риском. Для убытков типа "выплата выкупной суммы" это поле NULL.$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.insuredisn IS $COMM$Ссылка на застрахованного, по которому произошел страховой случай. Для убытков типа "выплата выкупной суммы" это поле NULL.
Если убыток относится к  коллективному договору, то это застрахованный по сертификату, определяемому полем SertificateISN$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.regtype IS $COMM$Тип урегулирования, заполняется автоматически$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.reqsum IS $COMM$Заявленная сумма$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.accsum IS $COMM$Сумма возмещения$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.sertificateisn IS $COMM$Ссылка на сертификат. Имеет смысл для коллективного договора$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.releasestart IS $COMM$Дата начала периода освобождения от уплаты взносов$COMM$;
COMMENT ON COLUMN life.lifagrclaimext.releaseend IS $COMM$Дата окончания периода освобождения от уплаты взносов$COMM$;


CREATE TABLE life.lifagrext (
    isn                              NUMERIC,
    lifvariantisn                    NUMERIC,
    lifstatusgraphisn                NUMERIC,
    currentstatusisn                 NUMERIC,
    nextpremiumdate                  TIMESTAMP,
    nextpremiumsumm                  NUMERIC(23,5),
    nextpayoutdate                   TIMESTAMP,
    nextpayoutsumm                   NUMERIC(23,5),
    underwritingflag                 NUMERIC(38) DEFAULT 0,
    reinssettledflag                 NUMERIC(38) DEFAULT 0,
    changeswantedflag                NUMERIC(38) DEFAULT 0,
    defaultpayformisn                NUMERIC,
    addtype                          NUMERIC,
    addisn                           NUMERIC,
    previsn                          NUMERIC,
    prevagrisn                       NUMERIC,
    nextisn                          NUMERIC,
    corporateagrisn                  NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    discr                            VARCHAR(1),
    extisn                           NUMERIC,
    creationdate                     TIMESTAMP DEFAULT current_timestamp,
    id                               VARCHAR(30),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    productisn                       NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE life.lifagrext IS $COMM$Расширяет таблицу договора для хранения договоров и вариантов страхования жизни.
Таблица содержит сертификаты к коллективному договору наряду с самим коллективным договором. При создании аддендума к коллективному договору создаются новые записи только для аддендумов к тем сертификатам, что реально изменились. Для того, чтобы получить все действующие сертификаты на текущий аддендум, необходимо выбрать все записи, у которых NextISN-null  и которые ссылаются на нулевой аддендум к коллективному договору по CorporateAgrISN. При удалении сертификата в аддендуме коллективного договора NextISN ссылается на последний аддендум к сертификату, т.е. такой аддендум ссылается сам на себя$COMM$;
COMMENT ON COLUMN life.lifagrext.isn IS $COMM$Ссылка на договор в Agreement. Нумерация сквозная, т.е. данный ключ генерируется той же последовательностью, что и ИСН в Agreement.$COMM$;
COMMENT ON COLUMN life.lifagrext.lifvariantisn IS $COMM$Ссылка на вариант страхования для договора. Для варианта (то есть договора в статусе "макет") запись ссылается сама на себя. При создании договора по макету значение этого поля просто не меняется.$COMM$;
COMMENT ON COLUMN life.lifagrext.lifstatusgraphisn IS $COMM$Ссылка на граф переходов статусов для данного договора.$COMM$;
COMMENT ON COLUMN life.lifagrext.currentstatusisn IS $COMM$Текущее состояние. Поля Agreement.Status и Agreement.Discr должны автоматически синхронизироваться.$COMM$;
COMMENT ON COLUMN life.lifagrext.nextpremiumdate IS $COMM$Дата ближайшей премии. Присваивается деревом расчета.$COMM$;
COMMENT ON COLUMN life.lifagrext.nextpremiumsumm IS $COMM$Сумма ближайшей премии. Присваивается деревом расчета$COMM$;
COMMENT ON COLUMN life.lifagrext.nextpayoutdate IS $COMM$Дата ближайшей выплаты для договоров страхования жизни с выплатой ренты. Присваивает$COMM$;
COMMENT ON COLUMN life.lifagrext.nextpayoutsumm IS $COMM$Сумма ближайшей рентной выплаты для договоров страхования жизни с выплатой ренты.  Присваивается деревом расчета$COMM$;
COMMENT ON COLUMN life.lifagrext.underwritingflag IS $COMM$Флаг, показывающий, что  андеррайтинг завершен. Присваивается деревом расчета. Может быть установлен принудительно $COMM$;
COMMENT ON COLUMN life.lifagrext.reinssettledflag IS $COMM$Флаг, показывающий, что  договоренность с перестраховщиком достигнута.  Присваивается деревом расчета. Может быть установлен принудительно $COMM$;
COMMENT ON COLUMN life.lifagrext.changeswantedflag IS $COMM$Флаг, показывающий, что требуется изменение условий договора. Справочное поле. Присваивается деревом события "Завершить андеррайтинг" или $COMM$;
COMMENT ON COLUMN life.lifagrext.defaultpayformisn IS $COMM$Ссылка на Дикти (Взаиморасчеты - форма оплаты). Используется при автоматическом формировании счетов и платежных документов. По умолчанию - то, что соответствует безналичному расчету$COMM$;
COMMENT ON COLUMN life.lifagrext.addtype IS $COMM$тип аддендума - сумма значений поля Code (Ветка Dicti: Договоры страхования - Тип аддендума - типы аддендумов страхования жизни) по всем выбранным на формуляре типам.$COMM$;
COMMENT ON COLUMN life.lifagrext.addisn IS $COMM$Для сертификата - ссылка на аддендум к коллективному договору$COMM$;
COMMENT ON COLUMN life.lifagrext.previsn IS $COMM$Предыдущий аддендум - поле, аналогичное одноименному в Agreement. Для коллективного договора избыточно$COMM$;
COMMENT ON COLUMN life.lifagrext.nextisn IS $COMM$Для сертификата - ссылка на изменивший его аддендум$COMM$;
COMMENT ON COLUMN life.lifagrext.corporateagrisn IS $COMM$Ссылка на коллективный договор (его 0 - й аддендум) для сертификата$COMM$;
COMMENT ON COLUMN life.lifagrext.datebeg IS $COMM$Дата начала аддендума. Кроме сертификатов, это поле избыточно по отношению к Agreement$COMM$;
COMMENT ON COLUMN life.lifagrext.dateend IS $COMM$Дата окончания аддендума.  Кроме сертификатов, это поле избыточно по отношению к Agreement$COMM$;
COMMENT ON COLUMN life.lifagrext.discr IS $COMM$Дискриминатор (английские буквы) "C" - corporate "P" - personal "S" - sertificate$COMM$;
COMMENT ON COLUMN life.lifagrext.extisn IS $COMM$Автоматически генерируемый первичный ключ$COMM$;
COMMENT ON COLUMN life.lifagrext.creationdate IS $COMM$Дата создания аддендума, должна быть одинакова для всех аддендумов к сертификатам, которые созданы в рамках аддендума к коллективному договору. Должна быть не больше даты начала аддендума.$COMM$;
COMMENT ON COLUMN life.lifagrext.id IS $COMM$Для нулевого аддендума к сертификату - номер сертификата в соответствии с правилами формирования номеров.
Нумерация сертификатов: номер сертификата состоит из номера договора без аддендума, разделителя в виде "/" и  номера сертификата внутри договора. Пример "УСЖСРОЧН:1934/154", где "УСЖСРОЧН" - код Варианта страхования.
Для ненулевого аддендума к сертификату номер аддендума.
Для коллективного и индивидуального  договоров данное поле избыточно.
Номер аддендума к сертификату не пристыковывается к номеру договора, как в случае с индивидуальным договором.
Для записей, описывающих базовый(ые) Вариант(ы) страхования, принимает значение "BASE$COMM$;


CREATE TABLE life.lifagroption (
    isn                              NUMERIC,
    ismandatory                      SMALLINT DEFAULT 0,
    agrisn                           NUMERIC,
    agrriskisn                       NUMERIC,
    name                             VARCHAR(64),
    remark                           VARCHAR(255),
    dispname                         VARCHAR(64),
    dependent                        SMALLINT DEFAULT 0,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE life.lifagroption IS $COMM$Описывает допустимые опции в договоре/Варианте страхования.  $COMM$;
COMMENT ON COLUMN life.lifagroption.ismandatory IS $COMM$Указывает, обязательна ли данная опция в договоре страхования.
0 - необязательная
1 - обязательная$COMM$;
COMMENT ON COLUMN life.lifagroption.agrisn IS $COMM$Связь опции с договором или вариантом страхования.$COMM$;
COMMENT ON COLUMN life.lifagroption.agrriskisn IS $COMM$Указывает на риск данной опции. Поле, на которое указывает ссылка может быть как реальным риском так и фиктивным риском - неразделимой совокупностью  рисков.$COMM$;
COMMENT ON COLUMN life.lifagroption.name IS $COMM$Имя опции. Уникально в пределах Варианта.$COMM$;
COMMENT ON COLUMN life.lifagroption.dispname IS $COMM$Имя, которое показывается в пользовательском интерфейсе.$COMM$;
COMMENT ON COLUMN life.lifagroption.dependent IS $COMM$Для необязательных опций - признак зависимости от основной опции
1 - зависимая
0 - независимая$COMM$;


CREATE TABLE life.lifagrrefund (
    isn                              NUMERIC,
    lifagrclaimextisn                NUMERIC,
    refundsum                        NUMERIC(20,2),
    lifagroptionisn                  NUMERIC,
    agrrefundisn                     NUMERIC,
    lifagrextisn                     NUMERIC,
    lifaddisn                        NUMERIC,
    flagv                            NUMERIC,
    flaga                            NUMERIC,
    flagd                            NUMERIC,
    singlepayment                    NUMERIC(11,2),
    isarranged                       SMALLINT DEFAULT 0,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    paypercent                       NUMERIC,
    status                           NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE life.lifagrrefund IS $COMM$Претензия по убытку в страховании жизни. В некоторых случаях может отображаться в стандартную претензию.$COMM$;
COMMENT ON COLUMN life.lifagrrefund.status IS $COMM$Статус претензии. 0 - Отказ$COMM$;
COMMENT ON COLUMN life.lifagrrefund.lifagrclaimextisn IS $COMM$Ссылка на убыток по которому произошла эта претензия.$COMM$;
COMMENT ON COLUMN life.lifagrrefund.refundsum IS $COMM$Сумма возмещения данной претензии. Заполнено только для претензий с типом урегулирования - единичная выплата.$COMM$;
COMMENT ON COLUMN life.lifagrrefund.lifagroptionisn IS $COMM$Опция, с которой связана претензия.$COMM$;
COMMENT ON COLUMN life.lifagrrefund.agrrefundisn IS $COMM$Ссылка на стандартную претензию АИС. Имеет смысл только для претензий урегулирующихся единичной выплатой.
Возможно потребуется для совместимости. Пока всегда NULL.$COMM$;
COMMENT ON COLUMN life.lifagrrefund.lifagrextisn IS $COMM$Ссылка на договор или аддендум, который урегулировал данную претензию. Имеет смысл только для претезий, которые урегулируются через аддендум или договор.$COMM$;
COMMENT ON COLUMN life.lifagrrefund.singlepayment IS $COMM$Сумма единичной выплаты$COMM$;
COMMENT ON COLUMN life.lifagrrefund.isarranged IS $COMM$Урегулирована (1)$COMM$;
COMMENT ON COLUMN life.lifagrrefund.paypercent IS $COMM$Процент выплаты от страховой суммы для убытков Страховой случай$COMM$;


CREATE TABLE life.lifagrrefundext (
    isn                              NUMERIC,
    lifrefundisn                     NUMERIC,
    code                             VARCHAR(20),
    description                      VARCHAR(255),
    qnt                              NUMERIC(8,4),
    classisn                         NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifagrrole (
    isn                              NUMERIC,
    parentisn                        NUMERIC,
    agrisn                           NUMERIC,
    roletype                         SMALLINT,
    name                             VARCHAR(255),
    remark                           VARCHAR(255),
    isinsuredrole                    SMALLINT,
    variantroleisn                   NUMERIC,
    objectisn                        NUMERIC,
    dispname                         VARCHAR(64),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE life.lifagrrole IS $COMM$Описывает структуру ролей в договоре страхования.$COMM$;
COMMENT ON COLUMN life.lifagrrole.parentisn IS $COMM$Ссылка на родительскую роль. Для подержания иерархической структуры ролей.$COMM$;
COMMENT ON COLUMN life.lifagrrole.agrisn IS $COMM$Ссылка на договор или Вариант страхования$COMM$;
COMMENT ON COLUMN life.lifagrrole.roletype IS $COMM$1 - именнованная роль
2 - груповая роль
3 - член групповой роли$COMM$;
COMMENT ON COLUMN life.lifagrrole.name IS $COMM$Имя роли$COMM$;
COMMENT ON COLUMN life.lifagrrole.remark IS $COMM$Описание роли$COMM$;
COMMENT ON COLUMN life.lifagrrole.variantroleisn IS $COMM$Ссылка на метаописание роли - запись о роли, связанную с Вариантом страхования (договором в статусе "макет"). Запись метаописания ассоциации через этот параметр ссылается сама на себя.
Это поле введено для облегчения реализации обработки деревьев расчета, а так же для доступа к возможным ассоциациям из метаописания.$COMM$;
COMMENT ON COLUMN life.lifagrrole.objectisn IS $COMM$Ссылка на объект в договоре страхования. $COMM$;
COMMENT ON COLUMN life.lifagrrole.dispname IS $COMM$Имя, которое показывается в пользовательском интерфейсе.$COMM$;


CREATE TABLE life.lifapplication (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    id                               VARCHAR(20),
    originaldoc                      VARCHAR(1) DEFAULT 'C',
    statusisn                        NUMERIC,
    rule                             VARCHAR(1) DEFAULT 'L',
    dateook                          TIMESTAMP,
    dateais                          TIMESTAMP,
    datepay                          TIMESTAMP,
    paysummrub                       NUMERIC(20,2),
    payno                            NUMERIC(38),
    paytype                          VARCHAR(1) DEFAULT 'O',
--WARNING: In table life.lifapplication column decode matches Greenplum keyword. Corrected to decode_.
    decode_                          NUMERIC(38) DEFAULT 0,
    reinsby                          NUMERIC(38) DEFAULT 0,
    dateapplrecieve                  TIMESTAMP,
    datepolicrecieve                 TIMESTAMP,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    systemstatus                     VARCHAR(1) DEFAULT 'A',
    appldate                         TIMESTAMP,
    checkdate                        TIMESTAMP
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifapplremark (
    isn                              NUMERIC,
    applisn                          NUMERIC,
    remark                           VARCHAR(1000),
    dateadd                          TIMESTAMP,
    addby                            NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifapplstathist (
    isn                              NUMERIC,
    applisn                          NUMERIC,
    statusisn                        NUMERIC,
    dateset                          TIMESTAMP,
    setby                            NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifassociation (
    isn                              NUMERIC,
    optionisn                        NUMERIC,
    roleisn                          NUMERIC,
    ismandatory                      SMALLINT,
    name                             VARCHAR(64),
    remark                           VARCHAR(255),
    variantoptionroleisn             NUMERIC,
    dispname                         VARCHAR(64),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE life.lifassociation IS $COMM$Ассоциация роли и опции. Соотносит роли договора страхования с опциями.$COMM$;
COMMENT ON COLUMN life.lifassociation.ismandatory IS $COMM$Указывает, обязательна ли данная опция для роли.$COMM$;
COMMENT ON COLUMN life.lifassociation.variantoptionroleisn IS $COMM$Ссылка на метаописание ассоциации опции и роли - запись о ассоциации связанную с вариантом страхования (договором в статусе макет). Запись метаописания ассоциации через этот параметр ссылается сама на себя.
Это поле введено для облегчения реализации обработки деревьев расчета.$COMM$;
COMMENT ON COLUMN life.lifassociation.dispname IS $COMM$Имя, которое показывается в пользовательском интерфейсе.$COMM$;


CREATE TABLE life.lifbenefreq (
    isn                              NUMERIC(22),
    regno                            VARCHAR(12),
    extno                            VARCHAR(12),
    status                           NUMERIC,
    reqdate                          TIMESTAMP,
    regdate                          TIMESTAMP,
    lifagrclaimext                   NUMERIC(22),
    benefisn                         NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN life.lifbenefreq.isn IS $COMM$Порядковый ISN$COMM$;
COMMENT ON COLUMN life.lifbenefreq.regno IS $COMM$Регистрационный номер$COMM$;
COMMENT ON COLUMN life.lifbenefreq.extno IS $COMM$Внешний номер заявления$COMM$;
COMMENT ON COLUMN life.lifbenefreq.status IS $COMM$0-template, 1-accepted,2-rejected$COMM$;
COMMENT ON COLUMN life.lifbenefreq.reqdate IS $COMM$Дата заявления
$COMM$;
COMMENT ON COLUMN life.lifbenefreq.regdate IS $COMM$Дата регистрации$COMM$;
COMMENT ON COLUMN life.lifbenefreq.lifagrclaimext IS $COMM$Ссылка на убыток в LIFE$COMM$;
COMMENT ON COLUMN life.lifbenefreq.benefisn IS $COMM$Выгодоприобретатель
ссылка на ISN в SUBJECT$COMM$;


CREATE TABLE life.lifbenefshare (
    isn                              NUMERIC(22),
    lifrefundisn                     NUMERIC(22),
    benefreqisn                      NUMERIC(22),
    reqshare                         NUMERIC(11,2),
    acceptshare                      NUMERIC(11,2),
    topass                           VARCHAR(1),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE life.lifbenefshare IS $COMM$Доли выгодоприобрететателей$COMM$;
COMMENT ON COLUMN life.lifbenefshare.topass IS $COMM$Указывает включать ли данную выкупную сумму в зачет очередного взноса или нет$COMM$;
COMMENT ON COLUMN life.lifbenefshare.isn IS $COMM$Порядковый ISN$COMM$;
COMMENT ON COLUMN life.lifbenefshare.lifrefundisn IS $COMM$Претензия
Ссылка на ISN в lifagrrefund$COMM$;
COMMENT ON COLUMN life.lifbenefshare.benefreqisn IS $COMM$Заявления выгодоприобретателя
Ссылка на ISN в lifbenefreq$COMM$;
COMMENT ON COLUMN life.lifbenefshare.reqshare IS $COMM$Запрошенный процент$COMM$;
COMMENT ON COLUMN life.lifbenefshare.acceptshare IS $COMM$Выделенный процент$COMM$;


CREATE TABLE life.lifempltrust (
    isn                              NUMERIC,
    subjisn                          NUMERIC,
    datebeg                          TIMESTAMP,
    dateend                          TIMESTAMP,
    id                               VARCHAR(20),
    dateissue                        TIMESTAMP,
    agrisn                           NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC,
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    trusttype                        NUMERIC(38)
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE life.lifempltrust IS $COMM$доверенности для сотрудников и агентов$COMM$;
COMMENT ON COLUMN life.lifempltrust.subjisn IS $COMM$ссылка на subject.isn для которого доверенность$COMM$;
COMMENT ON COLUMN life.lifempltrust.datebeg IS $COMM$дата начала доверенности$COMM$;
COMMENT ON COLUMN life.lifempltrust.dateend IS $COMM$дата окончания доверенности$COMM$;
COMMENT ON COLUMN life.lifempltrust.id IS $COMM$номер доверенности$COMM$;
COMMENT ON COLUMN life.lifempltrust.dateissue IS $COMM$дата выдачи$COMM$;
COMMENT ON COLUMN life.lifempltrust.agrisn IS $COMM$ссылка на agreement.isn, к которому относится доверенность$COMM$;
COMMENT ON COLUMN life.lifempltrust.trusttype IS $COMM$Тип доверенности. Ссылка на dicti$COMM$;


CREATE TABLE life.lifeventinstance (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    name                             VARCHAR(64),
    remark                           VARCHAR(255),
    histtype                         SMALLINT,
    exectype                         SMALLINT,
    templateisn                      NUMERIC,
    execdate                         TIMESTAMP,
    execauthor                       NUMERIC,
    status                           SMALLINT,
    wakeupdate                       TIMESTAMP,
    dispname                         VARCHAR(255),
    queuevisibility                  SMALLINT DEFAULT 0,
    exec_in_pers                     SMALLINT DEFAULT 1,
    exec_in_sert                     SMALLINT DEFAULT 1,
    created                          TIMESTAMP,
    createdby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN life.lifeventinstance.wakeupdate IS $COMM$Имеет смысл только для запланиированных событий.
Хранит дату, когда событие должно быть инициировано.$COMM$;
COMMENT ON COLUMN life.lifeventinstance.dispname IS $COMM$Имя, которое показывается в пользовательском интерфейсе.$COMM$;
COMMENT ON COLUMN life.lifeventinstance.queuevisibility IS $COMM$Определяет видимость события в очереди событий:
0 - событие отображается в очереди событий
1 - событие не отображается в очереди событий$COMM$;
COMMENT ON COLUMN life.lifeventinstance.exec_in_pers IS $COMM$1 - видимо 0 - невидимо
Введено для выполнения событий с одинаковыми отображаемыми именами, но различными внутренними. Таким образом, можно определять различное поведение одного и того же события в сертификате и в персональном договоре. Необходимо контролировать, чтобы в одном варианте ( в одной версии) было определено не более двух событий с одинаковыми отображаемыми именами. Одновременно, при наличии таких двух событий не должно быть ситуации, в которой какое-то одно видимо и в персональном договоре, и в сертификате, либо когда хотя бы одно видимо и там, и там. То есть допустима только ситуация, когда при наличии двух событий с одинаковыми отображаемыми именами одно видимо только в персональном, а другое - только в сертификате.$COMM$;
COMMENT ON COLUMN life.lifeventinstance.exec_in_sert IS $COMM$см. комментарий к LifEvetnInstance.Exec_in_pers$COMM$;
COMMENT ON COLUMN life.lifeventinstance.histtype IS $COMM$0 - событие можно удалить из истории после выполнения
1 - событие нельзя удалять из истории после исполнения$COMM$;
COMMENT ON COLUMN life.lifeventinstance.exectype IS $COMM$0 - автоматическое выполнение
1 - пользовательская инициация$COMM$;
COMMENT ON COLUMN life.lifeventinstance.templateisn IS $COMM$Метаописание (для события в очереди)$COMM$;
COMMENT ON COLUMN life.lifeventinstance.status IS $COMM$1 - выполнено
3 - запланировано$COMM$;


CREATE TABLE life.lifletterskz (
    isn                              NUMERIC,
    regdate                          TIMESTAMP,
    id                               VARCHAR(20),
    signby                           NUMERIC,
    method                           VARCHAR(100),
    issend                           SMALLINT DEFAULT 0,
    message                          VARCHAR(1000),
    agrisn                           NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifparamval (
    isn                              NUMERIC,
    valtype                          SMALLINT,
    name                             VARCHAR(255),
    parentisn                        NUMERIC,
    paramval                         VARCHAR(255),
    owner                            NUMERIC,
    flagsummary                      VARCHAR(6),
    remark                           VARCHAR(255),
    ownerisn                         NUMERIC,
    variantparamisn                  NUMERIC,
    dispname                         VARCHAR(255),
    paramnum                         SMALLINT DEFAULT 0,
    isopen                           SMALLINT DEFAULT 0,
    unerasable                       VARCHAR(1) DEFAULT '0',
    visibleonsimple                  VARCHAR(1) DEFAULT '0',
    editmode                         SMALLINT DEFAULT 0,
    rpsname                          VARCHAR(32),
    rps_observed                     NUMERIC(38),
    paramvala                        VARCHAR(255),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC
)
DISTRIBUTED BY (isn);

COMMENT ON TABLE life.lifparamval IS $COMM$Служит для описания параметров. Одновременно содержит как записи, описывающие структуру данных, так и записи, содержащие непосредственно данные.
$COMM$;
COMMENT ON COLUMN life.lifparamval.valtype IS $COMM$Тип параметра. Различают базовые, так называемые супертипы, которые определяют структуру записей в таблицах и просто типы, которые накладывают дополнительные ограничения на значение полей в структуре записей, определяемых супертипом.
Ниже следует перечисление супертипов:
- целое число
- действительное число
- дата
- строка
- масив. Существуют записи, ссылающиеся на эту по ParentSN. Для данных эта запись -  первый элемент массива. Второй элемент массива ссылается на
- ISN Этот тип введен для ссылки на данные в таблицах АИС и во избежание дублирования данных. $COMM$;
COMMENT ON COLUMN life.lifparamval.name IS $COMM$Для параметра, ограничивающего значение другого параметра - имя значения.
Для ограничений здесь хранится имя ограничения.
Для всех остальных имя параметра. Наследуется из метаданных о параметрах.
$COMM$;
COMMENT ON COLUMN life.lifparamval.parentisn IS $COMM$Для элемента массива отличного от первого ссылка на предыдущий элемент массива. Для первого элемента массива ссылка на запись  типа "массив".
Для подэлементов узла  - ссылка на узел.
Для параметра, ограничивающего значение другого параметра, отличного от первого в списке ограничений, - ссылка на предыдущий ограничивающий параметр. Для первого ограничивающего параметра ссылка на параметр, значение которого ограничивается. Такие параметры не связаны с договором, опцией или ролью в опции.
Для всех остальных (то есть для корневых элементов) - null.$COMM$;
COMMENT ON COLUMN life.lifparamval.paramval IS $COMM$Для параметра типа "ограничение" предупреждение, если ограничение нарушено, в противном случае null.
Для мета-данных в этом поле содержится значения по умолчанию или null.
Для данных - значения.
Для параметров, определяющих аналитические признаки, - название поля, в котором содержится искомое значение для признаков типа ИСН (FieldWithNаme) Это то  поле, которое содержит данные, помещаемые в поле AnalyticValue  таблицы LifAccount$COMM$;
COMMENT ON COLUMN life.lifparamval.owner IS $COMM$Определяет вид параметра с точки зрения отнесения к объекту
1 - параметр договора
2 - параметр опции
3 - параметр ассоциации опции и роли
4 - параметр события
5 - параметр, определяющий тип аналитического признака
Может быть NULL в случае, если параметр не соотнесен ни с одной логической сущностью - например, параметр, хранящий старое значение параметра, которое было изменено в результате андерайтинга.$COMM$;
COMMENT ON COLUMN life.lifparamval.flagsummary IS $COMM$Содержит дополнительную информацию для логики работы с параметрами.
Первый символ:
L - параметр имеет перечислимый тип
C - запись является элементом перечислимого типа
W - ограничение параметров
S - системный параметр
I - описание формы для параметра типа ISN
пробел - обычный параметр
Второй символ :
U - параметр редактируется андеррайтером
Третий символ:
А - элемент массива. Служит для того, чтобы отличать последующий элемент массива от детей текущего элемента в случае, когда массив состоит из параметров типа узел.
4 символ:
V - видимый
I - невидимый$COMM$;
COMMENT ON COLUMN life.lifparamval.remark IS $COMM$Описание параметра$COMM$;
COMMENT ON COLUMN life.lifparamval.ownerisn IS $COMM$Ссылка на сущность, к которой относится данный параметр.  В зависимости от значения поля Owner, может ссылаться на договор, опцию или ассоциацию опции и роли.$COMM$;
COMMENT ON COLUMN life.lifparamval.variantparamisn IS $COMM$Ссылка на метаописание данного параметра, то есть на параметр, связанный напрямую или опосредовано (через опцию или ассоциацию) с Вариантом страхования (договором в статусе "макет").
В договоре это поле ссылается на метаописание параметра в Варианте, а в Варианте - на запись в справочнике
Запись метаописания параметра в справочнике через это поле ссылается сама на себя.
Это поле введено для облегчения реализации обработки деревьев расчета.$COMM$;
COMMENT ON COLUMN life.lifparamval.dispname IS $COMM$Имя, которое показывается в пользовательском интерфейсе.$COMM$;
COMMENT ON COLUMN life.lifparamval.paramnum IS $COMM$Порядковый номер параметра при показе. Служит для упорядочивания параметров при показе (для того, чтобы порядок показа параметров был логически осмысленным и эргономически удобным)$COMM$;
COMMENT ON COLUMN life.lifparamval.isopen IS $COMM$При  показе признак того, что параметр по умолчанию "раскрыт". имеет смысл для узлов и массивов.$COMM$;
COMMENT ON COLUMN life.lifparamval.unerasable IS $COMM$Зарезервированный параметр, который обновляется триггером и поэтому никогда не удаляется.$COMM$;
COMMENT ON COLUMN life.lifparamval.visibleonsimple IS $COMM$Видимый пользователю на упрощенной форме договора$COMM$;
COMMENT ON COLUMN life.lifparamval.editmode IS $COMM$редактируется:
0 - никогда
1- всегда
3 - в зависимости от состояния$COMM$;


CREATE TABLE life.lifreins (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    daterequest                      TIMESTAMP,
    daterecieve                      TIMESTAMP,
    estimation                       VARCHAR(255),
    specials                         VARCHAR(255),
    remarks                          VARCHAR(255),
    datesendadd                      TIMESTAMP
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifuwdocuments (
    isn                              NUMERIC,
    owner                            NUMERIC,
    ownerisn                         NUMERIC,
    docname                          VARCHAR(100),
    isrecieved                       NUMERIC(38),
    daterequest                      TIMESTAMP,
    daterecieve                      TIMESTAMP
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifuwfinance (
    isn                              NUMERIC,
    lifuwobjectisn                   NUMERIC,
    results                          VARCHAR(20),
    substant                         VARCHAR(300),
    sumusd                           NUMERIC(20,2),
    changes                          VARCHAR(700)
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifuwmedical (
    isn                              NUMERIC,
    lifuwobjectisn                   NUMERIC,
    diagnoses                        VARCHAR(1000),
    planmo                           VARCHAR(20),
    datesetmo                        TIMESTAMP,
    datedonemo                       TIMESTAMP,
    substantrisk                     VARCHAR(1000),
    sumusd                           NUMERIC(20,2)
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifuwobjects (
    isn                              NUMERIC,
    agrisn                           NUMERIC,
    agrobjectisn                     NUMERIC,
    age                              NUMERIC(3,1),
    updated                          TIMESTAMP,
    updatedby                        NUMERIC,
    created                          TIMESTAMP,
    createdby                        NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifuwoffers (
    isn                              NUMERIC,
    lifuwobjectisn                   NUMERIC,
    datesend                         TIMESTAMP,
    daterecieve                      TIMESTAMP,
    selectvrnt                       VARCHAR(1),
    remarks                          VARCHAR(255),
    created                          TIMESTAMP,
    createdby                        NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifuwprofi (
    isn                              NUMERIC,
    lifuwobjectisn                   NUMERIC,
    post                             VARCHAR(300),
    jobplace                         VARCHAR(150),
    jobdiscr                         VARCHAR(255),
    load                             NUMERIC,
    comments                         VARCHAR(200)
)
DISTRIBUTED BY (isn);

COMMENT ON COLUMN life.lifuwprofi.comments IS $COMM$Комментарии$COMM$;


CREATE TABLE life.lifuwrejectrisk (
    isn                              NUMERIC,
    typerisk                         NUMERIC,
    owner                            NUMERIC,
    ownerisn                         NUMERIC,
    classriskisn                     NUMERIC,
    load                             NUMERIC,
    lifoptionisn                     NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifuwsport (
    isn                              NUMERIC,
    lifuwobjectisn                   NUMERIC,
    sportname                        VARCHAR(300),
    sportdiscr                       VARCHAR(1000),
    load                             NUMERIC
)
DISTRIBUTED BY (isn);



CREATE TABLE life.lifuwterra (
    isn                              NUMERIC,
    lifuwobjectisn                   NUMERIC,
    country                          VARCHAR(500),
    purpose                          VARCHAR(20),
    class                            VARCHAR(20),
    load                             NUMERIC
)
DISTRIBUTED BY (isn);



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



/*********** Errors and warnings **********
WARNING: No primary key defined for ais.buhbody_t
WARNING: In table ais.obj_attrib column exclude matches Greenplum keyword. Corrected to exclude_.
WARNING: In table ais.subword column new matches Greenplum keyword. Corrected to new_.
WARNING: In table life.lifapplication column decode matches Greenplum keyword. Corrected to decode_.
*******************************************/
