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




/*********** Errors and warnings **********
WARNING: In table life.lifapplication column decode matches Greenplum keyword. Corrected to decode_.
*******************************************/
