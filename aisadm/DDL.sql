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


