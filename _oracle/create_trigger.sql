CREATE OR REPLACE TRIGGER hist.histlog_ai
AFTER INSERT ON hist.histlog
REFERENCING NEW AS NEW
FOR EACH ROW
Begin
  insert into gp_user.histlog (
            ISN, NODE, TABLENAME, RECISN,
            AGRISN, ISN3, SESSIONID, TRANSID,
            STATUS, OPERATION, UPDATED, UPDATEDBY
        )
    values (
            :new.ISN, :new.NODE, :new.TABLENAME, :new.RECISN,
            :new.AGRISN, :new.ISN3, :new.SESSIONID, :new.TRANSID,
            'ÿ', :new.OPERATION, :new.UPDATED, :new.UPDATEDBY
        );
End;