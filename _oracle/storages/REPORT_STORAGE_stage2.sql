CREATE OR REPLACE PACKAGE "STORAGES"."REPORT_STORAGE" IS
    LoadIsn Number;
    cRepAgrCount   Number := 10000;
    cLoadHist constant number := 17;
    cLoadFull constant number := 59;
    cpReRun        constant number := 1;
    cpRunContinue  constant number := 2;      
    function CreateLoad(pDescription in Varchar2:=Null, pDateBeg in date:=null, pDateEnd in date:=null,pProcType in number:=null,pType in number:=null,
                        pClassIsn Number:=null,pDaterep Date:=Null) return Number;
END; -- Package spec

-- Procedure
function CreateLoad(pDescription in Varchar2:=Null, pDateBeg in date:=null, pDateEnd in date:=null,
         pProcType in number:=null, pType in number:=null, pClassIsn Number:=null,
         pDaterep Date:=Null) return Number is
  Pragma Autonomous_Transaction;
Begin
  insert into repload (procisn,loadtype,description,datebeg, dateend,classisn,Daterep)
  values (pProcType,pType,pDescription, pDateBeg, pDateEnd,pclassisn,pDaterep)
  returning isn into LoadIsn;
  Commit;
  Return LoadIsn;
End;

End;