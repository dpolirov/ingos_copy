CREATE OR REPLACE PACKAGE "STORAGES"."REPORT_STORAGE" 
  IS
/* страй пакет загрузки хранилища - не используется*/

LoadIsn Number;

   cRepAgrCount   Number := 10000;
  
   
   cLoadHist constant number := 17;
   cLoadFull constant number := 59;
   

    cpReRun        constant number := 1;
    cpRunContinue  constant number := 2;      
   



Procedure LoadRepAgr   (
      pLoadIsn   in   Number,
      pRunType in Number:=cpReRun
    );
    

function GetLoadIsn return number;

function CreateLoad(pDescription in Varchar2:=Null, pDateBeg in date:=null, pDateEnd in date:=null,pProcType in number:=null,pType in number:=null,
pClassIsn Number:=null,pDaterep Date:=Null) return Number;

procedure insertRe (pLoadIsn in Number, pLogIsn in Number := Null);


procedure LoadRepAgr_By_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number,
IsFull Number:=1
);



procedure LoadRepAgr_By_Hist_Isns
(
pLoadIsn Number,
pMinIsn Number,
pMaxIsn Number
);



procedure InsertEtc (pLoadIsn Number);


Function Get_merge_slq( ptoTable varchar2,pFromTable varchar2,pLinkFld Varchar2,
pLoadIsn Number,pExcludeUpdateFields Varchar2:='',pFromWhere Varchar2:='') return Varchar2;


Procedure LoadRepAgr_MakeFull(pLoadIsn in Number, pMinIsn in number:=null) ;

END; -- Package spec

CREATE OR REPLACE PACKAGE BODY "STORAGES"."REPORT_STORAGE" 
is

   Function InsertRepAgr(pLoadIsn in Number, pLogIsn in Number := Null,IsFull Number:=1)
      Return Number;

   Function InsertRepCond(pLoadIsn in Number, pLogIsn in Number := Null,IsFull Number:=1)
      Return Number;

   Procedure FlushIsn;

   procedure insertDict(pLoadIsn in Number,pLogIsn number:=null);

--   procedure  insertRe (pLoadIsn in Number,pLogIsn number:=null);

procedure Insert_Re_By_tt_rowid;

procedure MakeCityBufer;

--procedure InsertEtc (pLoadIsn Number);

Procedure LoadRepAgr_By_tt_RowId(pLoadIsn in Number, IsFull Number:=1) is
  vnAgrCount Number := 0;
  vLogIsn    Number;
  vBlockIsn  number;
Begin

/*
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

  vLogIsn:=RepLog_f(pLoadIsn,'LoadRepAgr_By_tt_RowId','InsertRepAgr Call',pAction=>'Begin',pBlockIsn=>vBlockIsn);
  Begin
    vnAgrCount:=InsertRepAgr(pLoadIsn,vLogIsn,IsFull);
    RepLog_i(pLoadIsn,'LoadRepAgr_By_tt_RowId','InsertRepAgr Call',pAction=>'End',
      pobjCount=>vnAgrCount,pBlockIsn=>vBlockIsn);
  Exception when others then
    RepLog_i(pLoadIsn,'LoadRepAgr_By_tt_RowId','InsertRepAgr Call, ERROR',pAction=>'End',
      pobjCount=>vnAgrCount,pErrMsg=>SQLCODE||': '||SQLERRM,pBlockIsn=>vBlockIsn);
    RAISE; --!!!!!
  End;

  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

  vLogIsn:=RepLog_f(pLoadIsn,'LoadRepAgr_By_tt_RowId','InsertRepCond Call',pAction=>'Begin',pBlockIsn=>vBlockIsn);
  Begin
    vnAgrCount:=InsertRepCond(pLoadIsn,vLogIsn,IsFull);
    RepLog_i(pLoadIsn,'LoadRepAgr_By_tt_RowId','InsertRepCond Call',pAction=>'End',pobjCount=>vnAgrCount, pBlockIsn=>vBlockIsn);
  Exception when others then
    RepLog_i(pLoadIsn,'LoadRepAgr_By_tt_RowId','InsertRepCond Call, ERROR',pAction=>'End',
      pobjCount=>vnAgrCount,pErrMsg=>SQLCODE||': '||SQLERRM,pBlockIsn=>vBlockIsn);
    RAISE; --!!!!!
  end;

*/
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

  RepLog_i(pLoadIsn,'LoadRepAgr_By_tt_RowId','InsertDict Call',pAction=>'Begin',pBlockIsn=>vBlockIsn);
  Begin
    InsertDict(pLoadIsn);
    RepLog_i(pLoadIsn,'LoadRepAgr_By_tt_RowId','InsertDict Call',pAction=>'End',pBlockIsn=>vBlockIsn);
  Exception when others then
    RepLog_i(pLoadIsn,'LoadRepAgr_By_tt_RowId','InsertDict Call, ERROR',pAction=>'End',pobjCount=>vnAgrCount,pErrMsg=>SQLCODE||': '||SQLERRM,pBlockIsn=>vBlockIsn);
    RAISE; --!!!!!
  End;
-- !!! ОДИН ОБЩИЙ СОММИТ
  COMMIT;
  /* RepLog_i (pLoadIsn, 'LoadRepAgr_By_tt_RowId', 'InsertRe Call', pAction => 'Begin');
     Begin
      insertRe(pLoadIsn);
      RepLog_i (pLoadIsn, 'LoadRepAgr_By_tt_RowId', 'InsertRe Call', pAction => 'End');
     Exception when others then
       RepLog_i (pLoadIsn, 'LoadRepAgr_By_tt_RowId', 'InsertRe Call,  ERROR',  pAction => 'End', pobjCount => vnAgrCount,pErrMsg => SQLCODE||': '|| SQLERRM);
       RAISE; --!!!!!
    End; */
End;

Procedure LoadRepAgr_MakeFull(pLoadIsn in Number, pMinIsn in number:=null) is
  vIsn        Number:=0;
  vMaxIsn     Number:=0;
  vTotalCount Number:=0;
  vTime       Number:=0;
  vTime0      Number:=0;
  vTimeDelta  Number:=0;
  vLoopCount  Number:=0;
  SesId       Number;
  vSql        Varchar2(4000);
  vBlockIsn   number;
  vBlockIsn1  number;
Begin
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

  RepLog_i(pLoadIsn,'LoadRepAgr_MakeFull',pAction=>'Begin',pBlockIsn=>vBlockIsn);

/*
  IF pMinIsn is Null Then
    execute immediate 'Truncate table RepAgr drop storage';
    execute immediate 'Truncate table RepCond drop storage';
    execute immediate 'Truncate table Rep_AgrX drop storage';
    vIsn:=-9e30;
  else
    vIsn:=pMinIsn;
  end if;
*/
  select count(*) dummy into vTotalCount from agreement where isn >=vIsn;

  select Seq_rep_block.NEXTVAL into vBlockIsn1 from dual;
     --GCC - для инициализации пакета.
  RepLog_i(pLoadIsn, 'LoadRepAgr_MakeFull', 'Main Loops',  pAction => 'Begin', pBlockIsn=>vBlockIsn1);
/*
  STORE_AND_DROP_TABLE_INDEX('RepAgr');
  STORE_AND_DROP_TABLE_INDEX('RepCond');
*/
  SesId:=PARALLEL_TASKS.createnewsession('LoadRepAgr');
  LOOP
    vLoopCount:=vLoopCount+1;
    vTime:=dbms_utility.Get_Time;
    if vLoopCount>1 then
      dbms_application_info.Set_Module('AGR: '||vLoopCount||'/'||ceil(vTotalCount/cRepAgrCount),
        round(vTimeDelta/100)||'* '||round((vTimeDelta/100/60)*vTotalCount/(cRepAgrCount))||' * '||
        round((vTime0/100/60)*vTotalCount/(cRepAgrCount*(vLoopCount-1))));
    end if;

    SELECT MAX(ISN) Into vMaxIsn
    FROM
      (SELECT /*+ INDEX_ASC(A X_AGREEMENT)*/ Isn
       FROM Agreement A
       WHERE Isn>vIsn and ROWNUM<=cRepAgrCount);

    Exit When vMaxIsn is Null;

    vSql:='Begin
             REPORT_STORAGE.LoadRepAgr_By_Isns('||ploadisn||','||vISn||','||vMaxIsn||');
           END;';
    PARALLEL_TASKS.processtask(sesid,vSql);

    vIsn:=vMaxIsn;
    RepLoad_U(pLoadIsn,pLastisnloaded=>vMaxIsn);

    vTimeDelta:=dbms_utility.Get_Time-vTime;
    vTime0:=vTime0+vTimeDelta;
  End LOOP;

  PARALLEL_TASKS.endsession(sesid);
  RepLog_i(pLoadIsn,'LoadRepAgr_MakeFull','Main Loops',pAction=>'End',pBlockIsn=>vBlockIsn1);
  RepLog_i(pLoadIsn,'LoadRepAgr_MakeFull',pAction=>'End',pBlockIsn=>vBlockIsn);

  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

  RepLog_i(pLoadIsn,'LoadRepAgr','Index Rebuild',pAction=>'Begin',pBlockIsn=>vBlockIsn);
/*  RESTORE_TABLE_INDEX('RepAgr');
  RESTORE_TABLE_INDEX('RepCond');
*/
  RepLog_i(pLoadIsn,'LoadRepAgr','Index Rebuild',pAction=>'End',pBlockIsn=>vBlockIsn);

  select SEQ_REP_BLOCK.nextval into vBlockIsn1 from dual;
/*
  RepLog_i(pLoadIsn,'LoadRepAgr','INSERT_AGRRE',pAction=>'Begin',pBlockIsn=>vBlockIsn1);
  INSERT_AGRRE(pLoadIsn);
  RepLog_i(pLoadIsn,'LoadRepAgr','INSERT_AGRRE',pAction=>'End',pBlockIsn=>vBlockIsn1);
/*
  select SEQ_REP_BLOCK.nextval into vBlockIsn1 from dual;
  RepLog_i(pLoadIsn,'LoadRepAgr','InsertEtc',pAction=>'Begin',pBlockIsn=>vBlockIsn1);
  InsertEtc(pLoadIsn );
  RepLog_i(pLoadIsn,'LoadRepAgr','InsertEtc',pAction=>'End',pBlockIsn=>vBlockIsn1);
*/
End;

Procedure LoadRepAgr_MakeHist(pLoadIsn in Number) is
     vMaxIsn integer:=0;
     vMinIsn integer:=0;
     SesId        Number;
     vSql Varchar2(4000);
     vBlockIsn number;
     vCnt Number:=0;
     vBufCnt Number;
     cLoadCnt Number:=100000;
Begin
     select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

     RepLog_i (pLoadIsn, 'LoadRepAgr_MakeHist', pAction => 'Begin', pBlockIsn=>vBlockIsn);
     STORE_AND_DROP_TABLE_INDEX('RepAgr',1);
     STORE_AND_DROP_TABLE_INDEX('RepCond',1);

      SesId:=PARALLEL_TASKS.createnewsession('LoadRepAgr mv');

      Update --+ Index( l)
       rep_MV_Log l
      Set
       GetStatus=1
      where Nvl(loadisn,0)=0 And Upper(SHEMANAME)='REPAGR';
      Commit;

      Update --+ Index( l)
       rep_MV_Log l
      Set
       GetStatus=1
      where Nvl(loadisn,0)=0 And Upper(SHEMANAME)='REPAGR_ADD_SUBJ';
      Commit;

      Update --+ Index( l)
       rep_MV_Log l
      Set
       GetStatus=1
      where Nvl(loadisn,0)=0 And Upper(SHEMANAME)='REPAGR_ADD_DEPT';
      Commit;

    /* загоняем логи в буфер и обрабатываем*/
       execute immediate 'truncate table tt_Isns';
       Insert Into  tt_Isns
       Select RowNum, RecIsn
       from
       (Select --+ Index( l X_REP_MV_LOG_SHEMA_LOAD)
        Distinct Recisn from rep_MV_Log l where Nvl(loadisn,0)=0 And Upper(SHEMANAME)='REPAGR' and GetStatus=1
        Union
        Select --+ Index( l X_REP_MV_LOG_SHEMA_LOAD) Ordered Use_Nl(l ar)
        Distinct Ar.AgrIsn
        from rep_MV_Log l,agrrole ar
         where Nvl(loadisn,0)=0 And Upper(SHEMANAME)='REPAGR_ADD_SUBJ' and GetStatus=1
         and ar.subjisn=l.Recisn
        Union
        Select --+ Index( l X_REP_MV_LOG_SHEMA_LOAD) Ordered Use_Nl(l ar)
        Distinct Ar.AgrIsn
        from rep_MV_Log l,agrrole ar
         where Nvl(loadisn,0)=0 And Upper(SHEMANAME)='REPAGR_ADD_DEPT' and GetStatus=1
         and ar.subjisn=l.Recisn
        );

        vBufCnt:=SQL%ROWCOUNT;
      Commit;

     vCnt:=0;

     For Cur In (Select /*+ Index_Asc(t x_tt_Isns) */ Isn, ObjIsn, RowNum Rn
                 from tt_Isns t) Loop
      -- контроль размера блоков по repcond
       Select --+ Index(ac x_agrcond_Agr)
       vCnt+Count(agrisn)
       Into vCnt
       from agrcond ac Where agrisn=cur.ObjIsn;

        If vCnt>cLoadCnt Or Cur.Rn=vBufCnt Then
         vCnt:=0;
         vMaxIsn:=Cur.Isn;
        --        vMaxIsn:=Cut_Table('tt_Isns','ISN',vMinIsn,pRowCount=>5000);
        --     Exit When vMaxIsn is Null;
         vSql:='Begin REPORT_STORAGE.loadrepagr_by_Hist_isns('||ploadisn||','||vMinIsn||','||vMaxIsn||'); END;';
         PARALLEL_TASKS.processtask(sesid,vSql);
         vMinIsn:=vMaxIsn;
       end if;
     End LOOP;

  PARALLEL_TASKS.endsession(sesid);

  RepLog_i (pLoadIsn, 'LoadRepAgr_MakeHist', pAction => 'End', pBlockIsn=>vBlockIsn);
  -- чистим лог
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

  RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'CLEAR LOG ADD', pAction => 'Begin', pBlockIsn=>vBlockIsn);

  Update --+ Index( h X_REP_MV_LOG_SHEMA_LOAD) Use_Hash(h)
     rep_MV_Log H
  Set
      Loadisn=pLoadIsn
  where   Nvl(loadisn,0)=0 And Upper(SHEMANAME)In ('REPAGR_ADD_SUBJ','REPAGR_ADD_DEPT')
    and GetStatus=1;

  COMMIT;

  RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'CLEAR LOG ADD', pAction => 'End', pBlockIsn=>vBlockIsn);

  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

  RepLog_i (pLoadIsn, 'LoadRepAgr_MakeHist', 'Index Rebuild', pAction => 'Begin', pBlockIsn=>vBlockIsn);
      RESTORE_TABLE_INDEX('RepAgr');
--      RESTORE_TABLE_INDEX('RepCond');
  RepLog_i (pLoadIsn, 'LoadRepAgr_MakeHist', 'Index Rebuild', pAction => 'End', pBlockIsn=>vBlockIsn);

     select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

     RepLog_i (pLoadIsn, 'LoadRepAgr_MakeHist', 'INSERT_AGRRE', pAction => 'Begin', pBlockIsn=>vBlockIsn);
     INSERT_AGRRE(Loadisn);
     RepLog_i (pLoadIsn, 'LoadRepAgr_MakeHist', 'INSERT_AGRRE', pAction => 'End', pBlockIsn=>vBlockIsn);

  COMMIT;

--If Trunc(Sysdate,'d')+5=Trunc(Sysdate) then -- по пятницам
 select SEQ_REP_BLOCK.nextval into vBlockIsn from dual;
 RepLog_i (pLoadIsn, 'LoadRepAgr_MakeHist','InsertEtc', pAction => 'Begin', pBlockIsn=>vBlockIsn);
   InsertEtc (pLoadIsn );
 RepLog_i (pLoadIsn, 'LoadRepAgr_MakeHist','InsertEtc', pAction => 'End', pBlockIsn=>vBlockIsn);
--end if;
End;

Procedure LoadRepAgr(pLoadIsn in Number, pRunType in Number:=cpReRun ) is
  vRunParam number;
  vMode number;
  vDtBeg date;
  vDtEnd date;
  vBlockIsn number;
  vBlockIsn1 number;
Begin
  LoadIsn:=pLoadIsn;

  select SEQ_REP_BLOCK.nextval into vBlockIsn from dual;

--  EXECUTE IMMEDIATE 'alter session set sort_area_size = 33554432';

  RepLog_i (pLoadIsn, 'LoadRepAgr', pAction => 'Begin', pBlockIsn=>vBlockIsn);

  select SEQ_REP_BLOCK.nextval into vBlockIsn1 from dual;




/*
  RepLog_i (pLoadIsn, 'LoadRepAgr','CityBuffer', pAction => 'Begin', pBlockIsn=>vBlockIsn1);
  MakeCityBufer; -- Заполнение таблицы Rep_City - прокомментировал 17.10.2006 Марин А.В.
  RepLog_i (pLoadIsn, 'LoadRepAgr','CityBuffer', pAction => 'End', pBlockIsn=>vBlockIsn1);




 -- загрузка структуры rep_dept для ускорения процесса загрузки
execute immediate 'truncate table rep_dept ';
  Insert Into  rep_dept  (select * from v_Dept);
  commit;

*/


--восстановление точки остановки

  Select LASTISNLOADED,f.loadtype,f.datebeg,f.dateend
  Into vRunParam,vMode,vDtBeg,vDtEnd
  from RepLoad f
  Where Isn=pLoadIsn;

  If pRunType=cpReRun then
    vRunParam:=null;
  end if;

  RepLoad_U(pLoadIsn,pLastrundate=>Sysdate);
  if vMode = cLoadFull then
    LoadRepAgr_MakeFull(pLoadIsn,vRunParam);
  else
    LoadRepAgr_MakeHist(pLoadIsn);
  end if;
  RepLoad_U(pLoadIsn,pLastenddate=>Sysdate);

  RepLog_i(pLoadIsn,'LoadRepAgr',pAction=>'End',pBlockIsn=>vBlockIsn);
End;

procedure insertRepAgr_Hist (pLoadIsn in Number) is
  vrc   Number;
  vBlockIsn number;
  vsql Varchar2(4000);
Begin
  vsql:='Begin '||report_storage.Get_merge_slq('Repagr','v_RepAgr','agrisn',ploadisn,'datebeg')||'; end;';
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  RepLog_i(pLoadIsn,'LoadRepAgr_By_Hist_Isns','Merge RepAgr',pAction=>'Begin',pBlockIsn=>vBlockIsn);
  execute immediate vsql;
  vrc:=SQL%ROWCount;
  RepLog_i(pLoadIsn,'LoadRepAgr_By_Hist_Isns','Merge RepAgr',pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);
  commit;
End;

Function insertRepAgr(pLoadIsn in Number, pLogIsn in Number := Null, IsFull Number:=1)
  Return Number is
  vrc Number;
  vBlockIsn number;
  vMinIsn Number;
  vMaxISn Number;
Begin
  IF IsFull=0 Then
    select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
    RepLog_i(pLoadIsn,'InsertRepAgr','Delete',pPrevIsn=>pLogIsn,pAction=>'Begin',pBlockIsn=>vBlockIsn);
    vrc:=0;
    DELETE /*+ INDEX(A X_RepAgr_Agr)*/ FROM RepAgr a
    WHERE AgrIsn In (Select Isn from tt_rowid);
    vrc:=SQL%ROWCount;
    RepLog_i(pLoadIsn,'InsertRepAgr','Delete',pPrevIsn=>pLogIsn,pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);
  end if;

  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  Select Min(Isn),Max(Isn) Into vMinIsn,vMaxISn from tt_RowId;
  RepLog_i(pLoadIsn,'InsertRepAgr','Insert',pPrevIsn=>vMinIsn,pAction=>'Begin',pBlockIsn=>vBlockIsn);
/*
  Insert Into RepAgr
  SELECT seq_Reports.NextVAL Isn,pLoadIsn, v.*
  FROM v_RepAgr v;
*/
  vrc:=SQL%ROWCount;
--  COMMIT;
  RepLog_i(pLoadIsn,'InsertRepAgr','Insert',pPrevIsn=>vMaxISn,pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);
  Return vrc;
End;

procedure insertRepCond_Hist (pLoadIsn in Number) is
  vBlockIsn number;
  vLoadCnt Number:=10000;
  vn Number;
  vsql Varchar2(4000);
Begin
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'Merge RepCond', pAction => 'Begin', pBlockIsn=>vBlockIsn);

  delete from ttt_repcond;
/*  insert Into ttt_repcond
  select tRUNC(1+ROWNUM/vLoadCnt),v.*
  from v_Repcond v;
  */
  Vn:=tRUNC(1+SQL%ROWCOUNT/vLoadCnt);
  FOR I IN 1..VN LOOP
     vsql:='Begin '||report_storage.Get_merge_slq('RepCond','ttt_repcond'  ,'condisn',ploadisn,'agrdatebeg','WHERE NtILE='||I||' ')||'; end;';
     execute immediate vsql;

     commit;
  END LOOP;
  RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'Merge RepCond',  pAction => 'End', pobjCount => vLoadCnt, pBlockIsn=>vBlockIsn);
End;

Function insertRepCond (pLoadIsn in Number, pLogIsn in Number := Null,IsFull Number:=1)
  Return Number is
  vrc    Number;
  vBlockIsn number;
  vMinIsn Number;
  vMaxISn Number;
Begin
  IF IsFull=0 Then
    select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
    RepLog_i(pLoadIsn,'InsertRepCond','Delete RepCond',pPrevIsn=>pLogIsn,pAction=>'Begin',pBlockIsn=>vBlockIsn);
    vrc:=0;
    DELETE /*+ INDEX(C X_RepCond2_Agr)*/ FROM RepCond c
    WHERE AgrIsn In (Select Isn from tt_rowId);
    vrc:=vrc+SQL%ROWCount;
    RepLog_i(pLoadIsn,'InsertRepCond','Delete RepCond',pPrevIsn=>pLogIsn,pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);
  end if;
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  Select Min(Isn),Max(Isn) Into vMinIsn,vMaxISn  from tt_RowId;
  RepLog_i(pLoadIsn,'InsertRepCond','Insert RepCond',pPrevIsn=>vMinIsn,pAction=>'Begin',pBlockIsn=>vBlockIsn);

  delete from ttt_repcond;
/*  insert Into ttt_repcond
  select pLoadIsn,v.*
  from v_Repcond v;

  INSERT INTO RepCond
  SELECT seq_Reports.NextVAL, v.*
  FROM ttt_repcond v;
*/
  vrc:=SQL%ROWCount;
--  COMMIT;
  RepLog_i(pLoadIsn,'InsertRepCond','Insert RepCond',pPrevIsn=>vMaxISn,pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);
  Return vrc;
End;

Function GetLoadIsn  Return Number is
Begin
  Return LoadIsn;
End;

Procedure FlushIsn is
Begin
  null;
End;
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

procedure insertDict(pLoadIsn in Number, pLogIsn number:=null) is
  vrc Number;
  vBlockIsn number;
Begin
/* execute immediate 'truncate table rep_tt_rules2groups';
   Insert Into rep_tt_rules2groups (Select * from Ais.tt_rules2groups);
   commit;*/

/*
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  RepLog_i(pLoadIsn,'InsertDict_CARGO','Delete',pPrevIsn=>pLogIsn,pAction=>'Begin',pBlockIsn=>vBlockIsn);
  vrc:=0;
  DELETE FROM Rep_AgrCargo a
  WHERE AgrIsn In (Select Isn from tt_rowId);
  vrc:=vrc+SQL%ROWCount;


  RepLog_i(pLoadIsn,'InsertDict_CARGO','Delete',pPrevIsn=>pLogIsn,pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  RepLog_i(pLoadIsn,'InsertDict_CARGO','Insert',pPrevIsn=>pLogIsn,pAction=>'Begin',pBlockIsn=>vBlockIsn);
  -- залили общий список
  Insert Into Rep_AgrCargo
  select t.*,SEQ_REPORTS.NextVal
  from
    (select --+ use_nl(t l c) ordered
       l.AgrIsn, Max(decode(c.classisn,10908816,1,0)) Sea,
       Decode(count(distinct c.classisn),0,0,1) Norm
     from tt_rowid t,Ais.agrlimit l,Ais.crgroute c
     where t.isn=L.agrisn And l.isn = c.isn
     Group by l.AgrIsn) t;

  vrc:=SQL%ROWCount;
--  COMMIT;
  RepLog_i(pLoadIsn,'InsertDict_CARGO','Insert',pPrevIsn=>pLogIsn,pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);

  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  RepLog_i(pLoadIsn,'InsertDict_AGRTUR','Delete',pPrevIsn=>pLogIsn,pAction=>'Begin',pBlockIsn=>vBlockIsn);
  vrc:=0;

  DELETE FROM REP_AGRTUR a
  WHERE AgrIsn In (Select Isn from tt_rowId);
  vrc:=vrc+SQL%ROWCount;

  RepLog_i(pLoadIsn,'InsertDict_AGRTUR','Delete',pPrevIsn=>pLogIsn,pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  RepLog_i(pLoadIsn,'InsertDict_AGRTUR','Insert',pPrevIsn=>pLogIsn,pAction=>'Begin',pBlockIsn=>vBlockIsn);

  Insert Into REP_AGRTUR
  Select S.*, SEQ_REPORTS.NEXTVAL
  from
    (select --+ use_nl (a l) ordered
       a.agrisn,
       Max(Decode(subclassisn,2570,1,2458716 ,1,0)),
       Max(Decode(subclassisn,13310216,1,0))
     from tt_rowId t, Ais.agrlimit a, Ais.agrlimitem l
     where a.agrisn = T.Isn
       and a.isn = l.limisn
       and l.subclassisn in (2570,2458716,13310216)
     Group by a.agrisn) S;

  vrc:=SQL%ROWCount;
--      COMMIT;
  RepLog_i(pLoadIsn,'InsertDict_AGRTUR','Insert',pPrevIsn=>pLogIsn,pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);


  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  RepLog_i(pLoadIsn,'InsertDict_AgrClause','Delete',pPrevIsn=>pLogIsn,pAction=>'Begin',pBlockIsn=>vBlockIsn);
  vrc:=0;

  DELETE FROM Rep_AgrClause a
  WHERE AgrIsn In (Select Isn from tt_rowid);
  vrc:=SQL%ROWCount;
  RepLog_i(pLoadIsn,'InsertDict_AgrClause','Delete',pPrevIsn=>pLogIsn,pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);

  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

  RepLog_i(pLoadIsn,'InsertDict_AgrClause','Insert',pPrevIsn=>pLogIsn,pAction=>'Begin',pBlockIsn=>vBlockIsn);

  Insert Into Rep_AgrClause
  (select --+ use_nl(a l) ordered
   A.*
   from tt_rowId t, Ais.AgrClause A
   where a.agrisn=T.Isn);

  vrc:=SQL%ROWCount;
--      COMMIT;
  RepLog_i(pLoadIsn,'InsertDict_AgrClause','Insert',pPrevIsn=>pLogIsn,pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);

----------------
  Delete From rep_objclass_Domestic Where AgrIsn In (Select Isn from tt_rowid);

  Insert Into rep_objclass_Domestic
  Select --+ Ordered Use_Nl (a o op oc t dt)
    A.Isn AgrIsn,o.ClassIsn ObjClassIsn,
    Max(decode(dt.parentisn,36776016 /*c.get('CarClassForeign') - КЛАСС МАШИН ИНОСТРАННОГО ПРОИЗВОДСТВА,'N',
                            36775916 /*c.get('CarClassLocal') - КЛАСС МАШИН ОТЕЧЕСТВЕННОГО ПРОИЗВОДСТВА,'Y')) domestic,
    op.ClassIsn ParentObjClassIsn
  From AGREEMENT a, Ais.AGROBJECT o, AGROBJECT op, OBJCAR oc, CARTARIF t, DICTI dt
  where a.Isn In (Select Isn from tt_rowid)
    and exists (select 'X' from dicti where isn=683209116 -- КОМПЛЕКСНОЕ СТРАХОВАНИЕ
                start with isn=a.ruleisn connect by prior parentisn=isn)
    and exists (select 'X' from dicti where isn=34711216 -- ТИП ДОГОВОРА СТРАХОВАНИЯ
                start with isn=a.classisn connect by prior parentisn=isn)
    and not exists (select /*+ index(j x_subject_class)  isn from subject j where isn=a.emplisn and classisn=491)
    and o.AgrIsn=A.Isn
    ANd op.rowid=(-- для группировки по родительским объектам
        select rowid from agrobject where parentisn is null
        start with isn=o.Isn
        connect by prior parentisn=isn and prior parentisn is not null)
    and op.descisn=oc.isn(+)
    and oc.tarifisn=t.isn(+)
    and dt.isn(+)= nvl(
    (select /*+index(rt x_rultariff_x1) max(rt.x2)
     from ais.rultariff rt
     where rt.TariffISN=703301816 -- C.Get('TRF_TariffGroup') - Тарифная группа для модификации ТС. (...ГО регион-кредит)
       and x1=t.modelisn
       AND (rt.DateBeg<=a.datebeg Or rt.DateBeg Between a.datebeg and a.dateend))
     ,oc.tariffgroupisn)
  Group by A.Isn, o.ClassIsn, op.ClassIsn;
------------------------------------------------------
-- авто пер-ние
--Insert Into res_avto_agr_re
-- врезка для перезагрузки по хисту
  Delete From Rep_AgrX Where AgrIsn In (Select Isn from tt_rowid);
-- виртуальная привязка перестрахования по автокаско
  Insert Into Rep_AgrX
  Select /*+ Ordered Use_Hash(re ag)  Ais.Seq_AgrX.NEXTVAL*100+2, ag.agrisn, re.sectisn, 0,
    ag.parentriskisn, re.reisn, 0, re.datebeg, re.dateend,0,limitusd
  from Storages.res_avto_re re,
    (SELECT --+ Ordered Use_Hash(ar)
       AR.ISN,RC.AGRISN,NVL(RC.PARENTRISKISN,RC.RISKISN) PARENTRISKISN,MAX(RC.LIMITUSD) LIMITUSD
     FROM ttt_REPCOND RC,RES_AVTO_RE AR
     WHERE AR.RISKRULEISN=RC.RISKRULEISN AND RC.AGRDATEBEG BETWEEN AR.DATEBEG AND AR.DATEEND
       AND AGRCLASSISN IN (SELECT ISN FROM DICTI
                           START WITH ISN=8738 -- ТИП ПРЯМОГО ДОГОВОРА
                           CONNECT BY PRIOR ISN=PARENTISN)
       AND AGRRULEISN<>753518300 -- т.е. не ОСАГО
     GROUP BY AR.ISN,RC.AGRISN,NVL(RC.PARENTRISKISN,RC.RISKISN)) Ag
   where ag.isn=re.isn;
---------------- Rep_agrext
  Delete From Rep_AgrExt Where AgrIsn In (Select Isn from tt_rowid);

  Insert into Rep_AgrExt
  Select agrisn,classisn,x1,x2,x3,x4,x5
  from AgrExt
  Where AgrIsn In (Select Isn from tt_rowid)
    and x1 in (select 1283165703 isn -- МЕЖДУНАРОДНАЯ ПРОГРАММА
               from dual
               union all
               Select Isn From Dicti
               Start With isn=1071775625 -- СТРАХОВАНИЕ ЗАЛОГОВОГО ИМУЩЕСТВА
               connect by prior Isn=Parentisn);
               */
               null;
End;

procedure insertRe(pLoadIsn in Number, pLogIsn in Number := Null) is
  vrc   Number;
  vBlockIsn number;
Begin
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  RepLog_i(pLoadIsn,'InsertReAgr','Delete',pPrevIsn=>pLogIsn,pAction=>'Begin',pBlockIsn=>vBlockIsn);

  vrc:=0;

  DELETE FROM Rep_AGRRe a
  WHERE AgrIsn in (Select Isn from tt_rowid);
  vrc :=vrc+ SQL%ROWCount;

  RepLog_i (pLoadIsn, 'InsertReAgr', 'Delete', pPrevIsn => pLogIsn, pAction => 'End', pobjCount => vrc, pBlockIsn=>vBlockIsn);
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  RepLog_i (pLoadIsn, 'InsertReAgr', 'Insert', pPrevIsn => pLogIsn, pAction => 'Begin', pBlockIsn=>vBlockIsn);

--     isn, agrisn, objisn, riskisn, sectisn, reisn, subjisn, sharepc
   -- все перестрахованные
  Insert Into Rep_AgrRe
      (isn, agrisn, subjisn, sharepc, outcompc, datebeg, dateend)
  Select Seq_Reports.NEXTVAL,agrisn,subjisn, sharepc,
       decode (BasePC,0,0,Least (100,Greatest (0,100*ComBasePC/BasePC))),Datebeg,Dateend
  From
    (select --+  INDEX (A X_AGRROLE_AGR ) ordered
       a.agrisn,A.subjisn,
       nvl(sum(Decode(sumclassisn,427,0,A.sharepc)),0) sharepc,
       nvl(sum(Decode(sumclassisn,427,0,A.base)),0) BasePC,
       nvl(sum(Decode(sumclassisn,427,A.base,0)),0) ComBasePC,
       Min(Datebeg) Datebeg,Max(Dateend) Dateend
     from tt_rowid t, Ais.agrrole a
     where t.isn=a.agrisn
       ANd A.orderno > 0
       and A.classisn = 435
       and nvl (A.sumclassisn,0) in (0, 414,427)
       and A.sharepc <> 0
       and A.calcflg = 'Y'
       and (nvl (A.sumclassisn,0) = 427 or instr(upper(A.formula),'SI') > 0)
     group by a.agrisn,A.subjisn)S;

  vrc := SQL%ROWCount;

    -- Туристы
  Merge Into Rep_AgrRe D
  Using (select --+  Index( r X_AGRROLE_AGR) ordered Use_Nl (t r t1 a)
           R.agrisn, t1.subjisn, Max(t1.reinspc) SHAREPC, Min(r.Datebeg) Datebeg,
           Max(r.Dateend) Dateend, Max(T1.REINSCOMMI) outcompc
         from tt_rowid t,Ais.agrrole r,finance.tourreinsurers t1,repagr a
         where t.isn=A.agrisn
           And A.AgrIsn= r.agrisn
           ANd A.RULEDEPT=707480016 -- c.get('PrivDept')
          and  r.subjisn = t1.subjisn
         Group by R.agrisn,t1.subjisn ) S
  On (D.agrisn=S.agrisn and D.subjisn=S.subjisn)
  WHEN MATCHED THEN Update Set D.SHAREPC=S.SHAREPC,D.DATEBEG=S.DATEBEG,D.Dateend=S.Dateend,D.outcompc=S.outcompc
  WHEN NOT MATCHED THEN INSERT (d.Isn,D.AgrIsn, D.subjisn,D.SHAREPC,D.outcompc,D.DATEBEG,D.Dateend)
  VALUES (Seq_Reports.NEXTVAL,S.AgrIsn, S.subjisn,S.SHAREPC,S.outcompc,S.datebeg,S.Dateend);

     -- Туристы шенгеН
  Merge Into Rep_AgrRe D
  Using (select --+ ordered  Index (a PK_REP_AGRTUR)
           a.agrisn,5618616 subjisn, 100/3 SHAREPC, 90/3 outcompc
         from tt_rowid t,REP_AGRTUR a
         where a.isshengen=1 and t.isn=a.agrisn) S
  On (D.agrisn=S.agrisn)
  WHEN MATCHED THEN Update Set D.SHAREPC=S.SHAREPC,D.subjisn=S.subjisn,D.OUTCOMPC=S.OUTCOMPC
  WHEN NOT MATCHED THEN INSERT (D.Isn,D.AgrIsn, D.subjisn,D.SHAREPC,D.OUTCOMPC)
  VALUES (Seq_Reports.NEXTVAL,S.AgrIsn, S.subjisn,S.SHAREPC,S.OUTCOMPC);

  Insert_Re_By_tt_rowid;

  COMMIT;
  RepLog_i(pLoadIsn,'InsertReAgr','Insert',pPrevIsn=>pLogIsn,pAction=>'End',pobjCount=>vrc,pBlockIsn=>vBlockIsn);
End;

procedure Insert_Re_By_tt_rowid Is
  p number;
  pr number;
  isum number;
  pq number;
  xper number:=0;
  vAgr Number:=0;
  vObj Number:=0;
--  db date;
--  de date;
  com1 Number;
  com2 Number;
  vRate Number;
  vDept0Isn Number;
Begin
 xper:=0;
for R in (select --+ ordered USE_NL (T A A1 S)
               a.isn aisn, x.isn xisn,a.datebeg,a.dateend,
               a.insuredsum, s.Rate, a1.classisn reclassisn, a1.deptisn redeptisn,
               a.limitsum,a1.datebase,a.currisn agrcurrisn,
              x.sectisn,x.xpc,nvl(a.reinspc,0) reinspc,nvl(a.sharepc,100) sharepc,s.secttype,s.currisn,
              s.limiteverymode,s.optionalcode,x.objisn,x.riskisn,x.reisn,X.DateBeg xDateBeg,X.DateEnd xDateEnd
         from tt_rowId t,(Select min (isn) isn, agrisn,sectisn,objisn,riskisn,reisn,xpc,DateBeg,DateEnd From  ais.agrx
          group by agrisn,sectisn,objisn,riskisn,reisn,xpc,DateBeg,DateEnd) x,ais.agreement a,ais.agreement a1, ais.resection S
         where t.isn=a.isn and t.isn=x.agrisn and x.sectisn=s.isn and a1.isn=x.reisn
         order by a.isn, nvl(x.objisn, 0), nvl(to_char(s.orderno),s.secttype||s.id) ) loop

IF (vAgr<>R.Aisn) or (vObj<>nvl(r.ObjISN,0)) Then
  vAgr:=R.Aisn;
  vObj:=nvl (R.ObjISN,0);
  p:=Nvl(r.reinspc,0);
  xper:=p;
 if R.objisn>0 or R.riskisn>0 then
    r.insuredsum:=ais.getisum2(r.aisn,53,r.datebeg,R.objisn,R.riskisn,r.limiteverymode,r.xisn);
    r.limitsum:=0;
 elsif r.limiteverymode='Y' then
    r.insuredsum:=ais.getisum2(r.aisn,53,r.datebeg,0,0,'Y');
    r.limitsum:=0;
   Else
    R.insuredsum:=getcrosscover(R.insuredsum,r.agrcurrisn,53,r.datebeg);
    R.limitsum:=getcrosscover(R.limitsum,r.agrcurrisn,53,r.datebeg);
 end if;
 isum:=((r.insuredsum+r.limitsum)*r.sharepc/100)*((100-r.reinspc)/100);
end if;
vRate := 1;
Com1 := 0;
Com2 := 0;
if isum<>0 then  --33
begin

  if r.secttype='QS' then --1
   select retention,getcrosscover(limitsum,r.currisn,53,sysdate),RECOMMISS,OVRCOMMISS
   into pq,pr,Com1,Com2
   from Ais.recond
   where sectisn=r.sectisn;
  if isum>pr then --2
   pq:=round((pr*100/isum),2)*pq/100;
  else
   pr:=isum;
  end if; --2

  if nvl(r.xpc,0)=0 then --3
     p:=(100-p)*(round(pr*100/isum,2)-pq)/100;
  else
     p:=r.xpc;
  end if; --3
  xper:=xper+p;
  isum:=isum*((100-p)/100);
 end if;--1

 if r.secttype='SP' then --1
   select getcrosscover(prioritysum,r.currisn,53,sysdate),
          getcrosscover(limitsum,r.currisn,53,sysdate),RECOMMISS,OVRCOMMISS
          into pr,pq,Com1,Com2
          from recond where sectisn=r.sectisn;
--  raise_application_error(-20010,'debug: isum:'||isum||' pr+pq:'||(pr+pq));
   if isum>pr+pq then --2
     pr:=isum-pq;
   end if; --2
   if nvl(r.xpc,0)=0 then --3
    p:=(100-xper)*round((isum-pr)*100/isum,2)/100;
   -- p:=round((isum-pr)*100/isum,2);
   else
    p:=r.xpc;
   end if;  --3
   xper:=xper+p;
   isum:=isum*((100-round((isum-pr)*100/isum,2))/100);
 end if;--1

 if r.secttype='XL' then --1
  vRate := nvl (r.Rate/100,1);
  select
    max(getcrosscover(limitsum+prioritysum,r.currisn,53,sysdate))
    into pr
    from recond where sectisn=r.sectisn;
  if pr<isum and r.optionalcode is null then --2
   if nvl(r.xpc,0)=0 then --3
    p:=(100-xper)*round(100-(isum-pr)*100/isum,2)/100;
   -- p:=round(100-(isum-pr)*100/isum,2);
   else
    p:=r.xpc;
   end if;                 --3

   xper:=xper+p;
   isum:=isum*((100-round((isum-pr)*100/isum,2))/100);
  else --2
   if nvl(r.xpc,0)=0 then --4
    p:=100-xper;
   else
    p:=r.xpc;
   end if;   --4
  end if; --2
 end if; --1


  For Cur In ( Select * from Rep_AgrRe Where AgrIsn=r.aisn and Nvl(SUBJISN,0)>0 ) Loop
     p:=(100-(Nvl(Cur.SHAREPC,0)))*p;
     p:=p/100;
     Exit;
  end loop;
--MZ 06/01/2004
  select max (agrisn)
  into vAgr
  from repcond
  where agrisn =  r.aisn
    and rownum = 1;
  select nvl (max (isn),0)
  into vDept0Isn
  from subdept
  where parentisn = 0
  start with isn = r.redeptisn
  connect by prior parentisn = isn;
  if (vAgr is null) then
    Insert Into Rep_AgrRe
     (isn,agrisn, objisn, riskisn, sectisn, reisn, sharepc, datebase, datebeg, dateend, condisn, datebegx, dateendx, agrxisn, OUTCOMPC, OUTCOMPC1, reclassisn, redept0isn)
    Values
     (Seq_Reports.NEXTVAL,r.aisn, r.objisn, r.riskisn, r.sectisn, r.reisn,  vRate*p, r.datebase, r.datebeg, r.dateend, null, r.Xdatebeg, r.Xdateend, r.xisn, Com1, Com2, r.reclassisn, vDept0Isn);
  else
    Insert Into Rep_AgrRe
     (isn, agrisn, objisn, riskisn, sectisn, reisn, sharepc, datebase, datebeg, dateend, condisn, datebegx, dateendx, agrxisn,OUTCOMPC,OUTCOMPC1, reclassisn, redept0isn)
    select
      Seq_Reports.NEXTVAL, r.aisn, r.objisn, r.riskisn, r.sectisn, r.reisn, vRate*p, r.datebase,
      Nvl(rc.Datebeg,r.datebeg), Nvl(rc.Dateend,r.dateend), rc.condisn, r.Xdatebeg, r.Xdateend, r.xisn,com1,com2,
      r.reclassisn, vDept0Isn
    from repcond rc
    where Rc.AgrIsn = r.aisn
     and (Rc.Isn is null or r.ObjIsn=0  or Rc.PARENtObjIsn  = r.ObjIsn  or Rc.ObjIsn =  r.ObjIsn)
     and (Rc.Isn is null or r.riskisn=0 or Rc.PARENTRISKISN = r.RiskIsn or Rc.RiskIsn = r.RiskIsn);
   end if;

 exception When No_Data_Found THen
   Null;
 end;
end if; --BAG!!! ---33

end loop;

end;


procedure MakeCityBufer
 Is
 Begin
 /*
  Execute Immediate 'Truncate Table Rep_City';

 Insert Into Rep_City
 (
Select A.cityisn ,
 A.regionisn,
 A.COUNTRYISN,
ParentIsn,
(Select Isn From Ais.Region WHere  Nvl(ParentIsn,0)=0 Start With Isn=A.ParentRegionIsn connect by prior parentisn=isn) Parentregionisn,
A.ParentCOUNTRYISN
from
(
Select Isn CityIsn,A.regionisn ,A.COUNTRYISN,A.Isn ParentIsn,A.regionisn Parentregionisn,A.COUNTRYISN ParentCOUNTRYISN
from Ais.City A
WHere Nvl(ParentIsn,0)=0
And Nvl(ACTIVE,'S')<>'S'
Union All
Select A.Isn,A.regionisn ,A.COUNTRYISN, Pc.Isn,Pc.regionisn ,Pc.COUNTRYISN
from Ais.City A,Ais.City Pc
WHere Nvl(A.ParentIsn,0)<>0
And Nvl(A.ACTIVE,'S')<>'S'
And Pc.RowId = (Select RowId from Ais.City  WHere Nvl(ParentIsn,0)=0  Start With Isn=A.Isn connect by prior parentisn=isn)
) a
 );
*/
Commit;

end;

procedure LoadRepAgr_By_Isns(pLoadIsn Number, pMinIsn Number, pMaxIsn Number, IsFull Number:=1) Is
   vBlockIsn number;
Begin
  Delete From tt_rowid;
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  RepLog_i(pLoadIsn,'LoadRepAgr_By_Isns','Insert tt_rowid',pAction=>'Begin',pBlockIsn=>vBlockIsn);

  INSERT INTO tt_rowid
    (SELECT /*+ INDEX_ASC(A X_AGREEMENT)*/ ROWID, Isn
     FROM Agreement A
     WHERE Isn>pMinIsn and Isn<=pMaxIsn);

  RepLog_i(pLoadIsn,'LoadRepAgr_By_Isns','Insert tt_rowid',pAction=>'End',pobjCount=>SQL%ROWCount,pBlockIsn=>vBlockIsn);
  LoadRepAgr_By_tt_RowId(pLoadIsn,IsFull);
end;

procedure LoadRepAgr_By_Hist_Isns(pLoadIsn Number, pMinIsn Number, pMaxIsn Number) Is
   vBlockIsn number;
   vBlockIsn1 number;
   vrc Number;
Begin
--------------------------------------------------------------------------------------------
-- заполняем буфер измененых данных

      Delete From  tt_rowid;
      select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
         RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'Insert tt_rowid',  pAction => 'Begin', pBlockIsn=>vBlockIsn);

         INSERT INTO tt_rowid
          (SELECT /*+ ordered use_nl(h a) INDEX_ASC(h X_rep_MV_Log)*/
                 a.ROWID,h.ObjIsn Agrisn
               FROM tt_Isns H,
                     Agreement a
            where
               H.Isn>pMinIsn And H.Isn<=pMaxIsn
           And h.ObjIsn=a.isn(+)
            );
          RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'Insert tt_rowid',  pAction => 'End', pobjCount => SQL%ROWCount, pBlockIsn=>vBlockIsn);

--------------------------------------------------------------------------------------------------------------
----- Ахтунг! Из repcond всегда сначала!!!!!!!!!!!!!
  -- удаляем записи с измененным полем партиционирования или удаленные из бд
   select Seq_rep_block.NEXTVAL into vBlockIsn1 from dual;
     RepLog_i (pLoadIsn, 'LoadRepCond_By_tt_RowId', 'Delete',  pAction => 'Begin', pBlockIsn=>vBlockIsn1);
      vrc:=0;
      DELETE    --+ Ordered USe_Nl(aa) Index(aa)
      FROM RepCond aa
            WHERE AgrIsn In
            (Select AgrIsn
             from
                  (Select --+ Ordered Use_Nl(ag) Index(ag)
                  ag.RowId rId,Nvl(a.datebeg,trunc(sysdate)) adb,Nvl(ag.datebeg,trunc(sysdate)) sdb,a.isn aisn,
                  ag.Agrisn
                  from tt_rowid t,agreement a  ,RepAgr ag
                  Where t.rId=a.rowid(+)
                  And  t.isn=ag.agrisn(+)
               )
            Where (adb<>sdb or aisn is null)
            );
      vrc := SQL%ROWCount;
     RepLog_i (pLoadIsn, 'LoadRepCond_By_tt_RowId', 'Delete',  pAction => 'End', pobjCount => vrc, pBlockIsn=>vBlockIsn1);

  -- удаляем записи с измененным полем партиционирования или удаленные из бд

   select Seq_rep_block.NEXTVAL into vBlockIsn1 from dual;
     RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'Delete',  pAction => 'Begin', pBlockIsn=>vBlockIsn1);
     vrc:=0;
     DELETE --+ Ordered USe_Nl(aa)
     FROM RepAgr aa
     WHERE RowId In (Select rId
                     from
                       (Select --+ Ordered Use_Nl(ag) Index(ag)
                          ag.RowId rId, Nvl(a.datebeg,trunc(sysdate)) adb, Nvl(ag.datebeg,trunc(sysdate)) sdb,
                          a.isn aisn, ag.isn sisn, t.isn agrisn
                        from tt_rowid t, agreement a, RepAgr ag
                        Where t.rId=a.rowid(+)
                          And t.isn=ag.agrisn(+))
                     Where (adb<>sdb or aisn is null));
     vrc := SQL%ROWCount;
     RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'Delete',  pAction => 'End', pobjCount => vrc, pBlockIsn=>vBlockIsn1);
-----------------------------------------------
  InsertRepagr_Hist(pLoadIsn);
----------------------------------------------------------
  InsertRepCond_Hist(pLoadIsn);
------------------------------------------------------------------------
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;

  RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'InsertDict Call', pAction => 'Begin', pBlockIsn=>vBlockIsn);
  insertDict(pLoadIsn);
  RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'InsertDict Call', pAction => 'End', pBlockIsn=>vBlockIsn);

  COMMIT;

-------------------------------------------------------------------------
  -- чистим лог
  select Seq_rep_block.NEXTVAL into vBlockIsn from dual;
  RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'CLEAR LOG', pAction => 'Begin', pBlockIsn=>vBlockIsn);

  Update /*+ Index( h X_REP_MV_LOG_SHEMA_LOAD) Use_Hash(h) */ rep_MV_Log H
  Set Loadisn=pLoadIsn
  where Nvl(loadisn,0)=0 And Upper(SHEMANAME)='REPAGR'
    and recisn in (Select Isn from tt_RowId)
    and GetStatus=1;

  COMMIT;
  RepLog_i (pLoadIsn, 'LoadRepAgr_By_Hist_Isns', 'CLEAR LOG', pAction => 'End', pBlockIsn=>vBlockIsn);
end;

procedure InsertEtc(pLoadIsn Number) is
  vMinIsn number:=0;
  vMaxIsn number;
  sesid number;
  vSql varchar(4000);
begin
  SesId:=PArallel_Tasks.createnewsession;

  vSql:='Begin
    execute immediate ''truncate table repagr_economic'';
    insert into repagr_economic
    select seq_reports.nextval,'||pLoadISN||', v.* from V_repagr_economic v;
    commit;
  End;';

  Parallel_Tasks.processtask(sesid,vsql);

  vSql:='Begin
    execute immediate ''truncate table repsubject'';
    insert into repsubject
    select distinct
      s.isn,
      s.juridical,
      s.resident,
      s.countryisn,
      s.parentisn,
      first_value(cityisn) over (partition by isn order by decode(cityisn,null,1,0), adrcode) cityisn,
      first_value(regionisn) over (partition by isn order by decode(cityisn,null,1,0), adrcode) regionisn,
--   (select count (distinct cityisn) from subaddr z where z.subjisn = s.isn ) nc,
      branchisn,ORGFORMISN, s.IsBazEl
    from
      (select --+ ordered use_nl(s sa r dc)
         s.isn,
         case when s.isn in ((select subjisn from ingo_group_subject where seq=20)) then 1
         else 0
         end IsBazEl,
         s.juridical,
         s.resident,
         s.countryisn,
         Nvl(s.parentisn,S.Isn) parentisn,
         nvl(dc.code,''99'') adrcode,
         sa.cityisn,
         r.regionisn,s.ORGFORMISN,S.branchisn
       from ais.subject s, subaddr sa, city r, dicti dc
       where sa.subjisn(+)=s.isn
         and r.isn(+)=sa.cityisn
         and dc.isn(+)=sa.classisn) s;
    commit;

    EXPORT_DATA.export_to_owb_by_FLD(''repsubject'',''Isn'');

  end;';

  Parallel_Tasks.processtask(sesid,vsql);

  vSql:='Begin
    execute immediate ''truncate table repdocsnotrub'';
    Insert into repdocsnotrub
    Select --+ Ordered Use_Nl(sa d) index (sa X_SUBACC_CLASS)
      Sa.Isn,d.Isn,d.agrisn
    from ais.Subacc sa ,docs d
    Where sa.currisn<>35 and sa.classisn=13447316 and DISCR=''J''
      and d.accisn=sa.isn
      and d.doc_type=21;
    commit;
  end;';

  Parallel_Tasks.processtask(sesid,vsql);
--ANd Not Exists(select 'X' from docsum ds Where DocIsn=d.Isn and  ds.discr='F' and RowNum<=1);

  vSql:='Begin
-- доводка Rep_AgrCargo
       -- залили паренты, тех кто попал в список
       Merge Into Rep_AgrCargo D
       Using (select --+ use_nl(t c a) ordered
                A.parentisn,
                Max(C.sea) Sea,
                Max(C.more1) more1
              from Rep_AgrCargo c, agreement a
              where c.agrisn=a.Isn and Nvl(a.parentisn,0)>0
              Group By A.ParentIsn) S
       On (D.AgrIsn=S.ParentIsn)
       WHEN MATCHED THEN Update Set D.Sea=S.Sea, D.More1=S.More1
       WHEN NOT MATCHED THEN INSERT (D.AgrIsn, D.Sea,D.More1,D.Isn)
         VALUES (S.ParentIsn, S.Sea, S.More1, SEQ_REPORTS.NextVal);

       commit;
       -- залили чилды, тех кто в списке
       Merge Into Rep_AgrCargo D
       Using (select --+ use_nl(t c a) ordered
                A.Isn,
                Max(C.sea) Sea,
                Max(C.more1) more1
              from Rep_AgrCargo c, agreement a
              where c.agrisn=a.parentIsn
                and Nvl(a.parentisn,0)>0
              Group By A.Isn) S
       On (D.AgrIsn=S.Isn)
       WHEN MATCHED THEN Update Set D.Sea=S.Sea, D.More1=S.More1
       WHEN NOT MATCHED THEN INSERT (D.AgrIsn, D.Sea,D.More1,D.Isn)
         VALUES (S.Isn, S.Sea,S.More1,SEQ_REPORTS.NextVal);
       commit;
    end;';

  Parallel_Tasks.processtask(sesid,vsql);
/*
  execute immediate 'truncate table REPCRGDOC';

  vMinIsn:=0;

  Loop
    Select Max(AgrIsn) Into vMAxIsn
    From
      (Select --+ Index_Asc(d X_CRGDOC_AGR)
       AgrIsn
       from ais.CrgDoc d
       Where AgrISn>vMinIsn and rownum<=100000);

    Exit When vMAxIsn Is Null;

    insert into REPCRGDOC
    Select Seq_reports.NEXTVAL,S.*
    From
      (Select --+ Ordered USe_Nl(sb)
       S.*,sb.juridical
       from
         (Select --+ Index_Asc(d X_CRGDOC_AGR) ordered use_Nl(d sb)
            d.agrisn, d.classisn, Max(d.objisn) objisn, Max(d.subjisn) subjisn
          from ais.CrgDoc d
          Where d.agrisn>vMinIsn and d.agrisn<=vMaxIsn
            and d.classisn=34709216 -- ПАСПОРТ ТРАНСПОРНОГО СРЕДСТВА
          Group by d.agrisn,d.classisn) S,subject sb
       Where sb.isn(+)=S.subjisn )S;
    Commit;

    vMinIsn:=vMaxIsn;
  end loop;


    EXPORT_DATA.export_to_owb_by_FLD('repcrgdoc','AgrIsn');
*/
  Parallel_Tasks.endsession(sesid);


end;

Function Get_merge_slq( ptoTable varchar2,pFromTable varchar2,pLinkFld Varchar2,
pLoadIsn Number,
pExcludeUpdateFields Varchar2:='',
pFromWhere Varchar2:='') return Varchar2
 is
sqlMerge long:='MERGE /*+ use_nl(d) Index(d) ordered */
                INTO ptoTable d USING (
   Select *
   from pFromTable
)s ON ( pLinkFld)
       WHEN MATCHED THEN UPDATE SET pUpdateColumns
       WHEN NOT MATCHED THEN INSERT (D.Isn,D.LoadIsn,pInsertColumns) VALUES (Storages.Seq_Reports.NextVal,'||pLoadIsn||',pfInsertColumns)';
type tColumns is table of varchar2(255);

 vCols tColumns:=tColumns();

 vUpdstr varchar2(4000);
 vInsstr varchar2(4000);

 begin

For Cur in ( Select column_name
            from user_tab_columns a
            Where table_name=Upper(ptoTable)
            and exists (Select column_name
                        from user_tab_columns b
                        Where table_name=Upper(pFromTable) and b.column_name=a.column_name)) Loop

 vCols.extend;
 vCols(vCols.Last):=Cur.column_name;
end loop;


   for i in 1..Nvl(vCols.Last,0) loop
    If Upper(vCols(i))<>trim(Upper(pLinkFld)) and
       Instr(Upper(pExcludeUpdateFields),vCols(i))=0   then
      vUpdstr:=vUpdstr||'d.'||vCols(i)||'=s.'||vCols(i)||',';
   end if;
      vInsstr:=vInsstr||'d.'||vCols(i)||',';
   end loop;
    vUpdstr:=Substr(vUpdstr,1,Length(vUpdstr)-1);
    vInsstr:=Substr(vInsstr,1,Length(vInsstr)-1);


    sqlMerge:=Replace(sqlMerge,'ptoTable',ptoTable);
    sqlMerge:=Replace(sqlMerge,'pFromTable',pFromTable||' '||pFromWhere);
    sqlMerge:=Replace(sqlMerge,'pLinkFld','d.'||pLinkFld||'=s.'||pLinkFld);
    sqlMerge:=Replace(sqlMerge,'pUpdateColumns',vUpdstr);
    sqlMerge:=Replace(sqlMerge,'pInsertColumns',vInsstr);
    sqlMerge:=Replace(sqlMerge,'pfInsertColumns',Replace(vInsstr,'d.','s.'));
  return sqlMerge;




 end;


End;