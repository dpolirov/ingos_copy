--------------------------------------------------------------------------------------------------
--
--   storage_adm.sa_params
--
--------------------------------------------------------------------------------------------------
--truncate table sa_params;
INSERT INTO sa_params (isn, name, cvalue) VALUES (1, 'comlex:auto:bottom_banner', '
    perform storage_adm.Set_Rep_NextRun(vTaskIsn ,null);
    perform storage_adm.SetLoadToTask(vTaskIsn, 0);
    perform shared_system.setparamn(''processfinished'', 1);

    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            raise exception ''% : % : %'', v_function_name, v_step, sqlerrm;
        END;
end;
');
INSERT INTO sa_params (isn, name, cvalue) VALUES (2, 'comlex:auto:top_banner', 'declare
    vTaskIsn        numeric;
    vChildTaskIsn   numeric;
    vProcessShortname varchar;
    vStartId        numeric;
    vLoadIsn        numeric;
    vStepNumber     numeric;
    vStatus         numeric;
    v_function_name CHARACTER VARYING = ''STORAGE_ADM.prcLoadStorageTest_663'';
    v_step          CHARACTER VARYING = ''NA'';
begin
    v_step = ''Get Params'';
    vTaskIsn = coalesce(shared_system.getparamn(''taskisn''), -999);
    vProcessShortname = coalesce(shared_system.getparamv(''processshortname''), ''unknown'');
    vStartId = coalesce(shared_system.getparamn(''startid''), -999);

    vLoadIsn = storage_adm.GetLoadFromTask(vTaskIsn);
    If vLoadisn  = 0 then
        vLoadIsn = storage_adm.createload(vTaskIsn);
        perform storage_adm.SetLoadToTask(vTaskIsn,vLoadIsn);
    end if;

    v_step = ''Do the job'';
    vStepNumber = storage_adm.GetLoadStep(vLoadIsn);
');
INSERT INTO sa_params (isn, name, cvalue) VALUES (3, 'comlex:core:failed:continue', '
    If vStepNumber <= {ITASKSTEP1} then
        vChildTaskIsn = {TASKNUMBER};
        If vStepNumber = {ITASKSTEP0} then
            perform storage_adm.EXECUTE_LOGED_REPORT(vChildTaskIsn,0,0); 
            perform storage_adm.SetLoadStep(vLoadIsn,{ITASKSTEP1}); 
        else
            vStatus = storage_adm.GetTaskStatus(vChildTaskIsn);
            If vStatus = 0  then --не запущенна или первый запуск
                perform storage_adm.SetLoadStep(vLoadisn,{ITASKSTEP2}); 
            elsif vStatus = 1 then  -- идет, ждем
                null;
            elsIf vStatus < 0 then -- Ошибка
                perform storage_adm.Set_Rep_NextRun(vTaskIsn ,null);
                perform storage_adm.LOGREP(true, vTaskIsn, vProcessShortname,''ERROR in ''||v_function_name||'' : ''||v_step||'' : ''||sqlerrm,vStartId,-1,null,null);
                RAISE Exception ''%'', storage_adm.GETTASKERRCODE(vChildTaskIsn);
            end if;
        end if;  
        perform shared_system.setparamn(''processfinished'', 0);
        return;
    end if;
');
INSERT INTO sa_params (isn, name, cvalue) VALUES (4, 'comlex:core:failed:stop', '
    If vStepNumber <= {ITASKSTEP1} then
        vChildTaskIsn = {TASKNUMBER};
        If vStepNumber = {ITASKSTEP0} then
            perform storage_adm.EXECUTE_LOGED_REPORT(vChildTaskIsn,0,0); 
            perform storage_adm.SetLoadStep(vLoadIsn,{ITASKSTEP1}); 
        else
            vStatus = storage_adm.GetTaskStatus(vChildTaskIsn);
            If vStatus = 0  then --не запущенна или первый запуск
                perform storage_adm.SetLoadStep(vLoadisn,{ITASKSTEP2}); 
            elsif vStatus = 1 then  -- идет, ждем
                null;
            elsIf vStatus < 0 then -- Ошибка
                perform storage_adm.Set_Rep_NextRun(vTaskIsn ,null);
                perform storage_adm.LOGREP(true, vTaskIsn, vProcessShortname,''ERROR in ''||v_function_name||'' : ''||v_step||'' : ''||sqlerrm,vStartId,-1,null,null);
                RAISE Exception ''%'', storage_adm.GETTASKERRCODE(vChildTaskIsn);
            end if;
        end if;  
        perform shared_system.setparamn(''processfinished'', 0);
        return;
    end if;
');
INSERT INTO sa_params (isn, name, cvalue) VALUES (5, 'comlex:user:bottom_banner', NULL);
INSERT INTO sa_params (isn, name, cvalue) VALUES (6, 'comlex:user:top_banner', NULL);
INSERT INTO sa_params (isn, name, cvalue) VALUES (7, 'sole:auto:bottom_banner', '
    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            raise exception ''% : % : %'', v_function_name, v_step, sqlerrm;
        END;
end;');
INSERT INTO sa_params (isn, name, cvalue) VALUES (8, 'sole:auto:top_banner', 'declare
    vTaskIsn        numeric;
    vProcessShortname varchar;
    vStartId        numeric;
    vLoadIsn        numeric;
    vStepNumber     numeric;
    vStatus         numeric;
    vDateRep        timestamp;
    v_function_name CHARACTER VARYING = ''STORAGE_ADM.prcLoadStorageTest_{PARENTISN}'';
    v_step          CHARACTER VARYING = ''NA'';
begin
    v_step = ''Get Params'';
    vTaskIsn = coalesce(shared_system.getparamn(''taskisn''), -999);
    vProcessShortname = coalesce(shared_system.getparamv(''processshortname''), ''unknown'');
    vStartId = coalesce(shared_system.getparamn(''startid''), -999);

    select storage_adm.createload(vTaskIsn) into vLoadIsn;
    perform storage_adm.SetLoadToTask(vTaskIsn,vLoadIsn);

    v_step = ''Do the job'';
');
INSERT INTO sa_params (isn, name, cvalue) VALUES (9, 'sole:core:bottom_banner', NULL);
INSERT INTO sa_params (isn, name, cvalue) VALUES (10, 'sole:core:top_banner', NULL);
INSERT INTO sa_params (isn, name, cvalue) VALUES (11, 'sole:user:bottom_banner', NULL);
INSERT INTO sa_params (isn, name, cvalue) VALUES (12, 'sole:user:top_banner', NULL);
INSERT INTO sa_params (isn, name, cvalue) VALUES (13, 'sole:core:failed:stop', '');
INSERT INTO sa_params (isn, name, cvalue) VALUES (14, 'sole:core:failed:continue', '');


--------------------------------------------------------------------------------------------------
--
--   storage_adm.sa_complex_ref
--   ! STAGE 2 only !
--
--------------------------------------------------------------------------------------------------
insert into sa_complex_ref(parentisn, childisn, childrownum) values (663,1026,0);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (663,667,1);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (663,678,2);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (663,1205,3);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (667,1430,5);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (667,727,10);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (667,719,20);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (667,721,40);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (667,723,50);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (667,1452,51);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (667,704,52);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (667,1407,55);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (667,726,60);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (667,1408,80);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (667,1409,90);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (678,839,1);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (965,1026,0);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (965,667,1);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (965,968,2);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (965,971,4);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (965,678,5);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (965,970,6);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (965,1205,7);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1265,965,1);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1265,1225,2);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1265,710,4);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1265,1326,5);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1292,1026,0);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1292,667,1);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1292,678,2);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1292,1288,3);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1292,1289,4);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1292,1291,5);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1292,1205,6);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,719,1);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,721,2);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,723,3);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,1452,4);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,1470,5);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,726,6);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,968,7);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,971,8);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,1407,9);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,1408,10);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,1409,11);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,678,12);
insert into sa_complex_ref(parentisn, childisn, childrownum) values (1471,970,13);

--------------------------------------------------------------------------------------------------
--
--   storage_adm.sa_sole_ref
--   ! STAGE 2 only !
-- get inserts
-- psql -c "select 'insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values ('||taskisn||','||atomrownum||','''||replace(atomsource,'''','''''')||''','''||atomname||''');' from storage_adm.sa_sole_ref order by taskisn,atomrownum;"|sed 's/ *$//g'|sed 's/^ //g'>sa_sole_ref_stage2.sql
--
--------------------------------------------------------------------------------------------------
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (704,12,'perform STORAGES.LoadBuh2Cond(vLoadIsn); ','LoadBuh2Cond');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (719,5, 'perform STORAGE_ADM.LoadStorage(27, 0);','RepAgrRoleAgr');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (719,10,'perform storage_adm.LoadStorage(13, 0);','RepAgr');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (721,1,' perform storage_adm.LoadStorage(12, 0);','RepRefund');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (723,1,' perform storage_adm.LoadStorage(15, 0);','RepBuhBody');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (726,1,'/* перенесено в "Операции после догрузки по логам"
LOAD_STORAGE_ADDS.AgrGrpAdds(loadisn);  
*/
null;
','LoadAgrAdds');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (726,2,'/* перенесено в "Операции после догрузки по логам"
 LOAD_STORAGE_ADDS.BuhBodyGrpAdds(Loadisn);
*/
null;','LoadBuhAdds');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (726,3,'/* перенесено в "Операции после догрузки по логам"
доля перестрах в убытках
LOAD_STORAGE_ADDS.refundgrpadds(loadisn);
*/
null;','LoadRefundAdds');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (726,6,'/* перенесено в "Операции после догрузки по логам" 
LOAD_STORAGE_ADDS.LoadAgrRuleExt(loadisn);
*/
null;','LoadAgrRuleExt');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (726,7,'
    perform storages.make_repbuhre2resection_new();
    perform storages.make_repbuh2resection();
    perform storages.make_repbuhre2directanalytics();
    perform storages.MAKE_AGRXINSUREDSUM4REFUND();
','Перестрахование');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (727,10,'perform LOAD_STORAGE_ADDS.insertdict();','LoadStruncBefore');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (727,20,'perform LOAD_STORAGE_ADDS.LoadAgrDetails();','LoadAgrDetails');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (727,30,'perform COGNOS.MAKE_DIC_TABLES();','Справочники для Cognos');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (839,2,' perform  STORAGES.Create_Agr_Analitiks();','Agr_analytics');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (839,3,' perform  storages.report_budget_load_budget_body_agrs();','Budget_body_agr');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (968,1,'
vDaterep = date_Trunc(''month'', current_timestamp)-1;

--LoadIsn =storage_adm.createload(pProcType=>12,pDaterep=>vDaterep,pclassisn=>1);
vLoadIsn =storage_adm.createload(12);
Update
   storage_adm.Sa_Processes
Set
  LASTRUNISN=vLoadIsn
 Where Isn=12;

-- сделали загрузку актуальной
Update storage_adm.repload Set
classisn = null
where PROCISN=12  and Daterep=vDaterep and isn<>vLoadIsn;

perform STORAGES.REPORT_RZU_LOADRESERVE(vLoadIsn,vDaterep );  


Update
   storage_adm.sa_processes
Set
  LASTRUNISN=0
 Where Isn=12;
','rsu');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (970,1,'

--LoadIsn:=REPORT_STORAGE.createload(ptype=>REPORT_STORAGE.cLoadFull,pProcType=>21);
vLoadIsn=REPORT_STORAGE.createload(21);

 Update 
    storage_adm.sa_processes
 Set
   LASTRUNISN=vLoadIsn
    Where Isn=21;

perform STORAGES.REP_STAT_FULL_REPBUHSUMMARY(vloadisn);


','prem,ubytki, com');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (971,1,'
    vDaterep = date_Trunc(''month'', current_timestamp)-1;
    perform STORAGES.REPORT_RNP_NEW_MAKE_RNP_ALL(vDaterep);
','rnp 2 in 1');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1015,1,'perform storage_adm.LoadStorage(1, 0);','ST_BuhBody');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1015,2,'perform storage_adm.LoadStorage(2, 0);','BuhBaseBody');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1015,3,'perform storage_adm.LoadStorage(3, 0);','DocSumBody');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1015,4,'perform storage_adm.LoadStorage(4, 0);','BuhTurn');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1015,5,'perform storage_adm.LoadStorage(5, 0);','BuhBody_Reins');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1015,6,'perform storage_adm.LoadStorage(6, 0);','rep_subject');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1015,7,'perform storage_adm.LoadStorage(7, 0);','BuhTurnCor');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1015,8,'perform storage_adm.LoadStorage(8, 0);','BuhTurnContr');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1015,9,'/* storage_adm.load_storage.LoadStorage(10,0,0);     */
null;','JuorAgrOSAGO');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1015,10,'perform storage_adm.LoadStorage(14, 0);','Nfs');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1015,11,'/* sts 23.04.2013 - убрал, т.к. сделана новая витрина MOTOR.REP_DOCS_DOCSUM_CLMINV_LINE
  и грузится она в моторе
Пока оставил до перевода отчета на новую витрину
 storage_adm.load_storage.LoadStorage(22,0,0);     
 */
','REP_DOCS_DOCSUM_CLMINV');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1026,1,'perform STORAGES.P_STORAGELOAD_SENDMAIL(''BE'');','Почта. Начало');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1205,0,'perform STORAGES.P_STORAGELOAD_SENDMAIL(''EN'');','Stop');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1288,1,'
vDaterep = date_Trunc(''month'', current_timestamp)-1 + interval ''1 month'';
--LoadIsn:=REPORT_STORAGE.createload(pProcType=>12,pDaterep=>vDaterep,pclassisn=>1);
vLoadIsn=storage_adm.createload(12);

Update
   storage_adm.sa_processes
Set
  LASTRUNISN=vLoadIsn
 Where Isn=12;

-- сделали загрузку актуальной
Update storage_adm.repload Set
classisn = null
where PROCISN=12  and Daterep=vDaterep and isn<>vLoadIsn;

perform STORAGES.REPORT_RZU_LOADRESERVE(vLoadIsn,vDaterep, null,null,null);  

 
 
Update
   storage_adm.sa_processes
Set
  LASTRUNISN=0
 Where Isn=12;


','rzu');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1289,1,'
vDaterep = date_Trunc(''month'', current_timestamp)-1 + interval ''1 month'';
  perform STORAGES.REPORT_RNP_NEW_MAKE_RNP_ALL(vDaterep);  
','rnp 2 in 1');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1291,1,'
  vDaterep =current_timestamp+1; 
--LoadIsn:=REPORT_STORAGE.createload(ptype=>REPORT_STORAGE.cLoadFull,pProcType=>21);
vLoadIsn=storage_adm.createload(21);

 Update 
    storage_adm.sa_processes
 Set
   LASTRUNISN=vLoadIsn
    Where Isn=21;

perform STORAGES.REP_STAT_FULL_REPBUHSUMMARY(vloadisn, vDaterep);


','prem, ubytok, commission');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1306,1,' 
  vloadisn = STORAGES.REPORT_RNP_NEW_GETACTIVELOAD(trunc(sysdate,''mm'')-1 + interval ''1 month'');
  perform STORAGES.INSERT_AGRRE(0, null);
  perform STORAGES.report_rnp_new_make_rnp_re_rsbu(vloadisn);
  perform STORAGES.report_rnp_new_make_rnp_re_msfo(vloadisn);
  perform STORAGES.report_rnp_new_make_rnp_re_subject(vloadisn);
','re');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1346,0,'-- грузим repbuh2obj с начала года по текущий месяц включительно
perform STORAGES.REP_MOTIVATION_LOAD_REPBUH2OBJ(date_trunc(''month'', current_timestamp-interval ''12 month''), date_trunc(''month'', current_timestamp) - interval ''1 month'');','repbuh2obj');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1366,0,'perform storages.INSERT_AGRRE(0);
perform storages.make_repbuhre2resection();
perform storages.make_repbuh2resection();
perform storages.make_repbuhre2directanalytics();
perform storages.MAKE_AGRXINSUREDSUM4REFUND();','Вызов');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1406,0,'perform shared_system.GET_STORAGE_STAT() ;','Stat');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1407,10,'perform storage_adm.LoadStorage(19, 0);','rep_agr_salers');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1408,10,'-- грузим repbuh2obj с начала года по текущий месяц включительно
perform STORAGES.REP_MOTIVATION_LOAD_REPBUH2OBJ(oracompat.trunc(oracompat.ADD_MONTHS(current_timestamp,-12),''MM''), oracompat.add_months(oracompat.trunc(current_timestamp, ''mm''), 1));

','RepBuh2Obj');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1409,10,'perform storage_adm.LoadStorage(20,0);','Rep_Agent_Ranks');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1410,10,'perform COGNOS.MAKE_DIC_TABLES(''Y'');','Обновление справочников Cognos');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1430,10,'perform PKG_MAKE_DICTI_TABLES.MAKE_MOTOR_DICTI_TABLES;','Формирование справочников');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1452,10,'perform LOAD_STORAGE_ADDS.AgrGrpAdds(vloadisn);  ','LoadAgrAdds');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1452,11,'perform LOAD_STORAGE_ADDS.BuhBodyGrpAdds(vLoadisn);','LoadBuhAdds');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1452,12,'/* доля перестрах в убытках*/
perform LOAD_STORAGE_ADDS.refundgrpadds(vloadisn);','LoadRefundAdds');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1452,13,'perform LOAD_STORAGE_ADDS.LoadAgrRuleExt(vloadisn);','LoadAgrRuleExt');
insert into sa_sole_ref(taskisn, atomrownum, atomsource, atomname) values (1470,0,'perform STORAGES.REPORT_BUH_STORAGE_NEW_LOADBUH2COND_BY_DATE(vLoadIsn,''01-sep-2013'');','repbuh2cond');


--recreate sole procedures
select coalesce(storage_adm.GenerateSoleProcedure(704, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(719, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(721, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(723, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(726, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(727, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(839, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(968, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(970, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(971, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1015, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1026, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1205, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1288, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1289, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1291, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1306, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1346, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1366, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1406, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1407, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1408, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1409, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1410, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1430, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1452, true, 0, null, null, null, null) ,'null');
select coalesce(storage_adm.GenerateSoleProcedure(1470, true, 0, null, null, null, null) ,'null');

select coalesce(storage_adm.GenerateComplexProcedure(663, true, null, 0, null, null, null) ,'null');
select coalesce(storage_adm.GenerateComplexProcedure(667, true, null, 0, null, null, null) ,'null');
select coalesce(storage_adm.GenerateComplexProcedure(678, true, null, 0, null, null, null) ,'null');
select coalesce(storage_adm.GenerateComplexProcedure(965, true, null, 0, null, null, null) ,'null');
select coalesce(storage_adm.GenerateComplexProcedure(1265, true, null, 0, null, null, null) ,'null');
select coalesce(storage_adm.GenerateComplexProcedure(1292, true, null, 0, null, null, null) ,'null');
select coalesce(storage_adm.GenerateComplexProcedure(1471, true, null, 0, null, null, null) ,'null');

--------------------------------------------------------------------------------------------------
--
--   storage_adm.sa_freq
--
--------------------------------------------------------------------------------------------------
insert into storage_adm.sa_freq (freq, dateadd, shortname, rconst) values (0,'NULL','По запросу','');  
insert into storage_adm.sa_freq (freq, dateadd, shortname, rconst) values (1,'coalesce(lastrun, current_timestamp) + interval ''1 day''','Ежедневный','dd');    
insert into storage_adm.sa_freq (freq, dateadd, shortname, rconst) values (2,'coalesce(lastrun, current_timestamp) + interval ''1 hour''','Ежечасный','');    
insert into storage_adm.sa_freq (freq, dateadd, shortname, rconst) values (7,'coalesce(lastrun, current_timestamp) + interval ''7 day''','Еженедельный','day'); 
insert into storage_adm.sa_freq (freq, dateadd, shortname, rconst) values (30,'coalesce(lastrun, current_timestamp) + interval ''1 month''','Ежемесячный','mon'); 
insert into storage_adm.sa_freq (freq, dateadd, shortname, rconst) values (90,'coalesce(lastrun, current_timestamp) + interval ''3 month''','Ежеквартальный','q');
insert into storage_adm.sa_freq (freq, dateadd, shortname, rconst) values (182,'coalesce(lastrun, current_timestamp) + interval ''6 month''','Полугодовой','');   
insert into storage_adm.sa_freq (freq, dateadd, shortname, rconst) values (365,'coalesce(lastrun, current_timestamp) + interval ''1 year''','Ежегодный','YYYY');

--------------------------------------------------------------------------------------------------
--
--   storage_adm.ss_processes
--   For stage 2 used 1,2,3,4,5,6,7,8,12,13,14,15,19,20,27,52
--
--------------------------------------------------------------------------------------------------
--stage 2
insert into storage_adm.ss_processes values(1,'ST_BuhBody','ST_BUHBODY','Y',10,30000,'N','');
insert into storage_adm.ss_processes values(2,'BuhBaseBody','ST_BODYDEBCRE,St_bodydebt_corr','Y',20,10000,'N','');    
insert into storage_adm.ss_processes values(3,'DocSumBody','st_docsumbody','Y',30,10000,'N','');
insert into storage_adm.ss_processes values(4,'BuhTurn','Обороты по всем счетам','Y',40,300,'N','');
insert into storage_adm.ss_processes values(5,'BuhBody_Reins','Проводки по перестрахованию','Y',50,10000,'N','');
insert into storage_adm.ss_processes values(6,'rep_subject','Витрина контрагентов для отчетности','Y',60,500,'Y',''); 
insert into storage_adm.ss_processes values(7,'BuhTurnCor','Обороты по счетам c корреспонденцией','Y',40,1000,'N','');
insert into storage_adm.ss_processes values(8,'BuhTurnContr','Обороты по счетам с аналитикой контрагента','Y',70,100,'N','');   
insert into storage_adm.ss_processes values(12,'REPREFUND','Загрузка Витрины REPREFUND','Y',140,5000,'N','');    
insert into storage_adm.ss_processes values(13,'AGRGROUP','Загрузка таблиц группы "договор"','Y',120,10000,'N','');   
insert into storage_adm.ss_processes values(14,'BODY_ISN_GROUP','Загрузка таблиц проводок (разные), Логируется Body.Isn','Y',130,30000,'N','');
insert into storage_adm.ss_processes values(15,'REPBUHBODYGROUP','Загрузка REPBUHBODY - проводки проходят серьезную очистку и результат - HEADISN','Y',110,5000,'N','');
insert into storage_adm.ss_processes values(19,'AgrSalers','Загрузка продавцов','Y',180,5000,'N','storage_adm.prc_rep_agr_salers_afterscript();');
insert into storage_adm.ss_processes values(20,'AgrAgents','Загрузка агентов','Y',190,5000,'N','');
insert into storage_adm.ss_processes values(23,'BEST_ADDR','Загрузка витрины с Крыловским адресом','Y',10,10000,'N','');   
insert into storage_adm.ss_processes values(27,'RepAgrRoleAgr','Загрузка витрины по ролям договора','Y',10,10000,'N','');  
insert into storage_adm.ss_processes values(52,'SUBJECT_ATTRIB','Загрузка витрины STORAGE_SOURCE.SUBJECT_ATTRIB','Y',10,100000,'N','');   
-- later stages
insert into storage_adm.ss_processes values(9,'RepAgr','договоры страхования','N',80,100000,'N','');
insert into storage_adm.ss_processes values(10,'JuorAgrOSAGO','ST_JuorAgrOSAGO','N',90,10000,'N','');
insert into storage_adm.ss_processes values(11,'JuorClaimOSAGO','ST_JuorClaimOSAGO','N',100,10000,'N','');
insert into storage_adm.ss_processes values(16,'MedAgrGroup','Загрузка Медицины','Y',150,1000,'N','');
insert into storage_adm.ss_processes values(17,'MedRefundGroup','Загрузка Медицины: Убытки','Y',160,1000,'N','');
insert into storage_adm.ss_processes values(18,'MedBuhGroup','Загрузка Медицины: Бухгалтерия','Y',170,1000,'N','');   
insert into storage_adm.ss_processes values(21,'CarRepAgr','Загрузка витрины CarRepAgr','Y',200,5000,'N','');    
insert into storage_adm.ss_processes values(22,'REP_DOCS_DOCSUM_CLMINV','Загрузка витрины REP_DOCS_DOCSUM_CLMINV - sts: отключил в загрузчике № 1015. Вместо него - включил 24 процесс в загрузчик 1330','N',220,10000,'N','');
insert into storage_adm.ss_processes values(24,'REP_DOCS_DOCSUM_CLMINV_LINE','Загрузка витрины по счетам, строкам калькуляции и их документам','N',10,10000,'N','');
insert into storage_adm.ss_processes values(25,'REP_DOCS_DOCSUM','Загрузка витрины по моторным документам и доксуммам','Y',10,5000,'N','');    
insert into storage_adm.ss_processes values(26,'REP_CLMINV_LINE','Загрузка витрины по моторным счетам, строкам калькуляции','Y',10,5000,'N','');    
insert into storage_adm.ss_processes values(28,'REP_CLIENT_QUEUE_CALLS','Загрузка витрины по контактам и звонкам','Y',10,10000,'N','');   
insert into storage_adm.ss_processes values(29,'TariffKoeff','Загрузка моторного ХД: Тарифы и скидки','Y',10,10000,'Y','');
insert into storage_adm.ss_processes values(30,'Carcond','Загрузка моторного ХД: Carcond','Y',10,1000,'N','');   
insert into storage_adm.ss_processes values(31,'Car_Attrib','Загрузка моторного ХД: Атрибуты авто','N',10,10000,'N','');   
insert into storage_adm.ss_processes values(32,'repagrobjectext','Загрузка моторного ХД: Факторы риска объекта','Y',10,50000,'N','');
insert into storage_adm.ss_processes values(33,'CARCOND_OBJ_SUM','Загрузка моторного ХД: CARCOND_OBJ_SUM','Y',20,25000,'N',''); 
insert into storage_adm.ss_processes values(35,'CAR_CUBE','Загрузка моторного ХД: CAR_CUBE','Y',10,50000,'N','');
insert into storage_adm.ss_processes values(36,'CAR_SUM','Загрузка моторного ХД: CAR_SUM','Y',10,50000,'N','');  
insert into storage_adm.ss_processes values(37,'MTPL_POLICIES','Загрузка моторного ХД: MTPL_POLICIES','Y',10,50000,'N','');
insert into storage_adm.ss_processes values(38,'MTPL_SUMMARIZED ','Загрузка моторного ХД: MTPL_SUMMARIZED','Y',10,50000,'N','');
insert into storage_adm.ss_processes values(39,'PVUFIXSUM','Загрузка моторного ХД: PVUFIXSUM','N',10,50000,'N','');   
insert into storage_adm.ss_processes values(40,'RISK2RPTCLASS','Загрузка моторного ХД: RISK2RPTCLASS','N',10,10000,'N','');
insert into storage_adm.ss_processes values(41,'CARREFUND','Загрузка моторного ХД: CARREFUND','N',10,50000,'N','');   
insert into storage_adm.ss_processes values(42,'CARDOCSUMINVOICELINE','Загрузка моторного ХД: CARDOCSUMINVOICELINE','N',10,10000,'N',''); 
insert into storage_adm.ss_processes values(43,'CAR_ATTRIB','Загрузка моторного ХД: CAR_ATTRIB','N',10,25000,'N',''); 
insert into storage_adm.ss_processes values(44,'CARDRIVER','Загрузка моторного ХД: CARDRIVER','N',10,20000,'N','');   
insert into storage_adm.ss_processes values(45,'DOPUSLUGI_SUMS','Загрузка моторного ХД: DOPUSLUGI_SUMS','N',10,250000,'N','');  
insert into storage_adm.ss_processes values(46,'REP_PRESALE','Загрузка моторного ХД: REP_PRESALE','N',10,10000,'N','');    
insert into storage_adm.ss_processes values(47,'REPAGR_AGENT','Загрузка моторного ХД: REPAGR_AGENT','N',10,10000,'N','');  
insert into storage_adm.ss_processes values(48,'CAR_DEALERS','Загрузка моторного ХД: CAR_DEALERS','N',10,10000,'N','');    
insert into storage_adm.ss_processes values(49,'AGR_DRIVERS','Загрузка моторного ХД: AGR_DRIVERS','N',10,50000,'N','');    
insert into storage_adm.ss_processes values(50,'REP_QUEUE_MAIL','Загрузка моторного ХД: REP_QUEUE_MAIL','N',10,10000,'N','');   
insert into storage_adm.ss_processes values(51,'Med_AgrCondBuh','Загрузка Медицины: MED_AGRCONDNUH','Y',230,100000,'N','');
insert into storage_adm.ss_processes values(53,'ClientDirectivity','Загрузка витрины storages.clientdirectivity','Y',10,5000,'N','');


--------------------------------------------------------------------------------------------------
--
--   storage_adm.ss_process_dest_tables
--   For stage 2 used 1,2,3,4,5,6,7,8,12,13,14,15,19,20,27,52
--
--------------------------------------------------------------------------------------------------
-- stage 2
insert into storage_adm.ss_process_dest_tables values(1,'STORAGES.st_buhbody','storage_adm.v_tt_buhbody','STORAGE_ADM.tt_buhbody','bodyisn','bodyisn',10,'x_st_buhbody_body',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(2,'STORAGES.st_bodydebcre','storage_adm.v_tt_bodydebcre','STORAGE_ADM.tt_bodydebcre','baseisn,db','baseisn',20,'x_st_bodydebcre_base',0,'','','','','');insert into storage_adm.ss_process_dest_tables values(3,'STORAGES.st_docsumbody','storage_adm.v_tt_docsumbody','STORAGE_ADM.tt_docsumbody','BODYISN,DSISN','BODYISN',null,'X_ST_DOCSUMBODY_DS',1,'DATEEND','DATEBEG','','select shared_system.gcc2_load();','','');
insert into storage_adm.ss_process_dest_tables values(4,'STORAGES.ST_BUH_TURN','storage_adm.v_tt_BUH_TURN','STORAGE_ADM.tt_BUH_TURN','PRM_KEY,coalesce(SUBKINDISN,-1),coalesce(OPRISN,-1),coalesce(CURRISN,-1),CODE,coalesce(DEB,timestamp ''1900-01-01 00:00:00'')','PRM_KEY',null,'X_ST_BUH_TURN_UNIC',0,'','','','','','PRM_KEY,coalesce(SUBKINDISN,-1) SUBKINDISN,coalesce(OPRISN,-1) OPRISN,coalesce(CURRISN,-1) CURRISN,CODE,coalesce(DEB,timestamp ''1900-01-01 00:00:00'') DEB');
insert into storage_adm.ss_process_dest_tables values(5,'STORAGES.st_buhbody_reins','storage_adm.v_tt_buhbody_reins','STORAGE_ADM.tt_buhbody_reins','bodyisn','bodyisn',null,'x_st_buhbody_reins_body',0,'','','','select shared_system.gcc2_load();','');
insert into storage_adm.ss_process_dest_tables values(6,'STORAGES.ST_REP_SUBJECT','storage_adm.V_REP_SUBJECT','STORAGE_ADM.tt_REP_SUBJECT','SubjIsn','SubjIsn',null,'x_REP_SUBJECT_subj',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(7,'STORAGES.st_buh_turn_corr','storage_adm.v_tt_buh_turn_corr','STORAGE_ADM.tt_buh_turn_corr','PRM_KEY,coalesce(SubKindISn,-1),coalesce(OPRISN,-1),CorCode','PRM_KEY',null,'x_st_buh_turn_Corr_unic',0,'','','','','','PRM_KEY,coalesce(SubKindISn,-1) subkindisn,coalesce(OPRISN,-1) oprisn,CorCode');
insert into storage_adm.ss_process_dest_tables values(8,'STORAGES.ST_BUH_TURN_CONTR','storage_adm.V_TT_BUH_TURN_CONTR','STORAGE_ADM.TT_BUH_TURN_CONTR','PRM_KEY,coalesce(OPRISN,-1),coalesce(Resident,''A''),coalesce(branchisn,0),coalesce(CURRISN,-1),coalesce(JURIDICAL,''A'')','PRM_KEY',null,'x_st_buh_turn_Contr_unic',0,'','','','select shared_system.gcc2_load();','','PRM_KEY,coalesce(OPRISN,-1) OPRISN,coalesce(Resident,''A'') Resident,coalesce(branchisn,0) branchisn,coalesce(CURRISN,-1) CURRISN,coalesce(JURIDICAL,''A'') JURIDICAL');
insert into storage_adm.ss_process_dest_tables values(12,'STORAGE_SOURCE.REPREFUND','storage_adm.V_REPREFUND','STORAGE_ADM.tt_reprefund','REFUNDISN,coalesce(AGREXTISN,0)','CLAIMISN',null,'X_REPREFUND_REF_EXT',0,'','','','select shared_system.gcc2_load();','');
insert into storage_adm.ss_process_dest_tables values(13,'STORAGE_SOURCE.REP_AGRCARGO','storage_adm.V_REPAGRCARGO','STORAGE_ADM.tt_REP_AGRCARGO','AgrIsn','AgrIsn',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(13,'STORAGE_SOURCE.rep_agrext','storage_adm.v_rep_agrext','STORAGE_ADM.tt_rep_agrext','AgrIsn','AgrIsn',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(13,'STORAGE_SOURCE.REP_AGRTUR','storage_adm.V_REPAGRTUR','STORAGE_ADM.tt_REP_AGRTUR','Agrisn','AgrIsn',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(13,'STORAGE_SOURCE.rep_longagraddendum','storage_adm.v_longagraddendum','STORAGE_ADM.tt_longagraddendum','ADDISN','AgrIsn',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(13,'STORAGE_SOURCE.rep_objclass_domestic','storage_adm.v_rep_objclass_domestic','STORAGE_ADM.tt_rep_objclass_domestic','AgrIsn,ObjClassIsn,Parentobjclassisn','AgrIsn',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(13,'STORAGE_SOURCE.REPAGR','storage_adm.V_REPAGR','STORAGE_ADM.tt_repagr','AgrIsn','AgrIsn',10,'',0,'','','','select shared_system.gcc2_load();','');
insert into storage_adm.ss_process_dest_tables values(13,'STORAGE_SOURCE.repagr_economic','storage_adm.V_repagr_economic','STORAGE_ADM.tt_repagr_economic','AgrIsn','AgrIsn',null,'',0,'','','','select shared_system.gcc2_load();','');
insert into storage_adm.ss_process_dest_tables values(13,'STORAGE_SOURCE.REPCOND','storage_adm.V_REPCOND','STORAGE_ADM.tt_repcond','CondIsn','AgrIsn',null,'X_REPCOND_COND',0,'','','','select shared_system.gcc2_load();','');
insert into storage_adm.ss_process_dest_tables values(13,'STORAGE_SOURCE.REPCRGDOC','storage_adm.v_repcrgdoc','STORAGE_ADM.tt_repcrgdoc','Agrisn,classisn','Agrisn',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(14,'storages.st_buhbody_nms','storage_adm.v_tt_buhbody_nms','STORAGE_ADM.tt_buhbody_nms','BODYISN','BODYISN',null,'X_buhbody_nms_BODY',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(15,'STORAGE_SOURCE.REPBUHBODY','storage_adm.V_REPBUHBODY','storage_adm.tt_repbuhbody','BODYISN,coalesce(DOCSUMISN,0),coalesce(factisn,0),coalesce(buhquitisn,0),coalesce(buhquitbodyisn,0),Dateval','HEADISN',10,'X_PREBUHBODY_ASPK',0,'','','','select shared_system.gcc2_load();','');
insert into storage_adm.ss_process_dest_tables values(15,'STORAGE_SOURCE.REPBUHQUIT','storage_adm.v_repbuhquit','storage_adm.tt_repbuhquit','BodyIsn,buhquitbodyisn,buhquitisn,coalesce(quitbodyisn,0),Dateval','HeadIsn',20,'X_REPBUHQUIT_BODY',0,'','','','select shared_system.gcc2_load();','');
insert into storage_adm.ss_process_dest_tables values(19,'STORAGES.REP_AGR_SALERS','storage_adm.V_REP_AGR_SALERS','STORAGE_ADM.TT_REP_AGR_SALERS','AGRISN, SALERISN, coalesce(SALERCLASSISN, 0), AGRSALERCLASSISN, DATEBEG, DATEEND, DEPTISN','AGRISN',10,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(19,'STORAGES.REP_AGR_SALERS_LINE','storage_adm.V_REP_AGR_SALERS_LINE','STORAGE_ADM.TT_REP_AGR_SALERS_LINE','AGRISN, DATEBEG, DATEEND','AGRISN',20,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(20,'MOTOR.AGRAGENT_RANKS','storage_adm.V_AGRAGENT_RANKS','STORAGE_ADM.TT_AGRAGENT_RANKS','AGRISN, AGENTISN, CLASSISN, AGENT_RANK, SHAREPC, AGRRANK_RANK, ARCLASSISN','AGRISN',20,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(20,'STORAGES.REP_AGENT_RANKS','storage_adm.V_REP_AGENT_RANKS','STORAGE_ADM.TT_REP_AGENT_RANKS','AGRISN, ADDISN, AGENTISN, ORDERNO','AGRISN',10,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(23,'STORAGE_SOURCE.SUBJ_BEST_ADDR','storage_adm.V_SUBJ_BEST_ADDR','STORAGE_ADM.TT_SUBJ_BEST_ADDR','SUBJISN','SUBJISN',10,'X_SUBADDR_SUBJ',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(27,'STORAGE_SOURCE.REPAGRROLEAGR','storage_adm.V_REPAGRROLEAGR','STORAGE_ADM.TT_REPAGRROLEAGR','AGRISN','AGRISN',10,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(52,'STORAGE_SOURCE.SUBJECT_ATTRIB','storage_adm.V_SUBJECT_ATTRIB','STORAGE_ADM.TT_SUBJECT_ATTRIB','SUBJISN','SUBJISN',null,'',0,'','','','','');
-- later stages
insert into storage_adm.ss_process_dest_tables values(9,'STORAGES.ST_REPAGR','STORAGES.V_TT_REPAGR','STORAGES.TT_REPAGR','AGRISN','AGRISN',null,'X_REPAGR',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(10,'STORAGES.ST_JuorAgrOSAGO','STORAGES.v_tt_JuorAgrOSAGO','STORAGES.tt_JuorAgrOSAGO','AGRISN','AGRISN',null,'X_ST_JOURAGROSAGO_AGR',1,'DTEND','DTBEG','','','');
insert into storage_adm.ss_process_dest_tables values(11,'STORAGES.ST_JuorClaimOSAGO','storage_adm.v_tt_JuorClaimOSAGO','STORAGE_ADM.tt_JuorClaimOSAGO','AGRISN, DISCR','AGRISN',null,'X_ST_JuorClaimOSAGO_AGR',1,'DTEND','DTBEG','','','');
insert into storage_adm.ss_process_dest_tables values(16,'MEDIC.ST_MEDAGENTAGR','storage_adm.V_MEDAGENTAGR','STORAGE_ADM.TT_MEDAGENTAGR','AGRISN','AGRISN',40,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(16,'MEDIC.ST_MEDAGREEMENT','storage_adm.V_MEDAGREEMENT','STORAGE_ADM.TT_MEDAGREEMENT','AGRISN','AGRISN',50,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(16,'MEDIC.ST_MEDAGRROLE','storage_adm.V_MEDAGRROLE','STORAGE_ADM.TT_MEDAGRROLE','ROLEISN','AGRISN',20,'X_MEDAGRROLE_ROLEISN',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(16,'MEDIC.ST_MEDAGRROLEAGR','storage_adm.V_MEDAGRROLEAGR','STORAGE_ADM.TT_MEDAGRROLEAGR','AGRISN','AGRISN',30,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(16,'MEDIC.ST_MEDCOND','storage_adm.V_MEDCOND','STORAGE_ADM.TT_MEDCOND','CONDISN','AGRISN',70,'X_MEDCOND',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(16,'MEDIC.ST_MEDCONDAGRDE','storage_adm.V_MEDCOND','STORAGE_ADM.TT_MEDCOND','CONDISN','AGRISN',80,'X_ST_MEDCONDAGRDE',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(16,'MEDIC.ST_MEDOBJECT','storage_adm.V_MEDOBJECT','STORAGE_ADM.TT_MEDOBJECT','OBJISN','AGRISN',60,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(16,'MEDIC.ST_MEDREPAGR','storage_adm.V_MEDREPAGR','STORAGE_ADM.TT_MEDREPAGR','AGRISN','AGRISN',10,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(17,'MEDIC.ST_MEDREFUND','storage_adm.V_MEDREFUND','STORAGE_ADM.TT_MEDREFUND','REFUNDISN,coalesce(EXTISN,0)','CLAIMISN',null,'X_MEDREFUND_PK',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(18,'MEDIC.ST_MEDSUM','storage_adm.V_MEDSUM','STORAGE_ADM.TT_MEDSUM','DOCSUMISN','DOCSUMISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(21,'MOTOR.CARREPAGR','storage_adm.V_CARREPAGR','STORAGE_ADM.TT_CARREPAGR','AGRISN','AGRISN',10,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(22,'MOTOR.REP_DOCS_DOCSUM_CLMINV','storage_adm.V_REP_DOCS_DOCSUM_CLMINV','STORAGE_ADM.TT_REP_DOCS_DOCSUM_CLMINV','DOC_SIGNED, DOCSUM_ISN','CLMINV_ISN',10,'X_DOCSUMCLMINV_DOCSUM',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(24,'MOTOR.REP_DOCS_DOCSUM_CLMINV_LINE','storage_adm.V_REP_DOCS_DOCSUM_CLMINV_LINE','STORAGE_ADM.TT_REP_DOCS_DOCSUM_CLMINV_LINE','CLMINVL_ISN, CLMINV_ISN, REFUNDISN, DOCSUM_ISN, DOC_ISN','CLMINV_ISN',10,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(25,'MOTOR.REP_DOCS_DOCSUM','storage_adm.V_REP_DOCS_DOCSUM','STORAGE_ADM.TT_REP_DOCS_DOCSUM','DOCSUM_ISN','AGRISN',10,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(26,'MOTOR.REP_CLMINV_LINE','storage_adm.V_REP_CLMINV_LINE','STORAGE_ADM.TT_REP_CLMINV_LINE','CLMINVL_ISN, CLMINV_ISN, REFUNDISN','CLMINV_ISN',10,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(27,'STORAGE_SOURCE.REPAGRROLEAGR','storage_adm.V_REPAGRROLEAGR','STORAGE_ADM.TT_REPAGRROLEAGR','AGRISN','AGRISN',10,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(29,'MOTOR.TARIFF_KOEFF','storage_adm.V_TARIFF_KOEFF','STORAGE_ADM.TT_TARIFF_KOEFF','AGRISN','AGRISN',30,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(29,'MOTOR.TARIFF_KOEFF_OBJ','storage_adm.V_TARIFF_KOEFF_OBJ','STORAGE_ADM.TT_TARIFF_KOEFF_OBJ','AGRISN, PARENTOBJISN, RPTCLASS, RPTCLASSISN','AGRISN',10,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(29,'MOTOR.TARIFF_KOEFF_RPT','storage_adm.V_TARIFF_KOEFF_RPT','STORAGE_ADM.TT_TARIFF_KOEFF_RPT','AGRISN, RPTCLASS, RPTCLASSISN','AGRISN',20,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(30,'MOTOR.CARCOND','storage_adm.V_CARCOND','STORAGE_ADM.TT_CARCOND','AGRISN, PARENTOBJISN, OBJISN, RPTCLASS, RPTCLASSISN','AGRISN',10,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(32,'MOTOR.REPAGROBJECTEXT','storage_adm.v_REPAGROBJECTEXT','STORAGE_ADM.TT_REPAGROBJECTEXT','OBJISN','OBJISN',10,'X_REPAGROBJECTEXT_OBJ',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(33,'MOTOR.CARCOND_OBJ_SUM','storage_adm.V_CARCOND_OBJ_SUM','STORAGE_ADM.TT_CARCOND_OBJ_SUM','AGRISN, PARENTOBJISN, RPTCLASS, RPTCLASSISN','PARENTOBJISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(33,'MOTOR.CARCOND_OBJ_SUM_DO','storage_adm.V_CARCOND_OBJ_SUM_DO','STORAGE_ADM.TT_CARCOND_OBJ_SUM_DO','AGRISN, PARENTOBJISN, RPTCLASS, RPTCLASSISN','PARENTOBJISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(35,'MOTOR.CAR_CUBE','storage_adm.V_CAR_CUBE','STORAGE_ADM.TT_CAR_CUBE','AGRISN, PARENTOBJISN, RPTCLASS, RPTCLASSISN, COND_DATEBEG, COND_DATEEND','PARENTOBJISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(36,'MOTOR.CARSUM','storage_adm.V_CARSUM','STORAGE_ADM.TT_CARSUM','CARSUMISN','CARSUMISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(37,'MOTOR.MTPL_POLICIES','storage_adm.V_MTPL_POLICIES','STORAGE_ADM.TT_MTPL_POLICIES','AGRISN','AGRISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(38,'MOTOR.MTPL_SUMMARIZED','storage_adm.V_MTPL_SUMMARIZED','STORAGE_ADM.TT_MTPL_SUMMARIZED','AGRISN','AGRISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(39,'MOTOR.PVUFIXSUM','storage_adm.V_PVUFIXSUM','STORAGE_ADM.TT_PVUFIXSUM','BUHBODYISN','BUHBODYISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(40,'MOTOR.TT_RISK2RPTCLASS','storage_adm.V_RISK2RPTCLASS','STORAGE_ADM.TT_RISK2RPTCLASS','XRPTCLASSISN','XRPTCLASSISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(41,'MOTOR.CARREFUND','storage_adm.V_CARREFUND','STORAGE_ADM.TT_CARREFUND','REFUNDISN','REFUNDISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(42,'MOTOR.CARDOCSUMINVOICELINE','storage_adm.V_CARDOCSUMINVOICELINE','STORAGE_ADM.TT_CARDOCSUMINVOICELINE','DOCISN','DOCISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(43,'MOTOR.CAR_ATTRIB','storage_adm.V_CAR_ATTRIB','STORAGE_ADM.TT_CAR_ATTRIB','OBJISN','OBJISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(44,'MOTOR.CARDRIVER','storage_adm.V_CARDRIVER','STORAGE_ADM.TT_CARDRIVER','AGRISN','AGRISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(45,'MOTOR.DOPUSLUGI_SUMS','storage_adm.V_DOPUSLUGI_SUMS','STORAGE_ADM.TT_DOPUSLUGI_SUMS','AGRISN','AGRISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(46,'MOTOR.REP_PRESALE','storage_adm.V_REP_PRESALE','STORAGE_ADM.TT_REP_PRESALE','PRESALEISN','PRESALEISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(47,'MOTOR.REPAGR_AGENT','storage_adm.V_REPAGR_AGENT','STORAGE_ADM.TT_REPAGR_AGENT','ISN','ISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(48,'MOTOR.CAR_DEALERS','storage_adm.V_CAR_DEALERS','STORAGE_ADM.TT_CAR_DEALERS','CARDEALERISN','CARDEALERISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(49,'MOTOR.T_AGR_DRIVERS','storage_adm.V_AGR_DRIVERS','STORAGE_ADM.TT_AGR_DRIVERS','AGRISN','AGRISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(50,'MOTOR.REP_QUEUE_MAIL','storage_adm.V_REP_QUEUE_MAIL','STORAGE_ADM.TT_REP_QUEUE_MAIL','ISN','ISN',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(51,'MEDIC.ST_MED_AGRCONDBUH','storage_adm.V_MED_AGRCONDBUH','TT_MED_AGRCONDBUH','AGRISN, coalesce(SUBACCISN, 0), coalesce(RULEISN, 0), coalesce(MONTHDATEVAL, timestamp ''1900-01-01 00:00:00''), coalesce(MAXDATEVAL, timestamp ''1900-01-01 00:00:00''), coalesce(RISKRULEISN, 0), coalesce(STATCODE, 0)','AGRISN',null,'X_MED_AGRCONDBUH_AGR',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(53,'STORAGES.CLIENTDIRECTIVITY','storage_adm.v_clientdirectivity','STORAGE_ADM.TT_CLIENTDIRECTIVITY','Agrisn','Agrisn',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(-1,'STORAGE_SOURCE.REP_AGRCLAUSE','storage_adm.v_REP_AGRCLAUSE','tt_REP_AGRCLAUSE','AgrIsn,Classisn','AgrIsn',null,'',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(-1,'STORAGES.ST_PREM_BY_DEPT','STORAGES.V_TT_PREM_BY_DEPT','STORAGES.TT_PREM_BY_DEPT','PRM_KEY,coalesce(DEPTISN,-1),coalesce(repr_deptisn,-1)','PRM_KEY',null,'X_ST_PREM_UNIC',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(-4,'STORAGES.ST_BUH_TURN_NFS','storage_adm.V_TT_BUHTURN_NFS','TT_BUHTURN_NFS','PRM_KEY,coalesce(OPRISN,-1),coalesce(OSCLASS,-1),coalesce(OSKIND,-1),coalesce(OSSTATUS,-1) ,coalesce(OKOFCODE,''-''),coalesce(ISBULDING,-1),CODE','PRM_KEY',null,'X_ST_BUH_TURN_NFS_UNIC',0,'','','','','');
insert into storage_adm.ss_process_dest_tables values(-13,'STORAGE_SOURCE.zeropremiumsumaddendum','storage_adm.v_zeropremiumsumaddendum','tt_zeropremiumsumaddendum','ADDISN','AgrIsn',null,'',0,'','','','','');



--------------------------------------------------------------------------------------------------
--
--   storage_adm.ss_process_source_tables
--   For stage 2 used 1,2,3,4,5,6,7,8,10,12,13,14,15,19,20,22,27
--
--------------------------------------------------------------------------------------------------
insert into storage_adm.ss_process_source_tables values(1,1,'AIS','BUHBODY_T','isn',null,'','
Select  --+ FULL(B) Parallel(b,12) 
   b.Isn
   from  ais.buhbody_t b
   where b.status = ''А''
and (b.Code Like ''77%'' or b.Code Like''78%'' or b.Code Like ''7619%'' or Code Like ''60%'' or Code like ''71%'')
and b.dateval>=timestamp ''01-jan-2002''
and b.dateval<>coalesce(b.Datequit,timestamp ''1-jan-3000'')
and coalesce(B.Damountrub,0)-coalesce(B.camountrub,0)<>0',0);    
insert into storage_adm.ss_process_source_tables values(2,2,'AIS','BUHBODY_T','Case
 When s.Parentisn Is null  Then  S.Isn 
 When (Select coalesce(Max(d.parentisn),759033300)
           from ais.dicti d
           where  s.oprisn=d.isn)= 759033300 /*идем вверх только по проводкам операций "автоматические" */
           and s.Parentisn<>s.Isn /*бывают проводки, ссылающиеся сами на себя*/
            Then
             ( select coalesce(first_value (b1.Isn) over (partition by null ORDER BY decode(b1.subaccisn,s.subaccisn,0,1), shared_system.get_level(__hier) desc) ,S.Isn)
                  from ais.buhbody_nh b1
                  where shared_system.is_subtree(__hier, s.isn)
                 )   
           
   Else
    coalesce(S.ParentIsn,S.Isn)
   End',null,'(SELECT DISTINCT FINDISN FROM storage_adm.SS_HISTLOG WHERE PROCISN=2)','select /*+ Full(b) Parallel (b,12) */
distinct b.Baseisn 
from STORAGES.ST_BUHBODY b 
where b.baseisn is not null',0);
insert into storage_adm.ss_process_source_tables values(3,3,'AIS','DOCSUM','DEBETISN',null,'','',0);  
insert into storage_adm.ss_process_source_tables values(4,4,'AIS','BUHBODY_T','ISN',null,'(SELECT --+ FULL(S) PARALLEL (S,32) ORDERED USE_NL(B)
 DISTINCT cast(DECODE(B.OPRISN,686696616/*C.GET(''OPARTQUIT'')*/,NULL,
cast(TO_CHAR(date_TRUNC(''MONTH'',B.DATEVAL),''YYYYMMDD'')||B.SUBACCISN as numeric)) as numeric)
FROM storage_adm.SS_HISTLOG S, AIS.BUHBODY_T B
WHERE S.PROCISN=4
AND S.LOADISN=storage_adm.GetLoadIsn()
AND S.FINDISN=B.ISN)','  select cast(to_char(k.month,''YYYYMMDD'')||BS.ISN as numeric)
 from 
 (select distinct k.month 
from STORAGES.ST_KALENDAR K 
WHERE k.month  between timestamp ''01-oct-1996'' 
and date_trunc(''month'',current_timestamp)- interval ''1 day''

 )K,
buhsubacc bs',0); 
insert into storage_adm.ss_process_source_tables values(5,5,'AIS','BUHBODY_T','isn',null,'','Select/*+ Ordered Use_Nl(b)  */
    B.Isn  
   from   
 (select /*+ Parallel (sc,8)*/
   Subaccisn
   from rep_statcode sc, STORAGES.V_REP_SUBACC4DEPT sa
    where Sc.statcode=Sa.statcode 
    and grp in (''Входящее перестрахование'',''Исходящее перестрахование'')
  ) Sa,
   ais.buhbody b
   where
  Sa.subaccisn=b.subaccisn
  and  b.status = ''А''
  and coalesce(b.dateval,timestamp ''01-jan-3000'')>=timestamp ''01-jan-2002''
  and coalesce(B.Damountrub,0)-coalesce(B.camountrub,0)<>0',0);  
insert into storage_adm.ss_process_source_tables values(6,6,'AIS','SUBJECT_T','ISN',null,'storage_adm.V_SS_LOAD_SUBJECT','SELECT /*+ FULL(S) PARALLEL(S,24) */ ISN FROM AIS.SUBJECT_T S',0);
insert into storage_adm.ss_process_source_tables values(7,6,'AIS','SUBADDR_T','SUBJISN',null,'','',0);
insert into storage_adm.ss_process_source_tables values(8,6,'AIS','SUBDOC','SUBJISN',null,'','',0);   
insert into storage_adm.ss_process_source_tables values(9,6,'AIS','OBJ_ATTRIB','DECODE(DISCR,''C'',OBJISN,NULL)',null,'','',0);
insert into storage_adm.ss_process_source_tables values(10,6,'AIS','SUBBANK','ISN',null,'','',0);
insert into storage_adm.ss_process_source_tables values(12,3,'AIS','DOCSUM','CREDITISN',null,'','select /*+ Full(b) Parallel (b 12) */
distinct b.Baseisn 
from STORAGES.ST_BUHBODY b',0);
insert into storage_adm.ss_process_source_tables values(13,4,'AIS','','ISN',null,'storage_adm.V_SS_LAST_PERIODS','',0);
insert into storage_adm.ss_process_source_tables values(14,7,'AIS','','ISN',null,'Select *
from
 storage_adm.V_SS_LAST_PERIODS
Where substr(prm_key,9) In
(Select Isn from ais.buhsubacc Bs
Where Bs.id Like ''009%''
or Bs.id Like ''01%''
or Bs.id Like ''02%''
or Bs.id Like ''03%''
or Bs.id Like ''09%''
or Bs.id Like ''26%''
or Bs.id Like ''5%''
or Bs.id Like ''68%''
or Bs.id Like ''70%''
or Bs.id Like ''77%''
or Bs.id Like ''78%''
or Bs.id Like ''79%''
or Bs.id Like ''80%''
or Bs.id Like ''83%''
or Bs.id Like ''84%''
or Bs.id Like ''91%''
or Bs.id Like ''96%''
or Bs.id Like ''98%''
or Bs.id Like ''99%''
or Bs.id Like ''Н26%''
or Bs.id Like ''Н81%''
or Bs.id Like ''Н91%''
)','  select cast(to_char(k.month,''YYYYMMDD'')||BS.ISN as numeric)
 from 
 (select distinct k.month 
from STORAGES.ST_KALENDAR K 
WHERE k.month  between timestamp ''01-oct-1996'' 
and date_trunc(''month'',current_timestamp)- interval ''1 day''

 )K,
ais.buhsubacc bs
Where Bs.id Like ''009%''
or Bs.id Like ''01%''
or Bs.id Like ''02%''
or Bs.id Like ''03%''
or Bs.id Like ''09%''
or Bs.id Like ''26%''
or Bs.id Like ''5%''
or Bs.id Like ''68%''
or Bs.id Like ''70%''
or Bs.id Like ''77%''
or Bs.id Like ''78%''
or Bs.id Like ''79%''
or Bs.id Like ''80%''
or Bs.id Like ''83%''
or Bs.id Like ''84%''
or Bs.id Like ''91%''
or Bs.id Like ''96%''
or Bs.id Like ''98%''
or Bs.id Like ''99%''
or Bs.id Like ''Н26%''
or Bs.id Like ''Н81%''
or Bs.id Like ''Н91%''',0);    
insert into storage_adm.ss_process_source_tables values(15,8,'AIS','','ISN',null,'select *
from 
 storage_adm.V_SS_LAST_PERIODS
Where substr(prm_key,9) In
(
Select Isn from ais.buhsubacc Bs
Where 
Bs.id Like ''50%''
or Bs.id Like ''55%''
or Bs.id Like ''57%''
or Bs.id Like ''58%''
or Bs.id Like ''793%''
OR Bs.Id like ''60%''
   or Bs.Id like ''66%''
   or Bs.Id like ''68%''
   or Bs.Id like ''69%''
   or Bs.Id like ''70%''
   or Bs.Id like ''71%''
   --or regexp_like(Bs.Id, ''^761[0-8]'')
   or (Bs.Id like ''761%'' and not Bs.Id like ''7619%'')
   or Bs.Id=''76190''
   --or regexp_like(Bs.Id, ''^76[2-5]'')
   --or regexp_like(Bs.Id, ''^76[7-9]'')
   or (Bs.Id like ''76%'' and not Bs.Id like ''760%'' and not Bs.Id like ''761%'' and not Bs.Id like ''766%'')
   or Bs.Id=''86001''
   or Bs.Id=''96009'' or Bs.Id=''96010''
)','  select cast(to_char(k.month,''YYYYMMDD'')||BS.ISN as numeric)
 from 
 (select distinct k.month 
from STORAGES.ST_KALENDAR K 
WHERE k.month  between timestamp ''01-oct-1996'' 
and date_trunc(''month'',current_timestamp)-interval ''1 day'' 

 )K,
ais.buhsubacc bs
Where Bs.id Like ''50%''
or Bs.id Like ''55%''
or Bs.id Like ''57%''
or Bs.id Like ''58%''
or Bs.id Like ''793%''
OR Bs.Id like ''60%''
   or Bs.Id like ''66%''
   or Bs.Id like ''68%''
   or Bs.Id like ''69%''
   or Bs.Id like ''70%''
   or Bs.Id like ''71%''
   or regexp_like(Bs.Id, ''^761[0-8]'')
   or Bs.Id=''76190''
   or regexp_like(Bs.Id, ''^76[2-5]'')
   or regexp_like(Bs.Id, ''^76[7-9]'')
   or Bs.Id=''86001''
   or Bs.Id=''96009'' or Bs.Id=''96010''',0);   
insert into storage_adm.ss_process_source_tables values(16,9,'AIS','AGREEMENT','ISN',null,'','SELECT ISN FROM ais.AGREEMENT',0);   
insert into storage_adm.ss_process_source_tables values(17,9,'AIS','AGRROLE','AGRISN',null,'','',0);  
insert into storage_adm.ss_process_source_tables values(18,9,'AIS','','ISN',null,'(SELECT DISTINCT AGRISN FROM ais.AGRROLE WHERE SUBJISN IN (
SELECT FINDISN FROM storage_adm.SS_HISTLOG WHERE PROCISN=9 AND TABLE_NAME=''SUBJECT_T'' AND LOADISN=GetLoadIsn()))','',0);    
insert into storage_adm.ss_process_source_tables values(19,10,'AIS','AGREEMENT','ISN',null,'storages.V_SS_LOAD_AGROSAGO_AGREEM','select isn as agrisn from agreement where datesign between timestamp ''01-jan-2011'' and date_trunc(''day'', current_timestamp)',0);
insert into storage_adm.ss_process_source_tables values(20,10,'AIS','BSO_AGRID','ISN',null,'storages.V_SS_LOAD_AGROSAGO_BSO_AGRID','',0);
insert into storage_adm.ss_process_source_tables values(21,10,'AIS','DOCSUM','ISN',null,'storages.V_SS_LOAD_AGROSAGO_DOCSUM','',0); 
insert into storage_adm.ss_process_source_tables values(22,11,'AIS','AGREEMENT','ISN',null,'V_SS_LOAD_CLAIMOSAGO_AGREEM','-- убытки ПВУ
select a.isn as agrisn 
from ais.agrclaim ac, ais.agreement a
where ac.datereg between timestamp ''01.01.2011'' and timestamp ''10.01.2011''
  and ac.agrisn = a.isn
  and a.ruleisn = shared_system.get(''AGRMOTORCOMPULSORY'')
union  
-- прекращенные договора
-- т.е.отбор аддендумов на прекращение договоров ОСАГО
select --+ordered Index(ad) Index(a)
  a.isn as agrisn  
from ais.agreement ad, ais.agreement a
where ad.datesign between timestamp ''01.01.2011'' and timestamp ''10.01.2011''
  and ad.discr = ''А''
  and ad.ruleisn = shared_system.get(''ADDCANCEL'')
  and ad.parentisn = a.isn
  and a.ruleisn = shared_system.get(''AGRMOTORCOMPULSORY'')',0);   
insert into storage_adm.ss_process_source_tables values(23,11,'AIS','BSO_AGRID','ISN',null,'V_SS_LOAD_CLAIMOSAGO_BSO_AGRID','',0);  
insert into storage_adm.ss_process_source_tables values(24,11,'AIS','DOCSUM','ISN',null,'V_SS_LOAD_CLAIMOSAGO_DOCSUM','',0);   
insert into storage_adm.ss_process_source_tables values(25,11,'AIS','REGRESS','ISN',null,'V_SS_LOAD_CLAIMOSAGO_REGRESS','',0); 
insert into storage_adm.ss_process_source_tables values(26,11,'RSA_CLEARING','BUFMSGXML','ISN',null,'V_SS_LOAD_CLAIMOSAGO_XML','',0);    
insert into storage_adm.ss_process_source_tables values(27,11,'AIS','QTASKXOBJ','ISN',null,'V_SS_LOAD_CLAIMOSAGO_QTASKXOBJ','',0);  
insert into storage_adm.ss_process_source_tables values(28,11,'AIS','QUEUE','ISN',null,'V_SS_LOAD_CLAIMOSAGO_QUEUE','',0);
insert into storage_adm.ss_process_source_tables values(29,11,'AIS','DOCS','ISN',null,'V_SS_LOAD_CLAIMOSAGO_DOCS','',0);
insert into storage_adm.ss_process_source_tables values(30,11,'AIS','AGRCLAIM','ISN',null,'V_SS_LOAD_CLAIMOSAGO_AGRCLAIM','',0);    
insert into storage_adm.ss_process_source_tables values(31,11,'AIS','AGRREFUND','ISN',null,'V_SS_LOAD_CLAIMOSAGO_AGRREFUND','',0);  
insert into storage_adm.ss_process_source_tables values(32,6,'AIS','SUBOWNER','SUBJISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(33,12,'AIS','AGRCLAIM','ISN',null,'','Select /*+ Full(a) Parallel(a,32)*/ Isn from ais.agrclaim a',null);
insert into storage_adm.ss_process_source_tables values(34,12,'AIS','AGRREFUND','CLAIMISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(35,12,'AIS','AGRREFUNDEXT','CLAIMISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(36,12,'AIS','CLAIMREFUNDCAR','ISN',null,'(SELECT DISTINCT CLAIMISN 
FROM  STORAGE_ADM.SS_HISTLOG S,ais.AGRREFUND AR
WHERE PROCISN=12 
  AND LOADISN=storage_adm.GetLoadIsn() 
  AND TABLE_NAME=''CLAIMREFUNDCAR''
  AND FINDISN=AR.ISN
)','',null); 
insert into storage_adm.ss_process_source_tables values(37,12,'AIS','AGRROLE','REFUNDISN',null,'(SELECT DISTINCT CLAIMISN 
FROM  STORAGE_ADM.SS_HISTLOG S,ais.AGRREFUND AR
WHERE PROCISN=12 
AND LOADISN=storage_adm.GetLoadIsn() 
AND TABLE_NAME=''AGRROLE''
AND FINDISN=AR.ISN
)','',null);
insert into storage_adm.ss_process_source_tables values(38,13,'AIS','AGREEMENT','ISN',null,'','Select /*+ Full(a) Parallel(a,32)*/ Isn from agreement a',0); 
insert into storage_adm.ss_process_source_tables values(39,13,'AIS','AGRCOND','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(40,13,'AIS','AGRROLE','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(41,13,'AIS','AGREXT','AGRISN',null,'','',null);    
insert into storage_adm.ss_process_source_tables values(42,13,'AIS','AGRADDR','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(43,13,'AIS','SUBJECT_T','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AGRISN
FROM  STORAGE_ADM.SS_HISTLOG S,
AIS.AGRROLE  AR
WHERE PROCISN=13 
  AND LOADISN=storage_adm.GetLoadIsn() 
  AND TABLE_NAME=''SUBJECT_T''
  AND FINDISN=AR.SUBJISN
  )','',null);  
insert into storage_adm.ss_process_source_tables values(44,13,'AIS','AGRRISK','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(45,13,'AIS','AGROBJECT','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(46,13,'AIS','AGRLIMIT','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(47,14,'AIS','BUHBODY_T','ISN',null,'','Select /*+ Full(a) Parallel(a,32)*/ Isn from AIS.buhbody_T a',0);  
insert into storage_adm.ss_process_source_tables values(48,15,'AIS','BUHBODY_T','ISN',null,'(SELECT /*+ FULL(T) PARALLEL (T)  ORDERED USE_NL(B BP) */
CASE 
 WHEN T.TABLE_NAME<>''BUHBODY_T'' AND B.ISN IS NOT NULL THEN B.HEADISN /*ЕСЛИ ДОКСУММА - ТО HEADISN ЕЕ ПРОВОДКИ И ВСЕ*/
 WHEN T.TABLE_NAME=''BUHBODY_T'' AND B.ISN IS NOT NULL  THEN/* ПРОВОДКА, НЕ УДАЛЕНА*/
   CASE 
WHEN D.ISN IS NOT NULL /*ТЕХНИЧЕСКАЯ ОПЕРАЦИЯ*/ THEN coalesce(BP.HEADISN,B.HEADISN) /* ГОЛОВА ПРОВОДКИ ПАПЫ */
ELSE B.HEADISN
   END  
 WHEN T.TABLE_NAME=''BUHBODY_T'' AND B.ISN IS  NULL THEN /* ПРОВОДКА, УДАЛЕНА, ЛЕЗЕМ В ИСТОРИЮ*/
    ( 
 SELECT MAX(HEADISN) HEADISN
 FROM HIST.BUHBODY_T H
 WHERE H.ISN=T.FINDISN
)
  ELSE NULL
  END
  
FROM STORAGE_ADM.SS_HISTLOG T 
  left join AIS.BUHBODY_T B  on T.FINDISN=B.ISN
  left join AIS.BUHBODY_T BP on B.PARENTISN=BP.ISN /*ПАПА ПРОВОДКИ*/ /* HEADISN ПАПЫ  ПРОВОДКИ */
  left join (SELECT * FROM   ais.DICTI D
WHERE D.PARENTISN=759033300 AND CODE IN(''200'',''02'',''03'') -- НЕОБХОДИМО ОГРАНИЧЕНИЕ ПО ОПЕРАЦИЯМ 200 02 03
) D on B.OPRISN=D.ISN /* ОПЕРАЦИЯ ПРОВОДКИ */ /* СПИСОК "АВТОМАТИЧЕСКИХ" ОПЕРАЦИЙ , ПО КОТОРЫМ НУЖНА ДОПОЛНИТЕЛЬНАЯ РАСШИФРОВКА - ПАПЫ БЕРЕМ */   
WHERE T.PROCISN=15
AND T.LOADISN=storage_adm.GetLoadIsn())
',
'Select /*+ FULL(b) PARALLEL (b,32 )  */
distinct Headisn
from ais.subacc4dept sa,
  Ais.BuhBody_T b
Where 
sa.statcode is not null
and sa.subaccisn=b.subaccisn
and b.status=''А''
and oprisn not in (9534116,9533916,9534516, 24422716)',null);
insert into storage_adm.ss_process_source_tables values(49,15,'AIS','DOCSUM','DEBETISN',null,'(SELECT null)','',null);   
insert into storage_adm.ss_process_source_tables values(50,15,'AIS','DOCSUM','CREDITISN',null,'(SELECT null)','',null);  
insert into storage_adm.ss_process_source_tables values(51,16,'AIS','AGREEMENT','ISN',null,'','Select /*+ Full(a) Parallel(a,32)*/ Isn from agreement a',null);   
insert into storage_adm.ss_process_source_tables values(52,16,'AIS','SUBJECT_T','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AGRISN
FROM  STORAGE_ADM.SS_HISTLOG S,
AIS.AGRROLE  AR
WHERE PROCISN=16 
AND LOADISN=GetLoadIsn() 
AND TABLE_NAME=''SUBJECT_T''
AND FINDISN=AR.SUBJISN
)','',null);
insert into storage_adm.ss_process_source_tables values(53,16,'AIS','AGRROLE','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(54,16,'AIS','AGRCOND','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(55,16,'AIS','AGREXT','AGRISN',null,'','',null);    
insert into storage_adm.ss_process_source_tables values(56,16,'AIS','AGROBJECT','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(57,16,'AIS','AGROBJEXT','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AGRISN
FROM  STORAGE_ADM.SS_HISTLOG S,
 AIS.AGROBJECT  A
   WHERE PROCISN=16 
AND LOADISN=GetLoadIsn() 
AND TABLE_NAME=''AGROBJEXT''
AND FINDISN=A.ISN
  )','',null);   
insert into storage_adm.ss_process_source_tables values(58,16,'AIS','AGRRISK','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(59,16,'AIS','AGRLIMIT','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(60,16,'AIS','AGROBJGROUP','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(61,17,'AIS','AGRCLAIM','ISN',null,'','Select /*+ Full(a) Parallel(a,32)*/ Isn from agrclaim a',null);
insert into storage_adm.ss_process_source_tables values(62,17,'AIS','AGRREFUND','CLAIMISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(63,17,'AIS','AGRREFUNDEXT','CLAIMISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(64,16,'AIS','SUBDEPT_T','ISN',null,'   (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AGRISN
FROM  STORAGE_ADM.SS_HISTLOG S,
 AIS.AGRROLE  AR
   WHERE PROCISN=16 
AND LOADISN=GetLoadIsn() 
AND TABLE_NAME=''SUBDEPT_T''
AND FINDISN=AR.SUBJISN
  )','',null); 
insert into storage_adm.ss_process_source_tables values(65,16,'AIS','SUBHUMAN_T','ISN',null,'   (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AGRISN
FROM  STORAGE_ADM.SS_HISTLOG S,
 AIS.AGRROLE  AR
   WHERE PROCISN=16 
AND LOADISN=GetLoadIsn() 
AND TABLE_NAME=''SUBHUMAN_T''
AND FINDISN=AR.SUBJISN
  )','',null);    
insert into storage_adm.ss_process_source_tables values(66,16,'AIS','AGROBJGROUPITEM','GROUPISN',null,'(SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AGRISN
FROM  STORAGE_ADM.SS_HISTLOG S,
 AIS.AGROBJGROUP A
   WHERE PROCISN=16 
AND LOADISN=GetLoadIsn() 
AND TABLE_NAME=''AGROBJGROUPITEM''
AND FINDISN=A.ISN
  )','',null);
insert into storage_adm.ss_process_source_tables values(67,16,'AIS','AGENT_COND','AGRISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(68,18,'AIS','DOCSUM','ISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(69,18,'AIS','DOCS_T','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT DS.ISN
FROM  STORAGE_ADM.SS_HISTLOG S,
 AIS.DOCSUM DS
   WHERE PROCISN=18 
AND LOADISN=GetLoadIsn() 
AND TABLE_NAME=''DOCS_T''
AND FINDISN=DS.DOCISN AND DS.DISCR = ''F'')','',null);
insert into storage_adm.ss_process_source_tables values(70,17,'AIS','AGRRISK','ISN',null,' (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT CLAIMISN
FROM  STORAGE_ADM.SS_HISTLOG S,
 AIS.AGRREFUND  AR
   WHERE PROCISN=17 
AND LOADISN=GetLoadIsn() 
AND TABLE_NAME=''AGRRISK''
AND FINDISN=AR.RISKISN
  )','',null);   
insert into storage_adm.ss_process_source_tables values(71,17,'AIS','AGRSERVICE','AGRISN',null,' (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT CLAIMISN
FROM  STORAGE_ADM.SS_HISTLOG S,
 AIS.AGRREFUND  AR
   WHERE PROCISN=17 
AND LOADISN=GetLoadIsn() 
AND TABLE_NAME=''AGRSERVICE''
AND FINDISN=AR.AGRISN)','',null);   
insert into storage_adm.ss_process_source_tables values(72,19,'AIS','AGRROLE','AGRISN',null,'','select /*+ parallel(AR 32) full(AR) */
distinct
  AR.AgrISN
from 
  AIS.AGRROLE AR
where
  AR.CLASSISN in ( 
    select 
 D.ISN
    from
 AIS.DICTI D
    where
 D.PARENTISN = 402   -- СУБЪЕКТЫ ДОГОВОРА
 and D.CODE in (''SALES_G'', ''SALES_F'')
  )',0); 
insert into storage_adm.ss_process_source_tables values(73,19,'AIS','AGREEMENT','coalesce(PARENTISN, ISN)',null,'','',0);
insert into storage_adm.ss_process_source_tables values(74,19,'AIS','AGRREFUND','AGRISN',null,'','',0);    
insert into storage_adm.ss_process_source_tables values(75,19,'AIS','BUHBODY_T','AGRISN',null,'','',0);    
insert into storage_adm.ss_process_source_tables values(76,19,'AIS','DOCSUM','AGRISN',null,'','',0);  
insert into storage_adm.ss_process_source_tables values(77,19,'AIS','OBJ_ATTRIB','case 
  when 
    CLASSISN = 1428587803
    and DISCR = ''C'' 
    and coalesce(oracompat.add_months(DATEBEG::date, -1), timestamp ''01-01-3000'') >= current_timestamp
  then ISN
end',null,'storage_adm.V_SS_LOAD_AGR_SALERS_OBJ_ATTR','',0);
insert into storage_adm.ss_process_source_tables values(78,20,'AIS','AGRROLE','AGRISN',null,'','select /*+ parallel(AR 32) full(AR) */
distinct
  AR.AgrISN
from 
  AIS.AGRROLE AR
where
  AR.CLASSISN in ( 
    437,   -- АГЕНТ
    438,   -- БРОКЕР
    2481446203, -- АГЕНТ (БОНУСНАЯ КОМИССИЯ)
    2530118403  -- ГЕНЕРАЛЬНЫЙ АГЕНТ
  )',0); 
insert into storage_adm.ss_process_source_tables values(79,21,'AIS','AGREEMENT','coalesce(PREVISN,ISN)',null,'','select ISN from motor.v_agreement',0); 
insert into storage_adm.ss_process_source_tables values(80,21,'AIS','AGREEMENT','ISN',null,'','',0);  
insert into storage_adm.ss_process_source_tables values(81,21,'AIS','BSO_AGRID','AGRISN',null,'','',0);    
insert into storage_adm.ss_process_source_tables values(82,21,'AIS','AGREXT','AGRISN',null,'','',0);  
insert into storage_adm.ss_process_source_tables values(83,21,'AIS','CRGDOC','AGRISN',null,'','',0);  
insert into storage_adm.ss_process_source_tables values(84,21,'AIS','AGRROLE','AGRISN',null,'','',0); 
insert into storage_adm.ss_process_source_tables values(85,21,'AIS','DOCSUM','AgrISN',null,'','',0);  
insert into storage_adm.ss_process_source_tables values(86,20,'AIS','DOCSUM','AGRISN',null,'','',0);  
insert into storage_adm.ss_process_source_tables values(87,22,'AIS','CLAIMINVOICE','ISN',null,'','select /*+ full(d) parallel(d, 32)*/
  d.ISN
from AIS.CLAIMINVOICE d',null);
insert into storage_adm.ss_process_source_tables values(88,22,'AIS','DOCSUM','INDOCISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(89,22,'AIS','DOCS_T','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S, 12)
  DISTINCT INDOCISN
FROM  STORAGE_ADM.SS_HISTLOG S, DOCSUM DS
WHERE PROCISN=22
AND LOADISN=GetLoadIsn()
AND TABLE_NAME=''DOCS_T''
AND FINDISN=DS.DOCISN)','',null);
insert into storage_adm.ss_process_source_tables values(90,23,'AIS','SUBADDR_T','SUBJISN',null,'','select /*+ full(SA) parallel(SA 32)*/
distinct
  SA.SUBJISN
from AIS.SUBADDR_T SA',null);    
insert into storage_adm.ss_process_source_tables values(91,21,'AIS','AGRCOND','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(92,21,'AIS','AGRTARIFF','case when TariffClassISN in (3390202503, 3678815503) then AgrISN end',null,'','',null);    
insert into storage_adm.ss_process_source_tables values(93,21,'AIS','QUEUE','case
  when
    ClassISN = 1175052903
    and ObjISN2 is null
    and FormISN = 26752116
    and Status = ''W''
    and Request = ''1''
  then ObjISN  
end',null,'','',null);
insert into storage_adm.ss_process_source_tables values(94,24,'AIS','CLAIMINVOICE','ISN',null,'','select /*+ full(d) parallel(d, 32)*/
  d.ISN
from AIS.CLAIMINVOICE d',null);
insert into storage_adm.ss_process_source_tables values(95,24,'AIS','DOCSUM','case when DISCR in (''P'', ''F'') then INDOCISN end',null,'','',null);    
insert into storage_adm.ss_process_source_tables values(96,24,'AIS','DOCS_T','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S, 12)
  DISTINCT INDOCISN
FROM  STORAGE_ADM.SS_HISTLOG S, DOCSUM DS
WHERE PROCISN=24
AND LOADISN=GetLoadIsn()
AND TABLE_NAME=''DOCS_T''
AND FINDISN=DS.DOCISN)','',null);
insert into storage_adm.ss_process_source_tables values(97,24,'AIS','CLAIMINVOICELINE','INVOICEISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(98,24,'AIS','AGRREFUND','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S, 12)
  DISTINCT CIL.INVOICEISN
FROM  STORAGE_ADM.SS_HISTLOG S, AIS.CLAIMINVOICELINE CIL
WHERE S.PROCISN=24
AND S.LOADISN=GetLoadIsn()
AND S.TABLE_NAME=''AGRREFUND''
AND S.FINDISN=CIL.REFUNDISN)','',null);  
insert into storage_adm.ss_process_source_tables values(99,24,'AIS','AGRCLAIM','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S, 12) ORDERED USE_NL(S R CIL)
  DISTINCT CIL.INVOICEISN
FROM  STORAGE_ADM.SS_HISTLOG S, AIS.AGRREFUND R, AIS.CLAIMINVOICELINE CIL
WHERE S.PROCISN=24
AND S.LOADISN=GetLoadIsn()
AND S.TABLE_NAME=''AGRCLAIM''
AND S.FINDISN=R.CLAIMISN
AND R.ISN=CIL.REFUNDISN)','',null);  
insert into storage_adm.ss_process_source_tables values(100,26,'AIS','CLAIMINVOICE','ISN',null,'','select /*+ full(d) parallel(d, 32)*/
  d.ISN
from AIS.CLAIMINVOICE d',null);
insert into storage_adm.ss_process_source_tables values(101,26,'AIS','DOCSUM','case when DISCR in (''P'', ''F'') then INDOCISN end',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(103,26,'AIS','CLAIMINVOICELINE','INVOICEISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(104,26,'AIS','AGRREFUND','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S, 12)
  DISTINCT CIL.INVOICEISN
FROM  STORAGE_ADM.SS_HISTLOG S, AIS.CLAIMINVOICELINE CIL
WHERE S.PROCISN=26
AND S.LOADISN=GetLoadIsn()
AND S.TABLE_NAME=''AGRREFUND''
AND S.FINDISN=CIL.REFUNDISN)','',null); 
insert into storage_adm.ss_process_source_tables values(105,26,'AIS','AGRCLAIM','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S, 12) ORDERED USE_NL(S R CIL)
  DISTINCT CIL.INVOICEISN
FROM  STORAGE_ADM.SS_HISTLOG S, AIS.AGRREFUND R, AIS.CLAIMINVOICELINE CIL
WHERE S.PROCISN=26
AND S.LOADISN=GetLoadIsn()
AND S.TABLE_NAME=''AGRCLAIM''
AND S.FINDISN=R.CLAIMISN
AND R.ISN=CIL.REFUNDISN)','',null); 
insert into storage_adm.ss_process_source_tables values(107,25,'AIS','DOCSUM','case when DISCR in (''P'', ''F'') then AGRISN end',null,'','Select /*+ full(ra) parallel(ra 32) */
  ra.AGRISN
from MOTOR.CARREPAGR ra',null); 
insert into storage_adm.ss_process_source_tables values(108,25,'AIS','DOCS_T','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(109,27,'AIS','AGRROLE','AGRISN',null,'','SELECT /*+ FULL(A) PARALLEL(a 32)*/ ISN FROM AIS.AGREEMENT A',0);
insert into storage_adm.ss_process_source_tables values(110,27,'AIS','SUBJECT_T','ISN',null,'storage_adm.V_SS_LOAD_AGRROLE_BY_SUBJECT','',null);
insert into storage_adm.ss_process_source_tables values(111,27,'AIS','SUBHUMAN_T','ISN',null,'storage_adm.V_SS_LOAD_AGRROLE_BY_SUBHUMAN','',null);   
insert into storage_adm.ss_process_source_tables values(112,28,'AIS','QUEUE','case when classisn in (select isn from MOTOR.D_REP_CLIENT_QUEUE_CALLS) then ISN end',null,'V_SS_LOAD_PROC_28_QUEUE','SELECT /*+ FULL(S) PARALLEL(S,24) */ ISN FROM AIS.SUBJECT_T S',null);   
insert into storage_adm.ss_process_source_tables values(113,28,'AIS','QUEPHONE','SUBJISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(114,28,'AIS','AGREEMENT','CLIENTISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(115,28,'AIS','SMSITEM','SUBJISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(116,28,'AIS','AGRCLAIM','SUBJISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(117,28,'AIS','CLAIMANKETA','CLAIMISN',null,'V_SS_LOAD_PROC_28_CLAIMANKETA','',null);  
insert into storage_adm.ss_process_source_tables values(118,28,'AIS','CC_OUTB_LIST_CLIENT','ISN',null,'V_SS_LOAD_PROC_28_CC','',null);   
insert into storage_adm.ss_process_source_tables values(119,29,'AIS','AGREEMENT','ISN',null,'','SELECT /*+ FULL(A) PARALLEL(A 32)*/ A.ISN 
FROM AIS.AGREEMENT A, motor.v_dicti_rule r
Where A.ruleisn=r.Isn',null); 
insert into storage_adm.ss_process_source_tables values(120,29,'AIS','AGREEMENT','PARENTISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(121,29,'AIS','AGRCOND','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(122,29,'AIS','AGRTARIFF','AGRISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(123,30,'AIS','AGREEMENT','ISN',null,'','SELECT /*+ FULL(A) PARALLEL(A 32)*/ ISN FROM AIS.AGREEMENT A',null);   
insert into storage_adm.ss_process_source_tables values(124,30,'AIS','AGREEMENT','PARENTISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(125,30,'AIS','AGRCOND','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(126,30,'AIS','AGRTARIFF','AGRISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(127,30,'AIS','AGRROLE','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(128,30,'AIS','AGREXT','decode(ClassISN, 1071774425, AGRISN)',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(129,30,'AIS','DOCSUM','case when DISCR = ''P'' and ClassISN = 414 then AGRISN end',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(130,30,'AIS','AGRLIMIT','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(131,21,'AIS','OBJ_ATTRIB','case 
  when 
    CLASSISN in (2647785103, 3028738303, 2638580803)
    and DISCR = ''C'' 
    and coalesce(oracompat.add_months(DATEBEG::date, -1), timestamp ''01-01-3000'') >= current_timestamp
  then ObjISN
end',null,'V_SS_LOAD_AGR_CLIENT_OBJ_ATTR','',0);    
insert into storage_adm.ss_process_source_tables values(132,26,'AIS','OBJ_ATTRIB','case 
  when 
    CLASSISN = 3522430403
    and DISCR = ''I'' 
  then ObjISN
end',null,'','',0);
insert into storage_adm.ss_process_source_tables values(133,25,'AIS','AGREEMENT','ISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(134,32,'AIS','agrobject','Isn',null,'','Select /*+ FULL(S) PARALLEL(S 32) */ Distinct ObjIsn from AIS.AgrObjectExt S',null);
insert into storage_adm.ss_process_source_tables values(135,32,'AIS','AgrObjectExt','objisn',null,'','',null);
insert into storage_adm.ss_process_source_tables values(136,33,'AIS','AGRCOND','OBJISN',null,'','select /*+ parallel(cc 32) full(cc) */ 
 distinct cc.ParentObjISN
 from MOTOR.CARCOND cc
where cc.ParentObjISN is not null',null);
insert into storage_adm.ss_process_source_tables values(137,33,'AIS','AGROBJECT','ISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(138,33,'AIS','AGROBJECT','PARENTISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(139,33,'AIS','BUHBODY_T','AGRISN',null,'(select 0 from dual)','',null);
insert into storage_adm.ss_process_source_tables values(148,36,'AIS','','FINDISN',null,'(WITH LI as (SELECT /*+ materialize */ LOAD_STORAGE.GETLOADISN AS LOADISN FROM DUAL)
SELECT --+ FULL(S) PARALLEL(S,32)
   DISTINCT FINDISN
   FROM STORAGE_ADM.SS_HISTLOG S, 
   LI
  WHERE S.PROCISN = 36 
    AND S.LOADISN = LI.LOADISN 
    AND S.TABLE_NAME 
IN (''AGRREFUND'', 
    ''DOCS_T'', 
    ''AGRCLAIM'',
    ''CARCOND_OBJ_SUM'',
    ''CARCOND_OBJ_SUM_DO'')
)','select /*+ parallel(c 32) full(c) */ agrisn from motor.carrepagr c',0);
insert into storage_adm.ss_process_source_tables values(151,37,'AIS','CRGDOC','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(152,37,'AIS','BSO_AGRID','AGRISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(153,37,'AIS','AGROBJECT','AGRISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(154,37,'AIS','OBJAGR','ISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(158,39,'AIS','BUHBODY','case when subaccisn = 2594787603
 and subjisn is null
 and parentisn is null 
then DOCISN end',null,'','SELECT /*+ parallel(bb 32) full(bb) */  bb.docisn
  FROM ais.buhbody bb,
 where bb.subaccisn = 2594787603
   and bb.subjisn is null
   and bb.parentisn is null',null); 
insert into storage_adm.ss_process_source_tables values(159,39,'AIS','AGRREFUND','case when classisn in (26310, 966409125)
 and to_char(refundid) = ''1''
then ISN end ',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(160,40,'AIS','DICX','DECODE(CLASSISN, 2232978903, CLASSISN1)',null,'','select X.CLASSISN1 XRPTCLASSISN,
  from AIS.DICX X
 where X.CLASSISN  = 2232978903',null);
insert into storage_adm.ss_process_source_tables values(161,40,'AIS','DICTI','ISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(162,41,'AIS','AGRREFUND','ISN',null,'','  select /*+ parallel(ar 32) full(ar) */ ar.isn 
    from ais.agrrefund ar',null);
insert into storage_adm.ss_process_source_tables values(163,41,'AIS','CLAIMREFUNDCAR','ISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(164,42,'AIS','DOCS','DECODE(doc_type, ''01'', ISN)',null,'','  select /*+ parallel(a 32) full(a) */ a.isn 
    from ais.docs a
   where a.doc_type = ''01''',null);   
insert into storage_adm.ss_process_source_tables values(165,42,'AIS','DOCSUM','DECODE(discr, ''P'', DOCISN)',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(166,43,'AIS','AGRCOND','OBJISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(168,43,'AIS','AGROBJECT','ISN',null,'','select --+ full(rc) parallel(rc 32)
  distinct
    rc.ObjISN
  from STORAGE_SOURCE.REPCOND rc
  where
    rc.ParentObjISN is null',null);    
insert into storage_adm.ss_process_source_tables values(169,44,'AIS','CRGDOC','case when classisn in (28007616, 28007716, 765912000) then AGRISN end',null,'','',null);
insert into storage_adm.ss_process_source_tables values(170,44,'AIS','AGREEMENT','ISN',null,'','Select /*+ Full(cd) Parallel(cd,32)*/ AgrIsn 
  from crgdoc cd 
 where cd.classisn in (28007616, 28007716, 765912000)',null); 
insert into storage_adm.ss_process_source_tables values(172,46,'AIS','PRESALE','ISN',null,'',' select /*+ Full(C) Parallel(C,32)*/ distinct C.ISN
   from AIS.PRESALE C',null);
insert into storage_adm.ss_process_source_tables values(173,46,'AIS','AGREEMENT','PRESALEISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(174,46,'AIS','QTASKXOBJ','OBJISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(175,47,'AIS','AGREEMENT',' DECODE(CLASSISN, 12415316, ISN)',null,'',' select /*+ Full(A) Parallel(A,32)*/ A.ISN
   from AIS.AGREEMENT A
   where A.CLASSISN = 12415316',null);  
insert into storage_adm.ss_process_source_tables values(176,48,'AIS','OBJ_ATTRIB','   case when classisn = 1693932103
    and ValN in (1695900203,1705759403)
    and discr = ''C'' 
   then isn end',null,'',' select /*+ Full(OA) Parallel(OA,32)*/ OA.ISN
   from AIS.OBJ_ATTRIB OA
   where OA.classisn = 1693932103
and OA.ValN in (1695900203,1705759403)
and OA.discr = ''C'' ',null); 
insert into storage_adm.ss_process_source_tables values(177,49,'AIS','SUBJBONUS','decode(ClassISN, 1779446703, AGRISN)',null,'','',null);
insert into storage_adm.ss_process_source_tables values(178,49,'AIS','AGRROLE','decode(ClassISN, 2093210103, AGRISN)',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(179,49,'AIS','AGREEMENT','ISN',null,'','select /*+ Full(C) Parallel(C,32)*/ C.AGRISN
 from motor.CARREPAGR C',null);
insert into storage_adm.ss_process_source_tables values(180,50,'AIS','DICTI','ISN',null,'','select /*+ full(d) parallel(d,32)*/ d.isn
  from dicti d 
  start with isn in (
    1091363103, -- ПЕЧАТЬ УВЕДОМЛЕНИЙ
    1681771803, -- ТИПЫ ИСХОДЯЩИХ ОБЗВОНОВ
    1010590225  -- СПИСОК АНКЕТИРУЕМЫХ
  )
  connect by prior d.isn = d.parentisn',null);
insert into storage_adm.ss_process_source_tables values(181,50,'AIS','QUEUE','CLASSISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(182,33,'AIS','AGROBJECT','PREVISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(183,33,'AIS','AGREEMENT','ISN',null,'(
WITH LI as (SELECT /*+ materialize */ LOAD_STORAGE.GETLOADISN AS LOADISN FROM DUAL)
SELECT DISTINCT C.OBJISN
  FROM AIS.AGRCOND C,   
(SELECT --+ FULL(S) PARALLEL(S,32)
   DISTINCT FINDISN
   FROM STORAGE_ADM.SS_HISTLOG S, 
   MOTOR.CARREPAGR RA,
   LI
  WHERE S.PROCISN = 33 
    AND S.LOADISN = LI.LOADISN
    AND S.FINDISN = RA.AGRISN 
    AND S.TABLE_NAME 
IN (''AGREEMENT'', 
    ''AGRROLE'', 
    ''BUHBODY_T'', 
    ''DOCSUM'', 
    ''CRGDOC'', 
    ''BSO_AGRID'', 
    ''AGRTARIFF'', 
    ''AGREXT'', 
    ''AGRRISK'')
 ) S
 WHERE S.FINDISN = C.AGRISN)','',null);   
insert into storage_adm.ss_process_source_tables values(184,33,'AIS','AGRTARIFF','AGRISN',null,'(select 0 from dual)','',null);
insert into storage_adm.ss_process_source_tables values(185,33,'AIS','AGRROLE','AGRISN',null,'(select 0 from dual)','',null);  
insert into storage_adm.ss_process_source_tables values(186,33,'AIS','SUBJECT_T','ISN',null,'(
WITH LI as (SELECT /*+ materialize */ LOAD_STORAGE.GETLOADISN AS LOADISN FROM DUAL)
SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT C.OBJISN
FROM  STORAGE_ADM.SS_HISTLOG S,
 MOTOR.CARREPAGR RA,
 AIS.AGRROLE AR,
 AIS.AGRCOND C,
 LI
WHERE S.PROCISN = 33 
  AND S.LOADISN = LI.LOADISN 
  AND S.TABLE_NAME = ''SUBJECT_T''
  AND RA.AGRISN = AR.AGRISN
  AND C.AGRISN = AR.AGRISN
  AND S.FINDISN = AR.SUBJISN  
  )','',null); 
insert into storage_adm.ss_process_source_tables values(187,33,'AIS','DOCSUM','AGRISN',null,'(select 0 from dual)','',null);   
insert into storage_adm.ss_process_source_tables values(189,33,'AIS','CRGDOC','AGRISN',null,'(select 0 from dual)','',null);   
insert into storage_adm.ss_process_source_tables values(190,33,'AIS','BSO_AGRID','AGRISN',null,'(select 0 from dual)','',null);
insert into storage_adm.ss_process_source_tables values(191,36,'AIS','CLAIMREFUNDCAR','ISN',null,'(WITH LI as (SELECT /*+ materialize */ LOAD_STORAGE.GETLOADISN AS LOADISN FROM DUAL)
  SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT C.AGRISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.DOCSUM C, LI
WHERE S.PROCISN = 36 
  AND S.LOADISN = LI.LOADISN
  AND S.TABLE_NAME = ''CLAIMREFUNDCAR''
  AND S.FINDISN = C.REFUNDISN)','',null); 
insert into storage_adm.ss_process_source_tables values(192,36,'AIS','AGRREFUND','AGRISN',null,'(select 0 from dual)','',null);
insert into storage_adm.ss_process_source_tables values(193,36,'AIS','DOCS_T','AGRISN',null,'(select 0 from dual)','',null);   
insert into storage_adm.ss_process_source_tables values(194,36,'AIS','AGRCLAIM','AGRISN',null,'(select 0 from dual)
  ','',null);   
insert into storage_adm.ss_process_source_tables values(197,33,'AIS','AGREXT','AGRISN',null,'(select 0 from dual)','',null);   
insert into storage_adm.ss_process_source_tables values(198,33,'AIS','AGRRISK','AGRISN',null,'(select 0 from dual)','',null);  
insert into storage_adm.ss_process_source_tables values(213,33,'AIS','OBJCAR','ISN',null,'(
WITH LI as (SELECT /*+ materialize */ LOAD_STORAGE.GETLOADISN AS LOADISN FROM DUAL)
SELECT --+ FULL(S) PARALLEL(S,32)
  DISTINCT AO.ISN
   FROM STORAGE_ADM.SS_HISTLOG S,
   MOTOR.CARREPAGR RA, 
   AIS.AGROBJECT AO,
   LI
  WHERE S.PROCISN = 33 
    AND S.LOADISN = LI.LOADISN
    AND S.TABLE_NAME = ''OBJCAR''
    AND RA.AGRISN = AO.AGRISN
    AND S.FINDISN = AO.DESCISN)','',null);
insert into storage_adm.ss_process_source_tables values(228,35,'AIS','','FINDISN',null,'(WITH LI AS (SELECT /*+ materialize */ LOAD_STORAGE.GETLOADISN AS LOADISN FROM DUAL)
SELECT /*+ FULL(SS) PARALLEL(SS,32) */ 
 DISTINCT SS.FINDISN 
 FROM STORAGE_ADM.SS_HISTLOG SS, LI 
WHERE SS.PROCISN = 35 
  AND SS.LOADISN = LI.LOADISN)','select /*+ parallel(c 32) full(c) */
 parentobjisn
  from motor.carcond_obj_sum c
UNION
select /*+ parallel(c 32) full(c) */
 parentobjisn
  from motor.carcond_obj_sum partition(ANY_DATE) c
 where c.DATEBEG is null
    or c.dateend is null',0);  
insert into storage_adm.ss_process_source_tables values(230,37,'AIS','BUHBODY','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(231,37,'AIS','DOCSUM','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(232,37,'AIS','AGRTARIFF','AGRISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(233,37,'AIS','AGREXT','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(234,37,'AIS','AGREEMENT','ISN',null,'','Select /*+ full(ra) parallel(ra 32) */
 distinct ra.AGRISN
 from MOTOR.CARREPAGR ra
where ra.ruleisn=753518300 -- продукт страхования ОСАГО !!!
  AND ra.discr=''Д''
  AND ra.status!=''А''  -- исключаем аннулированные договора',null);   
insert into storage_adm.ss_process_source_tables values(235,37,'AIS','AGRCOND','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(236,37,'AIS','AGRROLE','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(238,37,'AIS','OBJCAR','ISN',null,'(
WITH LI as (SELECT /*+ materialize */ LOAD_STORAGE.GETLOADISN AS LOADISN FROM DUAL)
SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AO.AGRISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGROBJECT  AO, LI
WHERE S.PROCISN = 37 
  AND S.LOADISN = LI.LOADISN
  AND S.TABLE_NAME = ''OBJCAR''
  AND S.FINDISN = AO.DESCISN)','',null);   
insert into storage_adm.ss_process_source_tables values(239,666,'AIS','BUHBODY','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(240,666,'AIS','DOCSUM','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(241,666,'AIS','AGREXT','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(242,666,'AIS','AGREEMENT','ISN',null,'','select /*+ parallel(m 32) full(m) */
distinct m.agrisn
  from motor.mtpl_policies m',null);    
insert into storage_adm.ss_process_source_tables values(243,666,'AIS','AGRCOND','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(244,666,'AIS','AGRROLE','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(245,666,'AIS','OBJCAR','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AO.AGRISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGROBJECT  AO
WHERE PROCISN = 38 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''OBJCAR''
  AND FINDISN = AO.DESCISN)','',null);   
insert into storage_adm.ss_process_source_tables values(246,666,'AIS','AGRTARIFF','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(247,666,'AIS','CRGDOC','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(248,666,'AIS','BSO_AGRID','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(249,666,'AIS','AGROBJECT','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(250,666,'AIS','OBJAGR','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(251,38,'AIS','AGRCLAIM','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(252,38,'AIS','AGRREFUND','AGRISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(253,38,'AIS','AGRREFUNDEXT','AGRISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(254,666,'AIS','AGRRISK','AGRISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(255,39,'AIS','AGRCLAIM','ISN',null,'(SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AR.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRREFUND  AR
WHERE PROCISN = 39 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGRCLAIM''
  AND FINDISN = AR.CLAIMISN)','',null);  
insert into storage_adm.ss_process_source_tables values(256,44,'AIS','AGREEMENT','ADDISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(257,44,'AIS','AGREEMENT','NEWADDISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(258,45,'AIS','AGREEMENT','ISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(259,45,'AIS','AGREEMENT','ADDISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(260,45,'AIS','AGREEMENT','NEWADDISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(261,45,'AIS','AGRCOND','AGRISN',null,'',' select /*+ Full(C) Parallel(C,32)*/ 
  distinct
    C.AgrISN
  from
    MOTOR.CARCOND C',null);    
insert into storage_adm.ss_process_source_tables values(262,45,'AIS','AGRRISK','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AC.AGRISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRCOND  AC
WHERE PROCISN = 45 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGRRISK''
  AND FINDISN = AC.RISKISN)','',null);  
insert into storage_adm.ss_process_source_tables values(263,45,'AIS','AGROBJECT','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AC.AGRISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRCOND  AC
WHERE PROCISN = 45 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGROBJECT''
  AND FINDISN = AC.OBJISN)','',null);    
insert into storage_adm.ss_process_source_tables values(264,45,'AIS','AGRLIMIT','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AC.AGRISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRCOND  AC
WHERE PROCISN = 45 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGRLIMIT''
  AND FINDISN = AC.LIMITISN)','',null);    
insert into storage_adm.ss_process_source_tables values(265,45,'AIS','AGRLIMITEM','LIMISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AC.AGRISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRCOND  AC
WHERE PROCISN = 45 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGRLIMITEM''
  AND FINDISN = AC.LIMITISN)','',null);  
insert into storage_adm.ss_process_source_tables values(266,43,'AIS','AGREEMENT','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AO.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGROBJECT  AO
WHERE PROCISN = 43 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGREEMENT''
  AND FINDISN = AO.AGRISN)','',null);
insert into storage_adm.ss_process_source_tables values(267,43,'AIS','OBJAGR','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AO.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGROBJECT  AO
WHERE PROCISN = 43 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''OBJAGR''
  AND FINDISN = AO.DESCISN)','',null);
insert into storage_adm.ss_process_source_tables values(268,43,'AIS','OBJCAR','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AO.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGROBJECT  AO
WHERE PROCISN = 43 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''OBJCAR''
  AND FINDISN = AO.DESCISN)','',null);
insert into storage_adm.ss_process_source_tables values(269,43,'AIS','CARTARIF','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AO.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGROBJECT AO, AIS.OBJCAR OC
WHERE PROCISN = 43 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''CARTARIF''
  AND AO.DESCISN = OC.ISN
  AND FINDISN = OC.TARIFISN)','',null);
insert into storage_adm.ss_process_source_tables values(270,43,'AIS','CARMODEL','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AO.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGROBJECT AO, AIS.OBJCAR OC, CARTARIF T
WHERE PROCISN = 43 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''CARMODEL''
  AND AO.DESCISN = OC.ISN
  AND OC.TARIFISN = T.ISN
  AND FINDISN = T.MODELISN)','',null);   
insert into storage_adm.ss_process_source_tables values(271,43,'AIS','AGRRISK','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AO.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGROBJECT AO, AIS.AGRCOND AC
WHERE PROCISN = 43 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGRRISK''
  AND AO.ISN = AC.OBJISN
  AND FINDISN = AC.RISKISN)','',null);
insert into storage_adm.ss_process_source_tables values(272,43,'AIS','AGRTARIFF','CONDISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AC.OBJISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRCOND AC
WHERE PROCISN = 43 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGRTARIFF''
  AND FINDISN = AC.ISN)','',null);    
insert into storage_adm.ss_process_source_tables values(273,43,'AIS','AGREEMENT','PARENTISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT AO.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGROBJECT  AO
WHERE PROCISN = 43 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGREEMENT''
  AND FINDISN = AO.AGRISN)','',null);    
insert into storage_adm.ss_process_source_tables values(274,42,'AIS','CLAIMINVOICE','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT H.DOCISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.DOCSUM  H
WHERE PROCISN = 42 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''CLAIMINVOICE''
  AND FINDISN = H.INDOCISN)','',null);
insert into storage_adm.ss_process_source_tables values(275,42,'AIS','CLAIMINVOICELINE','INVOICEISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT H.DOCISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.DOCSUM H
WHERE PROCISN = 42 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''CLAIMINVOICELINE''
  AND FINDISN = H.INDOCISN)','',null); 
insert into storage_adm.ss_process_source_tables values(276,41,'AIS','AGRCLAIM','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT R.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRREFUND R
WHERE PROCISN = 41 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGRCLAIM''
  AND FINDISN = R.CLAIMISN)','',null);    
insert into storage_adm.ss_process_source_tables values(277,41,'AIS','AGRRISK','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT R.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRREFUND R
WHERE PROCISN = 41 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGRRISK''
  AND FINDISN = R.RISKISN)','',null);  
insert into storage_adm.ss_process_source_tables values(278,41,'AIS','AGROBJECT','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT R.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRREFUND R
WHERE PROCISN = 41 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGROBJECT''
  AND FINDISN = R.OBJISN)','',null);    
insert into storage_adm.ss_process_source_tables values(279,41,'AIS','OBJCAR','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT R.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRREFUND R, AIS.AGROBJECT O
WHERE PROCISN = 41 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''OBJCAR''
  AND R.OBJISN = O.ISN
  AND FINDISN = O.DESCISN)','',null);    
insert into storage_adm.ss_process_source_tables values(280,41,'AIS','OBJAGR','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT R.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRREFUND R, AIS.AGROBJECT O
WHERE PROCISN = 41 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''OBJAGR''
  AND R.OBJISN = O.ISN
  AND FINDISN = O.DESCISN)','',null);    
insert into storage_adm.ss_process_source_tables values(281,41,'AIS','CARTARIF','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT R.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, 
 AIS.AGRREFUND R, 
 AIS.AGROBJECT O,
 AIS.OBJCAR OC
WHERE PROCISN = 41 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''CARTARIF''
  AND R.OBJISN = O.ISN
  AND OC.ISN = O.DESCISN
  AND FINDISN = OC.TARIFISN)','',null);   
insert into storage_adm.ss_process_source_tables values(282,41,'AIS','AGREEMENT','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT R.ISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.AGRREFUND R, AIS.AGRCLAIM L
WHERE PROCISN = 41 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''AGREEMENT''
  AND R.CLAIMISN = L.ISN
  AND FINDISN = L.AGRISN)','',null);
insert into storage_adm.ss_process_source_tables values(283,46,'AIS','QTASK','ISN',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT QO.OBJISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.QTASKXOBJ QO
WHERE PROCISN = 46 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''QTASK''
  AND FINDISN = QO.TASKISN)','',null);
insert into storage_adm.ss_process_source_tables values(284,46,'AIS','QUEUE',' case
   when classisn in (
2498233003,  -- КОНТАКТ ДЛЯ ЗАКЛЮЧЕНИЯ ДОГОВОРА
2500538903,  -- ПОДГОТОВКА ДОГОВОРА
2498684203,  -- ЗАКЛЮЧЕНИЕ ДОГОВОРА
3736480703   -- ПОВТОРНАЯ ВСТРЕЧА
   ) then isn end',null,'  (SELECT --+ FULL(S) PARALLEL(S,32)
DISTINCT QO.OBJISN
 FROM STORAGE_ADM.SS_HISTLOG S, AIS.QTASKXOBJ QO
WHERE PROCISN = 46 
  AND LOADISN=GetLoadIsn() 
  AND TABLE_NAME = ''QUEUE''
  AND FINDISN = QO.TASKISN)','',null);    
insert into storage_adm.ss_process_source_tables values(285,49,'AIS','CRGDOC','case when CLASSISN in ( 28007616,    -- Доверенность на управление
    28007716,    -- Генеральная доверенность
    765912000 )  -- Водительское удостоверение
then AGRISN end',null,'','',null);
insert into storage_adm.ss_process_source_tables values(286,49,'AIS','AGRTARIFF','AGRISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(287,49,'AIS','AGREEMENT','ADDISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(288,49,'AIS','AGREEMENT','NEWADDISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(289,51,'AIS','DOCSUM','AGRISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(290,51,'AIS','BUHBODY','AGRISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(291,38,'AIS','','FINDISN',null,'(WITH LI AS (SELECT /*+ materialize */ LOAD_STORAGE.GETLOADISN AS LOADISN FROM DUAL)
SELECT /*+ FULL(SS) PARALLEL(SS,32) */ 
 DISTINCT SS.FINDISN 
 FROM STORAGE_ADM.SS_HISTLOG SS, LI 
WHERE SS.PROCISN = 38 
  AND SS.LOADISN = LI.LOADISN)','select /*+ parallel(m 32) full(m) */
distinct m.agrisn
  from motor.mtpl_policies m',0);   
insert into storage_adm.ss_process_source_tables values(292,52,'AIS','SUBJECT_T','ISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(293,52,'AIS','SUBHUMAN_T','ISN',null,'','',null);  
insert into storage_adm.ss_process_source_tables values(294,52,'AIS','SUBPHONE_T','SUBJISN',null,'','',null);
insert into storage_adm.ss_process_source_tables values(295,52,'AIS','SUBADDR_T','SUBJISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(296,52,'AIS','QUEUE','CASE WHEN CLASSISN = 1175052903  
AND ObjISN2 is null
AND FormISN = 33024916  
AND Status = ''W''   
AND Request = ''1'' 
THEN OBJISN END',null,'','SELECT /*+ FULL(S) PARALLEL(S,32) */ ISN FROM AIS.SUBJECT_T S',null);   
insert into storage_adm.ss_process_source_tables values(297,52,'AIS','OBJ_ATTRIB','OBJISN',null,'','',null); 
insert into storage_adm.ss_process_source_tables values(298,53,'AIS','CLIENTDIRECTIVITY','ObjISN',null,'','  select /*+ full(CD) parallel(CD,32)*/ CD.Objisn
   from ais.ClientDirectivity CD
  where CD.TypeClientDir <> ''Отказ в выплате''
    and CD.Objisn is not null',null);  
insert into storage_adm.ss_process_source_tables values(299,53,'AIS','AGREEMENT','ISN',null,'','',null);   
insert into storage_adm.ss_process_source_tables values(300,53,'AIS','AGRCLAIM','ISN',null,'','',null);    
insert into storage_adm.ss_process_source_tables values(301,33,'AIS','AGREEMENT','PREVISN',null,'(select 0 from dual)','',null);    