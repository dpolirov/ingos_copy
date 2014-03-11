create or replace function STORAGES.prcLoadStorageTest_663() returns void as $BODY$
declare
    vTaskIsn        numeric;
    vChildTaskIsn   numeric;
    vProcessShortname varchar;
    vStartId        numeric;
    vLoadIsn        numeric;
    vStepNumber     numeric;
    vStatus         numeric;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.prcLoadStorageTest_663';
    v_step          CHARACTER VARYING = 'NA';
begin
    v_step = 'Get Params';
    vTaskIsn = coalesce(shared_system.getparamn('taskisn'), -999);
    vProcessShortname = coalesce(shared_system.getparamv('processshortname'), 'unknown');
    vStartId = coalesce(shared_system.getparamn('startid'), -999);
    
    vLoadIsn = storage_adm.GetLoadFromTask(vTaskIsn);
    If vLoadisn  = 0 then
        vLoadIsn = storage_adm.createload(vTaskIsn);
        perform storage_adm.SetLoadToTask(vTaskIsn,vLoadIsn);
    end if;

    
    v_step = 'Do the job';
    vStepNumber = storage_adm.GetLoadStep(vLoadIsn);

    -- block for child procedure begin
    If vStepNumber <= 1 then
        vChildTaskIsn = 1026;
        If vStepNumber = 0 then
            perform storage_adm.EXECUTE_LOGED_REPORT(vChildTaskIsn,0,0); 
            perform storage_adm.SetLoadStep(vLoadIsn,1); 
        else
            vStatus = storage_adm.GetTaskStatus(vChildTaskIsn);
            If vStatus = 0  then --не запущенна или первый запуск
                perform storage_adm.SetLoadStep(vLoadisn,2); 
            elsif vStatus = 1 then  -- идет, ждем
                null;
            elsIf vStatus < 0 then -- Ошибка
                perform storage_adm.Set_Rep_NextRun(vTaskIsn ,null);
                perform storage_adm.LOGREP(true, vTaskIsn, vProcessShortname,'ERROR in '||v_function_name||' : '||v_step||' : '||sqlerrm,vStartId,-1,null,null);
                RAISE Exception '%', storage_adm.GETTASKERRCODE(vChildTaskIsn);
            end if;
        end if;  
        perform shared_system.setparamn('processfinished', 0);
        return;
    end if;
    -- block for child procedure end
    
    
        -- block for child procedure begin
    If vStepNumber <= 3 then
        vChildTaskIsn = 1205;
        If vStepNumber = 2 then
            perform storage_adm.EXECUTE_LOGED_REPORT(vChildTaskIsn,0,0); 
            perform storage_adm.SetLoadStep(vLoadIsn,3); 
        else
            vStatus = storage_adm.GetTaskStatus(vChildTaskIsn);
            If vStatus = 0  then --не запущенна или первый запуск
                perform storage_adm.SetLoadStep(vLoadisn,4); 
            elsif vStatus = 1 then  -- идет, ждем
                null;
            elsIf vStatus < 0 then -- Ошибка
                perform storage_adm.Set_Rep_NextRun(vTaskIsn ,null);
                perform storage_adm.LOGREP(true, vTaskIsn, vProcessShortname,'ERROR in '||v_function_name||' : '||v_step||' : '||sqlerrm,vStartId,-1,null,null);
                RAISE Exception '%', storage_adm.GETTASKERRCODE(vChildTaskIsn);
            end if;
        end if;  
        perform shared_system.setparamn('processfinished', 0);
        return;
    end if;
    -- block for child procedure end
    
    perform storage_adm.Set_Rep_NextRun(vTaskIsn ,null);
    perform storage_adm.SetLoadToTask(vTaskIsn, 0);
    perform shared_system.setparamn('processfinished', 1);

    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            raise exception '% : % : %', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;
