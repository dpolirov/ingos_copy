-- Function: storages.prcloadstoragetest_1026()

-- DROP FUNCTION storages.prcloadstoragetest_1026();

CREATE OR REPLACE FUNCTION storages.prcloadstoragetest_1026()
  RETURNS void AS
$BODY$
declare
    vTaskIsn        numeric;
    vProcessShortname varchar;
    vStartId        numeric;
    vLoadIsn        numeric;
    vStepNumber     numeric;
    vStatus         numeric;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.prcLoadStorageTest_1026';
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
    perform storage_adm.SetLoadIsn(vLoadIsn)
    
    v_step = 'Do the job';
    perform STORAGE_ADM.LOADSTORAGE(1,0);

    EXCEPTION
        WHEN OTHERS THEN
        BEGIN            
            raise exception '% : % : %', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION storages.prcloadstoragetest_1026()
  OWNER TO gpadmin;
