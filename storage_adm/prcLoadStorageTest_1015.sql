create or replace function STORAGE_ADM.prcLoadStorageTest_1015() returns void as $BODY$
declare
    vTaskIsn        numeric;
    vProcessShortname varchar;
    vStartId        numeric;
    v_function_name CHARACTER VARYING = 'STORAGE_ADM.prcLoadStorageTest_1015';
    v_step          CHARACTER VARYING = 'NA';
begin
    
    v_step = 'Get Params';
    vTaskIsn = coalesce(shared_system.getparamn('taskisn'), -999);
    vProcessShortname = coalesce(shared_system.getparamv('processshortname'), 'unknown');
    vStartId = coalesce(shared_system.getparamn('startid'), -999);
    
    v_step = 'Do the job';
    --select storage_adm.LoadStorage(1,0,0);

    EXCEPTION
        WHEN OTHERS THEN
        BEGIN
            perform storage_adm.LOGREP(true, vTaskIsn,vProcessShortname,'ERROR in '||v_function_name||' : '||v_step||' : '||sqlerrm,vStartId,-1,null,null);
            raise exception 'ERROR in % : % : %', v_function_name, v_step, sqlerrm;
        END;
end;
$BODY$
language plpgsql;
