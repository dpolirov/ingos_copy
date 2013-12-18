create or replace function replicais.replication_notification() returns void as $BODY$
declare
    v_mail_body varchar;
    v_err_rec   record;
begin
    v_mail_body = '';
    for v_err_rec in (select replication_iteration,
                             replication_table_name,
                             error_dttm,
                             error_message,
                             notification_flag
                        from replicais.replication_errors
                        where notification_flag = 0)
    loop
        update replicais.replication_errors
            set notification_flag = 1
            where v_err_rec.replication_iteration = replication_iteration;
            
        v_mail_body := v_mail_body||'Replication iteration: '||v_err_rec.replication_iteration||', 
                      Replication table name: '||v_err_rec.replication_table_name||', 
                      Error timestamp: '||v_err_rec.error_dttm||', 
                      Error message: '||v_err_rec.error_message||E'\n';
        
    end loop;
    if v_mail_body <> '' then
        perform shared_system.send_email('Replication errors', v_mail_body);
    end if;

    v_mail_body = '';
    for v_err_rec in (select replication_table_name
                        from replicais.replication_new_tables
                        where notification_disabled = 0 and (notification_dttm is null or updated_dttm > notification_dttm + interval '1 day'))
    loop
        v_mail_body := v_mail_body||'New table found in HISTLOG: '||v_err_rec.replication_table_name ||E'\n';
        
        update replicais.replication_new_tables
            set notification_dttm = current_timestamp;
    end loop;
    if v_mail_body <> '' then
        perform shared_system.send_email('Replication new tables', v_mail_body);
    end if;
end;
$BODY$
language plpgsql
volatile;