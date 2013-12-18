create or replace function replicais.log_error (p_itr_num int, p_table_name varchar, p_message varchar) returns void as $BODY$
begin
    perform shared_system.autonomous_transaction(
        'insert into replicais.replication_errors(replication_iteration,
                                                  replication_table_name,
                                                  error_message
                                                 )                                              
            values(       ' || p_itr_num    || ', 
                        ''' || p_table_name || ''', 
                    $DATA$' || p_message    || '$DATA$)');
end;
$BODY$
language plpgsql
volatile;