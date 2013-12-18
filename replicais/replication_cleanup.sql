-- Function: replicais.replication_cleanup(integer, integer, character varying)

-- DROP FUNCTION replicais.replication_cleanup(integer, integer, character varying);

create or replace function replicais.replication_cleanup(p_start_isn integer, p_end_isn integer, p_table_name_list varchar[]) returns varchar as $BODY$
declare
    v_flag          int;
    v_chk           varchar;
    v_connect_id    int;
begin
    select id into v_connect_id
        from replicais.replication_connection_id;
    select flag into v_flag 
        from replicais.replication_flag; 
    
    select shared_system.execute_oracle(v_connect_id,'update gp_user.histunloaded h set h.flagslow = h.flagslow-'||v_flag||' 
                                           where bitand(flagslow,'||v_flag||') <> 0 and
                                           h.unloadisn between '||p_start_isn||' and '||p_end_isn||' 
                                           and h.tablename in (''' || array_to_string(p_table_name_list, ''',''') || ''')') into v_chk;
    return v_chk;
end;
$BODY$
language plpgsql 
volatile;