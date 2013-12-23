create view shared_system.query_locks_all as
    select  current_timestamp::timestamp,
            n.nspname,
            c.relname,
            l.locktype,
            l.transactionid,
            l.pid,
            l.mppsessionid,
            l.mode,
            l.granted,
            a.usename,
            a.query_start,
            a.backend_start,
            a.client_addr,
            substr(regexp_replace(a.current_query, E'[ \t\r\n]{2}', '', 'g'),1,20) as query_short,
            a.current_query
        from pg_locks as l 
            full join pg_stat_activity as a
            on l.mppsessionid = a.sess_id
            left join pg_class as c
            on l.relation = c.oid
            left join pg_namespace as n
            on c.relnamespace = n.oid
        order by 7,6,2,3;
        
create view shared_system.query_locks_blockers as
    select min(n.nspname)     as table_schema,
           min(c.relname)     as table_name,
           min(l.locktype)    as blocked_lock_type,
           min(a.query_start) as lock_start_dttm,               
           current_timestamp - min(a2.query_start) as lock_waiting_time,
           min(substr(regexp_replace(a.current_query, E'[ \t\r\n]{2}', '', 'g'),1,20))  as blocked_query_short,
           min(substr(regexp_replace(a2.current_query, E'[ \t\r\n]{2}', '', 'g'),1,20)) as waiting_query_short,
           min(a.current_query)  as blocked_query,
           min(a2.current_query) as waiting_query,
           l.relation, l.mppsessionid, rb.mppsessionid as mppsessionid2
        from pg_locks as l
            inner join (
                select relation, mppsessionid
                    from pg_locks
                    where not granted
                    group by relation, mppsessionid
            ) as rb on l.relation = rb.relation
            inner join pg_stat_activity as a  on a.sess_id  =  l.mppsessionid
            inner join pg_stat_activity as a2 on a2.sess_id = rb.mppsessionid
            inner join pg_class as c on l.relation = c.oid
            inner join pg_namespace as n on c.relnamespace = n.oid
        group by l.relation, l.mppsessionid, rb.mppsessionid;
        
create view shared_system.query_locks_blocked as
    select min(n.nspname)     as table_schema,
           min(c.relname)     as table_name,
           min(l.locktype)    as blocked_lock_type,
           current_timestamp - min(a.query_start) as lock_waiting_time,
           min(substr(regexp_replace(a.current_query, E'[ \t\r\n]{2}', '', 'g'),1,20))  as waiting_query_short,
           min(a.current_query)  as waiting_query,
           l.relation, l.mppsessionid
        from pg_locks as l
            inner join pg_stat_activity as a  on a.sess_id  =  l.mppsessionid
            inner join pg_class as c on l.relation = c.oid
            inner join pg_namespace as n on c.relnamespace = n.oid
        where not l.granted
        group by l.relation, l.mppsessionid;