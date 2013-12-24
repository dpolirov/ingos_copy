# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
        . /etc/bashrc
fi

# User specific aliases and functions
source /usr/local/os/os_path.sh
export PGDATABASE=ingos_dev
export PGPORT=5432
export PGHOST=mdw

export ORACLE_HOME=/usr/lib/oracle/11.2/client64
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME/lib
export PYTHONPATH=$PYTHONPATH:/usr/lib64/python2.6/site-packages

alias pga="psql -c \"select datname, procpid, sess_id, usename, current_timestamp-query_start as running, substr(regexp_replace(current_query, E'[ \t\r\n]{2}', '', 'g'),1,60) from pg_stat_activity\""
alias pgg="psql -c \"select datname, procpid, sess_id, usename, current_timestamp-query_start as running, substr(regexp_replace(current_query, E'[ \t\r\n]{2}', '', 'g'),1,60) from pg_stat_activity where current_query <> '<IDLE>';\""
alias rq="date; psql -c 'select * from gp_resq_activity;' "
alias rqs="date; psql -c 'select * from gp_resq_priority_statement;' "
alias mon="date;  psql gpperfmon -c 'SELECT * FROM system_now;'"
alias hh="date;  psql gpperfmon -c 'SELECT * FROM health_now;' | grep -v normal"
alias gsh='gpssh -f /home/gpadmin/gpconfigs/hostfile_segments'
alias qq="psql -c 'select * from gp_resqueue_status;'"
alias rqq="psql -c 'select * from gp_toolkit.gp_resq_role order by 1;'"
alias att="psql -c 'select * from pg_resqueue_attributes ;'"
alias lock="psql -c \"select * from pg_locks where granted='f' ;\""
alias repli="psql -c 'select replication_status, replication_worker, count(*) from replicais.replication_tasks_incr group by replication_status, replication_worker order by 1,2;'"
alias replf="psql -c 'select replication_status, count(*), min(added_dttm), max(added_dttm) from replicais.replication_tasks_full group by replication_status;'"
alias reperr="psql -c 'select replication_iteration, replication_table_name, error_dttm, notification_flag from replicais.replication_errors order by error_dttm desc limit 10;'"
alias gjobs="psql -c 'select q.job_isn, g.group_name, g.priority, q.job_submit_dttm, q.job_session_start_dttm, q.job_status from dbms_jobs.job_queue as q inner join dbms_jobs.job_group as g on q.job_group=g.group_isn order by q.job_submit_dttm desc limit 20;'"
alias locked="psql -c 'select table_schema, table_name, blocked_lock_type, segments_blocked, lock_waiting_time, waiting_query_short from shared_system.query_locks_blocked'"
alias locks="psql -c 'select table_schema, table_name, blocked_lock_type, lock_start_dttm, lock_waiting_time, blocked_query_short, waiting_query_short from shared_system.query_locks_blockers;'"
alias replistat="psql -c 'select * from replicais.replication_tasks_incr_monitor;'"
