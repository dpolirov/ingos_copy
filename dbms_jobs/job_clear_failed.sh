#!/bin/bash
# Script starts plpgsql procedure that clears jobs from queue that have RUN status
# but actually their transaction had finished or hanged in <idle> state
# Should be run from greenplum superuser account
source ~/.bashrc

echo \
"begin; \
LOCK TABLE DBMS_JOBS.JOB_SYNC IN EXCLUSIVE MODE NOWAIT; \
select DBMS_JOBS.job_clear_failed(); \
commit;" \
|psql 2>>~/utilities/log_job_runner_`date +%Y-%m`.log
