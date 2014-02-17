#!/bin/bash
# Script starts task manager of plsql-based ETL
# Should be run from greenplum superuser account
source ~/.bashrc

echo \
"begin; \
LOCK TABLE STORAGE_ADM.MX_TASKMANAGER IN EXCLUSIVE MODE NOWAIT; \
select STORAGE_ADM.TaskManager(); \
commit;" \
|psql 2>>~/utilities/logs/task_manager_`date +%Y-%m`.log
