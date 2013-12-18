#!/bin/bash

source /etc/profile
source ~/.bashrc

DATE_STR=`date +%Y%m%d`
COMMAND="
    begin;
    lock replicais.mx_replication_iterator nowait;
    select replicais.replication_iteration(3);
    commit;
"

echo "===== Iterator Started `date`"
psql -d ingos_dev -c "$COMMAND" >>/home/gpadmin/utilities/logs/replication_iteration_${DATE_STR}.log 2>&1
echo "===== Iterator Finished `date` with code $?"

COMMAND='select replicais.invoke_workers(3);'

echo "===== Worker Started `date`"
psql -d ingos_dev -c "$COMMAND" >>/home/gpadmin/utilities/logs/replication_iteration_${DATE_STR}.log 2>&1
echo "===== Worker Finished `date` with code $?"
