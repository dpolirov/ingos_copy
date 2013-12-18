#!/bin/bash

source /etc/profile
source ~/.bashrc

DATE_STR=`date +%Y%m`
COMMAND="select replicais.replicate_full_clear_failed();"

echo "===== Clear Started `date`"
psql -d ingos_dev -c "$COMMAND" >>/home/gpadmin/utilities/logs/replication_clear_failed_${DATE_STR}.log 2>&1
echo "===== Clear Finished `date` with code $?"
