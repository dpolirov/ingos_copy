#!/bin/bash

source /etc/profile
source ~/.bashrc

DATE_STR=`date +%Y%m`
COMMAND="select replicais.replication_notification();"

echo "===== Notification Started `date`"
psql -d ingos_dev -c "$COMMAND" >>/home/gpadmin/utilities/logs/replication_notification_${DATE_STR}.log 2>&1
echo "===== Notification Finished `date` with code $?"
