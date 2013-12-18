#!/bin/bash
source ~/.bashrc

for i in {1..3}; do
    nohup ~/utilities/job_runner.py >>~/utilities/log_job_runner_`date +%Y-%m`.log 2>&1 &
    sleep 20
done
