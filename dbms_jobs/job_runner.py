#!/usr/bin/python
# Script attempts to get one job from job_queue, execute it and log result into job_log

from time import sleep
from random import random
from pygresql.pg import DatabaseError
from gppylib.db import dbconn
from gppylib.gplog import *

def abort(message, conn = None):
    logger.error (message)
    if conn is not None:
        conn.close()
    exit(3)

# establish connection to database
#-------------------------------------------------------------------------------
def make_connection(hostname, user, password):
    try:
        dburl = dbconn.DbURL(hostname = hostname, username = user, password = password)
        conn = dbconn.connect(dburl)
    except Exception, e:
        abort(str(e))
    return conn

# get job for execution from queue
#-------------------------------------------------------------------------------
def get_job_for_execution(conn):
    # lock synchronization table before working with job_queue
    try:
        curs = dbconn.execSQL(conn, 'LOCK TABLE DBMS_JOBS.JOB_SYNC IN EXCLUSIVE MODE NOWAIT;');
    except DatabaseError, e:
        conn.close()
        if str(e).find('could not obtain lock') < 0 :
            abort(str(e))
        else :
            exit(0)
    # get job for execution
    try:
        curs = dbconn.execSQL(conn, 'select po_job_isn, po_job_code, po_job_attempt, po_job_attempt_limit from DBMS_JOBS.job_next_for_execution();')
        conn.commit();
    except DatabaseError, e:
        abort(str(e), conn)
    # fetch function output (job parameters) from cursor and convert it to named list
    job = dict(zip ([x[0] for x in curs.description], curs.fetchone())) 
    if job['po_job_isn'] is None:
       return None
    return job

# execute job
#-------------------------------------------------------------------------------
def execute_job (conn, job):
    try:
       curs = dbconn.execSQL(conn, job['po_job_code'])
       conn.commit()
       status = 'END'
       message = 'finished'
    except DatabaseError, e:
       conn.rollback()
       # if job failed then schedule job for rerun or mark it failed depeding on given attempt_limit
       if job['po_job_attempt'] < job['po_job_attempt_limit']:
          status = 'RERUN'
       else:
          status = 'FLD'
       message = str(e)
    return status, message

# update job status after execution
#-------------------------------------------------------------------------------
def set_job_status(conn, job, status, message):
    # lock synchronization table before working with job_queue
    # it is critical to get lock in this phase
    # because status of finished job must be changed from RUN
    sync_table_locked = False
    ex = None
    for i in range(20):
        try:
            curs = dbconn.execSQL(conn, 'LOCK TABLE DBMS_JOBS.JOB_SYNC IN EXCLUSIVE MODE NOWAIT');
            sync_table_locked = True
            break
        except Exception, e:
            conn.rollback()
            ex = e
            pass
        sleep (10. + random()*3.)
    if not sync_table_locked:
        abort('Cannot get lock on DBMS_JOBS.JOB_SYNC for 200 seconds. Job status for job_isn=%d was not updated. %s' % (job['po_job_isn'], str(ex)), conn)
    try:
        curs = dbconn.execSQL(conn,"select DBMS_JOBS.job_set_status(%d, %d, '%s', $ERRMSG$%s$ERRMSG$)" % (job['po_job_isn'], job['po_job_attempt'], status, message))
        conn.commit()
    except DatabaseError, e:
        abort('Cannot update status for job_isn=%d. %s' % (job['po_job_isn'], str(e)), conn)

# main
#-------------------------------------------------------------------------------
def main():
    hostname = 'localhost'
    user = 'gpadmin'
    password = ''
    conn = make_connection(hostname, user, password)
    job = get_job_for_execution(conn)
    if job is not None:
        status, message = execute_job(conn, job)
        set_job_status(conn, job, status, message) 
    conn.close()

logger = get_default_logger()
main()

