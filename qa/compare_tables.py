#!/usr/bin/python
#
# Copyright (C) Pivotal Inc 2013. All Rights Reserved. 
# Igor Putyatin (iputyatin@gopivotal.com)
#
# Script compares tables in Oracle in Greenplum based on MD5 hash calculated over string representation of records.
# Options:
#   -t schema.table             Required. Schema name and table name should be same in Greenplum and oracle
#   -k key1[,key2,...]          Optional. Keys for comparing. Default is ISN
#   -n number_of_samples        Optional. Number of sample records retrieved from Greenplum and Oracle for visual comparing
#                                           string representation of records in case of differences
#                                           Default is 10.  Specify -n 0 if you don't need samples
#   -c path                     Optional. Path to config file with credentials for Oracle and Greenplum databases.
#                                           Default -s ./compare_tables.conf 
#   -d                          Optional. Do not download hashtable from oracle. Set this option for second and subsequent
#                                           comparisons if data in Oracle has not been changed since previous run
#   -s                          Optional. Disable strict DDL check. If this option set extra columns in Greenplum are allowed
#
from pygresql.pg import DatabaseError
from gppylib.db import dbconn
from gppylib.gplog import *
from gppylib.gpparseopts import OptParser, OptChecker

import json
from optparse import Option, OptionParser 

import os
import time
from subprocess import Popen, PIPE
from tempfile import TemporaryFile

# set encoding parameter for cx_Oracle
os.environ["NLS_LANG"] = ".UTF8"
import cx_Oracle


# Options class
#-------------------------------------------------------------------------------
class QAOptions:
    table = ''
    schema = ''
    keys = 'ISN'
    #keys as list of strings
    keys_list = None
    strict = True
    reloadhash = True
    nsamples = 100
    # logins-passwords 2-dimensional dict
    connection_options = None

    # Parse input parameters and configuration file
    def __init__(self) :
        parser = OptParser(option_class=OptChecker)
        parser.remove_option('-h')    
        parser.add_option('-h', '-?', '--help',  action='store_true')
        parser.add_option('-t', '--table',       type='string')
        parser.add_option('-k', '--keys',        type='string')
        parser.add_option('-c', '--config',      type='string')
        parser.add_option('-n', '--nsamples',    type='int')
        parser.add_option('-s', '--strict',      action='store_true')
        parser.add_option('-d', '--download',    action='store_true')
        (parser_result, args) = parser.parse_args()
        helpstr = "Usage:" + os.linesep + "./compare_tables.py -t schema.table [-k key1,key2] [-c ./compare_tables.conf] [-d] [-e]"
        if parser_result.help :
            print helpstr
            sys.exit(0)
        if not parser_result.table:
            raise Exception('Failed to start utility. Please, specify table name with "-t" key')
            sys.exit(1)
        if '.' not in parser_result.table :
            raise Exception('Failed to start utility. Please, specify full table name with schema')
            sys.exit(1)
        
        
        (self.schema, self.table) = parser_result.table.split('.')
        self.keys = parser_result.keys or self.keys
        self.keys_list = sorted(self.keys.upper().replace(' ', '').split(','))
        configpath = parser_result.config or "./compare_tables.conf"
        try:
            configfile = open(configpath, 'rb')
            self.connection_options = json.load(configfile)
            configfile.close()
        except Exception, e :
            raise Exception('Could not read config file ' + configpath + '. ' +str(e))
        
        db_list = self.connection_options.keys()
        if "greenplum" in db_list and "oracle" in db_list :
            config_ok=True
            for key in db_list :
                ops_list = self.connection_options[key].keys()
                if not("server" in ops_list and "login" in ops_list and "password" in ops_list) :
                    config_ok=False
                    break
        else:
            config_ok=False
        if not config_ok :       
            raise Exception('Wrong structure in configuration file. Should be: "oracle" and "greenplum" on top level;  "login", "password", and "server" on second level')
                

        if parser_result.nsamples is not None :
            self.nsamples = parser_result.nsamples
        self.strict = not parser_result.strict
        self.reloadhash = not parser_result.download
 



# Oracle class
#-------------------------------------------------------------------------------

class OraHelper :

    connection = None
    connection_string = ""
    columns_dict = None
    
    schema = ""
    table = ""
    samplekeys_tablename = ""

    def __init__(self, schema, table) :
        self.schema = schema
        self.table = table
        self.samplekeys_tablename = "qa." + table + "_samplekeys"

    def connect(self, hostname, username, password) :
        self.connection_string = "%s/%s@%s" % (username, password, hostname)
        try :
            self.connection=cx_Oracle.connect(self.connection_string)            
        except cx_Oracle.DatabaseError, e:
            raise Exception("Cannot connect to Oracle :" + str(e))
    
    def getColumns(self) :
        #extract field list from oracle
        try :
            cursor = self.connection.cursor()
            cursor.execute(""" 
                select column_name, data_type, data_precision, data_scale, data_length
                    from all_tab_columns c 
                    where c.owner = '%s' and c.table_name = '%s' 
                    order by column_name """ % (self.schema.upper(), self.table.upper()))
            result = cursor.fetchall()
        except cx_Oracle.DatabaseError, e:
            raise Exception("Error getting DDL from Oracle :" + str(e))
        #create list of dictionaries
        if len(result) == 0:
            raise Exception("Table %s.%s not found in Oracle." % (self.schema, self.table))
        colnames = [x[0] for x in cursor.description]
        self.columns_dict = [dict(zip(colnames, x)) for x in result]

    def execute(self, query) :
        try :
            cursor = self.connection.cursor()
            cursor.execute(query)
            try :
                res = cursor.fetchall()
            except cx_Oracle.InterfaceError :
                res = None
            cursor.close()
            self.connection.commit()
            return res
        except Exception, e :
            raise Exception("Error while executing statements in Oracle: " + str(e) + " Query: " + query)
        


    def disconnect(self) :
        if self.connection is not None :
            try :
                self.connection.close()
            except Exception, e :
                raise Exception("Cannot close connection to Oracle: " + str(e))

# Greenplum class
#-------------------------------------------------------------------------------

class GPHelper :
    connection = None
    columns_list = []
    schema = ""
    table = ""
    hash_tablename = ""
    diff_tablename = ""
    samplekeys_tablename = ""
    sample_tablename = ""

    def __init__(self, schema, table) :
        self.schema = schema
        self.table = table
        self.hash_tablename       = "qa." + table + "_hash"
        self.diff_tablename       = "qa." + table + "_diff"
        self.samplekeys_tablename = "qa." + table + "_samplekeys"
        self.sample_tablename     = "qa." + table + "_sample"

    
    def connect(self, hostname, username, password) :
        url = dbconn.DbURL(hostname = hostname, username = username, password = password)
        try :
            self.connection = dbconn.connect(url)
        except DatabaseError, e:
            raise Exception("Cannot connect to Greenplum :" + str(e))

    def getColumns(self) :
        try:
            res = self.execute("""
            select upper(a.attname)  
                from pg_attribute a 
                where a.attrelid = 
                    (select oid from pg_class where relname = '%s' and relnamespace = 
                        (select oid from pg_namespace where nspname = '%s')) 
                    and a.attnum > 0
                    and not attisdropped
                order by a.attname; """ % (self.table.lower(), self.schema.lower()))
            self.columns_list = [x[0] for x in res]
        except DatabaseError, e:
            raise Exception("Error getting DDL from Greenplum :" + str(e))

        if len(self.columns_list) == 0 :
            raise Exception ("Table %s.%s not found in Greenplum" % (self.schema, self.table))


    def execute(self, query) :
        try :
            cursor = dbconn.execSQL(self.connection, query)
            self.connection.commit()
        except DatabaseError, e :
            raise Exception("Error while executing statements in Greenplum: " + str(e))
        try :
            return cursor.fetchall()
        except DatabaseError :
            return None


    def disconnect(self) :
        if self.connection is not None :
            try :
                self.connection.close()
            except Exception, e :
                raise Exception("Cannot close connection to Greenplum: " + str(e))
    

# Main class
#-------------------------------------------------------------------------------
class QAHelper :
    options = None
    orah = None
    gph = None


    def checkDDL(self, strict_check) :
        gp_columns = list (self.gph.columns_list)
        missing_columns = []

        for column in self.orah.columns_dict :
            try :
                gp_columns.remove(column['COLUMN_NAME'])
            except ValueError :
                missing_columns.append(column['COLUMN_NAME'])
        if len(missing_columns) > 0 :
            raise Exception ('DDL check failed. Columns %s are missing in target model' % ', '.join(missing_columns))
        if strict_check and len(gp_columns) > 0 :
            raise Exception ('DDL check failed. Columns %s are missing in source model' % ', '.join(gp_columns))
        


    def prepareCharExpressions (self, syntax, table_alias = '', take_keys = True, take_nonkeys = True, column_aliases = False, remove_newlines = False) :
        max_length = 100
        fields_to_char = []
        for column in self.orah.columns_dict :
            name = column['COLUMN_NAME']
            name_al = table_alias + name
            type = column['DATA_TYPE']
            precision = column['DATA_PRECISION']
            scale = column['DATA_SCALE']
            length = column['DATA_LENGTH']
            if type == 'NUMBER' :
                field_to_char = "coalesce(trim(to_char(%s,'%s%s%s')), '')" % (name_al,  "9" * (precision or 30),  '.' if scale != 0 else '',  "0" * (scale or 6))
            elif type == 'DATE' :
                field_to_char = "coalesce(to_char(%s, 'YYYYMMDDHH24MISS'), '')" % name_al
            elif type.startswith('TIMESTAMP') :
                if syntax == "GP" :
                    field_to_char = "coalesce(to_char(%s, 'YYYYMMDDHH24MISS.MS'), '')" % name_al
                else:
                    field_to_char = "coalesce(to_char(%s, 'YYYYMMDDHH24MISS.FF3'), '')" % name_al
            elif type in ('CHAR', 'VARCHAR2', 'NVARCHAR2', 'CLOB', 'RAW') :
                if length > max_length:
                    field_to_char = "substr(%s,1,%d)" % (name_al, max_length)
                else:
                    field_to_char = name_al
                if remove_newlines :
                    field_to_char = "replace(replace(%s, chr(13), 'r'), chr(10), 'n')" % field_to_char
                field_to_char = "coalesce(replace(%s, '|', ' '), '')" % field_to_char
            elif type in ('FLOAT') :
                field_to_char = "coalesce(to_char(%s,'%s.%s'), '')" % (name_al,    "9" * 30,   "0" * 6)
            else:
                field_to_char = "''"
            
            if column_aliases :
                field_to_char += ' as '+name
            if (name in self.options.keys_list and take_keys) or (name not in self.options.keys_list and take_nonkeys):
                fields_to_char.append(field_to_char)

        return fields_to_char
   

    def download(self, ora_query, gp_copy_statement) :
        if not ora_query.rstrip().endswith(';') :
            ora_query += ';'
        f = open('./query.sql','w+')  #TemporaryFile()
        f.write(os.linesep.join(["set pagesize 0", "set feedback off", "set colsep |", "SET LINESIZE 32767", ora_query]))
        f.seek(0)
        sqlplus_process = Popen(['sqlplus64', '-S', '-L',  self.orah.connection_string], stdin=f, stdout=PIPE, stderr=None)
        psql_process =  Popen(['psql', '-c', gp_copy_statement ], stdin=sqlplus_process.stdout, stdout=PIPE, stderr=PIPE)
        #fout = open('./result.log','w+')
        #psql_process = Popen('cat', stdin=sqlplus_process.stdout, stdout=fout, stderr=PIPE)
        res = psql_process.communicate()
        #fout.close()
        f.close()
        if "ERROR:" in res[1] :
            raise Exception ("Error occured while downloading from oracle: "+res[1])


    def downloadHash(self) :
        try :
            self.gph.execute("drop table if exists "+self.gph.hash_tablename+";")
            self.gph.execute("create table %s as select %s, cast('' as varchar(32)) __h from %s.%s limit 0 distributed by (%s);" % \
                (self.gph.hash_tablename, self.options.keys, self.gph.schema, self.gph.table, self.options.keys))
        except Exception, e :
            raise Exception("Error while downloading hash: "+str(e))

        keys_stmts = "||'|'||".join(self.prepareCharExpressions('ORA',take_nonkeys=False))
        hash_expr = '||'.join(self.prepareCharExpressions('ORA', take_keys=False))
        ora_hash_query = "select %s||'|'||cast(dbms_obfuscation_toolkit.md5(input=>SYS.utl_raw.cast_to_raw(convert(%s,'UTF8'))) as varchar2(32)) from %s.%s;" % \
                    (keys_stmts, hash_expr, self.orah.schema, self.orah.table)
        gp_copy_statement = "copy %s from stdin with delimiter '|';" % (self.gph.hash_tablename,)
        self.download(ora_hash_query, gp_copy_statement)


    def compare(self) :
        keys_coalesce_list = ', '.join(['coalesce(g.%s, o.%s) as %s' % (x,x,x) for x in self.options.keys_list])
        join_cond = ' and '.join(['g.%s=o.%s' % (x,x) for x in self.options.keys_list])
        hash_expr = '||'.join(self.prepareCharExpressions('GP', 'g.', take_keys=False))
        try:
            self.gph.execute("drop table if exists %s;" % self.gph.diff_tablename)
            compare_stmt = """
                create table {0} as
                select 
                case when g.{1} is null then -1 when o.{1} is null then 1 else 0 end as diff_type,
                {2}
                    from {3} o full join {4}.{5} g on {6}
                    where o.{1} is null or g.{1} is null or o.__h != upper(md5({7}));
                commit; 
                """.format ( self.gph.diff_tablename, \
                         self.options.keys_list[0], \
                        keys_coalesce_list, \
                        self.gph.hash_tablename, self.gph.schema, self.gph.table, join_cond, \
                        hash_expr)
            self.gph.execute(compare_stmt)
        except Exception, e :
            raise Exception("Error while comparing: "+str(e))


    def analyze(self) :
        try :
            res = self.gph.execute("select diff_type, count(*) from %s group by diff_type;" % self.gph.diff_tablename)
        except Exception, e :
            raise Exception("Error while calculating summary on diff table: "+str(e))
        in_ora_only=in_gp_only=in_both=0
        for dt in res :
            if dt[0] == -1 :
                in_ora_only = dt[1]
            elif dt[0] == 1 : 
                in_gp_only = dt[1]
            else : 
                in_both = dt[1]
        return (in_ora_only, in_gp_only, in_both)
 
    def prepareSample(self, samplecount) :
        # get sample from temp_diff table
        try :
            self.gph.execute("""
                drop table if exists {0};
                create table {0} as
                select * from (select {2} from {1} where diff_type=-1 limit {3}) a union 
                select * from (select {2} from {1} where diff_type= 0 limit {3}) b union 
                select * from (select {2} from {1} where diff_type= 1 limit {3}) c 
                """.format(self.gph.samplekeys_tablename, self.gph.diff_tablename, self.options.keys, samplecount))
            
            sample_ids = self.gph.execute("select * from %s" % self.gph.samplekeys_tablename)
        except Exception, e :
            raise Exception("Error while preparing sample subset: "+str(e))

        # create diff table and get samples from Greenplum
        query =  "drop table if exists %s;" % self.gph.sample_tablename
        keys_stmts = ', '.join(['t.'+x for x in self.options.keys_list])
        char_stmts = ', '.join(self.prepareCharExpressions('GP',take_keys=False, column_aliases = True, remove_newlines = True))
        join_cond = ' and '.join(['t.%s=s.%s' % (x,x) for x in self.options.keys_list])
        query += """create table %s as 
                    select %s, %s, cast('Greenplum' as varchar(10)) as __source
                        from %s.%s t 
                        inner join %s s on %s;""" % (self.gph.sample_tablename, keys_stmts, char_stmts, self.gph.schema, self.gph.table, self.gph.samplekeys_tablename, join_cond)
        try:
            self.gph.execute(query)
        except Exception, e :
            raise Exception("Error while creating sample subset: "+str(e))

        # download samples from Oracle
        #-----------------------------
        # prepare copy statement for loading samples in gp
        column_list = [x['COLUMN_NAME'] for x in self.orah.columns_dict]
        for key in self.options.keys_list :
            column_list.remove(key)
        columns_list_str = ', '.join(column_list)
        copy_stmt = "copy %s (%s, %s, __source) from stdin with delimiter '|';" % \
                    (self.gph.sample_tablename, self.options.keys, columns_list_str)

        # start psql-copy process
        psql_process =  Popen(['psql', '-c', copy_stmt ], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        # prepare selects from oracle - parametrized query
        keys_stmts = ', '.join(self.prepareCharExpressions('ORA',take_nonkeys=False))
        char_stmts = ', '.join(self.prepareCharExpressions('ORA',take_keys=False, remove_newlines = True))
        filter_stmt = ' and '.join(["%s=:%d" % (self.options.keys_list[i], i+1) for i in range(len(self.options.keys_list))])
        querytemplate = "select %s, %s, 'Oracle' as tmp_source from %s.%s where %s" % (keys_stmts, char_stmts, self.orah.schema, self.orah.table, filter_stmt)

        # get data from oracle and put it into stdin of psql process
        cursor = self.orah.connection.cursor()
        cursor.prepare(querytemplate)        
        for k in sample_ids:
            cursor.execute(None, k)
            res = cursor.fetchone()
            if res is not None :
                psql_process.stdin.write('|'.join([str(x or '') for x in res]) + os.linesep)

        # run psql and load data to greenplum
        res = psql_process.communicate()
        if "ERROR:" in res[1] :
            raise Exception ("Error occured while downloading from oracle: "+res[1])


    def main(self) :
        logger = get_default_logger()
        try:
            self.options = QAOptions()
            self.gph = GPHelper(self.options.schema, self.options.table)
            self.orah = OraHelper(self.options.schema, self.options.table)
            gp_opts = self.options.connection_options["greenplum"]
            self.gph.connect(gp_opts["server"], gp_opts["login"], gp_opts["password"])
            self.gph.getColumns()
            ora_opts = self.options.connection_options["oracle"]
            self.orah.connect(ora_opts["server"], ora_opts["login"], ora_opts["password"])
            self.orah.getColumns()
            self.checkDDL(self.options.strict)
            timestart = time.time()
            if self.options.reloadhash :
                print "Calculating hash in Oracle..."
                self.downloadHash()
            print "Comparing..."
            self.compare()
            (in_ora_only, in_gp_only, in_both) = self.analyze()
            if in_ora_only+in_gp_only+in_both == 0 :
                print "Tables are identical."
                self.gph.execute("drop table %s;" % self.gph.diff_tablename)
            else :
                samplemsg = ""
                if self.options.nsamples > 0 :
                    print "Sampling..."
                    self.prepareSample(self.options.nsamples)
                    samplemsg = "Created sample table. Query samples as below:\n" + "select * from %s order by %s,__source" % (self.gph.sample_tablename, self.options.keys)
                print "------------REPORT-----------------"
                print "Rows exist only in Oracle:      %d" % in_ora_only
                print "Rows exist only in Greenplum:   %d" % in_gp_only
                print "Rows with different hash value: %d" % in_both
                print samplemsg
            print "Comparison took " + str(round(time.time()-timestart,1))+" seconds"
            print ""
        except Exception,e :
            logger.error(str(e))
        finally :
            self.gph.disconnect()
            self.orah.disconnect()
            
            
qa = QAHelper()
qa.main()
