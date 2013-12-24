#!/usr/bin/python
# Copyright (C) Pivotal Inc 2013. All Rights Reserved. 
# Igor Putyatin (iputyatin@gopivotal.com)
#
# Script generates DDL for Greenplum based on Oracle catalog.
# Options:
#   -f <path>           Required. Path to text file containing list of migrated tables 
#   -d                  Optional. Generate DROP TABLE IF EXISTS statemets for each table.
#   -c <path>           Optional. Path to config file with credentials for Oracle database.
#                                           Default ./ddl_migration.conf 
import json
from optparse import Option, OptionParser
from gppylib.gpparseopts import OptParser, OptChecker
import os
# set encoding parameter for cx_Oracle
os.environ["NLS_LANG"] = ".UTF8"
import cx_Oracle

#----------------------------------------
#  list of greenplum keywords. "_" will be appended to column names that match keywords 
#----------------------------------------
keywords=["exclude", "new", "decode"]

#----------------------------------------
#   data type conversion decision tree
#----------------------------------------
def convertType(type, precision, scale, length) :
    if type == "NUMBER" :
        if precision is None and scale is None :
            newtype = "NUMERIC"
        elif scale == 0 :
            if precision is None :
                newtype = "NUMERIC(38)"
            elif precision <= 4 :
                newtype = "SMALLINT"
            elif precision <= 9 :
                newtype = "INT"
            elif precision <= 18 :
                newtype = "BIGINT"
            else :
                newtype = "NUMERIC(%d)" % precision
        else :
            newtype = "NUMERIC(%d,%d)" % (precision, scale)

    elif type == "FLOAT" :
        newtype = "FLOAT"

    elif type == "DATE" or type.startswith("TIMESTAMP")  :
        if "TIME ZONE" in type :
            newtype = "TIMESTAMPTZ"
        else :
            newtype = "TIMESTAMP"
    elif type.startswith("INTERVAL") :
        newtype = "INTERVAL"

    elif type in ("CHAR", "VARCHAR2", "NVARCHAR2") :
        newtype = "VARCHAR(%d)" % length
    elif type == "RAW" :
        newtype = "VARCHAR(%d)" % (length * 2)
    elif type in ("CLOB", "LONG", "LONG RAW") :
        newtype = "TEXT"
    elif type == "ROWID" :            
        newtype = "VARCHAR(18)"

    elif type in ("ANYDATA", "UNDEFINED", "BLOB") :
        newtype = "BYTEA"

    else :
        logerror("WARNING: Unknown type %s for column %s in table %s.%s. Converted to BYTEA." % (type,column,owner,table))
        newtype = "BYTEA"
    
    return newtype


# error handling
#----------------------------------------
errors = []
def logerror(msg) :
    errors.append(msg)
    print "--" + msg



# parsing input data
#----------------------------------------
class DDLOptions:
    tables_list = []
    connection_options=None
    drop = False
    # Parse input parameters and configuration file
    def __init__(self) :
        parser = OptParser(option_class=OptChecker)
        parser.remove_option("-h")    
        parser.add_option("-h", "-?", "--help",  action="store_true")
        parser.add_option("-f", "--file",        type="string")
        parser.add_option("-c", "--config",      type="string")
        parser.add_option("-d", "--drop",        action="store_true")
        (parser_result, args) = parser.parse_args()
        helpstr = "Usage:" + os.linesep + "./ddl_migration.py -f path"
        if parser_result.help :
            print helpstr
            sys.exit(0)
        if not parser_result.file:
            raise Exception("Failed to start utility. Please, specify path to input file with -f key")
            sys.exit(1)

        configpath = parser_result.config or "./ddl_migration.conf"
        try:
            configfile = open(configpath, "rb")
            self.connection_options = json.load(configfile)
            configfile.close()
        except Exception, e :
            raise Exception("Could not read config file " + configpath + ". " +str(e))
        
        config_ok=False
        if "oracle" in self.connection_options.keys() :
            ops_list = self.connection_options["oracle"].keys()
            if "server" in ops_list and "login" in ops_list and "password" in ops_list :
                config_ok=True
        if not config_ok :       
            raise Exception('Wrong structure in configuration file. Should be: "oracle" on top level;  "login", "password", and "server" on second level')

            
        try:
            inputfile = open(parser_result.file, "r")
            for line in inputfile.readlines() :
                line = line.strip()
                if len(line) > 0 and not line.startswith("#") and not line.startswith("--") :
                    self.tables_list.append(line)
            inputfile.close()
        except Exception, e :
            raise Exception("Could not read input file " + parser_result.file + ". " +str(e))

        self.drop = parser_result.drop



# main process
#-----------------------------------------------------------------------------
options = DDLOptions()
co = options.connection_options["oracle"]
connection_string = "%s/%s@%s" % (co["login"], co["password"], co["server"] )
try :
    connection = cx_Oracle.connect(connection_string)
    cursor = connection.cursor()
    cursor_comments = connection.cursor()
except cx_Oracle.DatabaseError, e:
    raise Exception("Cannot connect to Oracle :" + str(e))

cursor.prepare("""
select  cols.column_name,
        cols.data_type,
        cols.data_precision,
        cols.data_scale,
        cols.data_length,
        cols.data_default,
        pks.column_name as pk_column_name
    from all_tab_columns cols 
        left join (
            select conscols.column_name 
                from all_constraints cons
                    inner join all_cons_columns conscols
                        on cons.owner=conscols.owner 
                        and cons.table_name=conscols.table_name
                        and cons.constraint_name=conscols.constraint_name
                where cons.owner=:1 
                    and cons.table_name=:2 
                    and cons.constraint_type='P'
                    ) pks
        on pks.column_name = cols.column_name  
    where cols.owner=:3 
        and cols.table_name=:4 
    order by cols.column_id
""")

cursor_comments.prepare("""
select cc.column_name,
       tc.comments as table_comment,
       cc.comments as column_comment
    from all_tab_comments tc,
         all_col_comments cc
    where   tc.owner = :1 
        and tc.table_name = :2 
        and cc.owner = :3 
        and cc.table_name = :4
""")


for fulltablename in options.tables_list :
    if '.' not in fulltablename :
        logerror( "ERROR: Schema not specified for table %s" % fulltablename)
        continue
    (owner, table) = fulltablename.lower().split('.')[0:2]
    cursor.execute(None, (owner.upper(),table.upper(),owner.upper(),table.upper()))
    result = cursor.fetchall()
    #create list of dictionaries
    numcols = len(result)
    if numcols == 0:
        logerror("ERROR: Table %s.%s doesn't exist." % (owner,table))
        continue
    colnames = [x[0] for x in cursor.description]
    columns = [dict(zip(colnames, x)) for x in result]
    if options.drop :
        print 'DROP TABLE IF EXISTS %s.%s;' % (owner, table)
    print 'CREATE TABLE %s.%s (' % (owner, table)
    ncol = 1
    pklist = []
    comments = []
    columns_renamed={}
    for column in columns:
        name = originalname = column["COLUMN_NAME"].lower()
        default_value = column["DATA_DEFAULT"]

        # correct name if it is keyword in greenplum
        if name in keywords :
            logerror("WARNING: In table %s.%s column %s matches Greenplum keyword. Corrected to %s_." % (owner,table,name,name))
            name += '_'
        
        columns_renamed[originalname] = name
        
        # make pk list
        if column["PK_COLUMN_NAME"] is not None :
            pklist.append(name)
        
        default_stmt = ""
        if default_value is not None :
            default_value = default_value.strip()
            if default_value.upper() == "SYSDATE" :
                default_value = "current_timestamp"
            default_stmt = " DEFAULT " + default_value
                        
            
        # generate DDL for column       
        coldef = "    " + name.ljust(33) + convertType(column["DATA_TYPE"], column["DATA_PRECISION"], column["DATA_SCALE"], column["DATA_LENGTH"]) + default_stmt
        if ncol < numcols :
            coldef += ','        ncol += 1
        
        print coldef

    
    print ")"
    if len(pklist) == 0 :
        print ";"
        logerror("WARNING: No primary key defined for %s.%s" % (owner,table))
    else :
        print "DISTRIBUTED BY (%s);" % ','.join(pklist)

    # comments
    print
    cursor_comments.execute(None, (owner.upper(),table.upper(),owner.upper(),table.upper()))
    result = cursor_comments.fetchall()
    if len(result) > 0 :
        comments = [dict(zip([y[0] for y in cursor_comments.description], x)) for x in result]
        if comments[0]["TABLE_COMMENT"] is not None :
            print "COMMENT ON TABLE %s.%s IS $COMM$%s$COMM$;" % (owner, table, comments[0]["TABLE_COMMENT"])
        for comment in comments :
            if comment["COLUMN_COMMENT"] is not None :
                print "COMMENT ON COLUMN %s.%s.%s IS $COMM$%s$COMM$;" % (owner, table, columns_renamed[comment["COLUMN_NAME"].lower()], comment["COLUMN_COMMENT"])
    print
    print

if len(errors) != 0:
    print
    print "/*********** Errors and warnings **********"
    for msg in errors :
        print msg
    print "*******************************************/"
