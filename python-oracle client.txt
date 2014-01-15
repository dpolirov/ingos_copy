1. download sources
http://prdownloads.sourceforge.net/cx-oracle/cx_Oracle-5.1.tar.gz?download
unpack in any dir

2. Install Oracle soft: 
http://www.oracle.com/technetwork/topics/linuxx86-64soft-092277.html
Version 11.2.0.4.0 
Instant Client Package - Basic
Instant Client Package - SQL*Plus
Instant Client Package - SDK

3. Set in .bashrc
for all users who uses module!
export ORACLE_HOME=/usr/lib/oracle/11.2/client64
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME/lib

4. Install python-devel (Not necessary if you are running on master machine or if uploader already installed) 

sudo yum install python-devel

Installing:
 python-devel                              x86_64                              2.6.6-37.el6_4                                updates                              168 k
Updating for dependencies:
 expat                                     x86_64                              2.0.1-11.el6_2                                base                                  76 k
 python                                    x86_64                              2.6.6-37.el6_4                                updates                              4.8 M
 python-libs                               x86_64                              2.6.6-37.el6_4                               updates                              595 k
 zlib                                      x86_64                              1.2.3-29.el6                                  base                                  73 k
 
 

5- Add libpython2.6 in  

ln -s /data/yes/ext/python/lib/python2.6/config/libpython2.6.a /usr/local/lib


Pay attention to which python runs
6. python setup.py build
7. python setup.py install


8 - Validate 
Testing (Post Installation Quick Test)
--------------------------------------
A very quick installation test can be performed from the command line using
the Python interpreter. Below is an example of how this done. After importing
cx_Oracle there should be a line containing only '>>>' which indicates the
library successfully loaded.

    $ python
    Python 2.5.2 (r252:60911, Oct 25 2008, 19:37:28)
    [GCC 4.1.2 (Gentoo 4.1.2 p1.1)] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import cx_Oracle
    >>>

9 - Create schema qa inside Greenplum db
create schema qa;

10 - replace sqlplus64 with sqlplus on line 312 if got error : 
20131231:00:38:52:054419 compare_tables.py:default-[ERROR]:-[Errno 2] No such file or directory
