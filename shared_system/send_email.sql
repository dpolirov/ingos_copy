create or replace function shared_system.send_email (subject varchar, body varchar) returns void as $BODY$
import csv
from datetime import datetime
f = open('/home/gpadmin/emails.csv', 'a+')
f.write(str(datetime.now()) + '|' + subject + '|' + body + '\n')
f.close()
$BODY$
language plpythonu
volatile;

select shared_system.send_mail ('test', 'test body3');