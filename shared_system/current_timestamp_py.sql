create or replace function shared_system.current_timestamp_py () returns timestamp as $BODY$
from datetime import datetime
return datetime.now()
$BODY$
language plpythonu volatile;
