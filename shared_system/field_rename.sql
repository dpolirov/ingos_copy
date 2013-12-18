/*
 * Copyright (c) Pivotal Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  AGrishchenko@gopivotal.com
 * Date:   18 Dec 2013
 * Description: This function translates source field name to target.
 * Used for translation of the Oracle field names that cannot be used in GP
 * as they are reserved words in GP
 *
 * Input:
 *      mapping   - mapping between old and new field names in syntax 'old_name=new_name'
 *      value     - field name that should be translated
 *      direction - translation direction: 'forward', 'backward'
 *
 * Examples of usage:
 * select shared_system.field_rename (array['a=b','c=d'], 'a', 'forward'); --returns 'b'
 * select shared_system.field_rename (array['a=b','c=d'], 'f', 'forward'); --returns 'f'
 * select shared_system.field_rename (array['a=b','c=d'], 'd', 'backward'); --returns 'c'
 */
create or replace function shared_system.field_rename (mapping   varchar[],
                                                       value     varchar,
                                                       direction varchar) returns varchar as $BODY$
if direction != 'forward' and direction != 'backward':
    raise Exception ("Direction can be only 'forward' or 'backward'")
m = {}
for el in mapping:
    pair = el.strip().lower().split('=')
    if len(pair) != 2:
        raise Exception ("Pair '%s' has wrong syntax. Correct syntax is 'old_field=new_field'" % el)
    if direction == 'forward':
        m[pair[0]] = pair[1]
    else:
        m[pair[1]] = pair[0]
if not value.lower() in m:
    return value.lower()
else:
    return m[value.lower()]
$BODY$
language plpythonu
immutable;

create or replace function shared_system.field_rename (mapping   varchar[],
                                                       value     varchar) returns varchar as $BODY$
    select shared_system.field_rename($1, $2, 'backward');
$BODY$
language sql
immutable;

