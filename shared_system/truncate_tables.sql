/*
 * Copyright (c) Pivotal Inc, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko, A.Fursenko
 * Email:  AGrishchenko@gopivotal.com
 * Date:   02 Dec 2013
 * Description: This module contains function to truncate all the tables
 * in the specified schema list
 *
 * Examples of usage:
 * select helpers.truncate_tables(array['public']);                  --truncate public schema
 * select helpers.truncate_tables(array['public', 'test', 'test2']); --truncate public,test,test2 schemas
 */

/*
 * Description: Truncate all the tables in specified schema list
 * Input:
 *      p_schema_list  - array of schema names (case-sensitive, GP default is lower case)
 */
CREATE OR REPLACE FUNCTION shared_system.truncate_tables(p_schema_list varchar[]) RETURNS void AS $BODY$
DECLARE
	p_table_name varchar;
BEGIN
	for p_table_name in (
		select TABLE_SCHEMA || '.' || TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE array[TABLE_SCHEMA] <@ p_schema_list
	) loop
		raise notice 'truncate %', p_table_name;
		EXECUTE 'TRUNCATE '|| p_table_name;
	END LOOP;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;