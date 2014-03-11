/*
 * Copyright (c) EMC Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  Aleksey.Grishchenko@emc.com
 * Date:   08 Apr 2013
 * Description: This function allows you to get all a subtree with root in specified node
 * Limitations: Function uses array to store values, so if result set is more than 100k it fails
 * For instance, consider the tree
 *        1      -- level 1
 *       / \    
 *      2   3    -- level 2
 *     / \   \ 
 *    4   5   6  -- level 3
 * For '1' subtree will contain '1','2','3','4','5','6'
 * For '2' subtree will contain '2','4','5'
 * For '3' subtree will contain '3','6'
 * For '4', '5' and '6' subtree will contain only one node
 *
 * Example of usage:
 * -- Create sample data (see graphical sample above)
 * create table public.test_hier (isn int, parentisn int, value varchar, __hier int[], __hier_leaf boolean);
 * insert into  public.test_hier (isn, parentisn, value) values (1, null, 'one'), (2, 1, 'two'),
 *                      (3, 1, 'three'), (4, 2, 'four'), (5, 2, 'five'), (6, 3, 'six');
 * -- Fill hierarchy fields
 * select shared_system.refresh_hierarchy('public.test_hier', 'isn', 'parentisn', 'int');
 *
 * -- Select the hierarchy structure
 * select * from public.test_hier order by 1;
 * ISN  PARENTISN   VALUE   __HIER      __HIER_LEAF
 * 1    null        one     {1}         false
 * 2    null        two     {1,2}       false
 * 3    null        three   {1,3}       false 
 * 4    null        four    {1,2,4}     true
 * 5    null        five    {1,2,5}     true
 * 6    null        six     {1,3,6}     true
 *
 * -- Get hierarchy level for each node
 * select isn, shared_system.get_level(__hier) as level from public.test_hier order by 1;
 * ISN  LEVEL
 * 1    1
 * 2    2
 * 3    2
 * 4    3
 * 5    3
 * 6    3
 *
 * -- Get hierarchy level for subtree of node "2"
 * select isn, shared_system.get_level(__hier,2) as level from public.test_hier where shared_system.is_subtree(__hier,2) order by 1;
 * ISN  LEVEL
 * 2    1
 * 4    2
 * 5    2
 * Picture of subtree:
 *      2     -- level 1
 *     / \   
 *    4   5   -- level 2
 *
 * -- Get the root id for the subtree of node "2"
 * select isn, shared_system.root_id(__hier, 2) as root_id from public.test_hier order by 1;
 * ISN  ROOT_ID
 * 1    null      -- cannot be determined as it is not in subtree of "2"
 * 2    2
 * 3    null      -- cannot be determined as it is not in subtree of "2"
 * 4    2
 * 5    2
 * 6    null      -- cannot be determined as it is not in subtree of "2"
 *
 * -- Get path to root node for the subtree of node "2"
 * select isn, shared_system.connect_by_path(__hier,' then ',2) as path from public.test_hier order by 1;
 * ISN  ROOT_ID
 * 1    null      -- cannot be determined as it is not in subtree of "2"
 * 2    2
 * 3    null      -- cannot be determined as it is not in subtree of "2"
 * 4    2 then 4
 * 5    2 then 5
 * 6    null      -- cannot be determined as it is not in subtree of "2"
 *
 * -- Same for all the tree
 * select isn, shared_system.connect_by_path(__hier,' then ') as path from public.test_hier order by 1;
 * ISN  ROOT_ID
 * 1    1
 * 2    1 then 2
 * 3    1 then 3
 * 4    1 then 2 then 4
 * 5    1 then 2 then 5
 * 6    1 then 3 then 6
 *
 * -- Get 1-up parent
 * select isn, shared_system.get_parent(__hier, 1) as parent, shared_system.get_parent(__hier,2) as one_up_parent from public.test_hier order by 1;
 * ISN  PARENT  ONE_UP_PARENT
 * 1    null    null
 * 2    1
 * 3    1
 * 4    2       1
 * 5    2       1
 * 6    3       1
 *
 * -- Get values from parent nodes
 * select t1.value, t2.value as parent_value, t3.value as one_up_parent_value
 *      from public.test_hier as t1
 *          left join public.test_hier as t2 on t2.isn = shared_system.get_parent(t1.__hier,1)::int
 *          left join public.test_hier as t3 on t3.isn = shared_system.get_parent(t1.__hier,2)::int
 *      order by t1.isn;
 * VALUE    PARENT_VALUE    ONE_UP_PARENT_VALUE
 * one      null            null
 * two      one             null
 * three    one             null
 * four     two             one
 * five     two             one
 * six      three           one
 *
 * -- Clean up
 * drop table public.test_hier;
 */

/*  Description:
        Function to refresh hierarchy in a table. Table should contain fields __hier
        and __hier_leaf before running this function
    Parameters:
        p_table_name   - full-qualified table name
        p_id_field     - name of the ID field
        p_parent_field - name of the PARENT_ID field
        p_id_datatype  - data type of the ID field (character data types are not supported)
*/
create or replace function shared_system.refresh_hierarchy(p_table_name   varchar,
                                                         p_id_field     varchar,
                                                         p_parent_field varchar,
                                                         p_id_datatype  varchar) returns void as $BODY$
declare
	p_rowcount bigint;      -- Number of rows updated on current level
	p_rowcount_prev bigint; -- Number of rows updated on previous level
	p_fields varchar[];     -- Array of field names
	p_level int;            -- Number of level
begin
    -- In case tt_hier table exists (created as permanent) - drop it
	execute 'drop table if exists tt_hier';
    -- Create temp table to store hierarchy information
	execute 'create temporary table tt_hier (
                    id        ' || p_id_datatype || ',
                    parent_id ' || p_id_datatype || ',
                    __hier    ' || p_id_datatype || '[])
             on commit drop
             distributed by (id)';
    -- Initially fill it
    execute 'insert into tt_hier (id, parent_id, __hier)
                select  ' || p_id_field     || ',
                        ' || p_parent_field || ',
                        array[' || p_id_field || ']
                    from ' || p_table_name;
    -- Update hierarchy path to root node in cycle, level by level
	p_rowcount_prev = 0;
	p_level = 1;
	loop
		raise notice 'Processing level %', p_level;
		p_level = p_level + 1;
        -- Main update - gets list of root pretenders, finds their parents and append to the hierarchy array
		execute '
            update tt_hier as base
				set __hier = up.parent_id || __hier
                from (
                    select main.parent_id,
                           new_parents.id
                        from tt_hier as main
                            inner join (
                                select id
                                    from (
                                        select __hier[1] as id
                                            from tt_hier
                                        ) as q
                                    where id is not null
                                    group by id
                            ) as new_parents
                            on main.id = new_parents.id
                        where main.parent_id <> new_parents.id
                    ) as up
                where __hier[1] = up.id;';
		GET DIAGNOSTICS p_rowcount = ROW_COUNT;
        -- If no rows updated - hierarchy id ready
		if p_rowcount = 0 then
			exit;
		end if;
        -- If the same amount of rows updated - we have a cycle in a tree, raise error
		if p_rowcount = p_rowcount_prev then
			raise exception 'Cycle found in the table';
		end if;
		p_rowcount_prev = p_rowcount;
	end loop;
    -- Temp table to eliminate drop-create of target table
	execute 'create temporary table tt_tmp (like ' || p_table_name || ')
                with (appendonly=true)
                on commit drop';
    -- Save table data
	execute 'insert into tt_tmp
                select *
                from ' || p_table_name;
    -- Truncate it
	execute 'truncate ' || p_table_name;
    -- Get field list description without technical fields
	select shared_system.array_minus(
                shared_system.get_field_list(p_table_name),
                array['__hier', '__hier_leaf']::varchar[])
		into p_fields;
    -- Fill the source table with data
	execute 'insert into ' || p_table_name || ' (' || shared_system.put_to_str(p_fields) || ', __hier, __hier_leaf)
                select  ' || shared_system.put_to_str(p_fields, ',', 'tt_tmp.') || ',
                        tt_hier.__hier,
                        leaf.id is not null
                    from tt_tmp left join tt_hier on tt_hier.id = tt_tmp.' || p_id_field || '
                        left join (
                            select h1.id
                                from tt_hier as h1 
                                    left join tt_hier as h2
                                    on h1.id = h2.parent_id
                                where h2.id is null
                        ) as leaf
                        on leaf.id = tt_tmp.' || p_id_field;
end;
$BODY$
language plpgsql volatile;

/*  Description:
        Function to get the node level in specified hierarchy. If called with id=null,
        level is calculated until the root is reached, if not - level is calculated unless
        specified node is found (if not found - null is returned)
    Parameters:
        __hier  - hierarchy array filled by shared_system.refresh_hierarchy function
        id      - value of the root id
*/
create or replace function shared_system.get_level(__hier numeric[], id numeric) returns int as $BODY$
declare
    i        int;
    res      int;
    is_found boolean;
begin
    if id is null then
        res = array_upper(__hier, 1);
    else
        res      = 1;
        is_found = false;
        i        = array_upper(__hier, 1);
        loop
            if i = 0 then
                exit;
            end if;
            if __hier[i] = id then
                is_found = true;
                exit;
            end if;
            res = res + 1;
            i = i - 1;
        end loop;
        if not is_found then
            res = null;
        end if;
    end if;
    return res;
end;
$BODY$
language plpgsql immutable;

create or replace function shared_system.get_level(__hier numeric[]) returns int as $BODY$
    select shared_system.get_level($1, null);
$BODY$
language sql immutable;

/*  Description:
        Function returns the root id of the current hierarchy branch. If id=null,
        it returns the root node, if id is not null then if id is in the parent nodes
        for this node it returns id else it returns null
    Parameters:
        __hier  - hierarchy array filled by shared_system.refresh_hierarchy function
        id      - value of the root id
*/
create or replace function shared_system.root_id(__hier numeric[], id numeric) returns numeric as $BODY$
declare
    res      numeric;
begin
    if id is null then
        res = __hier[1];
    else
        if array[id] <@ __hier then
            res = id;
        else
            res = null;
        end if;
    end if;
    return res;
end;
$BODY$
language plpgsql immutable;

create or replace function shared_system.root_id(__hier numeric[]) returns numeric as $BODY$
    select shared_system.root_id($1, null);
$BODY$
language sql immutable;

/*  Description:
        Function returns the path (list of IDs) to root node in the branch. If the id is null,
        it returns path to root of the branch, if not - it return path to the specified id (if
        in is in parent nodes for specified hierarchy) or null if not
        This function does not return path
    Parameters:
        __hier  - hierarchy array filled by shared_system.refresh_hierarchy function
        dlm     - delimiter to be used to separate 
        id      - value of the root id
*/
create or replace function shared_system.connect_by_path(__hier numeric[], dlm varchar, id numeric) returns varchar as $BODY$
declare
    i        int;
    res      varchar;
    is_found boolean;
begin
    if id is null then
        res = array_to_string(__hier, dlm);
    else
        res = null;
        if array[id] <@ __hier then
            for i in 1 .. array_upper(__hier, 1) loop
                if __hier[i] = id then
                    res = array_to_string(__hier[i : array_upper(__hier,1)], dlm);
                    exit;
                end if;
            end loop;
        end if;
    end if;
    return res;
end;
$BODY$
language plpgsql immutable;

create or replace function shared_system.connect_by_path(__hier numeric[], dlm varchar) returns varchar as $BODY$
    select shared_system.connect_by_path($1, $2, null);
$BODY$
language sql immutable;

create or replace function shared_system.connect_by_path(__hier numeric[], id numeric) returns varchar as $BODY$
    select shared_system.connect_by_path($1, ',', $2);
$BODY$
language sql immutable;

create or replace function shared_system.connect_by_path(__hier numeric[]) returns varchar as $BODY$
    select shared_system.connect_by_path($1, ',', null);
$BODY$
language sql immutable;

/*  Description:
        Function returns the id of parent node "plevel" levels above the current node.
        If id is specified it is considered as root node and all the nodes above it are
        ignored
    Parameters:
        __hier  - hierarchy array filled by shared_system.refresh_hierarchy function
        plevel  - number of levels above current node
        id      - value of the root id
*/
create or replace function shared_system.get_parent(__hier numeric[], plevel int, id numeric) returns numeric as $BODY$
declare
    i          int;    
    res        numeric;
begin
    res = null;
    if id is null then
        i = array_upper(__hier, 1);
        if plevel+1 <= i then
            res = __hier[i-plevel];
        end if;
    else
        res = null;
        if array[id] <@ __hier then
            i = array_upper(__hier, 1);
            loop
                if (i = 0) or (__hier[i] = id) then
                    exit;
                end if;
                i = i - 1;
            end loop;
            if (i > 0) and array_upper(__hier,1) - i - plevel >= 0 then
                res = __hier[array_upper(__hier,1) - plevel];
            end if;
        end if;
    end if;
    return res;
end;
$BODY$
language plpgsql immutable;

create or replace function shared_system.get_parent(__hier numeric[], plevel int) returns numeric as $BODY$
    select shared_system.get_parent($1, $2, null);
$BODY$
language sql immutable;

/*  Description:
        Function returns "true" if the node is in subtree of specified node and "false" if not
    Parameters:
        __hier  - hierarchy array filled by shared_system.refresh_hierarchy function
        id      - value of the root id
*/
create or replace function shared_system.is_subtree(__hier numeric[], id numeric) returns boolean as $BODY$
    select array[$2] <@ $1;
$BODY$
language sql immutable;