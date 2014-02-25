/* 
 * Creates table that has one row on each amp 
 */
create table shared_system.segment_distributor as
select generate_series(1,100)::smallint as isn
distributed by (isn);

delete from shared_system.segment_distributor where isn not in (select  min(isn) from shared_system.segment_distributor group by gp_segment_id);

alter table shared_system.segment_distributor set with(reorganize=true);
