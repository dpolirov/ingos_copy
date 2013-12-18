set search_path to dbms_jobs,public;

-- create user and give it privileges
CREATE ROLE etlusr LOGIN
  PASSWORD 'changeme'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE RESOURCE QUEUE pg_default;

grant all on schema dbms_jobs to etlusr;
grant all on dbms_jobs.job_queue to etlusr;
grant all on dbms_jobs.job_sync to etlusr;
grant all on dbms_jobs.job_log to etlusr;
grant all on dbms_jobs.job_group to etlusr;
grant all on dbms_jobs.test to etlusr;

-- Create group
insert into job_group (GROUP_ISN, GROUP_NAME, PRIORITY, MAX_JOBS) values(1,'Group 1',1::smallint,3::smallint);

-- create test table
create table dbms_jobs.test(name varchar(100), data bigint) distributed by (data);

-- Submits many test jobs to queue
create or replace function submit_test_jobs(num_jobs int) returns void as $$
begin
    for i in 1 .. num_jobs loop
        perform dbms_jobs.job_submit('insert into dbms_jobs.test(name, data) select ''Job#'||nextval('job_seq')||''' as name, sum(generate_series) as data from generate_series(1,10000000+cast(1000000.*random() as int));',1,1);
    end loop;
end
$$ language plpgsql;

select submit_test_jobs(3);

-- jobs execution monitoring
select * from job_queue;
select * from test;
select * from job_log;


