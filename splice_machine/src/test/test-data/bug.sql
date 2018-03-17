connect 'jdbc:splice://localhost:1527/splicedb';
create schema test;
set schema test;
create table omslog (
host varchar(30),
date_time timestamp,
duration integer,
http_request varchar(100),
http_response integer,
bytes_returned integer,
transaction_id varchar(10),
total_time integer,
task_oms_time integer,
parse_task_oms_time integer,
parse_task_total_time integer,
parse_task_active_count integer,
parse_task_queue_length integer,
read_or_update char(1),
update_task_oms_time integer,
update_task_total_time integer,
update_task_active_count integer,
update_task_queue_length integer,
response_task_oms_time integer,
response_task_total_time integer,
response_task_active_count integer,
response_task_queue_length integer,
response_ser integer,
validation_time integer,
cache_creates integer,
cache_clears integer,
cache_hits bigint,
cache_misses bigint,
cache_evicts integer,
instances_accessed integer,
decompression_cache_hits bigint,
decompressions integer,
offload_count integer,
offload_requests integer,
offload_cache_hits integer,
gi_calls integer,
sql_read_count integer,
sql_read_time integer,
sql_read_time_max integer,
sql_update_count integer,
sql_update_time integer,
sql_update_time_max integer,
tenant_id varchar(10),
system_user_id varchar(10),
task_id varchar(10),
task_display_name varchar(12),
session_id varchar(30),
jsession_id varchar(40),
request_id varchar(40),
request_handler varchar(40),
swh_date date,
swh_dc varchar(30),
swh_server varchar(30),
swh_app varchar(30),
swh_env varchar(30));
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TEST', 'OMSLOG', null, null, '/Users/gdavis/workday/file1/t2.csv', ',', '"', 'YYYY-MM-dd HH:mm:ss');

-- Create a view - note this is multiple lines:
create view oms_sla_vw as select host, date_time, duration, http_request, http_response, bytes_returned, transaction_id, total_time, task_oms_time, parse_task_oms_time, parse_task_total_time, parse_task_active_count, parse_task_queue_length, read_or_update, update_task_oms_time, 
update_task_total_time, update_task_active_count, update_task_queue_length, response_task_oms_time, response_task_total_time, response_task_active_count, response_task_queue_length, response_ser, validation_time, cache_creates, cache_clears, cache_hits, cache_misses, cache_evicts, 
instances_accessed, decompression_cache_hits, decompressions, offload_count, offload_requests, offload_cache_hits, gi_calls, sql_read_count, sql_read_time, sql_read_time_max, sql_update_count, sql_update_time, sql_update_time_max, tenant_id, system_user_id, task_id, task_display_name, 
session_id, jsession_id, request_id, request_handler, swh_date, swh_dc, swh_server, swh_app, swh_env, CASE WHEN duration >=200 THEN 1 ELSE 0 END as count_over_sla_2, CASE WHEN duration >=500 THEN 1 ELSE 0 END as count_over_sla_5 from omslog;

-- create a view of a view
create view oms_sla_vw2 as select tenant_id, swh_date, count(*) as total_requests, sum(count_over_sla_2) as requests_gt_2, 100*(sum(count_over_sla_2)/count(*)) as pct_requests_gt_2, sum(count_over_sla_5) as requests_gt_5, 100*(sum(count_over_sla_5)/count(*)) as pct_requests_gt_5 from oms_sla_vw where http_request='ots' and swh_env = 'ENV1' and request_handler = 'UserInterfaceRequestHandler' group by tenant_id,swh_date;

-- The following fails:
select * from oms_sla_vw2 { limit 1 };
