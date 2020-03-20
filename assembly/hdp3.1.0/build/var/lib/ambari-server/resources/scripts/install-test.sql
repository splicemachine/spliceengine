-- install smoke test, can we do simple selects, orders, joins, etc.
maximumdisplaywidth 40;

call SYSCS_UTIL.SYSCS_GET_VERSION_INFO();

select * from sys.systables;
select * from sys.systables order by tablename;
select t.tablename, c.conglomeratenumber from sys.systables t, sys.sysconglomerates c where t.tableid = c.tableid;
select count(*) from sys.systables;

-- now create a schema, table, insert data and read it back out.
-- create schema money;
-- set schema money;
-- create table bills(face varchar(255), dollars int);
-- insert into bills values ('abe', 5);
-- insert into bills values ('benny', 100);
-- insert into bills values ('george', 1),('andy', 20),('alex', 10), ('tommy', 2);
-- select * from bills;
-- select * from bills order by dollars;
-- select * from bills order by face;
-- select count(*) from bills;
