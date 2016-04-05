insert into t (a,b,c) select a,b+262144,c from t --SPLICE-PROPERTIES useSpark=false
; --1<<18
insert into t (a,b,c) select a,b+524288,c from t --SPLICE-PROPERTIES useSpark=false
; --1<<19
select count(*) from t;
insert into t (a,b,c) select a,b+1048576,c from t --SPLICE-PROPERTIES useSpark=false
;--1<<20
select count(*) from t;
insert into t (a,b,c) select a,b+2097152,c from t --SPLICE-PROPERTIES useSpark=false
; --1<<21
insert into t (a,b,c) select a,b+4194304,c from t --SPLICE-PROPERTIES useSpark=false
; --1<<22
insert into t (a,b,c) select a,b+8388608,c from t --SPLICE-PROPERTIES useSpark=false
; --1<<23
select count(*) from t;
--insert into t (a,b,c) select a,b+16777216,c from t --SPLICE-PROPERTIES useSpark=true; --1<<24
----insert into t (a,b,c) select a,b+262144,c from t; --1<<25
----insert into t (a,b,c) select a,b+262144,c from t; --1<<26
----insert into t (a,b,c) select a,b+262144,c from t; --1<<27
select count(*) from t;
