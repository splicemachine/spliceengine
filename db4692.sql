drop table if exists t;

create table t (a int, b int,c varchar(1024)) --SPLICE-PROPERTIES partitionSize=16
;

insert into t (a,b,c) values (1,1,'asbasfb8asfasf98asdfgasldkgjnasdfp9a8sdhflaskdfjansdg87ahsdnflkq2u3ryaps9dvasdfpa8sdasdviuasdvasdf98ahsdflaksdfuyasdf9a8sdhflaksdfuhasd89v7ahsdnflkasudfhaspd978fhasdflaskdufha89sdhflaksdjfhpa9s8dfh8asdvasf8basfgasdflasdfpodabyadfb897sdhv sdfvksjdfgaysdvdv adfhgasdfkasdjfyapsdfi8ahsdfqlkrejqwdyfpa9s8eyrq234lkasucvyasclkajsgyaps9df8bhasldkgusfy7blaskfjgnasfbiau7hsfnblkasf8b7sz;cglviashjjpv9a8sfbaflboiahfd;askdjfasdfkajsdhgasfbasfbafbasgasdfasdfgagasdgasdgasdgasgasdgasgasdgasdgasdgasdgasdgadsgaagadsasdagasdflkjasdhfasdflkjashdfcbvcvxbadfgawefawed.vaxcjvyasdfa,scvjhasdfgawasdf;laksjdfalsdkfjasdf;asdfasdfasdfashellogoodbyetacoschickenapplesgranolayogurtbreadcheesedogspuppiespoodlesgoldensdeveloperscandywickerhatesfatpeopleanimalsasdsasdfasdfadfasdfasdfasdfasdfasdfasdfasdfasdf');
insert into t (a,b,c) select a,b+1,c from t;
insert into t (a,b,c) select a,b+2,c from t;
insert into t (a,b,c) select a,b+4,c from t;
insert into t (a,b,c) select a,b+8,c from t;
insert into t (a,b,c) select a,b+16,c from t;
insert into t (a,b,c) select a,b+32,c from t;
insert into t (a,b,c) select a,b+64,c from t;
insert into t (a,b,c) select a,b+128,c from t;
insert into t (a,b,c) select a,b+256,c from t;
insert into t (a,b,c) select a,b+512,c from t;
select count(*) from t;
insert into t (a,b,c) select a,b+1024,c from t;
insert into t (a,b,c) select a,b+2048,c from t;
insert into t (a,b,c) select a,b+4096,c from t;
select count(*) from t;
insert into t (a,b,c) select a,b+8192,c from t;
insert into t (a,b,c) select a,b+16384,c from t;
insert into t (a,b,c) select a,b+32768,c from t; --1<<15
select count(*) from t;
insert into t (a,b,c) select a,b+65536,c from t; --1<<16
insert into t (a,b,c) select a,b+131072,c from t --SPLICE-PROPERTIES useSpark=true
; --1<<17
select count(*) from t;

--will claim to have inserted more than 262,144 records. Count(*) will verify
insert into t (a,b,c) select a,b+262144,c from t --SPLICE-PROPERTIES useSpark=true
; --1<<18
select count(*) from t;
