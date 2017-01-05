select a, count(b), c from
(select
mul.j as a
, fail.i as b
, fail.j as c
from 
  (select * from mul
  union all select * from mul
  union all select * from mul
  union all select * from mul
  union all select * from mul
  union all select * from mul
  union all select * from mul
  union all select * from mul
  ) mul
  left join fail --splice-properties useSpark=true
  on mul.i = fail.i
) w 
group by a, c
order by a, c
