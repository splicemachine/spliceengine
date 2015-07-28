select * from
region, nation
where n_regionkey = r_regionkey
and r_name = 'AMERICA'
