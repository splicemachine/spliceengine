#!/usr/bin/env python3

import subprocess
import unittest
import os


def expected_output(spark, systable_count):
    return """splice> SELECT COUNT (*) FROM SYS.SYSTABLES{useSpark};
1                   
--------------------
{systable_count}                  

1 row selected
splice> create table t (a int);
0 rows inserted/updated/deleted
splice> insert into t values 1;
1 row inserted/updated/deleted
splice> select * from t{useSpark};
A          
-----------
1          

1 row selected
splice> drop table t;
0 rows inserted/updated/deleted
splice>""".format(systable_count=systable_count,useSpark=" --splice-properties useSpark=true\n" if spark else "")

class ControlQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        startup = "./splicemachine/bin/stop-splice.sh && ./splicemachine/bin/clean.sh && ./splicemachine/bin/start-splice.sh"

        os.system(startup)

    def run_test(self, spark: bool):
        self.maxDiff = None
        test_input = "./splicemachine/bin/sqlshell.sh -f {} | grep -v 'ELAPSED TIME' | awk '/= current connection/{{p=1;next}}{{if(p){{print}}}}'".format("basicspark.sql" if spark else "basic.sql")
        with os.popen(test_input) as o:
            output = o.read()
        output = output.strip() # Remove leading spaces and LFs
        print("Actual output: "+ output)
        self.assertTrue(any(output == expected_output(spark, count) for count in (46, 47, 52)))

    def test_control(self):
        self.run_test(False)

    def test_spark(self):
        self.run_test(True)

if __name__ == '__main__':
    unittest.main()
