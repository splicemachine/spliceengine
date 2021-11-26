/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.upgrade;

import com.splicemachine.access.hbase.HBasePartitionAdmin;
import com.splicemachine.db.impl.sql.catalog.SYSNATURALNUMBERSRowFactory;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Note: These tests need to be excluded in platform_it/pom.xml,
 * as they are only mean to be run with the check_upgrade.sh script.
 *
 * e.g.
 * bash pipelines/upgrade-testing/check_upgrade.sh 3.1.0.2000 -T 4
 * this downloads a tar.gz (here https://splice-snapshots.s3.amazonaws.com/upgrade_tests/platform_it_3.1.0.2000.tar.gz)
 * extracts that, then starts the cluster with that data (without clean), and then executes the UpgradeTestIT
 *
 * see also
 * pipelines/upgrade-testing/check_upgrade.sh
 * pipelines/upgrade-testing/create_upgrade_targz.sh
 * https://s3.console.aws.amazon.com/s3/buckets/splice-snapshots?region=us-east-1&prefix=upgrade_tests/&showversions=false
 */
public class UpgradeTestIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = UpgradeTestIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @Test // 1983
    public void testUpgradeScriptToAddBaseTableSchemaColumnsToSysTablesInSYSIBM() throws Exception {
        String expected =
                "NAME    | CREATOR |TYPE |COLCOUNT |KEYCOLUMNS | KEYUNIQUE |CODEPAGE | BASE_NAME | BASE_SCHEMA |\n" +
                "-------------------------------------------------------------------------------------------------\n" +
                "SYSTABLES | SYSIBM  |  V  |    9    |     0     |     0     |  1208   |   NULL    |    NULL     |";
        SpliceUnitTest.sqlExpectToString(methodWatcher,
                "select * from sysibm.SYSTABLES WHERE NAME='SYSTABLES' AND CREATOR='SYSIBM'",
                expected, false);
    }

    @Test // 1985
    public void testUpgradeScriptToAddSysNaturalNumbersTable() throws Exception {
        Assert.assertEquals(SYSNATURALNUMBERSRowFactory.MAX_NUMBER,
                methodWatcher.executeGetInt("select count(*) from SYS.SYSNATURALNUMBERS", 1) );
    }

    @Test // 1989
    public void testUpgradeScriptToAddIndexColUseViewInSYSCAT() throws Exception {
        SpliceUnitTest.sqlExpectToString(methodWatcher,
                "select TABLENAME  from sysvw.systablesview WHERE TABLENAME='INDEXCOLUSE'",
                        "TABLENAME  |\n" +
                        "-------------\n" +
                        "INDEXCOLUSE |",
                false);
    }

    @Test // 1992
    public void testUpgradeScriptForTablePriorities() throws Exception {
        try(Connection conn = ConnectionFactory.createConnection(new Configuration());
                Admin admin= conn.getAdmin()) {
            Assert.assertEquals(0, HBasePartitionAdmin.getToUpgradeStream(admin.listTableDescriptors()).count());
        }
    }

}
