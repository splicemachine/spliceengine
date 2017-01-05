/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.concurrency;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test concurrent broadcast join reusing right hand side rows
 */
public class ConcurrentBroadcastJoinIT {

    private static final String SCHEMA = ConcurrentBroadcastJoinIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);


    @BeforeClass
    public static void createdSharedTables() throws Exception {
        classWatcher.executeUpdate("create table fail (i int, j varchar(40))");
        classWatcher.executeUpdate("insert into fail values (1, 'Mutual Fund Taxable')");
        classWatcher.executeUpdate("insert into fail values (2, 'Treasury Bill')");
        classWatcher.executeUpdate("insert into fail values (3, 'Government Bond')");
        classWatcher.executeUpdate("insert into fail values (4, 'Generic Cash or Equivalents')");
        classWatcher.executeUpdate("insert into fail values (5, 'Convertible Corporate')");
        classWatcher.executeUpdate("insert into fail values (6, 'Federal Home Loan Mortgage Corp')");
        classWatcher.executeUpdate("insert into fail select * from fail");

        classWatcher.executeUpdate("create table mul (i int, j int, k int)");
        classWatcher.executeUpdate("insert into mul values (1,1,1)");
        classWatcher.executeUpdate("insert into mul values (1,1,2)");
        classWatcher.executeUpdate("insert into mul values (1,2,1)");
        classWatcher.executeUpdate("insert into mul values (2,1,1)");
        classWatcher.executeUpdate("insert into mul values (2,1,2)");
        classWatcher.executeUpdate("insert into mul values (3,2,1)");
        classWatcher.executeUpdate("insert into mul values (3,1,1)");
        classWatcher.executeUpdate("insert into mul values (3,1,2)");
        classWatcher.executeUpdate("insert into mul values (4,2,1)");
        classWatcher.executeUpdate("insert into mul values (4,1,1)");
        classWatcher.executeUpdate("insert into mul values (5,1,2)");
        classWatcher.executeUpdate("insert into mul values (5,2,1)");
        classWatcher.executeUpdate("insert into mul values (5,1,1)");
        classWatcher.executeUpdate("insert into mul values (6,1,2)");
        classWatcher.executeUpdate("insert into mul select * from mul");
        classWatcher.executeUpdate("insert into mul select * from mul");
        classWatcher.executeUpdate("insert into mul select * from mul");
    }

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    /* Regression for DB-5701 */
    @Test
    public void concurrentBroadcast() throws Exception {

        String fullFileName = SpliceUnitTest.getResourceDirectory() + "concurrent/script.sql";
        String query = IOUtils.toString(new FileInputStream(new File(fullFileName)));

        for (int i = 0; i < 20; ++i) {
            ResultSet rs = methodWatcher.executeQuery(query);
            String expectedResult = "" +
                    "A | 2  |               C                |\n" +
                    "------------------------------------------\n" +
                    " 1 |256 |     Convertible Corporate      |\n" +
                    " 1 |128 |Federal Home Loan Mortgage Corp |\n" +
                    " 1 |128 |  Generic Cash or Equivalents   |\n" +
                    " 1 |256 |        Government Bond         |\n" +
                    " 1 |256 |      Mutual Fund Taxable       |\n" +
                    " 1 |256 |         Treasury Bill          |\n" +
                    " 2 |128 |     Convertible Corporate      |\n" +
                    " 2 |128 |  Generic Cash or Equivalents   |\n" +
                    " 2 |128 |        Government Bond         |\n" +
                    " 2 |128 |      Mutual Fund Taxable       |";

            assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }


}
