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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SlowTest;
import com.splicemachine.test_tools.Rows;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Scott Fines
 *         Date: 8/18/15
 */
@Category(SlowTest.class)
public class UpdateMultiOperationIT{

    private static final String SCHEMA = UpdateMultiOperationIT.class.getSimpleName().toUpperCase();

    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @Rule
    public final SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createTables() throws Exception{
        Connection conn = spliceClassWatcher.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table WAREHOUSE (w_id int NOT NULL,w_ytd DECIMAL(12,2))")
                .withInsert("insert into WAREHOUSE (w_id,w_ytd) values (?,?)")
                .withRows(Rows.rows(Rows.row(292,new BigDecimal("300000.00"))))
                .create();
    }

    @Test
    public void testCanRepeatedlyUpdateTheSameRowWithoutError() throws Exception{

        TestConnection conn = spliceClassWatcher.getOrCreateConnection();
        /*
         * DB-3676 means that we need to ensure that autocommit is on, because otherwise
         *  this test will take 900 years to finish, and that would suck
         */
        conn.setAutoCommit(true);
        try(PreparedStatement ps = conn.prepareStatement("update warehouse set w_ytd = w_ytd+? where w_id = ?")){
            ps.setInt(2,292);
            File f = new File(SpliceUnitTest.getResourceDirectory()+"updateValues.raw");
            try(BufferedReader br = new BufferedReader(new FileReader(f))){
                String line;
                while((line = br.readLine())!=null){
                    BigDecimal bd = new BigDecimal(line.trim());
                    ps.setBigDecimal(1,bd);
                    ps.execute(); //perform the update
                }
            }
        }

        try(ResultSet rs = conn.query("select * from warehouse")){
            long rowCount = 0l;
            while(rs.next()){
                int wId = rs.getInt(1);
                Assert.assertFalse("Returned null!",rs.wasNull());
                Assert.assertEquals("Incorrect wId!",292,wId);

                BigDecimal value = rs.getBigDecimal(2);
                Assert.assertFalse("Returned null!",rs.wasNull());
                /*
                 * Note: this "correct" value is taken from Derby, which may not always be correct
                 * in reality(see DB-3675 for more information)
                 */
                Assert.assertEquals("Incorrect return value!",new BigDecimal("5428906.39"),value);
                rowCount++;
            }
            Assert.assertEquals("Incorrect row count!",1l,rowCount);
        }
    }
}
