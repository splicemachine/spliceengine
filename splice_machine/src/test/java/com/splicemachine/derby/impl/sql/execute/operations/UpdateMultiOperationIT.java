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

import com.carrotsearch.hppc.LongArrayList;
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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

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
                .withRows(Rows.rows(Rows.row(292,new BigDecimal("300000.00")),
                        Rows.row(293,new BigDecimal("300000.00"))))
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

        try(ResultSet rs = conn.query("select * from warehouse where w_id = 292")){
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

    @Test
    public void testVacuumResolvesPerformanceProblemsWithAutoCommit() throws Exception{
        int vacuumInterval = 127;
        List<LongArrayList> perfTimes = new ArrayList<>();

        try(TestConnection conn = spliceClassWatcher.getOrCreateConnection()){
            /*
             * This uses periodic VACUUM_TABLE commands to ensure that our performance (i.e. the
             * cost to perform an update) remains constant throughout the length of the test. Essentially,
             * this is ensuring that VACUUM properly cleans out extraneous MVCC data.
             */
            conn.setAutoCommit(false);

            try(PreparedStatement ps = conn.prepareStatement("update warehouse set w_ytd = w_ytd+? where w_id = ?");
                CallableStatement vacuum = conn.prepareCall("call SYSCS_UTIL.VACUUM_TABLE(?,?)")){
                vacuum.setString(1,SCHEMA);
                vacuum.setString(2,"warehouse");
                ps.setInt(2,293);

                File f = new File(SpliceUnitTest.getResourceDirectory()+"updateValues.raw");
                int c = 0;
                try(BufferedReader br = new BufferedReader(new FileReader(f))){
                    String line;
                    LongArrayList runPerfTimes = new LongArrayList(vacuumInterval);
                    while((line = br.readLine())!=null){
                        c++;
                        BigDecimal bd = new BigDecimal(line.trim());
                        long start = System.nanoTime();
                        ps.setBigDecimal(1,bd);
                        ps.execute(); //perform the update
                        long end = System.nanoTime();
                        runPerfTimes.add(end-start);

                        if((c & vacuumInterval)==0){
                            conn.commit(); //commit a batch, then vacuum so that we (in principle) clear out the old data
                            vacuum.execute();
                            perfTimes.add(runPerfTimes);
                            runPerfTimes = new LongArrayList(vacuumInterval);
                        }
                    }
                }
            }

            try(ResultSet rs = conn.query("select * from warehouse where w_id = 293")){
                Assert.assertTrue("No row returned!",rs.next());

                int wId = rs.getInt(1);
                Assert.assertFalse("Returned null!",rs.wasNull());
                Assert.assertEquals("Incorrect wId!",293,wId);

                BigDecimal value = rs.getBigDecimal(2);
                Assert.assertFalse("Returned null!",rs.wasNull());
                /*
                 * Note: this "correct" value is taken from Derby, which may not always be correct
                 * in reality(see DB-3675 for more information)
                 */
                Assert.assertEquals("Incorrect return value!",new BigDecimal("5428906.39"),value);

                Assert.assertFalse("Too many rows returned!",rs.next());
            }

            conn.commit();
        }
        assertStatisticallyConstant(vacuumInterval,perfTimes);
    }

    private void assertStatisticallyConstant(int vacuumInterval,List<LongArrayList> perfTimes){
        /*
         * The pattern is usually something like a sawtooth, with performance gradually increasing until
         * a vacuum occurs, and then performance scales down. What we really want to know is that the graph
         * is self-similar across different vacuum stages. I.e. run1[0] should be roughly the same as run2[0],run3[0],
         * and so on. If run2[0] is significantly higher than run1[0], then we have a problem.
         *
         * Of course, the JVM may choose to optimize some weird piece of this, which may result in subsequent runs
         * being significantly FASTER than previous ones, so we are only looking for time increasing. Also,
         * such an increase may be an outlier (i.e. during garbage collection), so we want to be careful.
         *
         * The way we do this check is to create a best-fit line across runs for each iteration. This line
         * should be essentially a horizontal line along the average of the runs. Then, we simply count high
         * outliers (values which are outside of 1 standard deviation). If there are more than a threshold, then
         * the test fails for that iteration. If it fails for enough iterations, then the test fails as well.
         */

        long[] measuredResults = new long[perfTimes.size()];
        int runFailThreshold = 16;
        int failThreshold = 4;
        int runFailCount=0;
        for(int i=0;i<vacuumInterval;i++){
            int j=0;
            double avg = 0L;
            double var =0L;
            for(LongArrayList run:perfTimes){
                long l=run.get(i);
                measuredResults[j] =l;
                double oldAvg = avg;
                avg = avg+(l-avg)/(j+1);
                var = var+(l-oldAvg)*(l-avg);
                j++;
            }

            double dev = Math.sqrt(var/(perfTimes.size()-1));

            int stdFailCount =0;

            for(long result:measuredResults){
                if((result-avg)>dev){
                    stdFailCount++;
                    if(stdFailCount>failThreshold) {
                        runFailCount++;
                        Assert.assertTrue("Failed too many std. dev tests",runFailCount<runFailThreshold);
                    }
                }
            }
        }
    }
}
