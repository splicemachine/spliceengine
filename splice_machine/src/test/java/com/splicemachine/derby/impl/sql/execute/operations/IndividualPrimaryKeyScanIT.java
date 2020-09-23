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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.SlowTest;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Tests designed to exercise some particular aspect of Primary Keys over a large(ish) data set--in this case,
 * 5000 records with a single primary key.
 *
 *
 * @author Scott Fines
 * Created on: 7/29/13
 */
// Skip this if you want because it takes forever and doesn't usually help much, but is nifty in some cases
@Category(SlowTest.class)
public class IndividualPrimaryKeyScanIT {
    private static Logger LOG = Logger.getLogger(PrimaryKeyScanIT.class);
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = IndividualPrimaryKeyScanIT.class.getSimpleName().toUpperCase();
    public static final String TABLE_NAME = "item";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,
            spliceSchemaWatcher.schemaName,
            "(i_id int, i_name varchar(24),i_price decimal(5,2),i_data varchar(50),i_im_id int not null, PRIMARY KEY (i_id))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description) {
                    try {
                        spliceTableWatcher.importData(SpliceUnitTest.getResourceDirectory() + "item5k.csv");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCanSeeEveryPrimaryKey() throws Exception {
        /*
         * We know that the file contains every primary key from 1 to 5000, so check them all.
         * Note: This test takes a long time to run (50 seconds on my machine), so skip it if you need
         * tests to run faster and you aren't worried about this.
         */
        PreparedStatement ps = methodWatcher.prepareStatement("select * from "+ spliceTableWatcher+" where i_id = ?");
        for(int i=1;i<=5000;i++){
            ps.setInt(1,i);
            ResultSet resultSet = ps.executeQuery();
            try{
                int count = 0;
                Assert.assertTrue(resultSet.next());
                do{
                    count++;
                }while(resultSet.next());

                Assert.assertEquals("More than one row returned!",1,count);
            }finally{
                resultSet.close();
            }
        }
    }

    @Test
    public void testCanSeeEveryPrimaryKeyWhenUsingCompoundQualifiers() throws Exception {
        /*
         * We know that the file contains every primary key from 1 to 5000, so check them all.
         * Note: This test takes a long time to run (50 seconds on my machine), so skip it if you need
         * tests to run faster and you aren't worried about this.
         */
        PreparedStatement ps = methodWatcher.prepareStatement("select * from "+ spliceTableWatcher+" where i_id < ? and i_id > ?");
        for(int i=1;i<=5000;i++){
            ps.setInt(1,i+1);
            ps.setInt(2,i-1);
            ResultSet resultSet = ps.executeQuery();
            try{
                int count = 0;
                Assert.assertTrue(resultSet.next());
                do{
                    count++;
                }while(resultSet.next());

                Assert.assertEquals("More than one row returned!",1,count);
            }finally{
                resultSet.close();
            }
        }
    }

    @Test
    public void testGroupedByCountWorksAsExpected() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select i_id, count(*) from "+ spliceTableWatcher+" group by i_id");
        int count=0;
        while(rs.next()){
            int groupedCount = rs.getInt(2);
            Assert.assertEquals("count is incorrect for key "+ rs.getInt(1),1,groupedCount);
            count++;
        }
        Assert.assertEquals("Not all rows returned!",5000,count);
    }

    @Test
    public void testGroupedByCountDescendingOrderWorksAsExpected() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select i_id, count(*) from "+ spliceTableWatcher+" group by i_id order by i_id desc");
        int count=0;
        int lastId = 5001;
        while(rs.next()){
            int id = rs.getInt(1);
            Assert.assertEquals("incorrect ordering", lastId - 1, id);
            int groupedCount = rs.getInt(2);
            Assert.assertEquals("count is incorrect for key "+ rs.getInt(1),1,groupedCount);

            count++;
            lastId = id;
        }
        Assert.assertEquals("Not all rows returned!",5000,count);
    }

}
