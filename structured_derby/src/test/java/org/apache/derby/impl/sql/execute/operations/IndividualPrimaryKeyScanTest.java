package org.apache.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
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
public class IndividualPrimaryKeyScanTest {
    private static Logger LOG = Logger.getLogger(PrimaryKeyScanTest.class);
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = IndividualPrimaryKeyScanTest.class.getSimpleName().toUpperCase();
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
         * Note: This test takes a long time to run (50 seconds on my machine), so @Ignore it out if you need
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
         * Note: This test takes a long time to run (50 seconds on my machine), so @Ignore it out if you need
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
}
