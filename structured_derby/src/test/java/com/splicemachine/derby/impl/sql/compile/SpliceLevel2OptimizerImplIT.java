package com.splicemachine.derby.impl.sql.compile;

import java.sql.PreparedStatement;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
@Ignore
public class SpliceLevel2OptimizerImplIT extends SpliceUnitTest {
    public static final String CLASS_NAME = SpliceLevel2OptimizerImplIT.class.getSimpleName();
    protected static SpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static final String CUSTOMER = "CUSTOMER";
    protected static final String CUSTOMER_IX1 = "CUSTOMER_IX1";
    protected static final String CUSTOMER_IX2 = "CUSTOMER_IX2";
    protected static final String CUSTOMER_IX3 = "CUSTOMER_IX3";
    protected static final String CUSTOMER_IX4 = "CUSTOMER_IX4";
    
    protected static SpliceTableWatcher CUSTOMER_TABLE = new SpliceTableWatcher(CUSTOMER,schemaWatcher.schemaName,
            "(col1 int NOT NULL, "+
            "col2 int, "+
            "col3 int, "+
            "col4 int, "+
            "col5 varchar(256),"
            + " primary key (col1)");
    protected static SpliceIndexWatcher CUSTOMER_IDX1 = new SpliceIndexWatcher(CUSTOMER,schemaWatcher.schemaName,CUSTOMER_IX1,schemaWatcher.schemaName,"(col1, col2)");
    protected static SpliceIndexWatcher CUSTOMER_IDX2 = new SpliceIndexWatcher(CUSTOMER,schemaWatcher.schemaName,CUSTOMER_IX1,schemaWatcher.schemaName,"(col2, col3)");
    protected static SpliceIndexWatcher CUSTOMER_IDX3 = new SpliceIndexWatcher(CUSTOMER,schemaWatcher.schemaName,CUSTOMER_IX1,schemaWatcher.schemaName,"(col2, col3, col4)");
    protected static SpliceIndexWatcher CUSTOMER_IDX4 = new SpliceIndexWatcher(CUSTOMER,schemaWatcher.schemaName,CUSTOMER_IX1,schemaWatcher.schemaName,"(col4)",true);
    
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(CUSTOMER_TABLE)
            .around(CUSTOMER_IDX1)
            .around(CUSTOMER_IDX2)
            .around(CUSTOMER_IDX3)
            .around(CUSTOMER_IDX4)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                    	spliceClassWatcher.setAutoCommit(false);
                    	PreparedStatement statement = spliceClassWatcher.prepareStatement("insert into customer (col1, col2, col3, col4, col5) values (?,?,?,?,?)");
                    	for (int i = 0; i<10000;i++) {
                    		statement.setInt(1, i);
                    		statement.setInt(2, i);
                    		statement.setInt(3, i);
                    		statement.setInt(4, i);
                    		statement.setString(5, "Winner winner chicken dinner, Tangled up in blue xxsfasdfgasdfaegsrafdsgsadfasfdasfaewsfewacascdsafafasdf");
                    		statement.execute();
                    	}
                    	spliceClassWatcher.commit();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

}
