package com.splicemachine.mrio.api;

import java.sql.PreparedStatement;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;

public class SMInputFormatIT extends BaseMRIOTest {	
	private static String testTableName = "SPLICETABLEINPUTFORMATIT.A";
	
	private static final String CLASS_NAME = SMInputFormatIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    // Table for ADD_MONTHS testing.
    private static final SpliceTableWatcher tableWatcherA = new SpliceTableWatcher(
    	"A", schemaWatcher.schemaName, "(col1 varchar(100) primary key, col2 varchar(100))");
   
    
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcherA)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try{
                        PreparedStatement ps1;
                        PreparedStatement ps2;
                        
                        // Each of the following inserted rows represents an individual test,
                        // including expected result (column 'col3'), for less test code in the
                        // test methods

                        ps1 = classWatcher.prepareStatement(
							"insert into " + tableWatcherA + " (col1, col2) values ('bbb', 'value bbb')");
                        ps2 = classWatcher.prepareStatement(
    							"insert into " + tableWatcherA + " (col1, col2) values ('aaa','value aaa')");
                       
                        ps1.execute();
                        Thread.sleep(10);
                        ps2.execute();
    
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        classWatcher.closeAll();
                    }
                }
            });
   
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
	
}
