package com.splicemachine.mrio.api.core;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class SMOutputFormatIT extends BaseMRIOTest {
	private static String testTableName = "SPLICETABLEOUTPUTFORMATIT.A";
	private static final String CLASS_NAME = SMOutputFormatIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    
    // Table for ADD_MONTHS testing.
    private static final SpliceTableWatcher tableWatcherA = new SpliceTableWatcher(
    	"A", schemaWatcher.schemaName, "(col1 varchar(100), col2 varchar(100))");
   
    
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcherA);
           
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
		
	
}
