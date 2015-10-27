package com.splicemachine.db.impl.load;

import com.splicemachine.derby.test.framework.*;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;

/**
 * Created by jleach on 10/26/15.
 */
public class ColumnInfoIT extends SpliceUnitTest{
    private static final Logger LOG = Logger.getLogger(ColumnInfoIT.class);
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(ColumnInfoIT.class.getSimpleName());
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("A",ColumnInfoIT.class.getSimpleName(),"(col1 varchar(256), col2 char(23), col3 numeric(12,2))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testGenerateInsertStatement() throws Exception{
        TestConnection testConnection = methodWatcher.createConnection();
        ColumnInfo columnInfo = new ColumnInfo(testConnection,spliceSchemaWatcher.toString(),"A",null,null,null);
    }


}


