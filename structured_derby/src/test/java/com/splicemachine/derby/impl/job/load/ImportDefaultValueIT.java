package com.splicemachine.derby.impl.job.load;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.perf.runner.qualifiers.Result;
import com.sun.xml.bind.v2.schemagen.xmlschema.Import;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by yifu on 6/13/14.
 */
public class ImportDefaultValueIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = ImportDefaultValueIT.class.getSimpleName().toUpperCase();
    protected static String TABLE_1 = "A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_1,spliceSchemaWatcher.schemaName,"(COL1 INT, COL2 CHAR(3)DEFAULT'abc')");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void textImportDefaultValue() throws Exception{
        PrintWriter writer = new PrintWriter("/tmp/Test.txt","UTF-8");
        writer.println(",abc");
        writer.println("2,");
        writer.println("3,ab");
        writer.close();
        PreparedStatement ps = methodWatcher.prepareStatement("call SYSCS_UTIL.IMPORT_DATA('"+spliceSchemaWatcher.schemaName+"','"+TABLE_1+"',null,'/tmp/Test.txt',',',null,null,null,null,0,null)");
        ps.execute();
        PreparedStatement s = methodWatcher.prepareStatement("select * from "+spliceSchemaWatcher.schemaName+"."+TABLE_1);
        ResultSet rs = s.executeQuery();
        rs.next();
        Assert.assertEquals("abc", rs.getString(2));
        rs.next();
        Assert.assertEquals("abc", rs.getString(2));
        rs.next();
        Assert.assertEquals("ab", rs.getString(2));
    }

}
