package com.splicemachine.derby.impl.job.load;

import com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by yifu on 6/13/14.
 */
public class ImportDefaultValueIT extends SpliceUnitTest{
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
        PreparedStatement ps = methodWatcher.prepareStatement("call SYSCS_UTIL.IMPORT_DATA('"+spliceSchemaWatcher.schemaName+"','"+TABLE_1+"','col1','"+getResourceDirectory()+"ImportDefaultValue.txt',',',null,null,null,null,0,null,true,null)");
        ps.execute();
        PreparedStatement s = methodWatcher.prepareStatement("select * from "+spliceSchemaWatcher.schemaName+"."+TABLE_1);
        List<String> expected  = Arrays.asList("abc","abc","abc");
        Collections.sort(expected);
        List<String> actual = Lists.newArrayListWithExpectedSize(expected.size());
        ResultSet rs = s.executeQuery();
        while(rs.next()){
            actual.add(rs.getString(2));
        }
        Collections.sort(actual);
        Assert.assertEquals("Incorrect results!",expected,actual);
    }

}
