package com.splicemachine.derby.impl.load;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.perf.runner.qualifiers.Result;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
public class ImportWithDifferentColumnOrderIT {
	 protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	    public static final String CLASS_NAME = ImportWithDifferentColumnOrderIT.class.getSimpleName().toUpperCase();
	    protected static String TABLE_1 = "A";
	    protected static String TABLE_2 = "B";
	    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
	    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_1,spliceSchemaWatcher.schemaName,"(COL1 INT, COL2 VARCHAR(10))");
	    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_2,spliceSchemaWatcher.schemaName,"(COL1 INT, COL2 VARCHAR(10), COL3 INT, COL4 VARCHAR(2))");

	    @ClassRule
	    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
	            .around(spliceSchemaWatcher)
	            .around(spliceTableWatcher1)
	            .around(spliceTableWatcher2);
	    @Rule
	    public SpliceWatcher methodWatcher = new SpliceWatcher();

	    @Test
	    public void textImportDefaultValue() throws Exception{
	        PrintWriter writer1 = new PrintWriter("/tmp/Test1.txt","UTF-8");
	        writer1.println("yuas,123");
	        writer1.println("YifuMa,52");
	        writer1.println("PeaceNLove,214");
	        writer1.close();
	        PrintWriter writer2 = new PrintWriter("/tmp/Test2.txt","UTF-8");
	        writer2.println();
	        writer2.println("mvnVSworld, 134,11,sd");
	        writer2.println("derbyWins, 97,6,os");
	        writer2.println("RadioHeadS,00192,43,ux");
	        writer2.close();
	        PreparedStatement ps1 = methodWatcher.prepareStatement("call SYSCS_UTIL.IMPORT_DATA('"+spliceSchemaWatcher.schemaName+"','"+TABLE_1+"','COL2,COL1','/tmp/Test1.txt',',',null,null,null,null,0,null)");
	        ps1.execute();
	        PreparedStatement ps2 = methodWatcher.prepareStatement("call SYSCS_UTIL.IMPORT_DATA('"+spliceSchemaWatcher.schemaName+"','"+TABLE_2+"','COL2,COL1,COL3,COL4','/tmp/Test2.txt',',',null,null,null,null,0,null)");
	        ps2.execute();
	        PreparedStatement s1 = methodWatcher.prepareStatement("select * from "+spliceSchemaWatcher.schemaName+"."+TABLE_1);
	        PreparedStatement s2 = methodWatcher.prepareStatement("select * from "+spliceSchemaWatcher.schemaName+"."+TABLE_2);
	        ResultSet rs1 = s1.executeQuery();
	        ResultSet rs2 = s2.executeQuery();
	        rs1.next();
	        Assert.assertEquals("yuas", rs1.getString(2));
	        rs1.next();
	        Assert.assertEquals("YifuMa", rs1.getString(2));
	        rs1.next();
	        Assert.assertEquals("PeaceNLove", rs1.getString(2));
	        rs2.next();
	        Assert.assertEquals("mvnVSworld", rs2.getString(2));
	        rs2.next();
	        Assert.assertEquals("derbyWins", rs2.getString(2));
	        rs2.next();
	        Assert.assertEquals("RadioHeadS", rs2.getString(2));
	    }

}
