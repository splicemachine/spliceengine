package com.splicemachine.derby.impl.load;
import com.google.common.collect.Lists;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
          List<String> expectedR1 = Arrays.asList("yuas","YifuMa","PeaceNLove");
          Collections.sort(expectedR1);
          List<String> actualR1 = Lists.newArrayListWithExpectedSize(expectedR1.size());
	        ResultSet rs1 = s1.executeQuery();
          while(rs1.next()){
              actualR1.add(rs1.getString(2));
          }
          Collections.sort(actualR1);
          Assert.assertEquals("Table 1 import incorrect!",expectedR1,actualR1);

          List<String> expectedR2 = Arrays.asList("mvnVSworld","derbyWins","RadioHeadS");
          Collections.sort(expectedR2);
          List<String> actualR2 = Lists.newArrayListWithExpectedSize(expectedR2.size());
          ResultSet rs2 = s2.executeQuery();
          while(rs2.next()){
              actualR2.add(rs2.getString(2));
          }
          Collections.sort(actualR2);
          Assert.assertEquals("Table 2 import incorrect!",expectedR2,actualR2);
	    }

}
