package com.splicemachine.derby.impl.load;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.derby.tools.ij;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.google.common.io.Closeables;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

import java.sql.SQLWarning;

public class EmptyDateImportedToDefaultFieldIT  extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	
	public static final String CLASS_NAME = "TROC";
	protected static final String T_TABLES = "PRODUCT";

	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
	protected static SpliceTableWatcher test_Table = new SpliceTableWatcher(T_TABLES,CLASS_NAME,
					"( CUSTOMER_PRODUCT_ID INTEGER NOT NULL PRIMARY KEY, SHIPPED_DATE TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP, SOURCE_SYS_CREATE_DTS TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP NOT NULL,SOURCE_SYS_UPDATE_DTS TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP NOT NULL,"+
									"SDR_CREATE_DATE TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP, SDR_UPDATE_DATE TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP,DW_SRC_EXTRC_DTTM TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP)");

	
	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
					.around(spliceSchemaWatcher)
					.around(test_Table)
					.around(new SpliceDataWatcher(){
							@Override
							protected void starting(Description description) {
									try {
											PreparedStatement ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s',',','\"',null,null,null)",CLASS_NAME,T_TABLES,getResource("datebugGood.tbl")));
											ps.execute();
									} catch (Exception e) {
										//StackTraceElement st[] = e.getStackTrace();
										//for(int i=0;i<st.length;i++){
										//	System.out.println(format("%d : %s : %d",i,st[i].getClassName(),st[i].getLineNumber()));
										//}
										System.out.println(e.getMessage());
										throw new RuntimeException(e);
									}
									finally {
											spliceClassWatcher.closeAll();
									}
							}

					});

	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

	@Test
	public void NullDateFieldTest(){
		try{
			CountRecords();
			ClearTable();
			ImportNullFields();
			CountRecords();
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void CountRecords() {
		System.out.println("test test");
		try {
			ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s.%s",CLASS_NAME,T_TABLES));
			rs.next();
			Assert.assertEquals(100, rs.getLong(1));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Test
	public void validateDataLoad() throws Exception {
			ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s.%s",CLASS_NAME,T_TABLES));
			rs.next();
			long tt = rs.getLong(1);
			Assert.assertEquals(100, tt);
	}

	public void ImportNullFields() throws Exception {
		try {
			PreparedStatement ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s',',','\"',null,null,null)",CLASS_NAME,T_TABLES,getResource("datebug.tbl")));
			ps.execute();
		} catch (Exception e) {
			//StackTraceElement st[] = e.getStackTrace();
			//for(int i=0;i<st.length;i++){
			//	System.out.println(format("%d : %s : %d",i,st[i].getClassName(),st[i].getLineNumber()));
			//}
			//System.out.println(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	public void ClearTable() throws Exception {
		try {
			runScript(new File(getSQLFile("clean.sql")),methodWatcher.getOrCreateConnection());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static String getResource(String name) {
			return getResourceDirectory()+"DateImport/data/"+name;
	}	
	
	protected static String getSQLFile(String name) {
			return getResourceDirectory()+"DateImport/query/"+name;
	}


	protected static boolean runScript(File scriptFile, Connection connection) {
			FileInputStream fileStream = null;
			try {
					fileStream = new FileInputStream(scriptFile);
					int result  = ij.runScript(connection,fileStream,"UTF-8",System.out,"UTF-8");
					return (result==0);
			}
			catch (Exception e) {
					return false;
			}
			finally {
					Closeables.closeQuietly(fileStream);
			}
	}


}
