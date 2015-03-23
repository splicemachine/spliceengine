package com.splicemachine.hbase.backup;

import java.io.File;
import java.sql.PreparedStatement;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class BackupIT extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = BackupIT.class.getSimpleName().toUpperCase();
	protected static String TABLE = "BACKUP_TABLE";
	protected static File backupDir; 

	
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher1 = 
			new SpliceTableWatcher(TABLE,spliceSchemaWatcher.schemaName,"(name varchar(40), title varchar(40), age int)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            ;

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Before
    public void setup() throws Exception
    {
    	String tmpDir = System.getProperty("java.io.tmpdir");
    	backupDir = new File(tmpDir, "backup");
    	backupDir.mkdirs();
    	System.out.println(backupDir.getAbsolutePath());
    }
    
    @After    
    public void tearDown() throws Exception
    {
    	backupDir.deleteOnExit();
    }
        
	@Test
	public void testBackup() throws Exception{
		loadData(spliceSchemaWatcher.schemaName,TABLE,getResourceDirectory()+"importTest.in","NAME,TITLE,AGE");
		backup();
		restore();
	}

    private void loadData(String schemaName, String tableName,String location,String colList) throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s','%s',null, '%s',',',null,null,null,null)",schemaName,tableName,colList,location));
        ps.execute();
    }
    
    private void backup() throws Exception
    {
    	System.out.println("Start backup ...");
    	PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('%s')", backupDir.getAbsolutePath()));
        ps.execute();
    	System.out.println("Backup completed.");

    }
    
    private void restore() throws Exception
    {
    	System.out.println("Start restore ...");
    	PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_RESTORE_DATABASE('%s')", backupDir.getAbsolutePath()));
        ps.execute();
    	System.out.println("Restore completed.");
    }
    
    
}
