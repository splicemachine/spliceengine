package com.splicemachine.backup;

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by jyuan on 4/21/15.
 */
@Category(com.splicemachine.test.BackupTest.class)
public class RestoreIT extends SpliceUnitTest {

    protected Connection connection;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @After
    public void tearDown() throws Exception
    {
        String tmpDir = System.getProperty("java.io.tmpdir");
        File backupDir = new File(tmpDir, "backup");
        backupDir.delete();
    }

    @Test
    public void testRestore() throws Exception{
        connection = methodWatcher.createConnection();
        restore();
    }

    private void restore() throws Exception
    {
        PreparedStatement ps = connection.prepareStatement(
                "select backup_id, filesystem from sys.sysbackup order by backup_id desc");
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        long backupId = rs.getLong(1);
        String directory = rs.getString(2);

        System.out.println("Start restore ...");
        ps = connection.prepareStatement(format("call SYSCS_UTIL.SYSCS_RESTORE_DATABASE('%s', %d)", directory, backupId));
        ps.execute();
        System.out.println("Restore completed.");
    }
}
