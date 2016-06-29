package com.splicemachine.backup;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.*;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import org.junit.runner.Description;

@Category(value = {SerialTest.class, SlowTest.class})
public class BackupIT extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static String TABLE_NAME1 = "A";
    protected static String TABLE_NAME2 = "B";
    private static final String SCHEMA_NAME = BackupIT.class.getSimpleName().toUpperCase();
    protected static File backupDir;

    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    protected static SpliceTableWatcher spliceTableWatcher1 =
            new SpliceTableWatcher(TABLE_NAME1, SCHEMA_NAME, "(I INT, D DOUBLE)");

    protected static SpliceTableWatcher spliceTableWatcher2 =
            new SpliceTableWatcher(TABLE_NAME2, SCHEMA_NAME, "(I INT, D DOUBLE)");

    protected Connection connection;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(format("insert into %s.%s (i, d) values (?, ?)", SCHEMA_NAME, TABLE_NAME1));
                        for (int j = 0; j < 100; ++j) {
                            for (int i = 0; i < 10; i++) {
                                ps.setInt(1, i);
                                ps.setDouble(2, i * 1.0);
                                ps.execute();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }).around(spliceTableWatcher2);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Before
    public void setup() throws Exception
    {
        connection = methodWatcher.createConnection();
        String tmpDir = System.getProperty("java.io.tmpdir");
        backupDir = new File(tmpDir, "backup");
        backupDir.mkdirs();
        System.out.println(backupDir.getAbsolutePath());
    }

    @Test
    public void negativeTests() throws Exception {

        PreparedStatement ps = connection.prepareStatement("call SYSCS_UTIL.SYSCS_CANCEL_DAILY_BACKUP(333)");
        ResultSet rs = ps.executeQuery();
        Assert.assertNotNull(rs.next());
        Assert.assertTrue(rs.getString(1).compareTo("Invalid job ID.")==0);

        ps = connection.prepareStatement(
                format("call SYSCS_UTIL.SYSCS_SCHEDULE_DAILY_BACKUP('%s', 'full', 200)", backupDir.getAbsolutePath()));
        rs = ps.executeQuery();
        Assert.assertNotNull(rs.next());
        Assert.assertTrue(rs.getString(1).compareTo("Hour must be in range [0, 23].")==0);

        ps = connection.prepareStatement(
                format("call SYSCS_UTIL.SYSCS_SCHEDULE_DAILY_BACKUP('%s', 'full', -10)", backupDir.getAbsolutePath()));
        rs = ps.executeQuery();
        Assert.assertNotNull(rs.next());
        Assert.assertTrue(rs.getString(1).compareTo("Hour must be in range [0, 23].") == 0);

    }

    @Test
    public void testFullBackup() throws Exception{
        backup("full");
        verifyFullBackup();
    }

    private void backup(String type) throws Exception {
        System.out.println("Start " + type + " backup ...");
        PreparedStatement ps = connection.prepareStatement(
                format("call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('%s', '%s')", backupDir.getAbsolutePath(), type));
        ps.execute();
        System.out.println("Backup completed.");

    }

    private void verifyFullBackup() throws Exception {
        String query = "select count(*) from sys.sysbackup where scope='D' and status='S' and incremental_backup=false";
        ResultSet rs = methodWatcher.executeQuery(query);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getInt(1) >= 1);
    }

    private void insertData(String tableName) throws Exception {
        PreparedStatement ps;
        try {
            ps = connection.prepareStatement(format("insert into %s.%s (i, d) values (?, ?)", SCHEMA_NAME, tableName));
            for (int j = 0; j < 100; ++j) {
                for (int i = 0; i < 10; i++) {
                    ps.setInt(1, i);
                    ps.setDouble(2, i * 1.0);
                    ps.execute();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



    private int count() throws Exception{
        PreparedStatement ps = connection.prepareStatement(
                format("select * from %s.%s", spliceSchemaWatcher.schemaName,TABLE_NAME1));
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while(rs.next()) {
            count++;
        }
        return count;
    }

    private long getBackupId() throws Exception {
        PreparedStatement ps = connection.prepareStatement(
                "select max(backup_id) from sys.sysbackup");
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        long backupId = rs.getLong(1);
        return backupId;
    }

    private int getBackupItems(long backupId) throws Exception{
        PreparedStatement ps = connection.prepareStatement(
                "select count(*) from sys.sysbackupitems where backup_id=?");
        ps.setLong(1, backupId);
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        int count = rs.getInt(1);
        return count;
    }

    private long getConglomerateNumber(String schemaName, String tableName) throws Exception {

        PreparedStatement ps = connection.prepareStatement(
                "select conglomeratenumber from sys.systables t, sys.sysconglomerates c, sys.sysschemas s where c.tableid=t.tableid and t.tablename=? and s.schemaname=? and s.schemaid=c.schemaid");
        ps.setString(1, tableName);
        ps.setString(2, schemaName);
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        long congId = rs.getLong(1);
        return congId;

    }

    private void verifyIncrementalBackup(long backupId, long backupItem, boolean expectedValue) throws Exception {

        PreparedStatement ps = connection.prepareStatement(
                "select item from sys.sysbackupitems where backup_id=? and item=?");
        ps.setLong(1, backupId);
        ps.setString(2, (new Long(backupItem)).toString());
        ResultSet rs = ps.executeQuery();
        boolean hasNext = rs.next();
        String output = null;
        if (hasNext) {
            output ="item = " + rs.getString(1) + "\n";
            rs.close();
            ps = connection.prepareStatement(
                    "select backup_item, region_name, file_name, include from sys.sysbackupfileset where backup_item=?");
            ps.setString(1, (new Long(backupItem)).toString());
            rs = ps.executeQuery();
            while(rs.next()) {
                output += rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3) + " | " + rs.getString(4) + "\n";
            }
        }
        Assert.assertTrue(output, hasNext == expectedValue);
    }

    private void restore(long backupId) throws Exception
    {
        System.out.println("Start restore ...");
        PreparedStatement ps = connection.prepareStatement(format("call SYSCS_UTIL.SYSCS_RESTORE_DATABASE('%s', %d)", backupDir.getAbsolutePath(), backupId));
        ps.execute();
        System.out.println("Restore completed.");
    }

    private void delete_backup(long backupId) throws Exception{
        PreparedStatement ps = connection.prepareStatement(format("call SYSCS_UTIL.SYSCS_DELETE_BACKUP(%d)", backupId));
        ps.execute();
    }
}
