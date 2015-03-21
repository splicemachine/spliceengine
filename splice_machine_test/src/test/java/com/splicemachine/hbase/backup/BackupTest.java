package com.splicemachine.hbase.backup;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import com.splicemachine.hbase.backup.Backup.BackupScope;
import com.splicemachine.hbase.backup.Backup.BackupStatus;

@Ignore
public class BackupTest {
	@Test
	public void testCreateBackup() throws SQLException, IOException {
        String path = File.createTempFile("splice", "backup").getPath();
        Backup backupNoParent = Backup.createBackup(path, BackupScope.D, -1L);
		Assert.assertTrue("incremental flag incorrect",backupNoParent.isIncrementalBackup() == false);
		Assert.assertTrue("no timestamp set",backupNoParent.getBeginBackupTimestamp() != null);
		Assert.assertTrue("backupScope not set properly",backupNoParent.getBackupScope().ordinal() == BackupScope.D.ordinal());		
		Assert.assertTrue("backupStatus not set properly",backupNoParent.getBackupStatus().ordinal() == BackupStatus.I.ordinal());		
	}

	@Test(expected=SQLException.class)
	public void testCreateBackupWithLargerParentTimestamp() throws SQLException, IOException {
        String path = File.createTempFile("splice", "backup").getPath();
		Backup.createBackup(path, BackupScope.D, Long.MAX_VALUE);
	}
	
	@Test
	public void testMarkBackupFailed() throws SQLException, IOException {
        String path = File.createTempFile("splice", "backup").getPath();
		Backup backupNoParent = Backup.createBackup(path, BackupScope.D, -1L);
		backupNoParent.markBackupFailed();
		Assert.assertTrue("backupFailedNotMarked",backupNoParent.getBackupStatus().F.ordinal() == BackupStatus.F.ordinal());
	}

	@Test
	public void testMarkBackupSuccesful() throws SQLException, IOException {
        String path = File.createTempFile("splice", "backup").getPath();
		Backup backupNoParent = Backup.createBackup(path, BackupScope.D, -1L);
		backupNoParent.markBackupSuccessful();
		Assert.assertTrue("backupSuccessfulNotMarked",backupNoParent.getBackupStatus().S.ordinal() == BackupStatus.S.ordinal());		
	}
}
