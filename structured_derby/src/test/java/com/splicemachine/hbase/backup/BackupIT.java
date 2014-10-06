package com.splicemachine.hbase.backup;

import com.splicemachine.hbase.backup.Backup.BackupScope;
import com.splicemachine.hbase.backup.Backup.BackupStatus;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

@Ignore
public class BackupIT {
	@Test
	public void testCreateBackup() throws SQLException, IOException {
        String path = File.createTempFile("splice", "backup").getPath();
        Backup backupNoParent = Backup.createBackup(path, BackupScope.D, -1L);
		Assert.assertTrue("incremental flag incorrect",backupNoParent.isIncrementalBackup() == false);
		Assert.assertTrue("no timestamp set",backupNoParent.getBeginBackupTimestamp() != null);
		Assert.assertTrue("backupScope not set properly",backupNoParent.getBackupScope().ordinal() == BackupScope.D.ordinal());		
		Assert.assertTrue("backupStatus not set properly",backupNoParent.getBackupStatus().ordinal() == BackupStatus.I.ordinal());		
	}

}
