package com.splicemachine.hbase.backup;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import java.io.File;
import java.net.URI;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;

/**
 * Created by jyuan on 3/20/15.
 */
public class IncrementalBackupTest {

    protected static File backupDir;

    @Before
    public void setup() throws Exception
    {
        String tmpDir = System.getProperty("java.io.tmpdir");
        backupDir = new File(tmpDir, "incrementalBackup");
        backupDir.mkdirs();
        System.out.println(backupDir.getAbsolutePath());


    }

    @Test
    public void testGetIncrementalChange() throws StandardException {

        String snapshotName = "s1";
        String lastSnapshotName = "s0";
        Configuration conf = SpliceConstants.config;
        try {
            String backupFileSystem = backupDir.getAbsolutePath();
            FileSystem fs = FileSystem.get(URI.create(backupFileSystem), conf);

            CreateIncrementalBackupTask incrementalBackupTask = new CreateIncrementalBackupTask();

            Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
            Path f0 = new Path(archiveDir.toString() + "/data/default/testtb/4567/cf/f0");
            fs.mkdirs(f0);
            f0 = new Path(archiveDir.toString() + "/testtb/4567/cf/f0");
            fs.mkdirs(f0);

            Path f1 = new Path(archiveDir.toString() + "/data/default/testtb/4567/cf/f1");
            fs.mkdirs(f1);
            f1 = new Path(archiveDir.toString() + "/testtb/4567/cf/f1");
            fs.mkdirs(f1);

            Path f2 = new Path(archiveDir.toString() + "/data/default/testtb/4567/cf/f2");
            fs.mkdirs(f2);
            f2 = new Path(archiveDir.toString() + "/testtb/4567/cf/f2");
            fs.mkdirs(f2);

            SnapshotUtils snapshotUtils = mock(SnapshotUtilsImpl.class);
            List<Object> files0 = new ArrayList<>();
            List<Object> files1 = new ArrayList<>();
            List<Object> files2 = new ArrayList<>();

            files1.add(new HFileLink(conf, new Path("table/region/cf/testtb=4567-f0")));
            files1.add(new HFileLink(conf, new Path("table/region/cf/testtb=4567-f1")));

            files2.add(new HFileLink(conf, new Path("table/region/cf/testtb=4567-f0")));
            files2.add(new HFileLink(conf, new Path("table/region/cf/testtb=4567-f2")));

            when(snapshotUtils.getSnapshotFilesForRegion(null, conf, fs, snapshotName)).thenReturn(files1);
            when(snapshotUtils.getSnapshotFilesForRegion(null, conf, fs, lastSnapshotName)).thenReturn(files0);

            Class klass = incrementalBackupTask.getClass();
            Field f = klass.getDeclaredField("snapshotUtils");
            f.setAccessible(true);
            f.set(incrementalBackupTask, snapshotUtils);

            f = klass.getDeclaredField("backupFileSystem");
            f.setAccessible(true);
            f.set(incrementalBackupTask, backupFileSystem);

            f = klass.getDeclaredField("snapshotName");
            f.setAccessible(true);
            f.set(incrementalBackupTask, snapshotName);

            f = klass.getDeclaredField("lastSnapshotName");
            f.setAccessible(true);
            f.set(incrementalBackupTask, lastSnapshotName);

            Method method = klass.getDeclaredMethod("getIncrementalChanges", null);
            method.setAccessible(true);
            List<Path> paths = (List<Path>) method.invoke(incrementalBackupTask);
            Assert.assertEquals(2, paths.size());
            Assert.assertTrue(paths.get(0).getName().compareTo("f0")== 0);
            Assert.assertTrue(paths.get(1).getName().compareTo("f1")== 0);

            // Another test
            snapshotName = "s2";
            lastSnapshotName = "s1";

            f = klass.getDeclaredField("snapshotName");
            f.setAccessible(true);
            f.set(incrementalBackupTask, snapshotName);

            f = klass.getDeclaredField("lastSnapshotName");
            f.setAccessible(true);
            f.set(incrementalBackupTask, lastSnapshotName);
            when(snapshotUtils.getSnapshotFilesForRegion(null, conf, fs, snapshotName)).thenReturn(files2);
            when(snapshotUtils.getSnapshotFilesForRegion(null, conf, fs, lastSnapshotName)).thenReturn(files1);
            paths = (List<Path>) method.invoke(incrementalBackupTask);
            Assert.assertEquals(1, paths.size());
            Assert.assertTrue(paths.get(0).getName().compareTo("f2")== 0);
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }
}
