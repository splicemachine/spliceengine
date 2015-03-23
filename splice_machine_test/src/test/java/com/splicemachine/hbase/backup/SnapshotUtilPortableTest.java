package com.splicemachine.hbase.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Test;

public class SnapshotUtilPortableTest {

	Path rootPath = new Path("hdfs://localhost/hbase");
	Path originPath = new Path("hdfs://localhost/hbase/data/default/Table/Region/CF/File");
	Path archivePath = new Path("hdfs://localhost/hbase/archive/data/default/Table/Region/CF/File");
	Path tmpPath = new Path("hdfs://localhost/hbase/.tmp/data/default/Table/Region/CF/File");
	Path incorrectPath = new Path("hdfs://localhost/bbase1/data/default/Table/Region/CF/File");

	
	@Test
	public void testArgsSplit() throws IOException
	{
		String table = "Table";
		String region= "Region";
		String family = "CF";
		String hfile = "File";
		Configuration conf = new Configuration();
		FSUtils.setRootDir(conf, rootPath);
		conf.set(FileSystem.FS_DEFAULT_NAME_KEY, rootPath.toString());
		String[] args = SnapshotUtilsPortable.getArgsForHFileLink(conf, originPath);
		assertEquals(args[0], table);
		assertEquals(args[1], region);
		assertEquals(args[2], family);
		assertEquals(args[3], hfile);
		
		args = SnapshotUtilsPortable.getArgsForHFileLink(conf, archivePath);
		assertEquals(args[0], table);
		assertEquals(args[1], region);
		assertEquals(args[2], family);
		assertEquals(args[3], hfile);
		
		args = SnapshotUtilsPortable.getArgsForHFileLink(conf, tmpPath);
		assertEquals(args[0], table);
		assertEquals(args[1], region);
		assertEquals(args[2], family);
		assertEquals(args[3], hfile);
		
		try{
			args = SnapshotUtilsPortable.getArgsForHFileLink(conf, incorrectPath);
			assertTrue(false);
		} catch(IOException e){
			
		}
		
	}
	

}
