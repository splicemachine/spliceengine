/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("-sf- needs to be re-implemented")
public class SnapshotUtilsTest {
	
	@Test
	public void getRefferedFileTest()
	{
		Path refFilePath = 
				new Path("/TABLE_A/a60772afe8c4aa3355360d3a6de0b292/fam_a/9fb67500d79a43e79b01da8d5d3017a4.88a177637e155be4d01f21441bf8595d");
		Path storeFilePath = 
				new Path("/TABLE_A/88a177637e155be4d01f21441bf8595d/fam_a/9fb67500d79a43e79b01da8d5d3017a4");

		Assert.fail("IMPLEMENT");
//		SnapshotUtilsImpl sui = (SnapshotUtilsImpl)SnapshotUtilsFactory.snapshotUtils;
//		assertEquals(storeFilePath, sui.getReferredFile(refFilePath));
	}
	
	@Test 
	public void getColumnFamilyTest() throws IOException
	{
		
	
		Configuration conf = new Configuration();
		Path rootDir= new Path("hdfs://localhost/hbase");
		FSUtils.setRootDir(conf, rootDir);
		conf.set(FileSystem.FS_DEFAULT_NAME_KEY, rootDir.toString());
		
		String table = "TABLE_A";
		String region = "a60772afe8c4aa3355360d3a6de0b292";
		String family = "fam_a";
		String hfile = "9fb67500d79a43e79b01da8d5d3017a4";
		Path linkPath = createPath(table, region, family, hfile);
		Assert.fail("IMPLEMENT");
//		HFileLink link = SnapshotUtilsImpl.newLink(conf, linkPath);
//		SnapshotUtilsImpl sui = (SnapshotUtilsImpl)SnapshotUtilsFactory.snapshotUtils;
//		assertTrue( new String(sui.getColumnFamily(link)).equals(family));
	}
	
	
	  /**
	   * Create an HFileLink relative path for the table/region/family/hfile location
	   * @param table Table name
	   * @param region Region Name
	   * @param family Family Name
	   * @param hfile HFile Name
	   * @return the relative Path to open the specified table/region/family/hfile link
	   */
	  public static Path createPath(final String table, final String region,
	      final String family, final String hfile) {
	    if (HFileLink.isHFileLink(hfile)) {
	      return new Path(family, hfile);
	    }
	    return new Path(family, createHFileLinkName(table, region, hfile));
	  }	
	
	/**
	   * Create a new HFileLink name
	   *
	   * @param tableName - Linked HFile table name
	   * @param regionName - Linked HFile region name
	   * @param hfileName - Linked HFile name
	   * @return file name of the HFile Link
	   */
	  public static String createHFileLinkName(final String tableName,
	      final String regionName, final String hfileName) {
	    String s = String.format("%s=%s-%s",
	        tableName.replace(':', '='),
	        regionName, hfileName);
	    return s;
	  }
	
	//TODO - this is compatible only with CDH5.x
//	@Test
//	public void getFilePathTest() throws IOException
//	{
//        Pair<String, String> p = new Pair<String, String> (HConstants.HBASE_DIR, 
//        		MockSnapshot.MOCK_HBASE_ROOT_DIR);
//        List<Pair<String, String>> list = new ArrayList<Pair<String, String>>();
//        list.add(p);
//        Configuration conf = MockSnapshot.mockHBaseConfiguration(list);
//        SpliceConstants.config = conf;	
//        
//        SnapshotFileInfoOrBuilder snapFileInfo = MockSnapshot.getMockSnapshotFileInfo(MockSnapshot.MOCK_HFILE1);
//		SnapshotUtilsImpl sui = (SnapshotUtilsImpl)SnapshotUtilsFactory.snapshotUtils;
//		Path path = new Path(MockSnapshot.MOCK_PATH1);
//		MockSnapshot.createFile(path);		
//		HFileLink ppath =sui.getFilePath(snapFileInfo);	
//		Path[] paths = ppath.getLocations();
//		boolean exists = false;
//		for(Path pth: paths){
//			exists = pth.toString().equals(MockSnapshot.MOCK_PATH1); 
//			if(exists) break;
//		}
//		assertTrue(exists);
//		MockSnapshot.deleteFile(path);		
//
//	}

}
