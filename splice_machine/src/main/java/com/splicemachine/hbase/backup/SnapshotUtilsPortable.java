package com.splicemachine.hbase.backup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;

public class SnapshotUtilsPortable {

	
	  public static HFileLink createHFileLink(Configuration conf, Path path) 
			  throws IOException
	  {
		  Path rootDir = FSUtils.getRootDir(conf);
		  Path rootArchive = HFileArchiveUtil.getArchivePath(conf);
		  String relPath = getRelativePath(rootDir, path);
		  return new HFileLink(rootDir, rootArchive, new Path(relPath)); 
	  }
	
	  private static String getRelativePath(Path rootDir, Path path) {
		  String rootPathStr = rootDir.toString();
		  String pathStr = path.toString();
		  
		  return pathStr.substring(rootPathStr.length());
	}

	public static String[] getArgsForHFileLink(Configuration conf, Path path)
			  throws IOException
	  {
		  Path rootDir = FSUtils.getRootDir(conf);
		  
		  String pathStr = path.toString();
		  String rootDirStr = rootDir.toString();
		  if( pathStr.startsWith(rootDirStr) == false){
			  throw new IOException("wrong hfile path: " + path);
		  }
		  int len = rootDirStr.length();
		  pathStr = pathStr.substring(len);
		  if(pathStr.startsWith(Path.SEPARATOR)) pathStr = pathStr.substring(1);
		  
		  String[] tokens = pathStr.split(Path.SEPARATOR);
		  
		  if(tokens[0].equals("data")) return fillArgsFrom(tokens, 2);
		
		  return fillArgsFrom(tokens, 3);  
		  
	  }
	  
	  private static String[] fillArgsFrom(String[] tokens, int i) {
		  String[] args = new String[4];
		  args[0] = tokens[i];
		  args[1] = tokens[i+1];
		  args[2] = tokens[i+2];
		  args[3] = tokens[i+3];
		  return args;
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
	        // replace namespace delimiter
	    	tableName.replace(':', '='),
	        regionName, hfileName);
	    return s;
	  }
	
}
