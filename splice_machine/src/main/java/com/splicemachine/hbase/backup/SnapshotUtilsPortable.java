package com.splicemachine.hbase.backup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.FSUtils;

public class SnapshotUtilsPortable {


	public static String[] getArgsForHFileLink(Configuration conf, Path path) throws IOException {
		  Path rootDir = FSUtils.getRootDir(conf);
		  
		  String pathStr = path.toString();
		  String rootDirStr = rootDir.toString();
		  if(!pathStr.startsWith(rootDirStr)){
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

}
