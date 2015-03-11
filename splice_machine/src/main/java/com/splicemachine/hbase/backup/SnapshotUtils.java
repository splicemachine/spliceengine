package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HRegion;
/**
 * HBase snapshot platform specific code API is defined here
 * @author vrodionov
 *
 */
public interface SnapshotUtils {
	
	public List<Path> getFilesForFullBackup(String snapshotName, HRegion region) throws IOException; 

	/**
	  * Extract the list of files (HFiles/HLogs) to copy using Map-Reduce.
	  * @return list of files referenced by the snapshot (pair of path and size)
	 */
	 public List<Path> getSnapshotFilesForRegion(final HRegion region, final Configuration conf,
	        final FileSystem fs, final Path snapshotDir) throws IOException ;
	 
	 /**
	  * Materializes snapshot reference file - creates real hfile in a local tmp directory.  
	  */
	 public Path materializeRefFile(Configuration conf, FileSystem fs, Path refFilePath )
	 	throws IOException;
}
