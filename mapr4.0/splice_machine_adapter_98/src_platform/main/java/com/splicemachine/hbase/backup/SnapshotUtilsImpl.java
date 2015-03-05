package com.splicemachine.hbase.backup;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.FileLink;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;

public class SnapshotUtilsImpl {
    static final Logger LOG = Logger.getLogger(SnapshotUtilsImpl.class);
	
    public List<Path> getFilesForFullBackup(String snapshotName, HRegion region) throws IOException {
		Configuration conf = SpliceConstants.config;
        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        FileSystem fs = rootDir.getFileSystem(conf);
        Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
        
		return getSnapshotFilesForRegion(region.getRegionNameAsString() ,conf, fs, snapshotDir);
	}

    /**
     * Extract the list of files (HFiles/HLogs) to copy 
     * @return list of files referenced by the snapshot
     */
    public List<Path> getSnapshotFilesForRegion(final String regionName, final Configuration conf,
        final FileSystem fs, final Path snapshotDir) throws IOException {
      SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

      final List<Path> files = new ArrayList<Path>();
      final String table = snapshotDesc.getTable();

      // Get snapshot files
      SnapshotReferenceUtil.visitReferencedFiles(fs, snapshotDir,
        new SnapshotReferenceUtil.FileVisitor() {
          public void storeFile (final String region, final String family, final String hfile)
              throws IOException {
        	if( regionName.equals(region) ){  
        		Path path = HFileLink.createPath(TableName.valueOf(table), region, family, hfile);
        		//long size = new HFileLink(conf, path).getFileStatus(fs).getLen();
        		files.add(path);
        	}
          }

          public void recoveredEdits (final String region, final String logfile)
              throws IOException {
            // copied with the snapshot referenecs
          }

          public void logFile (final String server, final String logfile)
              throws IOException {
            //long size = new HLogLink(conf, server, logfile).getFileStatus(fs).getLen();              
        	  files.add(new Path(server, logfile));
          }
      });

      return files;
    }
    
    public Path getAvailableFilePath( final Path relativePath)
            throws IOException {
          try {
        	  Configuration conf = SpliceConstants.config;
              Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
              FileSystem fs = rootDir.getFileSystem(conf);        	              
              FileLink link = new HFileLink(conf, relativePath);            
            return link.getAvailablePath(fs);
            
          } catch (FileNotFoundException e) {
            LOG.error("Unable to get the status for source file=" + relativePath, e);
            throw e;
          } catch (IOException e) {
            LOG.error("Unable to get the status for source file=" + relativePath, e);
            throw e;
          }
        }
    
 	
}
