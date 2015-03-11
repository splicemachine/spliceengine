package com.splicemachine.hbase.backup;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.FileLink;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotFileInfo;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;

public class SnapshotUtilsImpl {
    static final Logger LOG = Logger.getLogger(SnapshotUtilsImpl.class);
	
    public List<Path> getFilesForFullBackup(String snapshotName, HRegion region) throws IOException {
		Configuration conf = SpliceConstants.config;
        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        FileSystem fs = rootDir.getFileSystem(conf);
        Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
        
		return getSnapshotFilesForRegion(region ,conf, fs, snapshotDir);
	}

    /**
     * Extract the list of files (HFiles/HLogs) to copy using Map-Reduce.
     * @return list of files referenced by the snapshot (pair of path and size)
     */
    public List<Path> getSnapshotFilesForRegion(final HRegion region, final Configuration conf,
        final FileSystem fs, final Path snapshotDir) throws IOException {
      SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

      final List<Path> files = new ArrayList<Path>();
      // TODO 0.94 
      final TableName table = TableName.valueOf(snapshotDesc.getTable());

      // Get snapshot files
      LOG.info("Loading Snapshot '" + snapshotDesc.getName() + "' hfile list");
      SnapshotReferenceUtil.visitReferencedFiles(conf, fs, snapshotDir, snapshotDesc,
        new SnapshotReferenceUtil.SnapshotVisitor() {
          @Override
          public void storeFile(final HRegionInfo regionInfo, final String family,
              final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
            if (storeFile.hasReference()) {
              // copied as part of the manifest
            } else {
              String regionName = regionInfo.getEncodedName();
              if(isCurrentRegion(region, regionInfo) == false){
            	  // If not current return
            	  return;
              }
              String hfile = storeFile.getName();
              Path path = HFileLink.createPath(table, regionName, family, hfile);

              SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder()
                .setType(SnapshotFileInfo.Type.HFILE)
                .setHfile(path.toString())
                .build();
              
              files.add(getFilePath(fileInfo));
            }
          }



		@Override
          public void logFile (final String server, final String logfile)
              throws IOException {
            SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder()
              .setType(SnapshotFileInfo.Type.WAL)
              .setWalServer(server)
              .setWalName(logfile)
              .build();
            files.add(getFilePath(fileInfo));
          }
      });

      return files;
    }
    
    public Path getFilePath( final SnapshotFileInfo fileInfo)
            throws IOException {
          try {
        	  Configuration conf = SpliceConstants.config;
              Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
              FileSystem fs = rootDir.getFileSystem(conf);        	  
            FileLink link = null;
            switch (fileInfo.getType()) {
              case HFILE:
                Path inputPath = new Path(fileInfo.getHfile());
                link = new HFileLink(SpliceConstants.config, inputPath);
                break;
              case WAL:
            	LOG.warn("Unexpected log file in a snapshot: "+fileInfo.toString());  
                break;
              default:
                throw new IOException("Invalid File Type: " + fileInfo.getType().toString());
            }
            
            return link.getAvailablePath(fs);
            
          } catch (FileNotFoundException e) {
            LOG.error("Unable to get the status for source file=" + fileInfo.toString(), e);
            throw e;
          } catch (IOException e) {
            LOG.error("Unable to get the status for source file=" + fileInfo.toString(), e);
            throw e;
          }
        }
    
    private boolean isCurrentRegion(HRegion region, HRegionInfo regInfo) {		
		return region.getRegionNameAsString().equals(regInfo.getRegionNameAsString());
	}	
}
