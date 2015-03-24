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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotFileInfo;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotFileInfoOrBuilder;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.util.FSUtils;

import com.splicemachine.constants.SpliceConstants;

public class SnapshotUtilsImpl implements SnapshotUtils{
    static final Logger LOG = Logger.getLogger(SnapshotUtilsImpl.class);
	
    @Override
    public List<Object> getFilesForFullBackup(String snapshotName, HRegion region) throws IOException {
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
    @Override
    public List<Object> getSnapshotFilesForRegion(final HRegion region, final Configuration conf,
        final FileSystem fs, final Path snapshotDir) throws IOException {
      SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

      final List<Object> files = new ArrayList<Object>();
      final TableName table = TableName.valueOf(snapshotDesc.getTable());

      // Get snapshot files
      LOG.info("Loading Snapshot '" + snapshotDesc.getName() + "' hfile list");
      SnapshotReferenceUtil.visitReferencedFiles(conf, fs, snapshotDir, snapshotDesc,
        new SnapshotReferenceUtil.SnapshotVisitor() {
          @Override
          public void storeFile(final HRegionInfo regionInfo, final String family,
              final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
            
        	  boolean isReference = storeFile.hasReference();

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
              
              HFileLink filePath = getFilePath(fileInfo);
              if( isReference ) {
            	  // If its materialized reference - we add Path
            	  files.add(materializeRefFile(conf, fs, filePath, region));
              } else{
            	  // Otherwise we add HFileLink
            	  files.add(filePath);
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

    @Override
    public List<Object> getSnapshotFilesForRegion(final HRegion region, final Configuration conf,
                                                  final FileSystem fs, final String snapshotName) throws IOException {
        Path rootDir = FSUtils.getRootDir(conf);

        Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
        List<Object> paths = getSnapshotFilesForRegion(region, conf, fs, snapshotDir);

        return paths;
    }

    /**
     * Returns path to a file referenced in a snapshot
     * FIXME: race condition possible, if file gets archived during
     * backup operation. We should probably return HFileLink and process
     * this link accordingly.
     * @param fileInfo
     * @return path to a referenced file
     * @throws IOException
     */

	// for testing 
    public HFileLink getFilePath( final SnapshotFileInfoOrBuilder fileInfo)
            throws IOException {
          try {
        	//Configuration conf = SpliceConstants.config;
            //Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
            //FileSystem fs = rootDir.getFileSystem(conf);        	  
            HFileLink link = null;
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
            
            return link;//link.getAvailablePath(fs);
            
          } catch (FileNotFoundException e) {
            LOG.error("Unable to get the status for source file=" + fileInfo.toString(), e);
            throw e;
          } catch (IOException e) {
            LOG.error("Unable to get the status for source file=" + fileInfo.toString(), e);
            throw e;
          }
        }
	         
    /**
	  * Materializes snapshot reference file - creates real hfile 
	  * in a current region's tmp directory.
	  *   
	  */
    @Override
	public Path materializeRefFile(Configuration conf, FileSystem fs, HFileLink refFilePath, HRegion region )
	 	throws IOException
	{
    	// Create HalfStoreReader
    	LOG.info("Ref file link:"+ refFilePath);
    	Reference ref = readReference(fs, refFilePath);
    	HFileLink hfile = getReferredFileLink(refFilePath);
    	LOG.info("HFile link : "+hfile);
    	byte[] cf = getColumnFamily(hfile);
    	// This is a different region (daughter); 
    	Store store = region.getStore(cf);       
    	CacheConfig cacheConf = store.getCacheConfig();
    	// disable block cache completely
    	// do we really need this?
    	disableBlockCache(cacheConf);   
    	FSDataInputStreamWrapper in = new FSDataInputStreamWrapper(fs, hfile);
    	long length = hfile.getFileStatus(fs).getLen();    
    	/*DEBUG*/
    	LOG.info("path: "+hfile.getAvailablePath(fs)+" len="+length);
    	
    	HalfStoreFileReader fileReader = 
    			new HalfStoreFileReader(fs, hfile.getAvailablePath(fs), in, length, cacheConf, ref, conf);
    	// cache on read disabled, pred = false
    	StoreFileScanner scanner = fileReader.getStoreFileScanner(false, false);
    	// Create local file 
        // TODO maxKey (Integer.MAX_VALUE)
    	StoreFile.Writer writer = 
    			store.createWriterInTmp(Integer.MAX_VALUE, 
    					store.getFamily().getCompression(), false, 
    					true, true);
    	// we have scanner and we have writer
        // lets use them
    	return readWrite(scanner, writer);
    	
	}
    
    private Reference readReference(FileSystem fs, HFileLink link) 
    		throws IOException
    {
    	int totalAttempts = 0;
    	int maxAttempts = link.getLocations().length;
    	while( totalAttempts ++ <= maxAttempts){
    		try{
    			Path p = link.getAvailablePath(fs);
    			return Reference.read(fs, p);
    		} catch (Exception e){
    			if(totalAttempts == maxAttempts) {
    				throw e;
    			} 
    		}
    	}
    	// should not be here
    	return null;    
    }
    
    /**
     * Reads data from a parent HalfStoreFile and writes into tmp file
     * @param scanner
     * @param writer
     * @return path to a new HFile
     * @throws IOException
     */
    private Path readWrite(StoreFileScanner scanner, StoreFile.Writer writer)
    	throws IOException
    {

       	try{
       		KeyValue kv = null;
       		long maxMVCC = Long.MIN_VALUE;
       		while((kv = scanner.next()) != null){
       			writer.append(kv);
       			long mvcc = kv.getMvccVersion();
       			if ( mvcc > maxMVCC) {
       				maxMVCC = mvcc;
       			}
       		}
       		// TODO: We need to keep track of maxSeqId & isMajorCompaction
       		writer.appendMetadata( maxMVCC, false);
       		// TODO: that is does not work
       		return  writer.getPath();
       	} finally{
       		writer.close();
       		scanner.close();
       	}
    }
	
    /**
     * Returns column family name from store file path
     * @param path
     * @return column family name (as byte array)
     */
    public byte[] getColumnFamily(HFileLink link)
    {
    	Path path = link.getOriginPath();
    	return path.getParent().getName().getBytes();
    }
    /**
     * Disable block cache (not used?)
     * @param cacheConfig
     */
    private void disableBlockCache(CacheConfig cacheConfig)
    {
    	// no-op
    }
    /**
     * Returns path to a parent store file for a given reference file
     * 
     * Example:
     * Input:
     * /TABLE_A/a60772afe8c4aa3355360d3a6de0b292/fam_a/9fb67500d79a43e79b01da8d5d3017a4.88a177637e155be4d01f21441bf8595d
     * Output:
     * /TABLE_A/88a177637e155be4d01f21441bf8595d/fam_a/9fb67500d79a43e79b01da8d5d3017a4
     * @param refFilePath
     * @return parent store file path
     */
    public Path getReferredFile(Path refFilePath)
    {
    	String[] parts = refFilePath.getName().split("\\.");
    	// parts[0] - store file name
    	// parts[1] - encoded region name
    	Path p = refFilePath.getParent();
    	String columnFamily = p.getName();
    	p = p.getParent().getParent();
    	// Add region
    	p = new Path(p, parts[1]);
    	// Add columnFamily
    	p = new Path(p, columnFamily);
    	// Add store file name
    	p = new Path(p, parts[0]);
    	return p;
    }
    
    public HFileLink getReferredFileLink(HFileLink ref) throws IOException
    {
    	return new HFileLink(SpliceConstants.config, getReferredFile(ref.getOriginPath()));
    }
    /**
     * Checks if region info for the current region.
     * Essentially, it just a string compare
     * @param region
     * @param regInfo
     * @return true if yes
     */
    public boolean isCurrentRegion(HRegion region, HRegionInfo regInfo) {		
		return region.getRegionNameAsString().equals(regInfo.getRegionNameAsString());
	}	
}
