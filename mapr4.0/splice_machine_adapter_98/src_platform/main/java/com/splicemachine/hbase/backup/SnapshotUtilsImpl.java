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

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;

import com.splicemachine.constants.SpliceConstants;

public class SnapshotUtilsImpl implements SnapshotUtils {
    static final Logger LOG = Logger.getLogger(SnapshotUtilsImpl.class);
	
    public List<Object> getFilesForFullBackup(String snapshotName, HRegion region) throws IOException {
		Configuration conf = SpliceConstants.config;
        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        FileSystem fs = rootDir.getFileSystem(conf);
        Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
        
		return getSnapshotFilesForRegion(region ,conf, fs, snapshotDir);
	}

    /**
     * Extract the list of files (HFiles/HLogs) to copy 
     * @return list of files referenced by the snapshot
     */
    public List<Object> getSnapshotFilesForRegion(final HRegion reg, final Configuration conf,
        final FileSystem fs, final Path snapshotDir) throws IOException {
      final String regionName = reg.getRegionNameAsString();
      SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

      final List<Object> files = new ArrayList<Object>();
      final String table = snapshotDesc.getTable();

      // Get snapshot files
      LOG.info("Loading Snapshot '" + snapshotDesc.getName() + "' hfile list"); 
      SnapshotReferenceUtil.visitReferencedFiles(fs, snapshotDir,
        new SnapshotReferenceUtil.FileVisitor() {
          public void storeFile (final String region, final String family, final String hfile)
              throws IOException {
        	  LOG.info("look for: " + regionName);
        	  LOG.info("found   : " + region);
        	if( isRegionTheSame(regionName, region) ){  
        		Path path = HFileLink.createPath(TableName.valueOf(table), region, family, hfile);
        		//long size = new HFileLink(conf, path).getFileStatus(fs).getLen();
        		HFileLink link = new HFileLink(conf, path);
        		if( isReference(hfile) ) {
            	  	files.add(materializeRefFile(conf, fs, link, reg));
              	} else{
              		files.add(link);
              	}
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
    
    private boolean isRegionTheSame(String fullName, String shortId)
    {
    	return fullName.indexOf(shortId) >=0;
    }
    
    
    private boolean isReference( String fileName)
    {
    	return fileName.indexOf(".") > 0;
    }

    public Path getAvailableFilePath( final Path relativePath)
            throws IOException {
          try {
        	  Configuration conf = SpliceConstants.config;
              Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
              FileSystem fs = rootDir.getFileSystem(conf);        	              
              HFileLink link = new HFileLink(conf, relativePath);            
            return link.getAvailablePath(fs);
            
          } catch (FileNotFoundException e) {
            LOG.error("Unable to get the status for source file=" + relativePath, e);
            throw e;
          } catch (IOException e) {
            LOG.error("Unable to get the status for source file=" + relativePath, e);
            throw e;
          }
        }
    
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
