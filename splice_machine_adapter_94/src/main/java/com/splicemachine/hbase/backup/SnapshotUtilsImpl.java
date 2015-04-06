package com.splicemachine.hbase.backup;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;

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
      final String regionName = (reg != null) ? reg.getRegionNameAsString() : null;
      SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

      final List<Object> files = new ArrayList<Object>();
      final String table = snapshotDesc.getTable();

      // Get snapshot files
      LOG.info("Loading Snapshot '" + snapshotDesc.getName() + "' hfile list"); 
      
      SnapshotReferenceUtil.visitReferencedFiles(fs, snapshotDir,
        new SnapshotReferenceUtil.FileVisitor() {
          public void storeFile (final String region, final String family, final String hfile)
              throws IOException {
            // if reg is null, get snapshot files for all regions
          	if( regionName == null || isRegionTheSame(regionName, region) ){
        		Path path = HFileLink.createPath(table, region, family, hfile);
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
            // copied with the snapshot references
          }

          public void logFile (final String server, final String logfile)
              throws IOException {
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
    	/*DEBUG*/
    	LOG.info("path: "+hfile.getAvailablePath(fs));
    	Path inFile = hfile.getAvailablePath(fs);
    	Path outFile = copyHFileHalf(conf, inFile,  ref, store.getFamily());
    	return outFile;
	}
    
    /**
     * Copy half of an HFile into a new HFile.
     */
    private static Path copyHFileHalf(
        Configuration conf, Path inFile,  Reference reference,
        HColumnDescriptor familyDescriptor)
    throws IOException {
      FileSystem fs = inFile.getFileSystem(conf);
      CacheConfig cacheConf = new CacheConfig(conf);
      HalfStoreFileReader halfReader = null;
      StoreFile.Writer halfWriter = null;
      HFileDataBlockEncoder dataBlockEncoder = new HFileDataBlockEncoderImpl(
          familyDescriptor.getDataBlockEncodingOnDisk(),
          familyDescriptor.getDataBlockEncoding());
      try {
        halfReader = new HalfStoreFileReader(fs, inFile, cacheConf,
            reference, DataBlockEncoding.NONE);
        Map<byte[], byte[]> fileInfo = halfReader.loadFileInfo();

        int blocksize = familyDescriptor.getBlocksize();
        Algorithm compression = familyDescriptor.getCompression();
        BloomType bloomFilterType = familyDescriptor.getBloomFilterType();

        halfWriter = new StoreFile.WriterBuilder(conf, cacheConf,
            fs, blocksize)
                .withCompression(compression)
                .withDataBlockEncoder(dataBlockEncoder)
                .withBloomType(bloomFilterType)
                .withChecksumType(Store.getChecksumType(conf))
                .withBytesPerChecksum(Store.getBytesPerChecksum(conf))
                .withOutputDir(getHBaseTmpDir(conf))
                .build();
        HFileScanner scanner = halfReader.getScanner(false, false, false);
        scanner.seekTo();
        do {
          KeyValue kv = scanner.getKeyValue();
          halfWriter.append(kv);
        } while (scanner.next());

        for (Map.Entry<byte[],byte[]> entry : fileInfo.entrySet()) {
          if (shouldCopyHFileMetaKey(entry.getKey())) {
            halfWriter.appendFileInfo(entry.getKey(), entry.getValue());
          }
        }
        return halfWriter.getPath();
      } finally {
        if (halfWriter != null) halfWriter.close();
        if (halfReader != null) halfReader.close(cacheConf.shouldEvictOnClose());
      }
    }

    private static Path getHBaseTmpDir(Configuration conf) 
    		throws IOException 
    {
		Path rootDir = FSUtils.getRootDir(conf);
		Path tmpDir = new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY);
    	return tmpDir;
	}

	private static boolean shouldCopyHFileMetaKey(byte[] key) {
      return !HFile.isReservedFileInfoKey(key);
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

    @Override
    public List<Object> getSnapshotFilesForRegion(final HRegion region, final Configuration conf,
                                                  final FileSystem fs, final String snapshotName) throws IOException {
        Path rootDir = FSUtils.getRootDir(conf);

        Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
        List<Object> paths = getSnapshotFilesForRegion(region, conf, fs, snapshotDir);

        return paths;
    }
	
    /**
     * Returns column family name from store file path
     * @param link
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
}
