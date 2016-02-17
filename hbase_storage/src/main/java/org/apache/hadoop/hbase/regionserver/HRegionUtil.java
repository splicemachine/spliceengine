package org.apache.hadoop.hbase.regionserver;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.storage.StorageConfiguration;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Class for accessing protected methods in HBase.
 *
 * @author johnleach
 */
public class HRegionUtil extends BaseHRegionUtil{
    private static final Logger LOG=Logger.getLogger(HRegionUtil.class);
    private static long splitBlockSize = HConfiguration.INSTANCE.getLong(StorageConfiguration.SPLIT_BLOCK_SIZE);
    public static void lockStore(Store store){
        ((HStore)store).lock.readLock().lock();
    }

    public static void unlockStore(Store store){
        ((HStore)store).lock.readLock().unlock();
    }

    public static void updateWriteRequests(HRegion region,long numWrites){
        HBasePlatformUtils.updateWriteRequests(region,numWrites);
    }

    public static void updateReadRequests(HRegion region,long numReads){
        HBasePlatformUtils.updateReadRequests(region,numReads);
    }

    public static List<byte[]> getCutpoints(Store store, byte[] start, byte[] end) throws IOException {
        assert Bytes.compareTo(start, end) <= 0 || start.length == 0 || end.length ==0;
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getCutpoints");
        Collection<StoreFile> storeFiles;
        storeFiles = store.getStorefiles();
        HFileBlockIndex.BlockIndexReader fileReader;
        List<byte[]> cutPoints =new ArrayList<>();
        for (StoreFile file: storeFiles) {
            if (file != null) {
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "getCutpoints with file=%s",file.getPath());
                fileReader = file.createReader().getHFileReader().getDataBlockIndexReader();
                int size = fileReader.getRootBlockCount();
                int blockCounter = 0;
                long lastOffset = 0;
                for (int i =0; i<size;i++) {
                    blockCounter += fileReader.getRootBlockOffset(i) - lastOffset;
                    if (LOG.isTraceEnabled())
                        SpliceLogUtils.trace(LOG, "block %d, with blockCounter=%d",i,blockCounter);
                    byte[] possibleCutpoint = KeyValue.createKeyValueFromKey(fileReader.getRootBlockKey(i)).getRow();
                    if ((start.length == 0 || Bytes.compareTo(start, possibleCutpoint) < 0) && (end.length ==0 || Bytes.compareTo(end, possibleCutpoint) > 0)) { // Do not include cutpoints out of bounds
                        if (blockCounter >= splitBlockSize) {
                            lastOffset = fileReader.getRootBlockOffset(i);
                            blockCounter = 0;
                            cutPoints.add(possibleCutpoint); // Will have to create rowKey anyway for scan...
                        }
                    }
                }
            }
        }
        if (storeFiles.size() > 1) { 	    	  // have to sort, hopefully will not happen a lot if major compaction is working properly...
            Collections.sort(cutPoints, new Comparator<byte[]>() {
                @Override
                public int compare(byte[] left, byte[] right) {
                    return Bytes.compareTo(left, right) ;
                }});
        }
        return cutPoints;
    }
}