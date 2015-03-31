package com.splicemachine.derby.impl.stats;

import com.splicemachine.async.KeyValue;
import com.splicemachine.derby.iapi.catalog.TableStatisticsDescriptor;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import org.apache.hadoop.hbase.client.Result;

/**
 * @author Scott Fines
 *         Date: 3/31/15
 */
public abstract class TableStatsDecoder{
    private static volatile TableStatsDecoder INSTANCE;

    public static void setInstance(TableStatsDecoder decoder){
        INSTANCE = decoder;
    }

    public static TableStatsDecoder decoder(){
        return INSTANCE;
    }

    public TableStatisticsDescriptor decode(Result result,EntryDecoder cachedDecoder){
        MultiFieldDecoder decoder=cachedDecoder.get();
        setKeyInDecoder(result,decoder);
        long conglomId = decoder.decodeNextLong();
        String partitionId = decoder.decodeNextString();

        setRowInDecoder(result,cachedDecoder);

        BitIndex index=cachedDecoder.getCurrentIndex();
        decoder=cachedDecoder.get();
        long timestamp;
        boolean isStale;
        boolean inProgress;
        timestamp=index.isSet(2)?decoder.decodeNextLong():System.currentTimeMillis();
        isStale=!index.isSet(3) || decoder.decodeNextBoolean();
        inProgress=index.isSet(4) && decoder.decodeNextBoolean();

        TableStatisticsDescriptor stats = null;
        if(index.isSet(5))
            stats=decode(decoder,conglomId,partitionId,timestamp,isStale,inProgress);

        return stats;
    }

    public TableStatisticsDescriptor decode(KeyValue kv,EntryDecoder cachedDecoder){
        MultiFieldDecoder decoder=cachedDecoder.get();
        decoder.set(kv.key());
        long conglomId = decoder.decodeNextLong();
        String partitionId = decoder.decodeNextString();


        cachedDecoder.set(kv.value());
        BitIndex index=cachedDecoder.getCurrentIndex();
        decoder=cachedDecoder.get();
        long timestamp;
        boolean isStale;
        boolean inProgress;
        timestamp=index.isSet(0)?decoder.decodeNextLong():System.currentTimeMillis();
        isStale=!index.isSet(1) || decoder.decodeNextBoolean();
        inProgress=index.isSet(2) && decoder.decodeNextBoolean();

        TableStatisticsDescriptor stats = null;
        if(index.isSet(3))
            stats=decode(decoder,conglomId,partitionId,timestamp,isStale,inProgress);

        return stats;
    }

    protected abstract void setKeyInDecoder(Result result,MultiFieldDecoder cachedDecoder);

    protected abstract void setRowInDecoder(Result result,EntryDecoder cachedDecoder);

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private TableStatisticsDescriptor decode(MultiFieldDecoder decoder,
                                             long conglomId,
                                             String partitionId,
                                             long timestamp,
                                             boolean isStale,
                                             boolean inProgress){
        long rowCount = decoder.decodeNextLong();
        long size = decoder.decodeNextLong();
        int avgRowWidth = decoder.decodeNextInt();
        long queryCount = decoder.decodeNextLong();
        long localReadLat = decoder.decodeNextLong();
        long remoteReadLat = decoder.decodeNextLong();
        if(remoteReadLat<0){
            /*
             * We have a situation where it's difficult to obtain remote read
             * latency for base tables. In those cases, we store a number
             * <0 to indicate that we didn't collect it. When we detect
             * that, we will it in with at "configured remote latency"--e.g.
             * a (configurable) constant rate times the local latency
             */
            remoteReadLat = (long)(StatsConstants.remoteLatencyScaleFactor*localReadLat);
        }
        long writeLat = decoder.decodeNextLong();
        /*
         * Get the estimated latencies for opening and closing a scanner.
         *
         * If the number is <0, then we default to using the remote read latency as a measure.
         */
        long openLat = decoder.decodeNextLong();
        if(openLat<0)
            openLat = remoteReadLat;
        long closeLat = decoder.decodeNextLong();
        if(closeLat<0)
            closeLat = remoteReadLat;


        return new TableStatisticsDescriptor(conglomId,
                partitionId,
                timestamp,
                isStale,inProgress,
                rowCount,
                size,
                avgRowWidth,
                queryCount,
                localReadLat,
                remoteReadLat,
                writeLat,
                openLat,
                closeLat);
    }
}
