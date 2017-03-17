/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.function;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.util.*;

public class HFileGenerator <Op extends SpliceOperation>
        extends SpliceFlatMapFunction<Op,Iterator<Tuple2<Long,KVPair>>, String> implements Serializable {

    private long heapConglom;
    private List<Tuple2<Long, List<HFileGenerator.HashBucketKey>>> hashBucketKeys;
    private String bulkImportDirectory;
    private boolean initialized;
    private Map<HashBucketKey, List<KVPair>> buffer;
    private Map<Long, List<HFileGenerator.HashBucketKey>> conglomMap;
    private List<String> generatedHFiles;
    private FileSystem fs;
    private long txnId;
    private Configuration conf;

    public HFileGenerator() {
    }

    public HFileGenerator(OperationContext operationContext, long txnId, long heapConglom, String bulkImportDirectory,
                          List<Tuple2<Long, List<HFileGenerator.HashBucketKey>>> hashBucketKeys) {
        this.operationContext = operationContext;
        this.txnId = txnId;
        this.heapConglom = heapConglom;
        this.bulkImportDirectory = bulkImportDirectory;
        this.hashBucketKeys = hashBucketKeys;
    }

    @Override
    public Iterator<String> call(Iterator<Tuple2<Long, KVPair>> mainAndIndexRows) throws Exception {
        if (!initialized) {
            init();
            initialized = true;
        }
        while(mainAndIndexRows.hasNext()) {
            Tuple2<Long, KVPair> t = mainAndIndexRows.next();
            Long conglom = t._1;
            KVPair kvPair = t._2;
            hashKVPair(conglom, kvPair);
        }
        flushBuffers();
        return generatedHFiles.iterator();
    }

    /**
     * Sort data and flush to HFile
     * @throws Exception
     */
    private void flushBuffers() throws Exception {
        // For each conglomerate, write to HFiles
        for (HashBucketKey hashBucketKey : buffer.keySet()) {
            Long conglom = hashBucketKey.getConglom();
            String dir = hashBucketKey.getPath();
            List<KVPair> kvPairs = buffer.get(hashBucketKey);
            if (kvPairs.size() > 0) {
                Collections.sort(kvPairs, new Comparator<KVPair>() {
                    @Override
                    public int compare(KVPair o1, KVPair o2) {
                        return Bytes.compareTo(o1.getRowKey(), o2.getRowKey());
                    }
                });
                writeToHFile(conglom, new Path(dir), kvPairs);
            }
        }
    }

    /**
     * Write key/value to HFile
     * @param conglom
     * @param path
     * @param kvPairs
     * @throws Exception
     */
    private void writeToHFile (Long conglom, Path path, List<KVPair> kvPairs) throws Exception {
        StoreFile.Writer writer = getNewWriter(conf, path);
        try {
            for (KVPair pair : kvPairs) {
                KeyValue kv = new KeyValue(pair.getRowKey(), SIConstants.DEFAULT_FAMILY_BYTES,
                        SIConstants.PACKED_COLUMN_BYTES, txnId, pair.getValue());
                writer.append(kv);
                if (conglom == heapConglom) {
                    operationContext.recordWrite();
                }
            }
            generatedHFiles.add(path.toString());
        } finally {
            close(writer);
        }
    }

    private StoreFile.Writer getNewWriter(Configuration conf, Path familyPath)
            throws IOException {
        Compression.Algorithm compression = Compression.Algorithm.NONE;
        BloomType bloomType = BloomType.ROW;
        Integer blockSize = HConstants.DEFAULT_BLOCKSIZE;
        DataBlockEncoding encoding = DataBlockEncoding.NONE;
        Configuration tempConf = new Configuration(conf);
        tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
        HFileContextBuilder contextBuilder = new HFileContextBuilder()
                .withCompression(compression)
                .withChecksumType(HStore.getChecksumType(conf))
                .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
                .withBlockSize(blockSize);

        if (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
            contextBuilder.withIncludesTags(true);
        }

        contextBuilder.withDataBlockEncoding(encoding);
        HFileContext hFileContext = contextBuilder.build();
        try {
            return new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), fs)
                    .withOutputDir(familyPath).withBloomType(bloomType)
                    .withComparator(KeyValue.COMPARATOR)
                    .withFileContext(hFileContext).build();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void close(final StoreFile.Writer w) throws IOException {
        if (w != null) {
            w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
                    Bytes.toBytes(System.currentTimeMillis()));
            w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
                    Bytes.toBytes("bulk load"));//context.getTaskAttemptID().toString())); TODO JL
            w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
                    Bytes.toBytes(true));
            w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
                    Bytes.toBytes(false));
            w.appendTrackedTimestampsToMetadata();
            w.close();
        }
    }

    private void init() throws Exception {
        conf = HConfiguration.unwrapDelegate();
        fs = FileSystem.get(URI.create(bulkImportDirectory), conf);

        buffer = new HashMap<>();
        conglomMap = new HashMap<>();
        generatedHFiles = Lists.newArrayList();
        for (Tuple2<Long, List<HFileGenerator.HashBucketKey>> t : hashBucketKeys) {
            Long conglom = t._1;
            List<HFileGenerator.HashBucketKey> keys = t._2;
            Collections.sort(keys, HashBucketKey.getSortComparator());
            conglomMap.put(conglom, keys);
            for (HFileGenerator.HashBucketKey key : keys)
            buffer.put(key, Lists.newArrayList());
        }
    }

    private void hashKVPair(Long conglom, KVPair kvPair) {
        // Find a matching bucket
        List<HashBucketKey> keys = conglomMap.get(conglom);
        byte[] rowkey = kvPair.getRowKey();
        int i = Collections.binarySearch(keys, new HashBucketKey(-1, rowkey, rowkey, null),
                HashBucketKey.getSearchComparator());
        HashBucketKey key = keys.get(i);
        List<KVPair> rows = buffer.get(key);
        rows.add(kvPair);
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(txnId);
        out.writeLong(heapConglom);
        out.writeUTF(bulkImportDirectory);
        out.writeInt(hashBucketKeys.size());
        for (Tuple2<Long, List<HFileGenerator.HashBucketKey>> keys : hashBucketKeys) {
            long table = keys._1;
            out.writeLong(table);
            List<HFileGenerator.HashBucketKey> k = keys._2;
            out.writeInt(k.size());
            for (HFileGenerator.HashBucketKey hashBucketKey: k) {
                out.writeObject(hashBucketKey);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        txnId = in.readLong();
        heapConglom = in.readLong();
        bulkImportDirectory = in.readUTF();
        int n = in.readInt();
        hashBucketKeys = Lists.newArrayList();
        for (int i = 0; i < n; ++i) {
            long table = in.readLong();
            int m = in.readInt();
            List<HFileGenerator.HashBucketKey> keys = Lists.newArrayList();
            for (int j = 0; j < m; ++j) {
                HFileGenerator.HashBucketKey k = (HFileGenerator.HashBucketKey)in.readObject();
                keys.add(k);
            }
            hashBucketKeys.add(new Tuple2<>(table, keys));
        }
    }

    public static class HashBucketKey implements Externalizable{
        private long conglom;
        private byte[] startKey;
        private byte[] endKey;
        private String path;


        public HashBucketKey() {
        }

        public HashBucketKey(long conglom, byte[] startKey, byte[] endKey, String path) {
            this.conglom = conglom;
            this.startKey = startKey;
            this.endKey = endKey;
            this.path = path;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(conglom);
            ArrayUtil.writeByteArray(out, startKey);
            ArrayUtil.writeByteArray(out, endKey);
            out.writeUTF(path);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            conglom = in.readLong();
            startKey = ArrayUtil.readByteArray(in);
            endKey = ArrayUtil.readByteArray(in);
            path = in.readUTF();
        }

        public long getConglom() {
            return conglom;
        }
        
        public byte[] getStartKey() {
            return startKey;
        }

        public byte[] getEndKey() {
            return endKey;
        }

        public String getPath() {
            return path;
        }

        public static Comparator<HashBucketKey> getSearchComparator() {
            return new Comparator<HashBucketKey>() {
                @Override
                public int compare(HashBucketKey o1, HashBucketKey o2) {
                    // only compare with start key of o2, which is encoded key for
                    // a kvpair.
                    byte[] key = o2.startKey;
                    byte[] start = o1.getStartKey();
                    byte[] end = o1.getEndKey();

                    // both start key and end key are empty
                    if ((start == null  || start.length == 0) &&
                            (end == null || end.length == 0)) {
                        return 0;
                    }
                    if (start == null  || start.length == 0) {
                        // start key is empty
                        if (Bytes.compareTo(end, key) < 0)
                            return -1;
                        else
                            return 0;
                    }
                    else if (end == null || end.length == 0) {
                        // end key is empty
                        if (Bytes.compareTo(start, key) > 0)
                            return 1;
                        else
                            return 0;
                    }
                    if (Bytes.compareTo(start, key) <= 0 && Bytes.compareTo(end, key)>=0)
                        return 0;
                    else if (Bytes.compareTo(start, key) > 0)
                        return 1;
                    else
                        return -1;
                }
            };
        }

        public static Comparator<HashBucketKey> getSortComparator() {
            return new Comparator<HashBucketKey>() {
                @Override
                public int compare(HashBucketKey o1, HashBucketKey o2) {
                    // Compare using start key
                    byte[] key1 = o1.startKey;
                    byte[] key2 = o2.startKey;

                    if (key1 == null || key1.length == 0)
                        return -1;
                    else if (key2 == null || key2.length == 0)
                        return 1;
                    return Bytes.compareTo(key1, key2);
                }
            };
        }
    }
}