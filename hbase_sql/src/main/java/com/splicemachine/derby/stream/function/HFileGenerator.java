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
        extends SpliceFlatMapFunction<Op,Iterator<Tuple2<Long,Tuple2<byte[], byte[]>>>, String> implements Serializable {

    private List<BulkImportPartition> bulkImportPartitions;
    private boolean initialized;
    private List<String> generatedHFiles;
    private FileSystem fs;
    private long txnId;
    private Long heapConglom;
    private Configuration conf;
    private StoreFile.Writer writer;

    public HFileGenerator() {
    }

    public HFileGenerator(OperationContext operationContext,
                          Long heapConglom,
                          long txnId,
                          List<BulkImportPartition> bulkImportPartitions) {
        this.operationContext = operationContext;
        this.heapConglom = heapConglom;
        this.txnId = txnId;
        this.bulkImportPartitions = bulkImportPartitions;
    }

    @Override
    public Iterator<String> call(Iterator<Tuple2<Long, Tuple2<byte[], byte[]>>> mainAndIndexRows) throws Exception {

        try {
            while (mainAndIndexRows.hasNext()) {
                Tuple2<Long, Tuple2<byte[], byte[]>> t = mainAndIndexRows.next();
                Long conglom = t._1;
                Tuple2<byte[], byte[]> kvPair = t._2;
                byte[] rowKey = kvPair._1;
                byte[] value = kvPair._2;
                if (!initialized) {
                    init(conglom, rowKey);
                    initialized = true;
                }
                writeToHFile(rowKey, value);
                if (conglom.equals(heapConglom)) {
                    operationContext.recordWrite();
                }
            }
            generatedHFiles.add(writer.getPath().toString());
        }
        finally {
            close(writer);
        }
        return generatedHFiles.iterator();
    }

    /**
     *
     * @param rowKey
     * @param value
     * @throws Exception
     */
    private void writeToHFile (byte[] rowKey, byte[] value) throws Exception {
        KeyValue kv = new KeyValue(rowKey, SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.PACKED_COLUMN_BYTES, txnId, value);
        writer.append(kv);

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

    private void init(Long conglomerateId, byte[] rowKey) throws Exception {
        conf = HConfiguration.unwrapDelegate();
        generatedHFiles = Lists.newArrayList();
        BulkImportPartition partition = new BulkImportPartition(conglomerateId, rowKey, rowKey, null);
        int index = Collections.binarySearch(bulkImportPartitions, partition, BulkImportUtils.getSearchComparator());
        BulkImportPartition thisPartition = bulkImportPartitions.get(index);
        String filePath = thisPartition.getFilePath();
        fs = FileSystem.get(URI.create(filePath), conf);
        writer = getNewWriter(conf, new Path(filePath));
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(heapConglom);
        out.writeLong(txnId);
        out.writeInt(bulkImportPartitions.size());
        for (BulkImportPartition partition : bulkImportPartitions) {
            out.writeObject(partition);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        heapConglom = in.readLong();
        txnId = in.readLong();
        int n = in.readInt();
        bulkImportPartitions = Lists.newArrayList();
        for (int i = 0; i < n; ++i) {
            BulkImportPartition partition = (BulkImportPartition) in.readObject();
            bulkImportPartitions.add(partition);
        }
    }
}