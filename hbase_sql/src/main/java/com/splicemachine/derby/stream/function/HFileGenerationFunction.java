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
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jyuan on 3/21/17.
 */
public abstract class HFileGenerationFunction implements MapPartitionsFunction<Row, String>, Externalizable {

    private static final Logger LOG=Logger.getLogger(HFileGenerationFunction.class);

    protected StoreFile.Writer writer;
    protected long txnId;

    private List<String> hFiles = Lists.newArrayList();
    private boolean initialized;
    private FileSystem fs;
    private Configuration conf;
    private OperationContext operationContext;
    private Long heapConglom;
    private String compressionAlgorithm;
    private List<BulkImportPartition> partitionList;

    public HFileGenerationFunction() {
    }

    public HFileGenerationFunction(OperationContext operationContext,
                                   long txnId,
                                   Long heapConglom,
                                   String compressionAlgorithm,
                                   List<BulkImportPartition> partitionList) {
        this.txnId = txnId;
        this.operationContext = operationContext;
        this.heapConglom = heapConglom;
        this.compressionAlgorithm = compressionAlgorithm;
        this.partitionList = partitionList;
    }

    /**
     * Write key/values to an HFile
     * @param mainAndIndexRows
     * @return
     * @throws Exception
     */
    public Iterator<String> call(Iterator<Row> mainAndIndexRows) throws Exception {

        try {
            while (mainAndIndexRows.hasNext()) {
                Row row = mainAndIndexRows.next();
                Long conglomerateId = row.getAs("conglomerateId");
                byte[] key = Bytes.fromHex(row.getAs("key"));
                byte[] value = row.getAs("value");
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.error(LOG, "conglomerateId:%d, key:%s, value:%s",
                            conglomerateId, Bytes.toHex(key), Bytes.toHex(value));
                }
                if (!initialized) {
                    init(conglomerateId, key);
                    initialized = true;
                }
                writeToHFile(key, value);
                if (conglomerateId.equals(heapConglom)) {
                    if (operationContext != null)
                        operationContext.recordWrite();
                }
            }
            close(writer);
            if (hFiles.size() == 0) {
                hFiles.add("Empty");
            }
            return hFiles.iterator();
        } finally {
            close(writer);
        }
    }

    protected abstract void writeToHFile (byte[] rowKey, byte[] value) throws Exception;

    private void init(Long conglomerateId, byte[] key) throws IOException{
        conf = HConfiguration.unwrapDelegate();
        int index = Collections.binarySearch(partitionList,
                new BulkImportPartition(conglomerateId, key, key, null),
                BulkImportUtils.getSearchComparator());
        BulkImportPartition partition = partitionList.get(index);
        fs = FileSystem.get(URI.create(partition.getFilePath()), conf);
        writer = getNewWriter(conf, new Path(partition.getFilePath()));
        hFiles.add(writer.getPath().toString());
    }


    private StoreFile.Writer getNewWriter(Configuration conf, Path familyPath)
            throws IOException {
        Compression.Algorithm compression = Compression.getCompressionAlgorithmByName(compressionAlgorithm);
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
                    HBaseConfiguration.BULKLOAD_TASK_KEY);//context.getTaskAttemptID().toString())); TODO JL
            w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
                    Bytes.toBytes(true));
            w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
                    Bytes.toBytes(false));
            w.appendTrackedTimestampsToMetadata();
            w.close();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(operationContext != null);
        if (operationContext != null) {
            out.writeObject(operationContext);
        }
        out.writeLong(txnId);
        out.writeLong(heapConglom);
        out.writeUTF(compressionAlgorithm);
        out.writeInt(partitionList.size());
        for (int i = 0; i < partitionList.size(); ++i) {
            BulkImportPartition partition = partitionList.get(i);
            out.writeObject(partition);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if(in.readBoolean()) {
            operationContext = (OperationContext) in.readObject();
        }
        txnId = in.readLong();
        heapConglom = in.readLong();
        compressionAlgorithm = in.readUTF();
        int n = in.readInt();
        partitionList = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            BulkImportPartition partition = (BulkImportPartition) in.readObject();
            partitionList.add(partition);
        }
    }
}