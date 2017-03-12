/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.function;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.constants.SIConstants;
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
import org.apache.spark.sql.Row;
import scala.collection.Iterator;
import scala.collection.mutable.MutableList;
import scala.runtime.AbstractFunction1;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by jyuan on 3/21/17.
 */
public class HFileGenerationFunction  extends AbstractFunction1<Iterator<Row>, Iterator<String>> implements Externalizable {

    private static final Logger LOG=Logger.getLogger(HFileGenerationFunction.class);

    private MutableList HFiles = new MutableList();
    private boolean initialized;
    private FileSystem fs;
    private long txnId;
    private Configuration conf;
    private OperationContext operationContext;
    private Long heapConglom;
    private List<BulkImportPartition> partitionList;

    private BulkImportPartition currentPartition;
    private StoreFile.Writer writer;

    public HFileGenerationFunction() {
    }

    public HFileGenerationFunction(OperationContext operationContext,
                                   long txnId,
                                   Long heapConglom,
                                   List<BulkImportPartition> partitionList) {
        this.txnId = txnId;
        this.operationContext = operationContext;
        this.heapConglom = heapConglom;
        this.partitionList = partitionList;
    }

    @Override
    public Iterator<String> apply(Iterator<Row> mainAndIndexRows) {
        try {
            while (mainAndIndexRows.hasNext()) {
                Row row = mainAndIndexRows.next();
                Long conglomerateId = row.getAs("conglomerateId");
                byte[] key = Bytes.fromHex((String) row.getAs("key"));
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
                    operationContext.recordWrite();
                }
            }
            close(writer);
            if (HFiles.size() == 0) {
                HFiles.appendElem("Empty");
            }

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return HFiles.iterator();
    }

    private void writeToHFile (byte[] rowKey, byte[] value) throws Exception {
        KeyValue kv = new KeyValue(rowKey, SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.PACKED_COLUMN_BYTES, txnId, value);
        writer.append(kv);

    }

    private void prepareNextFile(Long conglomerateId, byte[] key) throws IOException {
        int index = Collections.binarySearch(partitionList,
                new BulkImportPartition(conglomerateId, key, key, null),
                BulkImportUtils.getSearchComparator());
        currentPartition = partitionList.get(index);
        if (fs == null)
            fs = FileSystem.get(URI.create(currentPartition.getFilePath()), conf);
        writer = getNewWriter(conf, new Path(currentPartition.getFilePath()));
        HFiles.appendElem(writer.getPath().toString());
    }

    private void init(Long conglomerateId, byte[] key) throws IOException{
        conf = HConfiguration.unwrapDelegate();
        prepareNextFile(conglomerateId, key);
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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(operationContext);
        out.writeLong(txnId);
        out.writeLong(heapConglom);
        out.writeInt(partitionList.size());
        for (int i = 0; i < partitionList.size(); ++i) {
            BulkImportPartition partition = partitionList.get(i);
            out.writeObject(partition);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        operationContext = (OperationContext) in.readObject();
        txnId = in.readLong();
        heapConglom = in.readLong();
        int n = in.readInt();
        partitionList = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            BulkImportPartition partition = (BulkImportPartition) in.readObject();
            partitionList.add(partition);
        }
    }
}