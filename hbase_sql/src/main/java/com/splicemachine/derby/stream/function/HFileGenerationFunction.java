/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;

/**
 * Created by jyuan on 3/21/17.
 */
public abstract class HFileGenerationFunction implements MapPartitionsFunction<Row, String>, Externalizable {

    private static final Logger LOG=Logger.getLogger(HFileGenerationFunction.class);

    public enum OperationType {
        INSERT,
        CREATE_INDEX,
        DELETE
    }

    protected OperationType operationType;
    protected StoreFileWriter writer;
    protected long txnId;

    private List<String> hFiles = Lists.newArrayList();
    private boolean initialized;
    private FileSystem fs;
    private Configuration conf;
    private OperationContext operationContext;
    private Long heapConglom;
    private String compressionAlgorithm;
    private List<BulkImportPartition> partitionList;
    private int[] pkCols;
    private String tableVersion;
    private Map<Long, Object> decoderMap = new HashedMap();

    private List<DDLMessage.TentativeIndex> tentativeIndexList;

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

    public HFileGenerationFunction(OperationContext operationContext,
                                   long txnId,
                                   Long heapConglom,
                                   String compressionAlgorithm,
                                   List<BulkImportPartition> partitionList,
                                   int[] pkCols,
                                   String tableVersion,
                                   ArrayList<DDLMessage.TentativeIndex> tentativeIndexList) {
        this.txnId = txnId;
        this.operationContext = operationContext;
        this.heapConglom = heapConglom;
        this.compressionAlgorithm = compressionAlgorithm;
        this.partitionList = partitionList;
        this.pkCols = pkCols;
        this.tableVersion = tableVersion;
        this.tentativeIndexList = tentativeIndexList;
    }

    public HFileGenerationFunction(OperationContext operationContext,
                                   long txnId,
                                   Long heapConglom,
                                   String compressionAlgorithm,
                                   List<BulkImportPartition> partitionList,
                                   String tableVersion,
                                   DDLMessage.TentativeIndex tentativeIndex) {
        this.txnId = txnId;
        this.operationContext = operationContext;
        this.heapConglom = heapConglom;
        this.compressionAlgorithm = compressionAlgorithm;
        this.partitionList = partitionList;
        this.tableVersion = tableVersion;
        this.tentativeIndexList = Lists.newArrayList();
        tentativeIndexList.add(tentativeIndex);
    }

    /**
     * Write key/values to an HFile
     * @param mainAndIndexRows
     * @return
     * @throws Exception
     */
    public Iterator<String> call(Iterator<Row> mainAndIndexRows) throws Exception {

        try {
            String lastKey = null;
            while (mainAndIndexRows.hasNext()) {
                Row row = mainAndIndexRows.next();
                Long conglomerateId = row.getAs("conglomerateId");
                String keyStr = row.getAs("key");
                if (lastKey != null && lastKey.equals(keyStr)) {
                    byte[] value = row.getAs("value");
                    byte[] key = Bytes.fromHex(keyStr);
                    handleConstraintViolation(conglomerateId, key, value);
                    continue;
                }
                lastKey = keyStr;
                byte[] key = Bytes.fromHex(keyStr);
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
            if (hFiles.isEmpty()) {
                hFiles.add("Empty");
            }
            return hFiles.iterator();
        } finally {
            close(writer);
        }
    }

    protected abstract void writeToHFile (byte[] rowKey, byte[] value) throws Exception;

    private void init(Long conglomerateId, byte[] key) throws IOException, StandardException{
        conf = HConfiguration.unwrapDelegate();
        int index = Collections.binarySearch(partitionList,
                new BulkImportPartition(conglomerateId, null, key, key, null),
                BulkImportUtils.getSearchComparator());
        BulkImportPartition partition = partitionList.get(index);
        fs = FileSystem.get(URI.create(partition.getFilePath()), conf);
        writer = getNewWriter(conf, partition);
        hFiles.add(writer.getPath().toString());
        initializeDecoders();
    }


    private StoreFileWriter getNewWriter(Configuration conf, BulkImportPartition partition)
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
            Path familyPath = new Path(partition.getFilePath());
            // Get favored nodes as late as possible. This is the best we can do. If the region gets moved after this
            // point, locality is not guaranteed.
            InetSocketAddress favoredNode = getFavoredNode(partition);
            StoreFileWriter.Builder builder =
                    new StoreFileWriter.Builder(conf, new CacheConfig(tempConf), new HFileSystem(fs))
                            .withOutputDir(familyPath).withBloomType(bloomType)
                            .withFileContext(hFileContext);

            if (favoredNode != null) {
                InetSocketAddress[] favoredNodes = new InetSocketAddress[1];
                favoredNodes[0] = favoredNode;
                builder.withFavoredNodes(favoredNodes);
            }

            return builder.build();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void close(final StoreFileWriter w) throws IOException {
        if (w != null) {
            w.appendFileInfo(HStoreFile.BULKLOAD_TIME_KEY,
                    Bytes.toBytes(System.currentTimeMillis()));
            w.appendFileInfo(HStoreFile.BULKLOAD_TASK_KEY,
                    HBaseConfiguration.BULKLOAD_TASK_KEY);//context.getTaskAttemptID().toString())); TODO JL
            w.appendFileInfo(HStoreFile.MAJOR_COMPACTION_KEY,
                    Bytes.toBytes(true));
            w.appendFileInfo(HStoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
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
        out.writeBoolean(pkCols != null);
        if (pkCols != null) {
            int n = pkCols.length;
            out.writeInt(n);
            for (int i = 0; i < n; ++i) {
                out.writeInt(pkCols[i]);
            }
        }
        out.writeBoolean(tableVersion!=null);
        if (tableVersion != null) {
            out.writeUTF(tableVersion);
        }
        out.writeInt(operationType.ordinal());
        out.writeBoolean(tentativeIndexList != null);
        if (tentativeIndexList != null) {
            out.writeInt(tentativeIndexList.size());
            for (DDLMessage.TentativeIndex tentativeIndex:tentativeIndexList) {
                byte[] message = tentativeIndex.toByteArray();
                out.writeInt(message.length);
                out.write(message);
            }
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
        if (in.readBoolean()) {
            n = in.readInt();
            pkCols = new int[n];
            for (int i = 0; i < n; ++i) {
                pkCols[i] = in.readInt();
            }
        }
        if (in.readBoolean()) {
            tableVersion = in.readUTF();
        }
        operationType = OperationType.values()[in.readInt()];
        if (in.readBoolean()) {
            tentativeIndexList = Lists.newArrayList();
            n = in.readInt();
            for (int i = 0; i < n; ++i) {
                byte[] message = new byte[in.readInt()];
                in.readFully(message);
                tentativeIndexList.add(DDLMessage.TentativeIndex.parseFrom(message));
            }
        }
    }

    private static InetSocketAddress getFavoredNode(BulkImportPartition partition) throws IOException {
        InetSocketAddress favoredNode = null;
        SConfiguration configuration = HConfiguration.getConfiguration();
        Connection connection = HBaseConnectionFactory.getInstance(configuration).getConnection();

        String regionName = partition.getRegionName();
        HRegionLocation regionLocation = MetaTableAccessor.getRegionLocation(connection,
                com.splicemachine.primitives.Bytes.toBytesBinary(regionName));

        if (regionLocation != null) {
            String hostname = regionLocation.getHostname();
            int port = regionLocation.getPort();

            InetSocketAddress address = new InetSocketAddress(hostname, port);
            if (!address.isUnresolved()) {
                favoredNode = address;
            } else {
                SpliceLogUtils.info(LOG, "Cannot resolve host %s to achieve better data locality.", hostname);
            }

        }
        else {
            SpliceLogUtils.info(LOG, "Cannot to get region location %s to achieve better data locality.", regionName);
        }
        return favoredNode;
    }

    private boolean isIndex(Long conglomId) {
        return (operationType == OperationType.CREATE_INDEX) ||
                (operationType == OperationType.INSERT && heapConglom.longValue() != conglomId.longValue());
    }

    private void initializeDecoders() throws StandardException{
        if (operationType == OperationType.INSERT) {
            // For bulk import, initialize decoder for base table row
            ExecRow execRow = operationContext.getOperation().getExecRowDefinition();
            KeyEncoder keyEncoder = BulkImportUtils.getKeyEncoder(execRow, pkCols, tableVersion, null);
            DataHash rowEncoder = BulkImportUtils.getRowHash(execRow, pkCols, tableVersion);
            PairDecoder pairDecoder = new PairDecoder(keyEncoder.getDecoder(), rowEncoder.getDecoder(), execRow);
            decoderMap.put(heapConglom, pairDecoder);
        }
        if (tentativeIndexList != null && tentativeIndexList.size() > 0) {
            // For each index, initialize a decoder for index column
            for (DDLMessage.TentativeIndex tentativeIndex : tentativeIndexList) {
                DDLMessage.Table tbl = tentativeIndex.getTable();
                DDLMessage.Index idx = tentativeIndex.getIndex();
                Long indexConglomerate = idx.getConglomerate();
                int n = idx.getDescColumnsCount();
                ExecRow execRow = new ValueRow(n);
                List<Integer> formatIds;
                if (idx.getNumExprs() <= 0) {
                    List<Integer> indexColsList = idx.getIndexColsToMainColMapList();
                    List<Integer> allFormatIds = tbl.getFormatIdsList();
                    formatIds = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        formatIds.add(allFormatIds.get(indexColsList.get(i)-1));
                    }
                } else {
                    formatIds = idx.getIndexColumnFormatIdsList();
                }
                DataValueDescriptor[] dvds = execRow.getRowArray();
                for (int i = 0; i < n; ++i) {
                    dvds[i] =  LazyDataValueFactory.getLazyNull(formatIds.get(i));
                }
                int[] pkCols = new int[execRow.nColumns()];
                for (int i = 0; i < pkCols.length; ++i) {
                    pkCols[i] = i+1;
                }
                boolean[] sortOrder = new boolean[n];
                for (int  i = 0; i < n; ++i) {
                    sortOrder[i] = !idx.getDescColumns(i);
                }
                KeyEncoder keyEncoder = BulkImportUtils.getKeyEncoder(execRow, pkCols, tableVersion, sortOrder);
                decoderMap.put(indexConglomerate, new Pair<>(execRow, keyEncoder.getDecoder()));
            }
        }
    }

    private void handleConstraintViolation(Long conglomerateId, byte[] key, byte[] value) throws StandardException{
        ExecRow execRow;
        if (isIndex(conglomerateId)) {
            Pair<ExecRow, KeyDecoder> p = (Pair<ExecRow, KeyDecoder>)decoderMap.get(conglomerateId);
            execRow = p.getFirst();
            KeyDecoder decoder = (KeyDecoder) p.getSecond();
            decoder.decode(key, 0, key.length, execRow);
        }else {
            PairDecoder decoder = (PairDecoder) decoderMap.get(conglomerateId);
            execRow = decoder.decode(key, value);
        }
        if (operationContext != null && operationContext.isPermissive()) {

            operationContext.recordBadRecord("Unique constraint violation, row: "
                    + execRow.toString(), null);
        }
        else {
            SpliceLogUtils.error(LOG, "Unique constraint violation, row: %s", execRow.toString());
            DataDictionary dd = operationContext.getActivation().getLanguageConnectionContext().getDataDictionary();
            ConglomerateDescriptor cd = dd.getConglomerateDescriptor(conglomerateId);
            com.splicemachine.db.catalog.UUID tableID = cd.getTableID();
            TableDescriptor td = dd.getTableDescriptor(tableID);
            String tableName = td.getName();
            String indexOrConstraintName;
            if (isIndex(conglomerateId)) {
                indexOrConstraintName = cd.getConglomerateName();
            }
            else {
                ConstraintDescriptor conDesc = dd.getConstraintDescriptor(td, cd.getUUID());
                indexOrConstraintName = conDesc.getConstraintName();
            }

            throw StandardException.newException(SQLState.LANG_DUPLICATE_KEY_CONSTRAINT, indexOrConstraintName, tableName);
        }
    }
}
