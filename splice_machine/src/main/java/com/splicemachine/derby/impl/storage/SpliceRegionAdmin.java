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

package com.splicemachine.derby.impl.storage;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.stream.function.FileFunction;
import com.splicemachine.derby.stream.function.MutableCSVTokenizer;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.derby.stream.utils.BooleanList;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.DataResultScanner;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.spark_project.guava.collect.Lists;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

/**
 * Created by jyuan on 8/14/17.
 */
public class SpliceRegionAdmin {

    private static final Logger LOG=Logger.getLogger(SpliceRegionAdmin.class);

    public static void GET_REGIONS(String schemaName,
                                   String tableName,
                                   String indexName,
                                   String startKey,
                                   String endKey,
                                   String columnDelimiter,
                                   String characterDelimiter,
                                   String timestampFormat,
                                   String dateFormat,
                                   String timeFormat,
                                   ResultSet[] results) throws Exception {

        // Check parameters
        schemaName = schemaName.toUpperCase();
        tableName = tableName.toUpperCase();

        TableDescriptor td = getTableDescriptor(schemaName, tableName);
        ConglomerateDescriptor index = null;
        if (indexName != null) {
            indexName = indexName.toUpperCase();
            index = getIndex(td, indexName);
            if (index == null) {
                throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND, indexName);
            }
        }

        // get row format for primary key or index
        ExecRow execRow = getExecRow(td, index);
        if (execRow == null) {
            throw StandardException.newException(SQLState.NO_PRIMARY_KEY,
                    td.getSchemaDescriptor().getSchemaName(), td.getName());
        }

        DataHash dataHash = getEncoder(td, index, execRow);
        KeyHashDecoder decoder =  dataHash.getDecoder();

        byte[] startKeyBytes = startKey != null ? getRowKey(td, index, execRow, startKey, columnDelimiter,
                characterDelimiter, timeFormat, dateFormat, timestampFormat) : new byte[0];
        byte[] endKeyBytes = endKey != null ? getRowKey(td, index, execRow, endKey, columnDelimiter,
                characterDelimiter, timeFormat, dateFormat, timestampFormat) : new byte[0];

        ResultColumnDescriptor[] columnInfo=new ResultColumnDescriptor[9];
        columnInfo[0]=new GenericColumnDescriptor("ENCODED_REGION_NAME",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,50));
        columnInfo[1]=new GenericColumnDescriptor("SPLICE_START_KEY",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,1024));
        columnInfo[2]=new GenericColumnDescriptor("SPLICE_END_KEY",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,1024));
        columnInfo[3]=new GenericColumnDescriptor("HBASE_START_KEY",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,1024));
        columnInfo[4]=new GenericColumnDescriptor("HBASE_END_KEY",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,1024));
        columnInfo[5]=new GenericColumnDescriptor("NUM_HFILES",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
        columnInfo[6]=new GenericColumnDescriptor("SIZE",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
        columnInfo[7]=new GenericColumnDescriptor("LAST_MODIFICATION_TIME",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP));
        columnInfo[8]=new GenericColumnDescriptor("REGION_NAME",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,1024));

        ArrayList<ExecRow> rows=new ArrayList<>();
        DataValueDescriptor[] dvds=new DataValueDescriptor[]{
                new SQLVarchar(),
                new SQLVarchar(),
                new SQLVarchar(),
                new SQLVarchar(),
                new SQLVarchar(),
                new SQLInteger(),
                new SQLLongint(),
                new SQLTimestamp(),
                new SQLVarchar()
        };

        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.getLanguageConnection();
        Activation activation = conn.getLanguageConnection().getLastActivation();

        TransactionController transactionExecute=activation.getLanguageConnectionContext().getTransactionExecute();
        Transaction rawStoreXact=((TransactionManager)transactionExecute).getRawStoreXact();
        BaseSpliceTransaction rawTxn=(BaseSpliceTransaction)rawStoreXact;
        TxnView txnView = rawTxn.getActiveStateTxn();
        // get all partitions
        PartitionFactory partitionFactory = SIDriver.driver().getTableFactory();
        String conglomId = Long.toString(index == null ? td.getHeapConglomerateId() : index.getConglomerateNumber());
        Configuration conf = (Configuration) SIDriver.driver().getConfiguration().getConfigSource().unwrapDelegate();

        Partition table = partitionFactory.getTable(conglomId);
        Iterable<? extends Partition> partitions =  table.subPartitions(startKeyBytes, endKeyBytes, true);
        List<Partition> partitionList = Lists.newArrayList(partitions);


        for (Partition partition : partitionList) {

            ExecRow row = new ValueRow(dvds.length);
            byte[] start = partition.getStartKey();
            byte[] end = partition.getEndKey();
            String name = partition.getName();
            String encodedName = partition.getEncodedName();
            try {
                decoder.set(start, 0, start.length);
                decoder.decode(execRow);
            }
            catch(Exception e) {
                SpliceLogUtils.info(LOG,"Start key %s cannot be decoded", Bytes.toHex(start));
                decodeNextRow(partition, decoder, execRow, false, txnView);
            }
            row.setRowArray(dvds);
            dvds[0].setValue(encodedName);
            dvds[1].setValue(execRow.toString());

            try {
                decoder.set(end, 0, end.length);
                decoder.decode(execRow);
            } catch (Exception e) {
                SpliceLogUtils.info(LOG,"End key %s cannot be decoded", Bytes.toHex(end));
                decodeNextRow(partition, decoder, execRow, true, txnView);
            }
            dvds[2].setValue(execRow.toString());

            dvds[3].setValue(Bytes.toStringBinary(start));
            dvds[4].setValue(Bytes.toStringBinary(end));

            FileStatus[] fileStatuses = getRegionFileStatuses(conf, conglomId, encodedName);
            int count = 0;
            long size = 0;
            long lastModificationTime = getFamilyModificationTime(conf, conglomId, encodedName);
            for (FileStatus fileStatus : fileStatuses) {
                if (!fileStatus.isFile())
                    continue;
                count++;
                size += fileStatus.getLen();
                if (fileStatus.getModificationTime() > lastModificationTime)
                    lastModificationTime = fileStatus.getModificationTime();

            }
            dvds[5].setValue(count);
            dvds[6].setValue(size);
            dvds[7].setValue(new DateTime(lastModificationTime));
            dvds[8].setValue(name);

            rows.add(row.getClone());
        }

        EmbedConnection defaultConn=(EmbedConnection) SpliceAdmin.getDefaultConn();
        Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap=new IteratorNoPutResultSet(rows,columnInfo,lastActivation);
        resultsToWrap.openCore();
        results[0] = new EmbedResultSet40(defaultConn,resultsToWrap,false,null,true);
    }

    public static void MERGE_REGIONS(String schemaName,
                                     String tableName,
                                     String indexName,
                                     String regionName1,
                                     String regionName2) throws Exception {

        // Check parameters
        schemaName = schemaName.toUpperCase();
        tableName = tableName.toUpperCase();

        TableDescriptor td = getTableDescriptor(schemaName, tableName);
        ConglomerateDescriptor index = null;
        if (indexName != null) {
            indexName = indexName.toUpperCase();
            index = getIndex(td, indexName);
            if (index == null) {
                throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND, indexName);
            }
        }

        PartitionAdmin admin= SIDriver.driver().getTableFactory().getAdmin();
        Partition p1 = getPartition(td, index, regionName1);
        Partition p2 = getPartition(td, index, regionName2);

        if (Bytes.compareTo(p1.getEndKey(), p2.getStartKey()) == 0 ||
                Bytes.compareTo(p1.getStartKey(), p2.getEndKey()) == 0) {
            admin.mergeRegions(regionName1, regionName2);
        }
        else
            throw StandardException.newException(SQLState.REGION_NOT_ADJACENT, regionName1, regionName2);

    }
    public static void COMPACT_REGION(String schemaName,
                                      String tableName,
                                      String indexName,
                                      String regionName) throws Exception {

        compact(schemaName, tableName, indexName, regionName, false);
    }

    public static void MAJOR_COMPACT_REGION(String schemaName,
                                            String tableName,
                                            String indexName,
                                            String regionName) throws Exception {

        compact(schemaName, tableName, indexName, regionName, true);
    }

    public static void compact(String schemaName,
                               String tableName,
                               String indexName,
                               String regionName,
                               boolean isMajor) throws Exception {
        // Check parameters
        schemaName = schemaName.toUpperCase();
        tableName = tableName.toUpperCase();

        TableDescriptor td = getTableDescriptor(schemaName, tableName);
        ConglomerateDescriptor index = null;
        if (indexName != null) {
            indexName = indexName.toUpperCase();
            index = getIndex(td, indexName);
            if (index == null) {
                throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND, indexName);
            }
        }

        Partition partition = getPartition(td, index, regionName);
        partition.flush();
        partition.compact(isMajor);
    }

    private static Partition getPartition(TableDescriptor td, ConglomerateDescriptor index, String regionName) throws Exception{
        // Get all regions for the table or index
        PartitionAdmin admin= SIDriver.driver().getTableFactory().getAdmin();
        String conglomId = Long.toString(index == null ? td.getHeapConglomerateId() : index.getConglomerateNumber());
        Iterable<? extends Partition> partitions =  admin.allPartitions(conglomId);
        List<Partition> partitionList = Lists.newArrayList(partitions);
        // Find the region and get its start key
        Partition partition = null;
        for(Partition p:partitionList) {
            String s = p.getEncodedName();
            if (s.compareTo(regionName) == 0) {
                partition = p;
                break;
            }
        }
        if (partition == null) {
            throw StandardException.newException(SQLState.REGION_DOESNOT_EXIST, regionName);
        }
        return partition;
    }

    /**
     * Returns startkey, endkey and encoded region name for the hbase region that contains the row that specified
     * in splitKey. The value can be a primary key value or an index value
     * @param schemaName
     * @param tableName
     * @param indexName
     * @param splitKey
     * @param columnDelimiter
     * @param characterDelimiter
     * @param timestampFormat
     * @param dateFormat
     * @param timeFormat
     * @param results
     * @throws Exception
     */
    public static void GET_ENCODED_REGION_NAME(String schemaName,
                                               String tableName,
                                               String indexName,
                                               String splitKey,
                                               String columnDelimiter,
                                               String characterDelimiter,
                                               String timestampFormat,
                                               String dateFormat,
                                               String timeFormat,
                                               ResultSet[] results) throws Exception {

        // Check parameters
        schemaName = schemaName.toUpperCase();
        tableName = tableName.toUpperCase();

        TableDescriptor td = getTableDescriptor(schemaName, tableName);
        ConglomerateDescriptor index = null;
        if (indexName != null) {
            indexName = indexName.toUpperCase();
            index = getIndex(td, indexName);
            if (index == null) {
                throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND, indexName);
            }
        }

        // get row format for primary key or index
        ExecRow execRow = getExecRow(td, index);
        if (execRow == null) {
            throw StandardException.newException(SQLState.NO_PRIMARY_KEY,
                    td.getSchemaDescriptor().getSchemaName(), td.getName());
        }

        byte[] rowKey = getRowKey(td, index, execRow, splitKey, columnDelimiter, characterDelimiter, timeFormat,
                dateFormat, timestampFormat);

        // Find the hbase region that contains the rowKey
        long conglomerateId = indexName != null ? index.getConglomerateNumber() : td.getHeapConglomerateId();
        Partition partition = getPartition(rowKey, conglomerateId);

        ArrayList<ExecRow> rows=new ArrayList<>(1);
        DataValueDescriptor[] dvds=new DataValueDescriptor[]{
                new SQLVarchar(),
                new SQLVarchar(),
                new SQLVarchar(),
        };

        // return startkey, endkey and encoded region that contains the row key
        ExecRow row=new ValueRow(dvds.length);
        row.setRowArray(dvds);
        dvds[0].setValue(partition.getEncodedName());
        dvds[1].setValue(Bytes.toStringBinary(partition.getStartKey()));
        dvds[2].setValue(Bytes.toStringBinary(partition.getEndKey()));

        rows.add(row);
        ResultColumnDescriptor[] columnInfo=new ResultColumnDescriptor[3];
        columnInfo[0]=new GenericColumnDescriptor("ENCODED_REGION_NAME",
                                                  DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,50));
        columnInfo[1]=new GenericColumnDescriptor("START_KEY",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,1024));
        columnInfo[2]=new GenericColumnDescriptor("END_KEY",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,1024));

        EmbedConnection defaultConn=(EmbedConnection) SpliceAdmin.getDefaultConn();
        Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap=new IteratorNoPutResultSet(rows,columnInfo,lastActivation);
        resultsToWrap.openCore();
        results[0] = new EmbedResultSet40(defaultConn,resultsToWrap,false,null,true);
    }


    /**
     * return startkey of the specified region. The region can be a hbase region that stores
     * data for splice base table or index
     * @param schemaName
     * @param tableName
     * @param indexName
     * @param encodedRegionName
     * @param results
     * @throws Exception
     */
    public static void GET_START_KEY(String schemaName,
                                     String tableName,
                                     String indexName,
                                     String encodedRegionName,
                                     ResultSet[] results) throws Exception {

        // Validate parameters
        schemaName = schemaName.toUpperCase();
        tableName = tableName.toUpperCase();
        TableDescriptor td = getTableDescriptor(schemaName, tableName);

        ConglomerateDescriptor index = null;
        if (indexName != null) {
            indexName = indexName.toUpperCase();
            index = getIndex(td, indexName);
            if (index == null) {
                throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND, indexName);
            }
        }

        // Get all regions for the table or index
        PartitionAdmin admin= SIDriver.driver().getTableFactory().getAdmin();
        String conglomId = Long.toString(index == null ? td.getHeapConglomerateId() : index.getConglomerateNumber());
        Iterable<? extends Partition> partitions =  admin.allPartitions(conglomId);
        List<Partition> partitionList = Lists.newArrayList(partitions);
        byte[] rowKey  = null;
        Partition partition = null;
        // Find the region and get its start key
        for(Partition p:partitionList) {
            String s = p.getEncodedName();
            if (s.compareTo(encodedRegionName) == 0) {
                rowKey = p.getStartKey();
                partition = p;
                break;
            }
        }
        if (rowKey == null) {
            throw StandardException.newException(SQLState.REGION_DOESNOT_EXIST, encodedRegionName);
        }

        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.getLanguageConnection();
        Activation activation = conn.getLanguageConnection().getLastActivation();

        TransactionController transactionExecute=activation.getLanguageConnectionContext().getTransactionExecute();
        Transaction rawStoreXact=((TransactionManager)transactionExecute).getRawStoreXact();
        BaseSpliceTransaction rawTxn=(BaseSpliceTransaction)rawStoreXact;
        TxnView txnView = rawTxn.getActiveStateTxn();

        // Decode startKey from byte[] to ExecRow
        ExecRow execRow = getExecRow(td, index);
        if (execRow != null) {
            DataHash dataHash = getEncoder(td, index, execRow);
            KeyHashDecoder decoder =  dataHash.getDecoder();
            try {
                decoder.set(rowKey, 0, rowKey.length);
                decoder.decode(execRow);
            }
            catch(Exception e) {
                SpliceLogUtils.info(LOG,"Start key %s cannot be decoded", Bytes.toHex(rowKey));
                decodeNextRow(partition, decoder, execRow, false, txnView);
            }
        }

        ArrayList<ExecRow> rows=new ArrayList<>(1);
        DataValueDescriptor[] dvds=new DataValueDescriptor[]{
                new SQLVarchar(),
        };

        ExecRow row=new ValueRow(dvds.length);
        row.setRowArray(dvds);
        dvds[0].setValue(execRow != null ? execRow.toString() : Bytes.toStringBinary(rowKey));

        rows.add(row);
        ResultColumnDescriptor[] columnInfo=new ResultColumnDescriptor[1];
        columnInfo[0]=new GenericColumnDescriptor("START_KEY",
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 1024));

        EmbedConnection defaultConn=(EmbedConnection) SpliceAdmin.getDefaultConn();
        Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap=new IteratorNoPutResultSet(rows,columnInfo,lastActivation);
        resultsToWrap.openCore();
        results[0] = new EmbedResultSet40(defaultConn,resultsToWrap,false,null,true);

    }

    /**
     *
     * @param rowKey rowkey for primary key or index value
     * @param conglomId conglomerate number for splice table
     * @return a region that contains the provided rowkey
     * @throws IOException
     */
    private static Partition getPartition(byte[] rowKey, long conglomId) throws IOException {

        PartitionAdmin admin= SIDriver.driver().getTableFactory().getAdmin();
        Iterable<? extends Partition> partitions =  admin.allPartitions(Long.toString(conglomId));
        List<Partition> partitionList = Lists.newArrayList(partitions);
        for(Partition p : partitionList) {
            byte[] start = p.getStartKey();
            byte[] end = p.getEndKey();
            if (Bytes.compareTo(rowKey, start) >= 0 && Bytes.compareTo(rowKey, end) < 0) {
                return p;
            }
        }
        assert false;
        return null;
    }

    private static DataHash getEncoder(TableDescriptor td, ConglomerateDescriptor index, ExecRow execRow) throws Exception {

        String version = td.getVersion();
        DescriptorSerializer[] serializers= VersionedSerializers
                .forVersion(version,true)
                .getSerializers(execRow.getRowArray());
        int[] rowColumns = IntArrays.count(execRow.nColumns());
        boolean[] sortOrder = null;
        if (index != null) {
            IndexRowGenerator irg = index.getIndexDescriptor();
            IndexDescriptor id = irg.getIndexDescriptor();
            sortOrder = id.isAscending();
        }
        DataHash dataHash = BareKeyHash.encoder(rowColumns, sortOrder, serializers);
        return dataHash;
    }

    private static TableDescriptor getTableDescriptor(String schemaName, String tableName) throws Exception {
        EmbedConnection defaultConn=(EmbedConnection) SpliceAdmin.getDefaultConn();
        Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
        LanguageConnectionContext lcc = lastActivation.getLanguageConnectionContext();
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        DataDictionary dd = lcc.getDataDictionary();

        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
        if (sd == null){
            throw StandardException.newException(SQLState.LANG_SCHEMA_DOES_NOT_EXIST, schemaName);
        }

        TableDescriptor td = dd.getTableDescriptor(tableName, sd, tc);
        if (td == null)
        {
            throw StandardException.newException(SQLState.TABLE_NOT_FOUND, tableName);
        }

        return td;
    }

    /**
     *
     * @param td table descriptor
     * @param index conglomerate descriptor for index
     * @return ExecRow for primary key or index
     * @throws Exception
     */
    private static ExecRow getExecRow(TableDescriptor td, ConglomerateDescriptor index) throws Exception {

        ExecRow execRow = null;
        if (index != null) {
            IndexRowGenerator irg = index.getIndexDescriptor();
            IndexDescriptor id = irg.getIndexDescriptor();
            boolean isUnique = id.isUnique();
            int[] positions = id.baseColumnPositions();
            int[] typeFormatIds = new int[positions.length];
            int i = 0;
            for(int position : positions) {
                ColumnDescriptor cd = td.getColumnDescriptor(position);
                typeFormatIds[i++] = cd.getType().getNull().getTypeFormatId();
            }
            execRow = WriteReadUtils.getExecRowFromTypeFormatIds(typeFormatIds);
        }
        else {
            ReferencedKeyConstraintDescriptor pk = td.getPrimaryKey();
            if (pk != null) {

                ColumnDescriptorList cds = pk.getColumnDescriptors();
                int[] typeFormatIds = cds.getFormatIds();
                execRow = WriteReadUtils.getExecRowFromTypeFormatIds(typeFormatIds);
            }
        }
        return execRow;
    }

    private static CsvPreference createCsvPreference(String columnDelimiter, String characterDelimiter) {
        SConfiguration config = EngineDriver.driver().getConfiguration();
        int maxQuotedLines = config.getImportMaxQuotedColumnLines();
        final char DEFAULT_COLUMN_DELIMITTER = ",".charAt(0);
        final char DEFAULT_STRIP_STRING = "\"".charAt(0);
        CsvPreference preference=new CsvPreference.Builder(
                characterDelimiter!=null && characterDelimiter.length()>0?characterDelimiter.charAt(0):DEFAULT_STRIP_STRING,
                columnDelimiter!=null && columnDelimiter.length()>0?columnDelimiter.charAt(0):DEFAULT_COLUMN_DELIMITTER,
                "\n").maxLinesPerRow(maxQuotedLines).build();
        return preference;
    }

    private static  ConglomerateDescriptor getIndex(TableDescriptor td, String indexName) {
        ConglomerateDescriptorList list = td.getConglomerateDescriptorList();
        for (ConglomerateDescriptor searchCD :list) {
            if (searchCD.isIndex() && !searchCD.isPrimaryKey() && indexName != null &&
                    searchCD.getObjectName().compareToIgnoreCase(indexName) == 0) {
                return searchCD;
            }
        }

        return null;
    }

    private static byte[] getRowKey(TableDescriptor td, ConglomerateDescriptor index, ExecRow execRow,
                             String splitKey, String columnDelimiter, String characterDelimiter,
                             String timeFormat, String dateFormat, String timestampFormat) throws Exception{
        // set up csv reader
        CsvPreference preference = createCsvPreference(columnDelimiter, characterDelimiter);
        Reader reader = new StringReader(splitKey);
        MutableCSVTokenizer tokenizer = new MutableCSVTokenizer(reader,preference);
        tokenizer.setLine(splitKey);
        List<String> read=tokenizer.read();
        BooleanList quotedColumns=tokenizer.getQuotedColumns();

        // return the primary key or index value in ExecRow
        ExecRow dataRow = FileFunction.getRow(read, quotedColumns, null,
                execRow, null, timeFormat, dateFormat, timestampFormat);

        // Encoded row value
        DataHash dataHash = getEncoder(td, index, execRow);
        dataHash.setRow(dataRow);
        byte[] rowKey = dataHash.encode();
        return rowKey;
    }

    private static FileStatus[] getRegionFileStatuses(Configuration conf,
                                                      String conglomId,
                                                      String encodedName) throws IOException{

        Path region = getRegionPath(conf, conglomId, encodedName);
        FileSystem fs = region.getFileSystem(conf);
        Path family = new Path(region, "V");

        FileStatus[] fileStatuses = fs.listStatus(family);
        return fileStatuses;
    }

    private static long getFamilyModificationTime(Configuration conf,
                                                  String conglomId,
                                                  String encodedName) throws IOException{

        Path region = getRegionPath(conf, conglomId, encodedName);
        FileSystem fs = region.getFileSystem(conf);
        FileStatus[] fileStatuses = fs.listStatus(region);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.getPath().getName().compareTo("V") == 0) {
                return fileStatus.getModificationTime();
            }
        }
        return 0;
    }

    private static Path getRegionPath(Configuration conf, String conglomId, String encodedName) throws IOException {
        Path p = new Path(conf.get(HConstants.HBASE_DIR));
        FileSystem fs = p.getFileSystem(conf);
        p =  p.makeQualified(fs);
        Path data = new Path(p, "data");
        Path splice = new Path(data, "splice");
        Path table = new Path(splice, conglomId);
        Path region = new Path(table, encodedName);
        return region;
    }

    private static void decodeNextRow(Partition partition, KeyHashDecoder decoder, ExecRow execRow, boolean reverse, TxnView txnView) throws IOException, StandardException{

        TxnOperationFactory txnOperationFactory = SIDriver.driver().getOperationFactory();
        DataScan scan = txnOperationFactory.newDataScan(txnView);

        scan.setSmall(true);
        scan.cacheRows(1);

        if (!reverse) {
            scan.startKey(partition.getStartKey());
        }
        else {
            scan.startKey(partition.getEndKey());
            scan.reverseOrder();
        }
        DataResultScanner scanner = partition.openResultScanner(scan);
        DataResult result = scanner.next();
        if (result != null) {
            byte[] key = result.key();
            decoder.set(key, 0, key.length);
            decoder.decode(execRow);
            SpliceLogUtils.info(LOG, "Use %s %s as %s", Bytes.toHex(key), execRow, reverse ? "endKey" : "startKey");
        }
    }
}
