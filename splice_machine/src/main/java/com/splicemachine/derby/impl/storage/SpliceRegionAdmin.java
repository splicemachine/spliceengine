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
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
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
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.IntArrays;
import org.apache.hadoop.hbase.util.Bytes;
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
        dvds[0] = new SQLChar(partition.getEncodedName());
        dvds[1] = new SQLChar(Bytes.toStringBinary(partition.getStartKey()));
        dvds[2] = new SQLChar(Bytes.toStringBinary(partition.getEndKey()));

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
        // Find the region and get its start key
        for(Partition p:partitionList) {
            String s = p.getEncodedName();
            if (s.compareTo(encodedRegionName) == 0) {
                rowKey = p.getStartKey();
                break;
            }
        }
        if (rowKey == null) {
            throw StandardException.newException(SQLState.REGION_DOESNOT_EXIST, encodedRegionName);
        }

        // Decode startkey from byte[] to ExecRow
        ExecRow execRow = getExecRow(td, index);
        if (execRow != null) {
            DataHash dataHash = getEncoder(td, index, execRow);
            KeyHashDecoder decoder =  dataHash.getDecoder();
            decoder.set(rowKey, 0, rowKey.length);
            decoder.decode(execRow);
        }

        ArrayList<ExecRow> rows=new ArrayList<>(1);
        DataValueDescriptor[] dvds=new DataValueDescriptor[]{
                new SQLVarchar(),
        };

        ExecRow row=new ValueRow(dvds.length);
        row.setRowArray(dvds);
        dvds[0] = new SQLChar(execRow != null ? execRow.toString() : Bytes.toStringBinary(rowKey));

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
        partitionList.sort(new Comparator<Partition>() {
            @Override
            public int compare(Partition o1, Partition o2) {
                byte[] k1 = o1.getStartKey();
                byte[] k2 = o2.getStartKey();
                if (k1 == null || k1.length == 0) {
                    if (k2 == null || k2.length == 0)
                        return 0;
                    else
                        return -1;
                }
                else {
                    if (k2 == null || k2.length == 0)
                        return 1;
                    else
                        return Bytes.compareTo(k1, k2);
                }
            }
        });

        //create a list of start keys
        byte[][] startKeys = new byte[partitionList.size()][];
        for (int i = 0; i < partitionList.size(); ++i) {
            Partition p = partitionList.get(i);
            byte[] k = p.getStartKey();
            startKeys[i] = k;
        }
        int index = binarySearchStartKey(startKeys, rowKey);
        assert index != -1;
        Partition p = partitionList.get(index);
        return p;
    }

    private static int binarySearchStartKey(byte[][] startKeys, byte[] rowKey) {

        int l = 0;
        int r = startKeys.length -1;

        while (l <= r) {
            int m = (l + r) / 2;
            if (m == 0 || m == startKeys.length -1 || Bytes.compareTo(startKeys[m], rowKey) == 0)
                return m;
            else if  (Bytes.compareTo(startKeys[m], rowKey) > 0 && Bytes.compareTo(startKeys[m-1], rowKey) <= 0){
                return m-1;
            }
            else if (Bytes.compareTo(startKeys[m], rowKey) < 0) {
                l = m + 1;
            }
            else if (Bytes.compareTo(startKeys[m], rowKey) > 0) {
                r = m - 1;
            }
        }
        return -1;
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
}
