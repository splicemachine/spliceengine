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

package com.splicemachine.derby.stream.control;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.stream.function.IndexTransformFunction;
import com.splicemachine.derby.stream.function.KVPairFunction;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.Partition;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import splice.com.google.common.collect.ArrayListMultimap;
import splice.com.google.common.collect.ListMultimap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.storage.CheckTableUtils.IndexFilter;
import com.splicemachine.derby.impl.storage.CheckTableUtils.LeadingIndexColumnInfo;
import com.splicemachine.derby.impl.storage.KeyByRowIdFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iapi.TableChecker;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.si.impl.driver.SIDriver;
import scala.Tuple2;

import java.util.*;

/**
 * Created by jyuan on 2/12/18.
 */
public class ControlTableChecker implements TableChecker {

    private String schemaName;
    private String tableName;
    private String indexName;
    private long tableCount = 0;
    private long indexCount = 0;
    private long invalidIndexCount = 0;
    private long missingIndexCount = 0;
    private DataSet<ExecRow> baseTable;
    private PairDataSet<String, ExecRow> filteredTable;
    private KeyHashDecoder tableKeyDecoder;
    private ExecRow tableKeyTemplate;
    private long maxCheckTableErrors;
    private ListMultimap<String, Tuple2<byte[], ExecRow>> indexData;
    private Map<String, ExecRow> tableData;
    private boolean fix;
    private TxnView txn;
    private long conglomerate;
    private DDLMessage.TentativeIndex tentativeIndex;
    private int[] baseColumnMap;
    private boolean isSystemTable;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ControlTableChecker (String schemaName,
                                String tableName,
                                DataSet table,
                                KeyHashDecoder tableKeyDecoder,
                                ExecRow tableKey,
                                TxnView txn,
                                boolean fix,
                                int[] baseColumnMap,
                                boolean isSystemTable) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.baseTable = table;
        this.tableKeyDecoder = tableKeyDecoder;
        this.tableKeyTemplate = tableKey;
        this.maxCheckTableErrors = SIDriver.driver().getConfiguration().getMaxCheckTableErrors();
        this.txn = txn;
        this.fix = fix;
        this.baseColumnMap = baseColumnMap;
        this.isSystemTable = isSystemTable;
    }

    @SuppressFBWarnings(value = "URF_UNREAD_FIELD",justification = "Intentional")
    @Override
    public List<String> checkIndex(PairDataSet index,
                                   String indexName,
                                   LeadingIndexColumnInfo leadingIndexColumnInfo,
                                   long conglomerate,
                                   DDLMessage.TentativeIndex tentativeIndex) throws Exception {

        this.indexName = indexName;
        this.conglomerate = conglomerate;
        this.tentativeIndex = tentativeIndex;

        filteredTable = baseTable.filter(new IndexFilter<>(leadingIndexColumnInfo))
                .index(new KeyByRowIdFunction());
        // Pull data into memory
        populateData(filteredTable, index);

        List<String> messages = new LinkedList<>();
        messages.addAll(checkMissingIndexes());
        messages.addAll(checkInvalidIndexes());
        if (indexCount - invalidIndexCount > tableCount - missingIndexCount) {
            messages.addAll(checkDuplicateIndexes());
        }
        return messages;
    }


    @Override
    public void setTableDataSet(DataSet tableDataSet) {
        this.baseTable = tableDataSet;
    }

    private void populateData(PairDataSet table, PairDataSet index) {

        tableData = new HashMap<>();
        tableCount=0;
        Iterator<Tuple2<String, ExecRow>> tableSource = ((ControlPairDataSet) table).source;
        while (tableSource.hasNext()) {
            tableCount++;
            Tuple2<String, ExecRow> t = tableSource.next();
            String rowId = t._1;
            ExecRow row = t._2;
            tableData.put(rowId, row);
        }

        indexData = ArrayListMultimap.create();
        indexCount = 0;
        Iterator<Tuple2<String, Tuple2<byte[], ExecRow>>> indexSource = ((ControlPairDataSet)index).source;
        while(indexSource.hasNext()) {
            indexCount++;
            Tuple2<String, Tuple2<byte[], ExecRow>> t = indexSource.next();
            String baseRowId = t._1;
            Tuple2<byte[], ExecRow> row = t._2;
            indexData.put(baseRowId, row);
        }
    }

    private List<String> checkDuplicateIndexes() throws StandardException {
        ArrayListMultimap<String, Tuple2<byte[], ExecRow>> result = ArrayListMultimap.create();
        long duplicateIndexCount = 0;
        for (String baseRowId : indexData.keySet()) {
            List<Tuple2<byte[], ExecRow>> rows = indexData.get(baseRowId);
            if (rows.size() > 1) {
                duplicateIndexCount += rows.size()-1;
                result.putAll(baseRowId, rows);
            }
        }
        if (duplicateIndexCount > 0) {
            if (fix) {
                fixDuplicateIndexes(result);
            }
            return reportDuplicateIndexes(result, duplicateIndexCount);
        }
        return new LinkedList<>();
    }

    private void fixDuplicateIndexes(ArrayListMultimap<String, Tuple2<byte[], ExecRow>> result) throws StandardException {
        try {
            WriteCoordinator writeCoordinator = PipelineDriver.driver().writeCoordinator();
            WriteConfiguration writeConfiguration = writeCoordinator.defaultWriteConfiguration();
            Partition indexPartition = SIDriver.driver().getTableFactory().getTable(Long.toString(conglomerate));
            RecordingCallBuffer<KVPair> writeBuffer = writeCoordinator.writeBuffer(indexPartition, txn, null, writeConfiguration);

            List<Integer> indexToMain = tentativeIndex.getIndex().getIndexColsToMainColMapList();
            for (String baseRowId : result.keySet()) {
                ExecRow baseRow = tableData.get(baseRowId);
                List<Tuple2<byte[], ExecRow>> indexRows = result.get(baseRowId);
                for (Tuple2<byte[], ExecRow> indexRow : indexRows) {
                    boolean duplicate = false;
                    DataValueDescriptor[] dvds = indexRow._2.getRowArray();
                    for (int i = 0; i < dvds.length - 1; ++i) {
                        int col = baseColumnMap[indexToMain.get(i) - 1];
                        if (!dvds[i].equals(baseRow.getColumn(col + 1))) {
                            duplicate = true;
                            break;
                        }
                    }
                    if (duplicate) {
                        writeBuffer.add(new KVPair(indexRow._2.getKey(), new byte[0], KVPair.Type.DELETE));
                    }
                }
            }
            writeBuffer.flushBuffer();
        }
        catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    private List<String> reportDuplicateIndexes(ArrayListMultimap<String, Tuple2<byte[], ExecRow>> result,
                                                long duplicateIndexCount) throws StandardException {
        List<String> messages = new LinkedList<>();
        if (fix) {
            messages.add(String.format("Removed the following %d indexes:", duplicateIndexCount));
        }
        else {
            messages.add(String.format("The following %d indexes are duplicates:", duplicateIndexCount));
        }

        int num = 0;
        List<Integer> indexToMain = tentativeIndex.getIndex().getIndexColsToMainColMapList();
        for (String baseRowId : result.keySet()) {
            ExecRow baseRow = tableData.get(baseRowId);
            List<Tuple2<byte[], ExecRow>> indexRows = result.get(baseRowId);
            for (Tuple2<byte[], ExecRow> indexRow : indexRows) {
                boolean duplicate = false;
                DataValueDescriptor[] dvds = indexRow._2.getRowArray();
                for (int i = 0; i < dvds.length - 1; ++i) {
                    int col = baseColumnMap[indexToMain.get(i)-1];
                    if (!dvds[i].equals(baseRow.getColumn(col+1))){
                        duplicate = true;
                        break;
                    }
                }
                if (duplicate) {
                    if (num >= maxCheckTableErrors) {
                        messages.add("...");
                        return messages;
                    }
                    messages.add(indexRow._2 + "@" + Bytes.toHex(indexRow._1) + "=>" + baseRowId);
                    num++;
                }
            }
        }

        return messages;
    }

    private List<String> checkInvalidIndexes() throws StandardException {
        invalidIndexCount = 0;

        ArrayListMultimap<String, Tuple2<byte[], ExecRow>> result = ArrayListMultimap.create();
        for (String baseRowId : indexData.keySet()) {
            if (!tableData.containsKey(baseRowId)) {
                List<Tuple2<byte[], ExecRow>> rows = indexData.get(baseRowId);
                result.putAll(baseRowId, rows);
                invalidIndexCount += rows.size();
            }
        }

        if (invalidIndexCount > 0) {
            if (fix) {
                fixInvalidIndexes(result);
            }
            return reportInvalidIndexes(result);
        }
        return new LinkedList<>();
    }

    private List<String> reportInvalidIndexes(ArrayListMultimap<String, Tuple2<byte[], ExecRow>> result) throws StandardException {
        List<String> messages = new LinkedList<>();
        int i = 0;
        if (fix) {
            messages.add(String.format("The following %d indexes are deleted:", invalidIndexCount));
        }
        else {
            messages.add(String.format("The following %d indexes are invalid:", invalidIndexCount));
        }

        for (String baseRowId : result.keySet()) {
            List<Tuple2<byte[], ExecRow>> indexRows = result.get(baseRowId);
            for (Tuple2<byte[], ExecRow> indexRow : indexRows) {
                if (i >= maxCheckTableErrors) {
                    messages.add("...");
                    return messages;
                }
                messages.add(indexRow._2 + "@" + Bytes.toHex(indexRow._1) + "=>" + baseRowId);
                i++;
            }
        }
        return  messages;
    }


    private void fixInvalidIndexes(ArrayListMultimap<String, Tuple2<byte[], ExecRow>> result) throws StandardException {
        try {
            WriteCoordinator writeCoordinator = PipelineDriver.driver().writeCoordinator();
            WriteConfiguration writeConfiguration = writeCoordinator.defaultWriteConfiguration();
            Partition indexPartition = SIDriver.driver().getTableFactory().getTable(Long.toString(conglomerate));
            RecordingCallBuffer<KVPair> writeBuffer = writeCoordinator.writeBuffer(indexPartition, txn, null, writeConfiguration);

            for (String baseRowId : result.keySet()) {
                List<Tuple2<byte[], ExecRow>> indexRows = result.get(baseRowId);
                for (Tuple2<byte[], ExecRow> indexRow : indexRows) {
                    writeBuffer.add(new KVPair(indexRow._2.getKey(), new byte[0], KVPair.Type.DELETE));
                }
            }
            writeBuffer.flushBuffer();
        }
        catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    private List<String> checkMissingIndexes() throws StandardException {
        missingIndexCount = 0;
        Map<String, ExecRow> result = new HashMap<>();
        for (Map.Entry<String, ExecRow>  entry : tableData.entrySet()) {
            String rowId = entry.getKey();
            ExecRow row = entry.getValue();
            if (!indexData.containsKey(rowId)) {
                missingIndexCount++;
                result.put(rowId, row);
            }
        }
        if (missingIndexCount > 0) {
            if (fix) {
                fixMissingIndexes(result);
            }
            return reportMissingIndexes(result);
        }
        return new LinkedList<>();
    }

    private void fixMissingIndexes(Map<String, ExecRow> result) throws StandardException {
        List<Integer> baseColumnMapList = Lists.newArrayList();
        for (int i = 0; i < baseColumnMap.length; ++i) {
            if (baseColumnMap[i] >= 0) {
                baseColumnMapList.add(i+1);
            }
        }
        DataSet<ExecRow> dataSet = new ControlDataSet<>(result.values().iterator());
        PairDataSet dsToWrite = dataSet
                .map(new IndexTransformFunction(tentativeIndex, baseColumnMapList, isSystemTable), null, false, true, "Prepare Index")
                .index(new KVPairFunction(), false, true, "Add missing indexes");
        DataSetWriter writer = dsToWrite.directWriteData()
                .destConglomerate(tentativeIndex.getIndex().getConglomerate())
                .txn(txn)
                .build();
        writer.write();
    }


    private List<String> reportMissingIndexes(Map<String, ExecRow> result) throws StandardException {
        List<String> messages = new LinkedList<>();

        int i = 0;
        if (fix) {
            messages.add(String.format("Created indexes for the following %d rows from base table %s.%s:", result.size(),
                    schemaName, tableName));
        }
        else {
            messages.add(String.format("The following %d rows from base table %s.%s are not indexed:", result.size(),
                    schemaName, tableName));
        }
        for (Map.Entry<String, ExecRow> entry : result.entrySet()) {
            if (i >= maxCheckTableErrors) {
                messages.add("...");
                break;
            }
            byte[] key = entry.getValue().getKey();
            if (tableKeyTemplate.nColumns() > 0) {
                tableKeyDecoder.set(key, 0, key.length);
                tableKeyDecoder.decode(tableKeyTemplate);
                ValueRow r = (ValueRow) tableKeyTemplate.getClone();
                messages.add(r.toString());
            }
            else {
                messages.add(entry.getKey());
            }
            i++;
        }
        return  messages;
    }
}
