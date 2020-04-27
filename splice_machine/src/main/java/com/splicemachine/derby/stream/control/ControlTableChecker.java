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

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.stream.function.IndexTransformFunction;
import com.splicemachine.derby.stream.function.KVPairFunction;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.Partition;
import org.spark_project.guava.collect.ArrayListMultimap;
import org.spark_project.guava.collect.ListMultimap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.storage.CheckTableJob.IndexFilter;
import com.splicemachine.derby.impl.storage.CheckTableJob.LeadingIndexColumnInfo;
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
    private KeyHashDecoder indexKeyDecoder;
    private ExecRow indexKeyTemplate;
    private long maxCheckTableErrors;
    private ListMultimap<String, byte[]> indexData;
    private Map<String, ExecRow> tableData;
    private boolean fix;
    private TxnView txn;
    private long conglomerate;
    private DDLMessage.TentativeIndex tentativeIndex;

    public ControlTableChecker (String schemaName,
                                String tableName,
                                DataSet table,
                                KeyHashDecoder tableKeyDecoder,
                                ExecRow tableKey,
                                TxnView txn,
                                boolean fix) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.baseTable = table;
        this.tableKeyDecoder = tableKeyDecoder;
        this.tableKeyTemplate = tableKey;
        this.maxCheckTableErrors = SIDriver.driver().getConfiguration().getMaxCheckTableErrors();
        this.txn = txn;
        this.fix = fix;
    }

    @Override
    public List<String> checkIndex(PairDataSet index,
                                   String indexName,
                                   KeyHashDecoder indexKeyDecoder,
                                   ExecRow indexKey,
                                   LeadingIndexColumnInfo leadingIndexColumnInfo,
                                   long conglomerate,
                                   DDLMessage.TentativeIndex tentativeIndex) throws Exception {

        this.indexName = indexName;
        this.indexKeyDecoder = indexKeyDecoder;
        this.indexKeyTemplate = indexKey;
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
        Iterator<Tuple2<String, byte[]>> indexSource = ((ControlPairDataSet)index).source;
        while(indexSource.hasNext()) {
            indexCount++;
            Tuple2<String, byte[]> t = indexSource.next();
            String baseRowId = t._1;
            byte[] indexKey = t._2;
            indexData.put(baseRowId, indexKey);
        }
    }

    private List<String> checkDuplicateIndexes() throws StandardException {
        List<String> messages = new LinkedList<>();
        ArrayListMultimap<String, byte[]> result = ArrayListMultimap.create();
        long duplicateIndexCount = 0;
        for (String baseRowId : indexData.keySet()) {
            List<byte[]> rows = indexData.get(baseRowId);
            if (rows.size() > 1) {
                duplicateIndexCount += rows.size();
                result.putAll(baseRowId, rows);
            }
        }
        int i = 0;
        if (duplicateIndexCount > 0) {
            messages.add(String.format("The following %d indexes are duplicates:", duplicateIndexCount));
            for (String baseRowId : result.keySet()) {
                List<byte[]> indexKeys = result.get(baseRowId);
                for (byte[] indexKey : indexKeys) {
                    if (i >= maxCheckTableErrors) {
                        messages.add("...");
                        return messages;
                    }
                    indexKeyDecoder.set(indexKey, 0, indexKey.length);
                    indexKeyDecoder.decode(indexKeyTemplate);
                    messages.add(indexKeyTemplate.getClone().toString()+ "=>" + baseRowId);
                    i++;
                }
            }
        }

        return messages;
    }

    private List<String> checkInvalidIndexes() throws StandardException {
        invalidIndexCount = 0;

        ArrayListMultimap<String, byte[]> result = ArrayListMultimap.create();
        for (String baseRowId : indexData.keySet()) {
            if (!tableData.containsKey(baseRowId)) {
                List<byte[]> rows = indexData.get(baseRowId);
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

    private List<String> reportInvalidIndexes(ArrayListMultimap<String, byte[]> result) throws StandardException {
        List<String> messages = new LinkedList<>();
        int i = 0;
        if (fix) {
            messages.add(String.format("The following %d indexes are deleted:", invalidIndexCount));
        }
        else {
            messages.add(String.format("The following %d indexes are invalid:", invalidIndexCount));
        }

        for (String baseRowId : result.keySet()) {
            List<byte[]> keys = result.get(baseRowId);
            for (byte[] key : keys) {
                if (i >= maxCheckTableErrors) {
                    messages.add("...");
                    return messages;
                }
                indexKeyDecoder.set(key, 0, key.length);
                indexKeyDecoder.decode(indexKeyTemplate);
                messages.add(indexKeyTemplate.getClone().toString() + "=>" + baseRowId);
                i++;
            }
        }
        return  messages;
    }


    private void fixInvalidIndexes(ArrayListMultimap<String, byte[]> result) throws StandardException {
        try {
            WriteCoordinator writeCoordinator = PipelineDriver.driver().writeCoordinator();
            WriteConfiguration writeConfiguration = writeCoordinator.defaultWriteConfiguration();
            Partition indexPartition = SIDriver.driver().getTableFactory().getTable(Long.toString(conglomerate));
            RecordingCallBuffer<KVPair> writeBuffer = writeCoordinator.writeBuffer(indexPartition, txn, null, writeConfiguration);

            List<String> messages = new LinkedList<>();
            for (String baseRowId : result.keySet()) {
                List<byte[]> keys = result.get(baseRowId);
                for (byte[] key : keys) {
                    writeBuffer.add(new KVPair(key, new byte[0], KVPair.Type.DELETE));
                }
            }
            writeBuffer.flushBuffer();
        }
        catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    private List<String> checkMissingIndexes() throws StandardException {
        List<String> messages = new LinkedList<>();
        missingIndexCount = 0;
        Map<String, ExecRow> result = new HashMap<>();
        for (String rowId : tableData.keySet()) {
            if (!indexData.containsKey(rowId)) {
                missingIndexCount++;
                result.put(rowId, tableData.get(rowId));
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
        List<String> messages = new LinkedList<>();

        DataSet<ExecRow> dataSet = new ControlDataSet<>(result.values().iterator());
        PairDataSet dsToWrite = dataSet
                .map(new IndexTransformFunction(tentativeIndex), null, false, true, "Prepare Index")
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
            messages.add(String.format("Create index for the following %d rows from base table %s.%s:", result.size(),
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
                messages.add(tableKeyTemplate.getClone().toString());
            }
            else {
                messages.add(entry.getKey());
            }
            i++;
        }

        return  messages;
    }
}
