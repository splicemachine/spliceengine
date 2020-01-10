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
    private PairDataSet<String, byte[]> filteredTable;
    private KeyHashDecoder tableKeyDecoder;
    private ExecRow tableKeyTemplate;
    private KeyHashDecoder indexKeyDecoder;
    private ExecRow indexKeyTemplate;
    private long maxCheckTableErrors;
    private LeadingIndexColumnInfo leadingIndexColumnInfo;
    private ListMultimap<String, byte[]> indexData;
    private Map<String, byte[]> tableData;

    public ControlTableChecker (String schemaName,
                                String tableName,
                                DataSet table,
                                KeyHashDecoder tableKeyDecoder,
                                ExecRow tableKey) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.baseTable = table;
        this.tableKeyDecoder = tableKeyDecoder;
        this.tableKeyTemplate = tableKey;
        this.maxCheckTableErrors = SIDriver.driver().getConfiguration().getMaxCheckTableErrors();
    }

    @Override
    public List<String> checkIndex(PairDataSet index,
                                   String indexName,
                                   KeyHashDecoder indexKeyDecoder,
                                   ExecRow indexKey,
                                   LeadingIndexColumnInfo leadingIndexColumnInfo) throws Exception {

        this.indexName = indexName;
        this.indexKeyDecoder = indexKeyDecoder;
        this.indexKeyTemplate = indexKey;
        this.leadingIndexColumnInfo = leadingIndexColumnInfo;

        filteredTable = baseTable.filter(new IndexFilter<>(this.leadingIndexColumnInfo))
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
        Iterator<Tuple2<String, byte[]>> tableSource = ((ControlPairDataSet) table).source;
        while (tableSource.hasNext()) {
            tableCount++;
            Tuple2<String, byte[]> t = tableSource.next();
            String rowId = t._1;
            byte[] row = t._2;
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
        List<String> messages = new LinkedList<>();
        ArrayListMultimap<String, byte[]> result = ArrayListMultimap.create();
        for (String baseRowId : indexData.keySet()) {
            if (!tableData.containsKey(baseRowId)) {
                List<byte[]> rows = indexData.get(baseRowId);
                result.putAll(baseRowId, rows);
                invalidIndexCount += rows.size();
            }
        }

        int i = 0;
        if (invalidIndexCount > 0) {
            messages.add(String.format("The following %d indexes are invalid:", invalidIndexCount));
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
        }
        return  messages;
    }

    private List<String> checkMissingIndexes() throws StandardException {
        List<String> messages = new LinkedList<>();
        missingIndexCount = 0;
        Map<String, byte[]> result = new HashMap<>();
        for (String rowId : tableData.keySet()) {
            if (!indexData.containsKey(rowId)) {
                missingIndexCount++;
                result.put(rowId, tableData.get(rowId));
            }
        }

        int i = 0;
        if (missingIndexCount > 0) {
            messages.add(String.format("The following %d rows from base table %s.%s are not indexed:", result.size(), schemaName, tableName));
            for (Map.Entry<String, byte[]> entry : result.entrySet()) {
                if (i >= maxCheckTableErrors) {
                    messages.add("...");
                    break;
                }
                byte[] key = entry.getValue();
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
        }
        return  messages;
    }
}
