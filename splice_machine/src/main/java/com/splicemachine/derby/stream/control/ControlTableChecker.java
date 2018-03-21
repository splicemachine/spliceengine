package com.splicemachine.derby.stream.control;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iapi.TableChecker;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.si.impl.driver.SIDriver;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    private PairDataSet<String, byte[]> table;
    private KeyHashDecoder tableKeyDecoder;
    private ExecRow tableKeyTemplate;
    private KeyHashDecoder indexKeyDecoder;
    private ExecRow indexKeyTemplate;
    private long maxCheckTableErrors;

    private ListMultimap<String, byte[]> indexData;
    private Map<String, byte[]> tableData;

    public ControlTableChecker (String schemaName,
                                String tableName,
                                PairDataSet table,
                                KeyHashDecoder tableKeyDecoder,
                                ExecRow tableKey) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.table = table;
        this.tableKeyDecoder = tableKeyDecoder;
        this.tableKeyTemplate = tableKey;
        this.maxCheckTableErrors = SIDriver.driver().getConfiguration().getMaxCheckTableErrors();
    }

    @Override
    public List<String> checkIndex(PairDataSet index,
                                   String indexName,
                                   KeyHashDecoder indexKeyDecoder,
                                   ExecRow indexKey) throws Exception {

        this.indexName = indexName;
        this.indexKeyDecoder = indexKeyDecoder;
        this.indexKeyTemplate = indexKey;

        // Pull data into memory
        populateData(table, index);

        List<String> messages = Lists.newLinkedList();
        messages.addAll(checkMissingIndexes());
        messages.addAll(checkInvalidIndexes());
        if (indexCount - invalidIndexCount > tableCount - missingIndexCount) {
            messages.addAll(checkDuplicateIndexes());
        }
        return messages;
    }


    private void populateData(PairDataSet table, PairDataSet index) {
        if (tableData == null) {
            tableData = new HashMap<>();
            Iterator<Tuple2<String, byte[]>> tableSource = ((ControlPairDataSet) table).source;
            while (tableSource.hasNext()) {
                tableCount++;
                Tuple2<String, byte[]> t = tableSource.next();
                String rowId = t._1;
                byte[] row = t._2;
                tableData.put(rowId, row);
            }
        }

        indexData = ArrayListMultimap.create();
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
        List<String> messages = Lists.newLinkedList();
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
                        break;
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
        List<String> messages = Lists.newLinkedList();
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
                        break;
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
        List<String> messages = Lists.newLinkedList();

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
