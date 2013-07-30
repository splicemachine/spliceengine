package com.splicemachine.si.data.light;

import com.splicemachine.si.api.Clock;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.impl.PushBackIterator;
import com.splicemachine.si.impl.SICompactionState;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LStore implements STableReader<LTable, LTuple, LGet, LGet, LKeyValue, LScanner, Object>,
        STableWriter<LTable, LTuple, LTuple, LTuple, Object, LRowLock, OperationStatus> {
    private final Map<String, Map<String, LRowLock>> locks = new HashMap<String, Map<String, LRowLock>>();
    private final Map<String, Map<LRowLock, String>> reverseLocks = new HashMap<String, Map<LRowLock, String>>();
    private final Map<String, List<LTuple>> relations = new HashMap<String, List<LTuple>>();
    private final LDataLib tupleReaderWriter = new LDataLib();
    private final Clock clock;

    public LStore(Clock clock) {
        this.clock = clock;
    }

    public String toString() {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, List<LTuple>> entry : relations.entrySet()) {
            String relationName = entry.getKey();
            result.append(relationName);
            result.append("\n");
            List<LTuple> tuples = entry.getValue();
            for (LTuple t : tuples) {
                result.append(t.toString());
                result.append("\n");
            }
            result.append("----");
        }
        return result.toString();
    }

    @Override
    public LTable open(String tableName) {
        return new LTable(tableName);
    }

    @Override
    public LTuple get(LTable table, LGet get) {
        Iterator<LTuple> results = runScan(table, get);
        if (results.hasNext()) {
            LTuple result = results.next();
            assert !results.hasNext();
            return result;
        }
        return null;
    }

    @Override
    public Iterator scan(LTable table, LGet scan) {
        return runScan(table, scan);
    }

    private Iterator<List<LKeyValue>> scanRegion(LTable table, LGet scan) throws IOException {
        final Iterator<LTuple> iterator = runScan(table, scan);
        List<List<LKeyValue>> results = new ArrayList<List<LKeyValue>>();
        while(iterator.hasNext()) {
            results.add(iterator.next().values);
        }
        return results.iterator();
    }

    private Iterator runScan(LTable table, LGet get) {
        List<LTuple> tuples = relations.get(table.relationIdentifier);
        if (tuples == null) {
            tuples = new ArrayList<LTuple>();
        }
        List<LTuple> results = new ArrayList<LTuple>();
        for (LTuple t : tuples) {
            if ((t.key.equals(get.startTupleKey)) ||
                    ((t.key.compareTo((String) get.startTupleKey) > 0) && (t.key.compareTo((String) get.endTupleKey) < 0))) {
                results.add(filterCells(t, get.families, get.columns, get.effectiveTimestamp));
            }
        }
        sort(results);
        return results.iterator();
    }

    private void sort(List<LTuple> results) {
        Collections.sort(results, new Comparator<Object>() {
            @Override
            public int compare(Object tuple1, Object tuple2) {
                return ((LTuple) tuple1).key.compareTo(((LTuple) tuple2).key);
            }
        });
    }

    private LTuple filterCells(LTuple t, List families,
                               List columns, Long effectiveTimestamp) {
        List<LKeyValue> newCells = new ArrayList<LKeyValue>();
        for (LKeyValue c : t.values) {
            if ((families == null && columns == null) ||
                    ((families != null) && families.contains(c.family)) ||
                    ((columns != null) && columnsContain(columns, c))) {
                if (effectiveTimestamp != null) {
                    if (c.timestamp <= effectiveTimestamp) {
                        newCells.add(c);
                    }
                } else {
                    newCells.add(c);
                }
            }
        }
        return new LTuple(t.key, newCells);
    }

    private boolean columnsContain(List<List<Object>> columns, LKeyValue c) {
        for (List<Object> column : columns) {
            if ((column.get(0).equals(c.family)) && (column.get(1).equals(c.qualifier))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close(LTable table) {
    }

    @Override
    public void write(LTable table, LTuple put) throws IOException {
        write(table, Arrays.asList(put));
    }

    @Override
    public void write(LTable table, LTuple put, boolean durable) throws IOException {
        write(table, Arrays.asList(put));
    }

    @Override
    public void write(LTable table, LTuple put, LRowLock rowLock) throws IOException {
        write(table, put);
    }

    @Override
    public void write(LTable table, List<LTuple> puts) {
        synchronized (this) {
            final String relationIdentifier = table.relationIdentifier;
            List<LTuple> newTuples = relations.get(relationIdentifier);
            if (newTuples == null) {
                newTuples = new ArrayList<LTuple>();
            }
            for (LTuple t : puts) {
                newTuples = writeSingle(table, t, newTuples);
            }
            relations.put(relationIdentifier, newTuples);
        }
    }

    @Override
    public OperationStatus[] writeBatch(LTable table, Pair<LTuple, LRowLock>[] puts) throws IOException {
        for (Pair<LTuple, LRowLock> p : puts) {
            write(table, p.getFirst());
        }
        OperationStatus[] result = new OperationStatus[puts.length];
        for (int i=0; i<result.length; i++) {
            result[i] = new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
        }
        return result;
    }

    @Override
    public boolean checkAndPut(LTable table, final Object family, final Object qualifier, Object expectedValue, LTuple put) throws IOException {
        LRowLock lock = null;
        try {
            lock = lockRow(table, put.key);
            LGet get = new LGet(put.key, put.key, null, null, null);
            Iterator results = runScan(table, get);
            LTuple result = (LTuple) results.next();
            assert !results.hasNext();
            boolean match = false;
            sortValues(result.values);
            for (LKeyValue kv : result.values) {
                if (kv.family.equals(family) && kv.qualifier.equals(qualifier)) {
                    match = kv.value.equals(expectedValue);
                    break;
                }
            }
            if (match) {
                write(table, put, lock);
                return true;
            } else {
                return false;
            }
        } finally {
            if (lock != null) {
                unLockRow(table, lock);
            }
        }
    }

    static void sortValues(List<LKeyValue> results) {
        Collections.sort(results, new Comparator<Object>() {
            @Override
            public int compare(Object simpleCell, Object simpleCell2) {
                final LKeyValue v2 = (LKeyValue) simpleCell2;
                final LKeyValue v1 = (LKeyValue) simpleCell;
                if (v1.family.equals(v2.family)) {
                    if (v1.qualifier.equals(v2.qualifier)) {
                        Long t1 = v1.timestamp;
                        if (t1 == null) {
                            t1 = 0L;
                        }
                        Long t2 = v2.timestamp;
                        if (t2 == null) {
                            t2 = 0L;
                        }
                        return t2.compareTo(t1);
                    } else {
                        return v1.qualifier.compareTo(v2.qualifier);
                    }
                }
                return v1.family.compareTo(v2.family);
            }
        });
    }

    @Override
    public LRowLock lockRow(LTable sTable, Object rowKey) {
        synchronized (this) {
            String table = sTable.relationIdentifier;
            Map<String, LRowLock> lockTable = locks.get(table);
            Map<LRowLock, String> reverseLockTable = reverseLocks.get(table);
            if (lockTable == null) {
                lockTable = new HashMap<String, LRowLock>();
                reverseLockTable = new HashMap<LRowLock, String>();
                locks.put(table, lockTable);
                reverseLocks.put(table, reverseLockTable);
            }
            LRowLock currentLock = lockTable.get(rowKey);
            if (currentLock == null) {
                LRowLock lock = new LRowLock();
                lockTable.put((String) rowKey, lock);
                reverseLockTable.put(lock, (String) rowKey);
                return lock;
            }
            throw new RuntimeException("row is already locked: " + table + " " + rowKey);
        }
    }

    @Override
    public void unLockRow(LTable sTable, LRowLock lock) {
        synchronized (this) {
            String table = sTable.relationIdentifier;
            Map<String, LRowLock> lockTable = locks.get(table);
            Map<LRowLock, String> reverseLockTable = reverseLocks.get(table);
            if (lockTable == null) {
                throw new RuntimeException("unlocking unknown lock: " + table);
            }
            String row = reverseLockTable.get(lock);
            if (row == null) {
                throw new RuntimeException("unlocking unknown lock: " + table + " " + row);
            }
            lockTable.remove(row);
            reverseLockTable.remove(lock);
        }
    }

    private long getCurrentTimestamp() {
        return clock.getTime();
    }

    private List<LTuple> writeSingle(LTable lTable, LTuple newTuple, List<LTuple> currentTuples) {
        List<LKeyValue> newValues = new ArrayList<LKeyValue>();
        for (LKeyValue c : newTuple.values) {
            if (c.timestamp == null) {
                newValues.add(new LKeyValue(newTuple.key, c.family, c.qualifier, getCurrentTimestamp(), c.value));
            } else {
                newValues.add(c);
            }
        }
        LTuple modifiedNewTuple = new LTuple(newTuple.key, newValues);

        List<LTuple> newTuples = new ArrayList<LTuple>();
        boolean matched = false;
        for (LTuple t : currentTuples) {
            if (newTuple.key.equals(t.key)) {
                matched = true;
                List<LKeyValue> values = new ArrayList<LKeyValue>();
                filterOutKeyValuesBeingReplaced(values, t, newValues);
                values.addAll(newValues);
                newTuples.add(new LTuple(newTuple.key, values));
            } else {
                newTuples.add(t);
            }
        }
        if (!matched) {
            newTuples.add(modifiedNewTuple);
        }
        return newTuples;
    }

    @Override
    public void delete(LTable table, LTuple delete, LRowLock lock) throws IOException {
        final String relationIdentifier = table.relationIdentifier;
        final List<LTuple> tuples = relations.get(relationIdentifier);
        final List<LTuple> newTuples = new ArrayList<LTuple>();
        for (LTuple tuple : tuples) {
            LTuple newTuple = tuple;
            if (tuple.key.equals(delete.key)) {
                final List<LKeyValue> values = tuple.values;
                final List<LKeyValue> newValues = new ArrayList<LKeyValue>();
                if (!delete.values.isEmpty()) {
                    for (LKeyValue value : values) {
                        boolean keep = true;
                        for (LKeyValue deleteValue : (delete).values) {
                            if (value.family.equals(deleteValue.family)
                                    && value.qualifier.equals(deleteValue.qualifier)
                                    && value.timestamp.equals(deleteValue.timestamp)) {
                                keep = false;
                            }
                        }
                        if (keep) {
                            newValues.add(value);
                        }
                    }
                }
                newTuple = new LTuple(tuple.key, newValues);
            }
            newTuples.add(newTuple);
        }
        relations.put(relationIdentifier, newTuples);
    }

    /**
     * Only carry over KeyValues that are not being replaced by incoming KeyValues.
     */
    private void filterOutKeyValuesBeingReplaced(List<LKeyValue> values, LTuple t, List<LKeyValue> newValues) {
        for (LKeyValue currentKv : t.values) {
            boolean collides = false;
            for (LKeyValue newKv : newValues) {
                if (currentKv.family.equals(newKv.family) &&
                        currentKv.qualifier.equals(newKv.qualifier) &&
                        currentKv.timestamp.equals(newKv.timestamp)) {
                    collides = true;
                }
            }
            if (!collides) {
                values.add(currentKv);
            }
        }
    }

    public void compact(Transactor transactor, String tableName) throws IOException {
        final List<LTuple> rows = relations.get(tableName);
        final List<LTuple> newRows = new ArrayList<LTuple>(rows.size());
        final SICompactionState compactionState = transactor.newCompactionState();
        for (LTuple row : rows) {
            final ArrayList<LKeyValue> mutatedValues = new ArrayList<LKeyValue>();
            transactor.compact(compactionState, row.values, mutatedValues);
            LTuple newRow = new LTuple(row.key, mutatedValues, row.attributes);
            newRows.add(newRow);
        }
        relations.put(tableName, newRows);
    }

    @Override
    public LScanner openRegionScanner(LTable table, LGet scan) throws IOException {
        return new LScanner(new PushBackIterator<List<LKeyValue>>(scanRegion(table, scan)));
    }

    @Override
    public List<LKeyValue> nextResultsOnRegionScanner(LScanner scanner) throws IOException {
        return scanner.next();
    }

    @Override
    public void seekOnRegionScanner(LScanner scanner, Object rowKey) throws IOException {
       scanner.seek(rowKey);
    }

    @Override
    public void closeRegionScanner(LTable table) {
    }
}
