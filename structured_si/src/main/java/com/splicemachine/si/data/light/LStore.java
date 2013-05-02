package com.splicemachine.si.data.light;

import com.splicemachine.si.api.Clock;
import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.data.api.SScan;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.si.impl.TransactionStatus;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LStore implements STableReader, STableWriter {
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
    public STable open(String tableName) {
        return new LTable(tableName);
    }

    @Override
    public Object get(STable table, SGet get) {
        Iterator results = runScan(table, (LGet) get);
        if (results.hasNext()) {
            Object result = results.next();
            assert !results.hasNext();
            return result;
        }
        return null;
    }

    @Override
    public Iterator scan(STable table, SScan scan) {
        return runScan(table, (LGet) scan);
    }

    private Iterator runScan(STable table, LGet get) {
        List<LTuple> tuples = relations.get(((LTable) table).relationIdentifier);
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
    public void close(STable table) {
    }

    @Override
    public void write(STable table, Object put) {
        write(table, Arrays.asList(put));
    }

    @Override
    public void write(STable table, Object put, boolean durable) {
        write(table, Arrays.asList(put));
    }

    @Override
    public void write(STable table, Object put, SRowLock rowLock) {
        write(table, put);
    }

    @Override
    public void write(STable table, List puts) {
        synchronized (this) {
            final String relationIdentifier = ((LTable) table).relationIdentifier;
            List<LTuple> newTuples = relations.get(relationIdentifier);
            if (newTuples == null) {
                newTuples = new ArrayList<LTuple>();
            }
            for (Object t : puts) {
                newTuples = writeSingle(table, (LTuple) t, newTuples);
            }
            relations.put(relationIdentifier, newTuples);
        }
    }

    @Override
    public boolean checkAndPut(STable table, final Object family, final Object qualifier, Object expectedValue, Object put) throws IOException {
        LTuple lPut = (LTuple) put;
        SRowLock lock = null;
        try {
            lock = lockRow(table, lPut.key);
            LGet get = new LGet(lPut.key, lPut.key, null, null, null);
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

    static void sortValues(List results) {
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
    public SRowLock lockRow(STable sTable, Object rowKey) {
        synchronized (this) {
            String table = ((LTable) sTable).relationIdentifier;
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
    public void unLockRow(STable sTable, SRowLock lock) {
        synchronized (this) {
            String table = ((LTable) sTable).relationIdentifier;
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

    private List<LTuple> writeSingle(STable sTable, LTuple newTuple, List<LTuple> currentTuples) {
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

    public void compact(SICompactionState compactionState, String tableName) throws IOException {
        final List<LTuple> rows = relations.get(tableName);
        final List<LTuple> newRows = new ArrayList<LTuple>(rows.size());
        for( LTuple row : rows ) {
            final ArrayList mutatedValues = new ArrayList();
            compactionState.mutate(row.values, mutatedValues);
            LTuple newRow = new LTuple(row.key, mutatedValues, row.attributes);
            newRows.add(newRow);
        }
        relations.put(tableName, newRows);
    }
}
