package com.splicemachine.si2.data.light;

import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SRowLock;
import com.splicemachine.si2.data.api.SScan;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;

import java.util.ArrayList;
import java.util.Arrays;
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
            if ((t.key.equals((String) get.startTupleKey)) ||
                    ((t.key.compareTo((String) get.startTupleKey) > 0) && (t.key.compareTo((String) get.endTupleKey) < 0))) {
                results.add(filterCells(t, get.families, get.columns, get.effectiveTimestamp));
            }
        }
        return results.iterator();
    }

    private LTuple filterCells(LTuple t, List families,
                               List columns, Long effectiveTimestamp) {
        List<LKeyValue> newCells = new ArrayList<LKeyValue>();
        for (LKeyValue c : t.values) {
            if ((families == null && columns == null) ||
                    ((families != null) && families.contains(c.family)) ||
                    ((columns != null) && columnsContain(columns, c))) {
                if (effectiveTimestamp != null) {
                    if (c.timestamp >= effectiveTimestamp) {
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
                newValues.add(new LKeyValue(c.family, c.qualifier, getCurrentTimestamp(), c.value));
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
                List<LKeyValue> values = new ArrayList<LKeyValue>(t.values);
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
    public boolean checkAndPut(STable table, Object family, Object qualifier, Object value, Object put) {
        synchronized (this) {
            Object key = tupleReaderWriter.getResultKey(put);

            List columns = Arrays.asList(Arrays.asList(family, qualifier));
            LGet get = new LGet(key, key, null, columns, null);
            Object result = get(table, get);
            if (result != null) {
                Object currentCellValue = tupleReaderWriter.getResultValue(result, family, qualifier);
                if ((currentCellValue == null && value != null) || !currentCellValue.equals(value)) {
                    return false;
                }
            } else {
                if (value != null) {
                    return false;
                }
            }

            final String relationIdentifier = ((LTable) table).relationIdentifier;
            List<LTuple> newTuples = relations.get(relationIdentifier);
            if (newTuples == null) {
                newTuples = new ArrayList<LTuple>();
            }
            newTuples = writeSingle(table, (LTuple) put, newTuples);
            relations.put(relationIdentifier, newTuples);
            return true;
        }
    }
}
