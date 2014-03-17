package com.splicemachine.si.data.light;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.api.Clock;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.utils.CloseableIterator;
import com.splicemachine.utils.ForwardingCloseableIterator;

public class LStore implements STableReader<LTable, LGet, LGet>,
        STableWriter<LTable, LTuple, LTuple, LTuple> {
    private final Map<String, Map<byte[], SRowLock>> locks = Maps.newTreeMap();
    private final Map<String, Map<SRowLock, byte[]>> reverseLocks = Maps.newTreeMap();
    private final Map<String, List<LTuple>> relations = Maps.newTreeMap();
    private final Clock clock;

    private final AtomicInteger lockIdGenerator = new AtomicInteger(0);

    public LStore(Clock clock) {
        this.clock = clock;
    }

    static void sortValues(List<Cell> results) {
        Collections.sort(results, new KeyValue.KVComparator());
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
    public void close(LTable table) {
    }

    @Override
    public String getTableName(LTable table) {
        return table.relationIdentifier;
    }

    @Override
    public Result get(LTable table, LGet get) {
        Iterator<Result> results = runScan(table, get);
        if (results.hasNext()) {
            Result result = results.next();
            assert !results.hasNext();
            return result;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CloseableIterator<Result> scan(LTable table, LGet scan) {
        return new ForwardingCloseableIterator<Result>(runScan(table, scan)) {
            @Override
            public void close() throws IOException {
                //no-op
            }
        };
    }

    @Override
    public void openOperation(LTable table) throws IOException {
    }

    @Override
    public void closeOperation(LTable table) throws IOException {
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    private Iterator<List<Cell>> scanRegion(LTable table, LGet scan) throws IOException {
        final Iterator<Result> iterator = runScan(table, scan);
        List<List<Cell>> results = Lists.newArrayList();
        while (iterator.hasNext()) {
            results.add(Lists.newArrayList(iterator.next().rawCells()));
        }
        return results.iterator();
    }

    private Iterator<Result> runScan(LTable table, LGet get) {
        List<LTuple> tuples = relations.get(table.relationIdentifier);
        if (tuples == null) {
            tuples = new ArrayList<LTuple>();
        }
        List<LTuple> results = new ArrayList<LTuple>();
        for (LTuple t : tuples) {
            if (get.startTupleKey == null || (Arrays.equals(t.key, (byte[]) get.startTupleKey)) ||
                    ((Bytes.compareTo(t.key, get.startTupleKey) > 0) &&
                            (get.endTupleKey == null || Bytes.compareTo(t.key, (byte[]) get.endTupleKey) < 0))) {
                results.add(filterCells(t, get.families, get.columns, get.effectiveTimestamp));
            }
        }
        sort(results);
        return Lists.transform(results, new Function<LTuple, Result>() {
            @Override
            public Result apply(@Nullable LTuple input) {
                return Result.create(input.getValues());
            }
        }).iterator();
    }

    private void sort(List<LTuple> results) {
        Collections.sort(results, new Comparator<Object>() {
            @Override
            public int compare(Object tuple1, Object tuple2) {
                LTuple t1 = (LTuple) tuple1;
                LTuple t2 = (LTuple) tuple2;
                return Bytes.compareTo(t1.key, t2.key);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private LTuple filterCells(LTuple t, List<byte[]> families,
                               List<List<byte[]>> columns, Long effectiveTimestamp) {
        if (effectiveTimestamp == null)
            effectiveTimestamp = Long.MAX_VALUE;
        Set<Cell> newCells = Sets.newHashSet();
        if (columns == null && families == null) {
            newCells.addAll(t.values);
        }
        if (columns != null) {
            for (Cell c : t.values) {
                if (columnsContain(columns, c) && c.getTimestamp() <= effectiveTimestamp)
                    newCells.add(c);
            }
        }
        if (families != null) {
            for (Cell c : t.values) {
                for (byte[] family : families) {

                    if (CellUtil.matchingFamily(c, family) && c.getTimestamp() <= effectiveTimestamp) {
                        newCells.add(c);
                        break;
                    }
                }
            }
        }
        return new LTuple(t.key, Lists.newArrayList(newCells));
    }

    private boolean columnsContain(List<List<byte[]>> columns, Cell c) {
        for (List<byte[]> column : columns) {

            if (CellUtils.singleMatchingColumn(c, column.get(0), column.get(1)))
                return true;
        }
        return false;
    }

    @Override
    public void write(LTable table, LTuple put) throws IOException {
        write(table, Arrays.asList(put));
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
                newTuples = writeSingle(t, newTuples);
            }
            relations.put(relationIdentifier, newTuples);
        }
    }

    @Override
    public OperationStatus[] writeBatch(LTable table, LTuple[] puts) throws IOException {
        for (LTuple p : puts) {
            write(table, p);
        }
        OperationStatus[] result = new OperationStatus[puts.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
        }
        return result;
    }

    @Override
    public SRowLock tryLock(LTable lTable, byte[] rowKey) {
        return lockRow(lTable, rowKey);
    }

    @Override
    public SRowLock lockRow(LTable sTable, byte[] rowKey) {
        synchronized (this) {
            String table = sTable.relationIdentifier;
            Map<byte[], SRowLock> lockTable = locks.get(table);
            Map<SRowLock, byte[]> reverseLockTable = reverseLocks.get(table);
            if (lockTable == null) {
                lockTable = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                reverseLockTable = Maps.newHashMap();
                locks.put(table, lockTable);
                reverseLocks.put(table, reverseLockTable);
            }
            SRowLock currentLock = lockTable.get(rowKey);
            if (currentLock == null) {
                // FIXME: jc - need to acquire a lock from HRegion
				currentLock = new LRowLock(lockIdGenerator.incrementAndGet());
                lockTable.put(rowKey, currentLock);
                reverseLockTable.put(currentLock, rowKey);
                return currentLock;
            }
            throw new RuntimeException("row is already locked: " + table + " " + rowKey);
        }
    }

    @Override
    public void unLockRow(LTable sTable, SRowLock lock) {
        synchronized (this) {
            String table = sTable.relationIdentifier;
            Map<byte[], SRowLock> lockTable = locks.get(table);
            Map<SRowLock, byte[]> reverseLockTable = reverseLocks.get(table);
            if (lockTable == null) {
                throw new RuntimeException("unlocking unknown lock: " + table);
            }
            byte[] row = reverseLockTable.get(lock);
            if (row == null) {
                throw new RuntimeException("unlocking unknown lock: " + table + " ");
            }
            lockTable.remove(row);
            reverseLockTable.remove(lock);
        }
    }

    @Override
    public boolean checkAndPut(LTable table, final byte[] family, final byte[] qualifier, byte[] expectedValue,
                               LTuple put) throws IOException {
        SRowLock lock = null;
        try {
            lock = lockRow(table, put.key);
            LGet get = new LGet(put.key, put.key, null, null, null);
            Result result = get(table, get);
            boolean match = false;
            boolean found = false;
            if (result == null) {
                match = (expectedValue == null);
            } else {
                ArrayList<Cell> results = Lists.newArrayList(result.rawCells());
                sortValues(results);
                for (Cell kv : results) {
                    if (CellUtils.singleMatchingColumn(kv, family, qualifier)) {
                        match = Arrays.equals(kv.getValueArray(), expectedValue);
                        found = true;
                        break;
                    }
                }
            }
            if (match || (expectedValue == null && !found)) {
                write(table, put);
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

    private long getCurrentTimestamp() {
        return clock.getTime();
    }

    private List<LTuple> writeSingle(LTuple newTuple, List<LTuple> currentTuples) {
        List<Cell> newValues = Lists.newArrayList();
        for (Cell c : newTuple.values) {
            if (c.getTimestamp() < 0) {
                newValues.add(new KeyValue(newTuple.key, c.getFamilyArray(), c.getQualifierArray(),
                                           getCurrentTimestamp(), c.getValue()));
            } else {
                newValues.add(c);
            }
        }
        LTuple modifiedNewTuple = new LTuple(newTuple.key, newValues);

        List<LTuple> newTuples = Lists.newArrayList();
        boolean matched = false;
        for (LTuple t : currentTuples) {
            if (Arrays.equals(newTuple.key, t.key)) {
                matched = true;
                List<Cell> values = Lists.newArrayList();
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
    public void delete(LTable table, LTuple delete) throws IOException {
        final String relationIdentifier = table.relationIdentifier;
        final List<LTuple> tuples = relations.get(relationIdentifier);
        final List<LTuple> newTuples = Lists.newArrayList();
        for (LTuple tuple : tuples) {
            LTuple newTuple = tuple;
            if (Arrays.equals(tuple.key, delete.key)) {
                final List<Cell> values = tuple.values;
                final List<Cell> newValues = Lists.newArrayList();
                if (!delete.values.isEmpty()) {
                    for (Cell value : values) {
                        boolean keep = true;
                        for (Cell deleteValue : (delete).values) {

                            if (CellUtils.singleMatchingColumn(deleteValue, value.getFamilyArray(),
                                                               value.getQualifierArray()) && value.getTimestamp() ==
                                    deleteValue.getTimestamp()) {
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
    private void filterOutKeyValuesBeingReplaced(List<Cell> values, LTuple t, List<Cell> newValues) {
        for (Cell currentKv : t.values) {
            boolean collides = false;
            for (Cell newKv : newValues) {

                if (CellUtils.singleMatchingColumn(currentKv, newKv.getFamilyArray(), newKv.getQualifierArray()) &&
                        currentKv.getTimestamp() == newKv.getTimestamp()) {
                    collides = true;
                }
            }
            if (!collides) {
                values.add(currentKv);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void compact(Transactor transactor, String tableName) throws IOException {
        final List<LTuple> rows = relations.get(tableName);
        final List<LTuple> newRows = new ArrayList<LTuple>(rows.size());
        final SICompactionState compactionState = transactor.newCompactionState();
        for (LTuple row : rows) {
            final ArrayList<Cell> mutatedValues = Lists.newArrayList();
            compactionState.mutate(row.values, mutatedValues);
            LTuple newRow = new LTuple(row.key, mutatedValues, row.getAttributesMap());
            newRows.add(newRow);
        }
        relations.put(tableName, newRows);
    }

}
