package com.splicemachine.si.data.light;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.splicemachine.si.api.Clock;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.utils.CloseableIterator;
import com.splicemachine.utils.ForwardingCloseableIterator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LStore implements STableReader<LTable, LGet, LGet>,
        STableWriter<LTable, LTuple, LTuple, LTuple> {
    private final Map<String, Map<byte[], Integer>> locks = Maps.newTreeMap();
    private final Map<String, Map<Integer, byte[]>> reverseLocks = Maps.newTreeMap();
    private final Map<String, List<LTuple>> relations = Maps.newTreeMap();
    private final Clock clock;

		private final AtomicInteger lockIdGenerator = new AtomicInteger(0);

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
        return new ForwardingCloseableIterator<Result>(runScan(table,scan)) {
						@Override
						public void close() throws IOException {
							//no-op
						}
				};
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
		private Iterator<List<KeyValue>> scanRegion(LTable table, LGet scan) throws IOException {
        final Iterator<Result> iterator = runScan(table, scan);
        List<List<KeyValue>> results = Lists.newArrayList();
				while (iterator.hasNext()) {
            results.add(Lists.newArrayList(iterator.next().raw()));
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
            if (get.startTupleKey == null || (Arrays.equals(t.key,(byte[])get.startTupleKey)) ||
                    ((Bytes.compareTo(t.key, get.startTupleKey) > 0) &&
														(get.endTupleKey == null || Bytes.compareTo(t.key,(byte[]) get.endTupleKey) < 0))) {
								results.add(filterCells(t, get.families, get.columns, get.effectiveTimestamp));
            }
        }
        sort(results);
				return Lists.transform(results,new Function<LTuple, Result>() {
						@Override
						public Result apply(@Nullable LTuple input) {
								return new Result(input.getValues());
						}
				}).iterator();
    }

    private void sort(List<LTuple> results) {
        Collections.sort(results, new Comparator<Object>() {
            @Override
            public int compare(Object tuple1, Object tuple2) {
								LTuple t1 = (LTuple)tuple1;
								LTuple t2 = (LTuple)tuple2;
								return Bytes.compareTo(t1.key,t2.key);
            }
        });
    }

    @SuppressWarnings("unchecked")
		private LTuple filterCells(LTuple t, List<byte[]> families,
                               List<List<byte[]>> columns, Long effectiveTimestamp) {
				if(effectiveTimestamp==null)
						effectiveTimestamp = Long.MAX_VALUE;
        Set<KeyValue> newCells = Sets.newHashSet();
				if(columns==null && families==null){
						newCells.addAll(t.values);
				}
				if(columns!=null){
						for(KeyValue c:t.values){
								if(columnsContain(columns,c) && c.getTimestamp()<= effectiveTimestamp)
										newCells.add(c);
						}
				}
				if(families!=null){
						for(KeyValue c:t.values){
								for(byte[] family:families){
										if(c.matchingFamily(family) && c.getTimestamp()<=effectiveTimestamp){
												newCells.add(c);
												break;
										}
								}
						}
				}
//        for (KeyValue c : t.values) {
//            if ((families == null && columns == null) ||
//                    ((families != null) && families.contains(c.getFamily())) ||
//                    ((columns != null) && columnsContain(columns, c))) {
//                if (effectiveTimestamp != null) {
//                    if (c.getTimestamp() <= effectiveTimestamp) {
//                        newCells.add(c);
//                    }
//                } else {
//                    newCells.add(c);
//                }
//            }
//        }
        return new LTuple(t.key, Lists.newArrayList(newCells));
    }

    private boolean columnsContain(List<List<byte[]>> columns, KeyValue c) {
        for (List<byte[]> column : columns) {
						if(c.matchingColumn(column.get(0),column.get(1)))
								return true;
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
    public void write(LTable table, LTuple put, Integer rowLock) throws IOException {
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
                newTuples = writeSingle(t, newTuples);
            }
            relations.put(relationIdentifier, newTuples);
        }
    }

    @Override
    public OperationStatus[] writeBatch(LTable table, Pair<LTuple, Integer>[] puts) throws IOException {
        for (Pair<LTuple, Integer> p : puts) {
            write(table, p.getFirst());
        }
        OperationStatus[] result = new OperationStatus[puts.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
        }
        return result;
    }

    @Override
    public boolean checkAndPut(LTable table, final byte[] family, final byte[] qualifier, byte[] expectedValue, LTuple put) throws IOException {
        Integer lock = null;
        try {
            lock = lockRow(table, put.key);
            LGet get = new LGet(put.key, put.key, null, null, null);
						Result result = get(table, get);
            boolean match = false;
            boolean found = false;
            if (result == null) {
                match = (expectedValue == null);
            } else {
								ArrayList<KeyValue> results = Lists.newArrayList(result.raw());
								sortValues(results);
                for (KeyValue kv : results) {
										if(kv.matchingColumn(family,qualifier)){
                        match = Arrays.equals(kv.getValue(), expectedValue);
                        found = true;
                        break;
                    }
                }
            }
            if (match || (expectedValue == null && !found)) {
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

    static void sortValues(List<KeyValue> results) {
				Collections.sort(results,new KeyValue.KVComparator());
//        Collections.sort(results, new Comparator<Object>() {
//            @Override
//            public int compare(Object simpleCell, Object simpleCell2) {
//                final LKeyValue v2 = (LKeyValue) simpleCell2;
//                final LKeyValue v1 = (LKeyValue) simpleCell;
//                if (Arrays.equals(v1.family,v2.family)) {
//                    if (v1.qualifier.equals(v2.qualifier)) {
//                        Long t1 = v1.timestamp;
//                        if (t1 == null) {
//                            t1 = 0L;
//                        }
//                        Long t2 = v2.timestamp;
//                        if (t2 == null) {
//                            t2 = 0L;
//                        }
//                        return t2.compareTo(t1);
//                    } else {
//                        return Bytes.compareTo(v1.qualifier,v2.qualifier);
//                    }
//                }
//                return Bytes.compareTo(v1.family,v2.family);
//            }
//        });
    }

    @Override
    public Integer lockRow(LTable sTable, byte[] rowKey) {
        synchronized (this) {
            String table = sTable.relationIdentifier;
            Map<byte[], Integer> lockTable = locks.get(table);
            Map<Integer, byte[]> reverseLockTable = reverseLocks.get(table);
            if (lockTable == null) {
                lockTable = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                reverseLockTable = Maps.newHashMap();
                locks.put(table, lockTable);
                reverseLocks.put(table, reverseLockTable);
            }
            Integer currentLock = lockTable.get(rowKey);
            if (currentLock == null) {
								currentLock = lockIdGenerator.incrementAndGet();
                lockTable.put(rowKey, currentLock);
                reverseLockTable.put(currentLock, rowKey);
                return currentLock;
            }
            throw new RuntimeException("row is already locked: " + table + " " + rowKey);
        }
    }

    @Override
    public void unLockRow(LTable sTable, Integer lock) {
        synchronized (this) {
            String table = sTable.relationIdentifier;
            Map<byte[], Integer> lockTable = locks.get(table);
            Map<Integer, byte[]> reverseLockTable = reverseLocks.get(table);
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

    private long getCurrentTimestamp() {
        return clock.getTime();
    }

    private List<LTuple> writeSingle(LTuple newTuple, List<LTuple> currentTuples) {
        List<KeyValue> newValues = Lists.newArrayList();
        for (KeyValue c : newTuple.values) {
            if (c.getTimestamp() <0) {
                newValues.add(new KeyValue(newTuple.key, c.getFamily(), c.getQualifier(), getCurrentTimestamp(), c.getValue()));
            } else {
                newValues.add(c);
            }
        }
        LTuple modifiedNewTuple = new LTuple(newTuple.key, newValues);

        List<LTuple> newTuples = Lists.newArrayList();
        boolean matched = false;
        for (LTuple t : currentTuples) {
            if (Arrays.equals(newTuple.key,t.key)) {
                matched = true;
                List<KeyValue> values = Lists.newArrayList();
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
    public void delete(LTable table, LTuple delete, Integer lock) throws IOException {
        final String relationIdentifier = table.relationIdentifier;
        final List<LTuple> tuples = relations.get(relationIdentifier);
        final List<LTuple> newTuples = Lists.newArrayList();
        for (LTuple tuple : tuples) {
            LTuple newTuple = tuple;
            if (Arrays.equals(tuple.key,delete.key)) {
                final List<KeyValue> values = tuple.values;
                final List<KeyValue> newValues = Lists.newArrayList();
                if (!delete.values.isEmpty()) {
                    for (KeyValue value : values) {
                        boolean keep = true;
                        for (KeyValue deleteValue : (delete).values) {
														if(deleteValue.matchingColumn(value.getFamily(),value.getQualifier()) && value.getTimestamp()==deleteValue.getTimestamp()){
//                            if (Arrays.equals(value.family,deleteValue.family)
//                                    && value.qualifier.equals(deleteValue.qualifier)
//                                    && value.timestamp.equals(deleteValue.timestamp)) {
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
    private void filterOutKeyValuesBeingReplaced(List<KeyValue> values, LTuple t, List<KeyValue> newValues) {
        for (KeyValue currentKv : t.values) {
            boolean collides = false;
            for (KeyValue newKv : newValues) {
								if(currentKv.matchingColumn(newKv.getFamily(),newKv.getQualifier())&&
												currentKv.getTimestamp()==newKv.getTimestamp()){
//                if (Arrays.equals(currentKv.family,newKv.family) &&
//                        currentKv.qualifier.equals(newKv.qualifier) &&
//                        currentKv.timestamp.equals(newKv.timestamp)) {
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
            final ArrayList<KeyValue> mutatedValues = Lists.newArrayList();
						compactionState.mutate(row.values, mutatedValues);
            LTuple newRow = new LTuple(row.key, mutatedValues, row.getAttributesMap());
            newRows.add(newRow);
        }
        relations.put(tableName, newRows);
    }

    @Override
    public void closeOperation(LTable table) throws IOException {
    }

    @Override
    public void openOperation(LTable table) throws IOException {
    }

}
