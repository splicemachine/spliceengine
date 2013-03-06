package com.splicemachine.si2.data.light;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SRowLock;
import com.splicemachine.si2.data.api.SScan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LDataLib implements SDataLib {

    @Override
    public Object newRowKey(Object... args) {
        StringBuilder builder = new StringBuilder();
        for (Object a : args) {
            builder.append(a);
        }
        return builder.toString();
    }

    private boolean nullSafeComparison(Object o1, Object o2) {
        return (o1 == null && o2 == null) || ((o1 != null) && o1.equals(o2));
    }

    public boolean valuesMatch(Object family1, Object family2) {
        return nullSafeComparison(family1, family2);
    }

    @Override
    public Object encode(Object value) {
        return value;
    }

    @Override
    public Object decode(Object value, Class type) {
        if (value == null) {
            return value;
        }
        if (type.equals(value.getClass())) {
            return value;
        }
        throw new RuntimeException("types don't match " + value.getClass().getName() + " " + type.getName() + " " + value);
    }

    @Override
    public void addKeyValueToPut(Object put, Object family, Object qualifier, Long timestamp, Object value) {
        LTuple lTuple = (LTuple) put;
        final LKeyValue newCell = new LKeyValue((String) family, (String) qualifier, timestamp, value);
        lTuple.values.add(newCell);
    }

    @Override
    public void addAttribute(Object operation, String attributeName, Object value) {
        LTuple lTuple = (LTuple) operation;
        lTuple.attributes.put(attributeName, value);
    }

    @Override
    public Object getAttribute(Object operation, String attributeName) {
        LTuple lTuple = (LTuple) operation;
        return lTuple.attributes.get(attributeName);
    }

    @Override
    public Object newResult(Object key, List keyValues) {
        return new LTuple((String) key, new ArrayList<LKeyValue>(keyValues));
    }

    @Override
    public Object newPut(Object key) {
        return newPut(key, null);
    }

    @Override
    public Object newPut(Object key, SRowLock lock) {
        return new LTuple((String) key, new ArrayList(), (LRowLock) lock);
    }


    @Override
    public SGet newGet(Object rowKey, List families, List columns, Long effectiveTimestamp) {
        return new LGet(rowKey, rowKey, families, columns, effectiveTimestamp);
    }

    @Override
    public SScan newScan(Object startRowKey, Object endRowKey, List families, List columns, Long effectiveTimestamp) {
        return new LGet(startRowKey, endRowKey, families, columns, effectiveTimestamp);
    }

    @Override
    public Object getResultKey(Object result) {
        return getTupleKey(result);
    }

    @Override
    public Object getPutKey(Object put) {
        return getTupleKey(put);
    }

    private Object getTupleKey(Object result) {
        return ((LTuple) result).key;
    }

    private List getValuesForColumn(LTuple tuple, Object family, Object qualifier) {
        List<LKeyValue> values = ((LTuple) tuple).values;
        List<LKeyValue> results = new ArrayList<LKeyValue>();
        for (Object vRaw : values) {
            LKeyValue v = (LKeyValue) vRaw;
            if (valuesMatch(v.family, family) && valuesMatch(v.qualifier, qualifier)) {
                results.add(v);
            }
        }
        sort(results);
        return results;
    }

    @Override
    public List getResultColumn(Object result, Object family, Object qualifier) {
        List<Object> values = getValuesForColumn((LTuple) result, family, qualifier);
        sort(values);
        return values;
    }

    private void sort(List results) {
        Collections.sort(results, new Comparator<Object>() {
            @Override
            public int compare(Object simpleCell, Object simpleCell2) {
                return Long.valueOf(((LKeyValue) simpleCell2).timestamp).compareTo(((LKeyValue) simpleCell).timestamp);
            }
        });
    }

    @Override
    public Object getResultValue(Object result, Object family, Object qualifier) {
        final List valuesForColumn = getValuesForColumn((LTuple) result, family, qualifier);
        if (valuesForColumn.isEmpty()) {
            return null;
        }
        return ((LKeyValue) valuesForColumn.get(0)).value;
    }

    @Override
    public List listResult(Object result) {
        return listPut(result);
    }

    @Override
    public List listPut(Object put) {
        return ((LTuple) put).values;
    }

    @Override
    public Object getKeyValueFamily(Object keyValue) {
        return ((LKeyValue) keyValue).family;
    }

    @Override
    public Object getKeyValueQualifier(Object keyValue) {
        return ((LKeyValue) keyValue).qualifier;
    }

    @Override
    public Object getKeyValueValue(Object keyValue) {
        return ((LKeyValue) keyValue).value;
    }

    @Override
    public long getKeyValueTimestamp(Object keyValue) {
        return ((LKeyValue) keyValue).timestamp;
    }

}
