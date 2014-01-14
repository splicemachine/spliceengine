package com.splicemachine.si.data.light;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.si.data.api.SDataLib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LDataLib implements SDataLib<Object, LTuple, LKeyValue, Object, LTuple, LTuple, LGet, LGet, LRowLock, Object> {

    @Override
    public Object newRowKey(Object... args) {
        StringBuilder builder = new StringBuilder();
        for (Object a : args) {
            Object toAppend = a;
            if (a instanceof Short) {
                toAppend = String.format("%1$06d", a);
            } else if (a instanceof Long) {
                toAppend = String.format("%1$020d", a);
            } else if (a instanceof Byte) {
                toAppend = String.format("%1$02d", a);
            }
            builder.append(toAppend);
        }
        return builder.toString();
    }

    @Override
    public Object increment(Object key) {
        if (key instanceof String) {
            String s = (String) key;
            final byte[] bytes = BytesUtil.unsignedCopyAndIncrement(s.getBytes());
            return new String(bytes);
        } else {
            throw new RuntimeException("local data library does not implement increment for key of type: " + key.getClass().getSimpleName());
        }
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
    public boolean valuesEqual(Object value1, Object value2) {
        return value1.equals(value2);
    }

    @Override
    public void addKeyValueToPut(LTuple put, Object family, Object qualifier, Long timestamp, Object value) {
        addKeyValueToTuple(put, family, qualifier, timestamp, value);
    }

    private void addKeyValueToTuple(LTuple tuple, Object family, Object qualifier, Long timestamp, Object value) {
        LTuple lTuple = tuple;
        final LKeyValue newCell = new LKeyValue(lTuple.key, (String) family, (String) qualifier, timestamp, value);
        lTuple.values.add(newCell);
    }

    @Override
    public void addAttribute(Object operation, String attributeName, Object value) {
        if (operation instanceof LGet) {
            ((LGet) operation).attributes.put(attributeName, value);
        } else {
            LTuple lTuple = (LTuple) operation;
            lTuple.attributes.put(attributeName, value);
        }
    }

    @Override
    public Object getAttribute(Object operation, String attributeName) {
        if (operation instanceof LGet) {
            return ((LGet) operation).attributes.get(attributeName);
        } else {
            return ((LTuple) operation).attributes.get(attributeName);
        }
    }

    @Override
    public LTuple newResult(Object key, List<LKeyValue> keyValues) {
        return new LTuple((String) key, new ArrayList<LKeyValue>(keyValues));
    }

    @Override
    public LTuple newPut(Object key) {
        return newPut(key, null);
    }

    @Override
    public LTuple newPut(Object key, LRowLock lock) {
        return new LTuple((String) key, new ArrayList(), lock);
    }

    @Override
    public Object newFailStatus() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public LGet newGet(Object rowKey, List<Object> families, List<List<Object>> columns, Long effectiveTimestamp) {
        return new LGet(rowKey, rowKey, families, columns, effectiveTimestamp);
    }

    @Override
    public Object getGetRow(LGet get) {
        return get.startTupleKey;
    }

    @Override
    public void setGetTimeRange(LGet get, long minTimestamp, long maxTimestamp) {
        assert minTimestamp == 0L;
        get.effectiveTimestamp = maxTimestamp - 1;
    }

    @Override
    public void setGetMaxVersions(LGet get) {
    }

    @Override
    public void setGetMaxVersions(LGet get, int max) {
    }

    @Override
    public void addFamilyToGet(LGet get, Object family) {
        get.families.add(family);
    }

    @Override
    public void addFamilyToGetIfNeeded(LGet get, Object family) {
        ensureFamilyDirect(get, family);
    }

    @Override
    public void setScanTimeRange(LGet get, long minTimestamp, long maxTimestamp) {
        assert minTimestamp == 0L;
        get.effectiveTimestamp = maxTimestamp - 1;
    }

    @Override
    public void setScanMaxVersions(LGet get) {
    }

    @Override
    public void setScanMaxVersions(LGet get, int max) {
    }

    @Override
    public void addFamilyToScan(LGet get, Object family) {
        get.families.add(family);
    }

    @Override
    public void addFamilyToScanIfNeeded(LGet get, Object family) {
        ensureFamilyDirect(get, family);
    }

    private void ensureFamilyDirect(LGet lGet, Object family) {
        if (lGet.families.isEmpty() && (lGet.columns == null || lGet.columns.isEmpty())) {
        } else {
            if (lGet.families.contains(family)) {
            } else {
                lGet.families.add(family);
            }
        }
    }

    @Override
    public LGet newScan(Object startRowKey, Object endRowKey, List families, List columns, Long effectiveTimestamp) {
        return new LGet(startRowKey, endRowKey, families, columns, effectiveTimestamp);
    }

    @Override
    public Object getResultKey(LTuple result) {
        return getTupleKey(result);
    }

    @Override
    public Object getPutKey(LTuple put) {
        return getTupleKey(put);
    }

    private Object getTupleKey(Object result) {
        return ((LTuple) result).key;
    }

    private List<LKeyValue> getValuesForColumn(LTuple tuple, Object family, Object qualifier) {
        List<LKeyValue> values = tuple.values;
        List<LKeyValue> results = new ArrayList<LKeyValue>();
        for (Object vRaw : values) {
            LKeyValue v = (LKeyValue) vRaw;
            if (valuesMatch(v.family, family) && valuesMatch(v.qualifier, qualifier)) {
                results.add(v);
            }
        }
        LStore.sortValues(results);
        return results;
    }

    private List<LKeyValue> getValuesForFamily(LTuple tuple, Object family) {
        List<LKeyValue> values = tuple.values;
        List<LKeyValue> results = new ArrayList<LKeyValue>();
        for (Object vRaw : values) {
            LKeyValue v = (LKeyValue) vRaw;
            if (valuesMatch(v.family, family)) {
                results.add(v);
            }
        }
        return results;
    }

    @Override
    public List<LKeyValue> getResultColumn(LTuple result, Object family, Object qualifier) {
        List<LKeyValue> values = getValuesForColumn(result, family, qualifier);
        LStore.sortValues(values);
        return values;
    }

    @Override
    public Object getResultValue(LTuple result, Object family, Object qualifier) {
        final List<LKeyValue> valuesForColumn = getValuesForColumn(result, family, qualifier);
        if (valuesForColumn.isEmpty()) {
            return null;
        }
        return valuesForColumn.get(0).value;
    }

    @Override
    public Map getResultFamilyMap(LTuple result, Object family) {
        final List<LKeyValue> valuesForFamily = getValuesForFamily(result, family);
        final Map familyMap = new HashMap();
        for (LKeyValue kv : valuesForFamily) {
            familyMap.put(kv.qualifier, kv.value);
        }
        return familyMap;
    }

    @Override
    public List<LKeyValue> listResult(LTuple result) {
        return listPut(result);
    }

    @Override
    public List<LKeyValue> listPut(LTuple put) {
        final List<LKeyValue> values = put.values;
        LStore.sortValues(values);
        return values;
    }


    @Override
    public Object getKeyValueRow(LKeyValue keyValue) {
        return keyValue.rowKey;
    }

    @Override
    public Object getKeyValueFamily(LKeyValue keyValue) {
        return keyValue.family;
    }

    @Override
    public Object getKeyValueQualifier(LKeyValue keyValue) {
        return keyValue.qualifier;
    }

    @Override
    public Object getKeyValueValue(LKeyValue keyValue) {
        return keyValue.value;
    }

    @Override
    public long getKeyValueTimestamp(LKeyValue keyValue) {
        return keyValue.timestamp;
    }

    @Override
    public LTuple newDelete(Object rowKey) {
        return newPut(rowKey, null);
    }

    @Override
    public void addKeyValueToDelete(LTuple delete, Object family, Object qualifier, long timestamp) {
        addKeyValueToTuple(delete, family, qualifier, timestamp, null);
    }

    @Override
    public boolean matchingColumn(LKeyValue keyValue, Object family, Object qualifier) {
        return valuesMatch(keyValue.family, family) && valuesMatch(keyValue.qualifier, qualifier);
    }

    @Override
    public boolean matchingFamily(LKeyValue keyValue, Object family) {
        return valuesMatch(keyValue.family, family);
    }

    @Override
    public boolean matchingQualifier(LKeyValue keyValue, Object qualifier) {
        return valuesMatch(keyValue.qualifier, qualifier);
    }

    @Override
    public boolean matchingValue(LKeyValue keyValue, Object value) {
        return valuesMatch(keyValue.value, value);
    }

    @Override
    public boolean matchingFamilyKeyValue(LKeyValue keyValue, LKeyValue other) {
        return valuesMatch(keyValue.family, other.family);
    }

    @Override
    public boolean matchingQualifierKeyValue(LKeyValue keyValue, LKeyValue other) {
        return other == null ? false : valuesMatch(keyValue.qualifier, other.qualifier);
    }

    @Override
    public boolean matchingValueKeyValue(LKeyValue keyValue, LKeyValue other) {
        return other == null ? false : valuesMatch(keyValue.value, other.value);
    }

    @Override
    public boolean matchingRowKeyValue(LKeyValue keyValue, LKeyValue other) {
        return other == null ? false : valuesMatch(keyValue.rowKey, other.rowKey);
    }

    @Override
    public LKeyValue newKeyValue(LKeyValue keyValue, Object value) {
        return new LKeyValue(keyValue.rowKey, keyValue.family, keyValue.qualifier, keyValue.timestamp, value);
    }

    @Override
    public LKeyValue newKeyValue(Object rowKey, Object family, Object qualifier, Long timestamp, Object value) {
        return new LKeyValue((String) rowKey, (String) family, (String) qualifier, timestamp, value);
    }
}
