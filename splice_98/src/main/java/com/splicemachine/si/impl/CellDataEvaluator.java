package com.splicemachine.si.impl;

public class CellDataEvaluator {/*implements DataEvaluator<Cell>{
	static private CellDataEvaluator INSTANCE;
	
	static {
		INSTANCE = new CellDataEvaluator();
	}

	private CellDataEvaluator() {}
	
	public static CellDataEvaluator getInstance() {
		return INSTANCE;
	}
	
	@Override
	public boolean singleMatchingColumn(Cell element, byte[] family,
			byte[] qualifier) {
		return CellUtils.singleMatchingColumn(element, family, qualifier);
	}

	@Override
	public boolean singleMatchingFamily(Cell element, byte[] family) {
		return CellUtils.singleMatchingFamily(element, family);
	}

	@Override
	public boolean singleMatchingQualifier(Cell element, byte[] qualifier) {
		return CellUtils.singleMatchingQualifier(element, qualifier);
	}

	@Override
	public boolean matchingValue(Cell element, byte[] value) {
		return CellUtils.matchingValue(element, value);
	}

	@Override
	public boolean matchingFamilyKeyValue(Cell element, Cell other) {
		return CellUtils.matchingFamilyKeyValue(element, other);
	}

	@Override
	public boolean matchingQualifierKeyValue(Cell element, Cell other) {
		return CellUtils.matchingQualifierKeyValue(element, other);
	}

	@Override
	public boolean matchingRowKeyValue(Cell element, Cell other) {
		return CellUtils.matchingRowKeyValue(element, other);
	}

	@Override
	public Cell newValue(Cell element, byte[] value) {
		return CellUtils.newKeyValue(element, value);
	}

	@Override
	public Cell newValue(byte[] rowKey, byte[] family, byte[] qualifier,
			Long timestamp, byte[] value) {
		return CellUtils.newKeyValue(rowKey, family, qualifier, timestamp, value);
	}

	@Override
	public Comparator getComparator() {
		return KeyValue.COMPARATOR; // Is this right? JL
	}
	
	@Override
	public long getTimestamp(Cell element) {
		return element.getTimestamp();
	}

	@Override
	public boolean isAntiTombstone(Cell element, byte[] antiTombstone) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getFamilyAsString(Cell element) {
		return Bytes.toString(CellUtil.cloneFamily(element));
	}

	@Override
	public String getQualifierAsString(Cell element) {
		return Bytes.toString(CellUtil.cloneQualifier(element));
	}
		*/
}
