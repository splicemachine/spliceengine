package com.ir.hbase.index.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import com.ir.constants.bytes.SortableByteUtil;
import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexColumn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;        

public class SingleColumnValueFilter extends FilterBase {
	static final Log LOG = LogFactory.getLog(SingleColumnValueFilter.class);
	protected String columnFamily;
	protected String columnQualifier;
	private CompareOp compareOp;
	private Index index;
	private IndexColumn icol;
	private boolean foundColumn = false;
	private boolean matchedColumn = false;
	private boolean filterIfMissing = false;
	private boolean latestVersionOnly = true;
	private byte[] value;


	public SingleColumnValueFilter() {
	}

	public SingleColumnValueFilter(final String columnFamily, final String columnQualifier,
			final CompareOp compareOp, final byte[] value, Index index) {
		this.columnFamily = columnFamily;
		this.columnQualifier = columnQualifier;
		this.compareOp = compareOp;
		this.index = index;
		this.icol = index.getIndexColumn(columnFamily, columnQualifier);
		this.value = icol.toBytes(value, false);
	}

	/**
	 * 
	 * Return Compare Operation
	 * 
	 * @return compare operation
	 */
	public CompareOp getOperator() {
		return compareOp;
	}

	/**
	 * 
	 * Return family name of this column
	 * 
	 * @return byte array of family name
	 */
	public byte[] getFamily() {
		return Bytes.toBytes(columnFamily);
	}
	/**
	 * 
	 * Return qualifier of this column
	 * 
	 * @return byte array of column qualifier
	 */
	public byte[] getQualifier() {
		return Bytes.toBytes(columnQualifier);
	}
	/**
	 * 
	 * Compare given keyValue with Column Family and Column Qualifier and return the code representing result
	 *  
	 * @param keyValue key value to be compared
	 * @return the code representing the result
	 */
	public ReturnCode filterKeyValue(KeyValue keyValue) {
		if (this.matchedColumn) {
			return ReturnCode.INCLUDE;
		} else if (this.latestVersionOnly && this.foundColumn) {
			return ReturnCode.NEXT_ROW;
		}
		if (!keyValue.matchingColumn(this.getFamily(), this.getQualifier())) {
			return ReturnCode.INCLUDE;
		}
		foundColumn = true;
		if (filterColumnValue(keyValue.getBuffer(),
				keyValue.getValueOffset(), keyValue.getValueLength())) {
			return this.latestVersionOnly? ReturnCode.NEXT_ROW: ReturnCode.INCLUDE;
		}
		this.matchedColumn = true;
		return ReturnCode.INCLUDE;
	}
	/**
	 * 
	 * Compare column value from data specified by offset and length 
	 * with the field defined in indexDefinition. The result is base the 
	 * performed Compare Operation.  
	 * 
	 * @param data byte array to be compared
	 * @param offset start point of the sub-array to be compared
	 * @param length length of the sub-array to be compared
	 * @return boolean result based on the Compare Operation
	 */
	public boolean filterColumnValue(final byte [] data, final int offset,
			final int length) {
		int compareResult = Bytes.compareTo(value, icol.toBytes(Arrays.copyOfRange(data, offset, offset + length), false));
		switch (this.compareOp) {
		case LESS:
			return compareResult <= 0;
		case LESS_OR_EQUAL:
			return compareResult < 0;
		case EQUAL:
			return compareResult != 0;
		case NOT_EQUAL:
			return compareResult == 0;
		case GREATER_OR_EQUAL:
			return compareResult > 0;
		case GREATER:
			return compareResult >= 0;
		default:
			throw new RuntimeException("Unknown Compare op " + compareOp.name());
		}
	}

	/**
	 * 
	 * tell whether or not filter the row
	 * 
	 * @return boolean value whether or not filter the row
	 */
	public boolean filterRow() {
		return this.foundColumn? !this.matchedColumn: this.filterIfMissing;
	}

	/**
	 * 
	 * reset found column and matched column
	 *
	 */
	public void reset() {
		foundColumn = false;
		matchedColumn = false;
	}
	/**
	 * 
	 * return filter if missing that tells whether or not filter the the row if
	 * foundColumn returns false.
	 * 
	 * @return true if filter at missing, false otherwise 
	 */
	public boolean getFilterIfMissing() {
		return filterIfMissing;
	}

	/**
	 * 
	 * set filterIfMissing varialbe that tells whether or not filter the the row if
	 * foundColumn returns false.
	 * 
	 * @param filterIfMissing
	 */
	public void setFilterIfMissing(boolean filterIfMissing) {
		this.filterIfMissing = filterIfMissing;
	}

	/**
	 * 
	 * tell whether or not to get latest version only when performing comparison
	 * 
	 * @return true if getting latest version only
	 */
	public boolean getLatestVersionOnly() {
		return latestVersionOnly;
	}

	/**
	 * 
	 * set latestVersionOnly variable that tells whether or not to 
	 * get latest version only when performing comparison
	 * 
	 * @param latestVersionOnly
	 */
	public void setLatestVersionOnly(boolean latestVersionOnly) {
		this.latestVersionOnly = latestVersionOnly;
	}

	/**
	 * 
	 * Read fields and set bits from binary stream to initialize local variables 
	 * and configure operations.
	 * 
	 * @param input binary stream
	 * 
	 */
	@Override
	public void readFields(final DataInput in) throws IOException {  
		this.columnFamily = Bytes.toString(Bytes.readByteArray(in));
		this.columnQualifier = Bytes.toString(Bytes.readByteArray(in));
		this.value = Bytes.readByteArray(in);
		if(this.value.length == 0) {
			this.value = null;
		}
		this.compareOp = CompareOp.valueOf(in.readUTF());
		this.index = Index.toIndex(Bytes.toString(Bytes.readByteArray(in)));
		this.icol = index.getIndexColumn(columnFamily, columnQualifier);
		this.foundColumn = in.readBoolean();
		this.matchedColumn = in.readBoolean();
		this.filterIfMissing = in.readBoolean();
		this.latestVersionOnly = in.readBoolean();
	}

	/**
	 * 
	 * write fields and set bits to binary stream.
	 * 
	 * @param output binary stream
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		Bytes.writeByteArray(out, Bytes.toBytes(this.columnFamily));
		Bytes.writeByteArray(out, Bytes.toBytes(this.columnQualifier));
		Bytes.writeByteArray(out, this.value);
		out.writeUTF(compareOp.name());
		Bytes.writeByteArray(out, Bytes.toBytes(index.toJSon()));
		out.writeBoolean(foundColumn);
		out.writeBoolean(matchedColumn);
		out.writeBoolean(filterIfMissing);
		out.writeBoolean(latestVersionOnly);
	}
}