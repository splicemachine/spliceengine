package com.splicemachine.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;

/**
 * This filter is used to filter rows based on whether or not the column key exists
 * (for IS NULL or IS NOT NULL filtering). Since HBase only stores non-null values, 
 * checking whether or not column exists is enough for the filtering.
 * It takes a CompareFilter.CompareOp operator (equal, not equal), a column family 
 * and a column qualifier
 * 
 * @author jessiezhang
 */

public class ColumnNullableFilter extends FilterBase {
	static final Log LOG = LogFactory.getLog(ColumnNullableFilter.class);

	protected byte [] columnFamily;
	protected byte [] columnQualifier;
	private CompareOp compareOp;
	private boolean foundColumn = false;
	private boolean filterIfMissing = false;

	/**
	 * Writable constructor, do not use.
	 */
	public ColumnNullableFilter() {
	}

	/**
	 * Constructor for binary compare of the value of a single column.  If the
	 * column is found and the condition passes, all columns of the row will be
	 * emitted.  If the condition fails, the row will not be emitted.
	 * <p>
	 * Use the filterIfMissing flag to set whether the rest of the columns
	 * in a row will be emitted if the specified column to check is not found in
	 * the row.
	 *
	 * @param family name of column family
	 * @param qualifier name of column qualifier
	 * @param compareOp operator
	 */
	public ColumnNullableFilter(final byte [] family, final byte [] qualifier, final CompareOp compareOp) {
		this.columnFamily = family;
		this.columnQualifier = qualifier;
		this.compareOp = compareOp;
		if (CompareFilter.CompareOp.EQUAL.equals(compareOp))  //is null
			this.filterIfMissing = false;
		else //IS NOT NULL
			this.filterIfMissing = true; 
	}

	/**
	 * @return operator
	 */
	public CompareOp getOperator() {
		return compareOp;
	}

	/**
	 * @return the family
	 */
	public byte[] getFamily() {
		return columnFamily;
	}

	/**
	 * @return the qualifier
	 */
	public byte[] getQualifier() {
		return columnQualifier;
	}

	/**
	 * Get whether entire row should be filtered if column is not found.
	 * @return true if row should be skipped if column not found, false if row
	 * should be let through anyways
	 */
	public boolean getFilterIfMissing() {
		return filterIfMissing;
	}

	/**
	 * Set whether entire row should be filtered if column is not found.
	 * <p>
	 * If true, the entire row will be skipped if the column is not found.
	 * <p>
	 * If false, the row will pass if the column is not found.  This is default.
	 * @param filterIfMissing flag
	 */
	public void setFilterIfMissing(boolean filterIfMissing) {
		this.filterIfMissing = filterIfMissing;
	}

	public ReturnCode filterKeyValue(KeyValue keyValue) {
		if (this.foundColumn) 
			return ReturnCode.NEXT_ROW;

		if (!keyValue.matchingColumn(this.columnFamily, this.columnQualifier)) {
			this.foundColumn = false;
			return ReturnCode.INCLUDE;
		}

		this.foundColumn = true;
		return ReturnCode.INCLUDE;
	}

	public boolean filterRow() {
		// If column was found, return false if it was filterIfMissing, true if it was not
		// If column not found, return true if filterMissing, false if not
		return this.filterIfMissing ? !this.foundColumn : this.foundColumn;
	}

	public void reset() {
		foundColumn = false;
	}

	public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
		Preconditions.checkArgument(filterArguments.size() == 3 || filterArguments.size() == 4,
				"Expected 3 or 4 but got: %s", filterArguments.size());
		byte [] family = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
		byte [] qualifier = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));
		CompareOp compareOp = ParseFilter.createCompareOp(filterArguments.get(2));

		ColumnNullableFilter filter = new ColumnNullableFilter(family, qualifier, compareOp);

		//if (CompareFilter.CompareOp.EQUAL.equals(compareOp))  //is null
		//	filter.setFilterIfMissing(false);
		//else //IS NOT NULL
		//	filter.setFilterIfMissing(true); 
		
		if (filterArguments.size() == 4) {
			boolean filterIfMissing = ParseFilter.convertByteArrayToBoolean(filterArguments.get(3));
			filter.setFilterIfMissing(filterIfMissing);
		}
		return filter;
	}

	public void readFields(final DataInput in) throws IOException {
		this.columnFamily = Bytes.readByteArray(in);
		if(this.columnFamily.length == 0) {
			this.columnFamily = null;
		}
		this.columnQualifier = Bytes.readByteArray(in);
		if(this.columnQualifier.length == 0) {
			this.columnQualifier = null;
		}
		this.compareOp = CompareOp.valueOf(in.readUTF());
		this.foundColumn = in.readBoolean();
		this.filterIfMissing = in.readBoolean();
	}

	public void write(final DataOutput out) throws IOException {
		Bytes.writeByteArray(out, this.columnFamily);
		Bytes.writeByteArray(out, this.columnQualifier);
		out.writeUTF(compareOp.name());
		out.writeBoolean(foundColumn);
		out.writeBoolean(filterIfMissing);
	}
}