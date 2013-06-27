package com.splicemachine.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This filter is used to filter rows based on whether or not the column key exists
 * (for IS NULL or IS NOT NULL filtering).  Since HBase only stores non-null values,
 * checking whether or not column exists is enough for the filtering. Additionally,
 * this filter will short circuit when an attempt to make a numerical
 * <code>(<, >, =, <=, >=, etc)</code> with a null value -- no columns will every match.
 *
 * @see org.apache.hadoop.hbase.filter.SingleColumnValueFilter
 *
 * @author jessiezhang
 */

public class ColumnNullableFilter extends FilterBase {

    protected byte [] columnFamily;
	protected byte [] columnQualifier;
	private boolean foundColumn = false;
	private boolean filterIfMissing = false;
    private boolean isNullNumericalComparison;

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
     * @param isNullNumericalComparison comes from the qualifier,
     *                                  if isOrderedNulls == false && null involved in comparison,
     *                                  the result is unknown
     * @param filterIfMissing whether we should us "IS NULL" or "IS NOT NULL" filter
     *                        criteria. <code>true => "IS NOT NULL"; false => "IS NULL"</code>
     */
	public ColumnNullableFilter(final byte[] family,
                                final byte[] qualifier,
                                final boolean isNullNumericalComparison,
                                final boolean filterIfMissing) {
		this.columnFamily = family;
		this.columnQualifier = qualifier;
        this.isNullNumericalComparison = isNullNumericalComparison;
        this.filterIfMissing = filterIfMissing; // true => "IS NOT NULL"; false => "IS NULL"
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

	public ReturnCode filterKeyValue(KeyValue keyValue) {
        if (this.foundColumn) {
			return ReturnCode.INCLUDE;
        }

        if (this.isNullNumericalComparison) {
            // a numerical comparison with null will never match any columns
            return ReturnCode.NEXT_ROW;
        }

        this.foundColumn = keyValue.matchingColumn(this.columnFamily, this.columnQualifier) && keyValue.getValue().length > 0;

        return ReturnCode.INCLUDE;
    }

    public boolean filterRow() {
        /*
         * For SQL "IS NULL" and "IS NOT NULL" criteria...
         * If foundColumn == true, it means that the col had a non-null value (see filterKeyValue)
         * Given that, and:
         *      if filterIfMissing == true (SQL "IS NOT NULL"), and:
         *          foundColumn == true, return false (don't filter - the col was non-null and we want non-nulls)
         *          foundColumn == false, return true (filter - the col was null and we don't want nulls)
         *      if filterIfMissing == false (SQL "IS NULL"), and:
         *          foundColumn == true, return true (filter - the col was not null but we want nulls)
         *          foundColumn == false, return false (don't filter - the col was null and we want nulls)
         */
        if (isNullNumericalComparison) {
            // a numerical comparison with null should never match any columns
            return true;
        } else if (filterIfMissing) {
            return !foundColumn;
        } else {
            return foundColumn;
        }
	}

	public void reset() {
		foundColumn = false;
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
		this.foundColumn = in.readBoolean();
        this.isNullNumericalComparison = in.readBoolean();
		this.filterIfMissing = in.readBoolean();
	}

	public void write(final DataOutput out) throws IOException {
		Bytes.writeByteArray(out, this.columnFamily);
		Bytes.writeByteArray(out, this.columnQualifier);
		out.writeBoolean(foundColumn);
        out.writeBoolean(isNullNumericalComparison);
		out.writeBoolean(filterIfMissing);
	}
}