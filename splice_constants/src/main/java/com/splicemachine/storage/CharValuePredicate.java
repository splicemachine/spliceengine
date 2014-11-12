package com.splicemachine.storage;

import org.apache.hadoop.hbase.filter.CompareFilter;

/**
 * A Predicate that compares Char values.
 *
 * with Char data types, the insertion will have additional whitespace (potentially),
 * but the scan is not required (and often doesn't) have the same whitespace. In order to
 * not break the case when users specifically include whitespace, we don't want to trim
 * during insertion. So instead we use this comparison (instead of the ValuePredicate)
 * which will ignore the whitespace byte during comparison.
 *
 * @author Scott Fines
 * Date: 3/4/14
 */
public class CharValuePredicate extends ValuePredicate{

		public CharValuePredicate(CompareFilter.CompareOp compareOp,
															int column,
															byte[] compareValue,
															boolean removeNullEntries,
															boolean desc) {
				super(compareOp, column, compareValue, removeNullEntries,desc);
		}

		@Override
		protected int doComparison(byte[] data, int offset, int length) {
				if(length<=compareValue.length){
						/*
						 * We are looking at data that cannot contain extra
						 * whitespace, so just delegate to our parent behavior
						 */
						return super.doComparison(data,offset,length);
				}

				//find any whitespace in the data
				int pos = offset+length-1;
				//go backwards in steps of 8
				while(pos>=offset &&(pos-offset>=compareValue.length) && data[pos]==34){ //34 is the encoded value of a whitespace terminator
					pos--;
				}

				int adjustedWhitespaceLength = pos-offset+1;

				return super.doComparison(data,offset,adjustedWhitespaceLength);
		}

		@Override
		public byte[] toBytes() {
				return super.toBytes();
		}

		@Override protected PredicateType getType() { return PredicateType.CHAR_VALUE; }
}
