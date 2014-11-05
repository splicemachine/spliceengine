package com.splicemachine.si.api;

import org.apache.hadoop.hbase.filter.Filter;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/9/14
 */
public interface SIFilter<Data> {

		/**
		 * Reset the filter for the next row.
		 */
		void nextRow();

		/**
		 * @return the accumulator used in the filter
		 */
		RowAccumulator getAccumulator();

		/**
		 * Filter the specified keyvalue transactionally.
		 * @param kv the key value to filter
		 * @return a return code denoting whether or not this KeyValue should be included
		 * or not.
		 */
		Filter.ReturnCode filterKeyValue(Data kv) throws IOException;
}
