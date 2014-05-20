package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.stats.MetricFactory;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Companion Builder class for SITableScanner
 * @author Scott Fines
 * Date: 4/9/14
 */
public class TableScannerBuilder {
		private MeasuredRegionScanner scanner;
		private	ExecRow template;
		private	MetricFactory metricFactory;
		private	Scan scan;
		private	int[] rowColumnMap;
		private	String transactionID;
		private	int[] keyColumnEncodingOrder;
		private	int[] keyColumnTypes;
		private	int[] keyDecodingMap;
		private	FormatableBitSet accessedKeys;
		private	String indexName;
		private	String tableVersion;

		private SIFilterFactory filterFactory;
		private boolean[] keyColumnSortOrder;

		public TableScannerBuilder scanner(MeasuredRegionScanner scanner) {
				assert scanner !=null :"Null scanners are not allowed!";
				this.scanner = scanner;
				return this;
		}

		public TableScannerBuilder template(ExecRow template) {
				assert template !=null :"Null template rows are not allowed!";
				this.template = template;
				return this;
		}

		public TableScannerBuilder metricFactory(MetricFactory metricFactory) {
				this.metricFactory = metricFactory;
				return this;
		}

		public TableScannerBuilder scan(Scan scan) {
				assert scan!=null : "Null scans are not allowed!";
				this.scan = scan;
				return this;
		}

		public TableScannerBuilder transactionID(String transactionID) {
				assert transactionID!=null: "No transaction id specified";
				this.transactionID = transactionID;
				return this;
		}

		/**
		 * Set the row decoding map.
		 *
		 * For example, if your row is (a,b,c,d), and the key columns are (c,a).Now, suppose
		 * that you are returning rows (a,c,d); then, the row decoding map would be [-1,-1,-1,2] (d's position
		 * in the entire row is 3, so it has to be located at that index, and it's location in the decoded row is 2,
		 * so that's the value).
		 *
		 * Note that the row decoding map should be -1 for all row elements which are kept in the key.
		 *
		 * @param rowDecodingMap the map for decoding the row values.
		 * @return a Builder with the rowDecodingMap set.
		 */
		public TableScannerBuilder rowDecodingMap(int[] rowDecodingMap) {
				assert rowDecodingMap!=null: "Null column maps are not allowed";
				this.rowColumnMap = rowDecodingMap;
				return this;
		}


		/**
		 * Set the encoding order for the key columns.
		 *
		 * For example, if your row is (a,b,c,d), the keyColumnOrder is as follows
		 *
		 * 1. keys = (a)   => keyColumnEncodingOrder = [0]
		 * 2. keys = (a,b) => keyColumnEncodingOrder = [0,1]
		 * 3. keys = (a,c) => keyColumnEncodingOrder = [0,2]
		 * 4. keys = (c,a) => keyColumnEncodingOrder = [2,0]
		 *
		 * So, in general, the keyColumnEncodingOrder is the order in which keys are encoded,
		 * referencing their column position IN THE ENTIRE ROW.
		 *
		 * @param keyColumnEncodingOrder the order in which keys are encoded, referencing their column
		 *                               position in the ENTIRE ROW.
		 * @return a Builder with the keyColumnEncodingOrder set
		 */
		public TableScannerBuilder keyColumnEncodingOrder(int[] keyColumnEncodingOrder) {
				this.keyColumnEncodingOrder = keyColumnEncodingOrder;
				return this;
		}

		/**
		 * Set the sort order for the key columns, IN THE ORDER THEY ARE ENCODED.
		 *
		 * That is, if the table is (a,b,c,d) and the key is (a asc,c desc,b desc), then
		 * {@code keyColumnEncodingOrder = [0,2,1]}, and {@code keyColumnSortOrder = [true,false,false]}
		 *
		 * @param keyColumnSortOrder the sort order of each key, in the order in which keys are encoded.
		 * @return a builder with keyColumnSortOrder set
		 */
		public TableScannerBuilder keyColumnSortOrder(boolean[] keyColumnSortOrder){
				this.keyColumnSortOrder = keyColumnSortOrder;
				return this;
		}

		/**
		 * Set the types of the key columns, IN THE ORDER IN WHICH THEY ARE ENCODED.
		 *  So if the keyColumnEncodingOrder = [2,0], then keyColumnTypes[0] should be the type
		 *  of column 2, while keyColumnTypes[1] is the type of column 0.
		 *
		 * @param keyColumnTypes the data types for ALL key columns, in the order the keys were encoded
		 * @return a Builder with the key column types set
		 */
		public TableScannerBuilder keyColumnTypes(int[] keyColumnTypes) {
				this.keyColumnTypes = keyColumnTypes;
				return this;
		}

		/**
		 * Specify the location IN THE DESTINATION ROW where the key columns are intended.
		 *
		 * For example, Suppose you are scanning a row (a,b,c,d), and the key is (c,a). Now
		 * say you want to return (a,c,d). Then your keyColumnEncodingOrder is [2,0], and
		 * your keyDecodingMap is [1,0] (the first entry is 1 because the destination location of
		 * column c is 1; the second entry is 0 because a is located in position 0 in the main row).
		 *
		 * @param keyDecodingMap the map from the keyColumnEncodingOrder to the location in the destination
		 *                       row.
		 * @return a Builder with the key decoding map set.
		 */
		public TableScannerBuilder keyDecodingMap(int[] keyDecodingMap) {
				this.keyDecodingMap = keyDecodingMap;
				return this;
		}

		/**
		 * Specify which key columns IN THE ENCODING ORDER are to be decoded.
		 *
		 * For example, suppose you are scanning a row (a,b,c,d) with a key of (c,a). Now say
		 * you want to return (a,b,d). Then accessedKeyColumns = {1}, because 1 is the location IN THE KEY
		 * of the column of interest.
		 *
		 * This can be constructed if you have {@code keyColumnEncodingOrder} and {@code keyDecodingMap},
		 * as defined by:
		 *
		 * for(int i=0;i<keyColumnEncodingOrder.length;i++){
		 *     int decodingPosition = keyDecodingMap[keyColumnEncodingOrder[i]];
		 *     if(decodingPosition>=0)
		 *     	accessedKeyColumns.set(i);
		 * }
		 *
		 * Note: the above assumes that keyDecodingMap has a negative number when key columns are not interesting to us.
		 *
		 * @param accessedKeyColumns the keys which are to be decoded, IN THE KEY ENCODING ORDER.
		 * @return a Builder with the accessedKeyColumns set.
		 */
		public TableScannerBuilder accessedKeyColumns(FormatableBitSet accessedKeyColumns) {
				this.accessedKeys = accessedKeyColumns;
				return this;
		}

		public TableScannerBuilder indexName(String indexName) {
				this.indexName = indexName;
				return this;
		}

		public TableScannerBuilder tableVersion(String tableVersion) {
				this.tableVersion = tableVersion;
				return this;
		}

		public TableScannerBuilder filterFactory(SIFilterFactory filterFactory) {
				this.filterFactory = filterFactory;
				return this;
		}

		public SITableScanner build(){
				return new SITableScanner(scanner,
								template,
								metricFactory,
								scan,
								rowColumnMap,
								transactionID,
								keyColumnEncodingOrder,
								keyColumnSortOrder,
								keyColumnTypes,
								keyDecodingMap,
								accessedKeys,
								indexName,
								tableVersion,
								filterFactory);

		}
}
