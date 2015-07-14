package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.*;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.mrio.api.SpliceTableMapReduceUtil;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.commons.lang.SerializationUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Base64;

/**
 * Companion Builder class for SITableScanner
 * @author Scott Fines
 * Date: 4/9/14
 */
public class TableScannerBuilder implements Externalizable {
		protected MeasuredRegionScanner scanner;
		protected	ExecRow template;
		protected	MetricFactory metricFactory;
		protected	Scan scan;
		protected	int[] rowColumnMap;
		protected TxnView txn;
		protected	int[] keyColumnEncodingOrder;
		protected	int[] keyColumnTypes;
		protected	int[] keyDecodingMap;
		protected	FormatableBitSet accessedKeys;
	    protected  boolean reuseRowLocation = true;
		protected	String indexName;
		protected	String tableVersion;
		protected SIFilterFactory filterFactory;
		protected boolean[] keyColumnSortOrder;
		protected TransactionalRegion region;
		protected int[] execRowTypeFormatIds;

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
		public TableScannerBuilder execRowTypeFormatIds(int[] execRowTypeFormatIds) {
			assert execRowTypeFormatIds!=null : "Null ExecRow formatIDs are not allowed!";
			this.execRowTypeFormatIds = execRowTypeFormatIds;
			return this;
	}

		public TableScannerBuilder transactionID(String transactionID) {
				throw new UnsupportedOperationException("REMOVE");
//				assert transactionID!=null: "No transaction id specified";
//				this.transactionID = transactionID;
//				return this;
		}

		public TableScannerBuilder transaction(TxnView txn){
				assert txn!=null: "No Transaction specified";
				this.txn = txn;
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

		public TableScannerBuilder reuseRowLocation(boolean reuseRowLocation) {
			this.reuseRowLocation = reuseRowLocation;
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

		public TableScannerBuilder region(TransactionalRegion region){
				this.region = region;
				return this;
		}

		public SITableScanner build(){
				return new SITableScanner(SIFactoryDriver.siFactory.getDataLib(),scanner,
								region,
								template,
								metricFactory==null?Metrics.noOpMetricFactory():metricFactory,
								scan,
								rowColumnMap,
								txn,
								keyColumnEncodingOrder,
								keyColumnSortOrder,
								keyColumnTypes,
								keyDecodingMap,
								accessedKeys,
						        reuseRowLocation,
								indexName,
								tableVersion,
								filterFactory);

		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {	
			ArrayUtil.writeIntArray(out, execRowTypeFormatIds);
			out.writeUTF(SpliceTableMapReduceUtil.convertScanToString(scan));
			ArrayUtil.writeIntArray(out, rowColumnMap);
			TransactionOperations.getOperationFactory().writeTxn(txn, out);
			ArrayUtil.writeIntArray(out, keyColumnEncodingOrder);
            out.writeBoolean(keyColumnSortOrder != null);
            if (keyColumnSortOrder != null) {
			    ArrayUtil.writeBooleanArray(out, keyColumnSortOrder);
            }
			ArrayUtil.writeIntArray(out, keyColumnTypes);
            out.writeBoolean(keyDecodingMap != null);
            if (keyDecodingMap != null) {
                ArrayUtil.writeIntArray(out, keyDecodingMap);
            }
			out.writeObject(accessedKeys);
			out.writeBoolean(indexName!=null);
			if (indexName!=null)
				out.writeUTF(indexName);
			out.writeBoolean(tableVersion!=null);
			if (tableVersion!=null)
				out.writeUTF(tableVersion);			

		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
			execRowTypeFormatIds = ArrayUtil.readIntArray(in);
			scan = SpliceTableMapReduceUtil.convertStringToScan(in.readUTF());
			rowColumnMap = ArrayUtil.readIntArray(in);
			txn = TransactionOperations.getOperationFactory().readTxn(in);
			keyColumnEncodingOrder = ArrayUtil.readIntArray(in);
            if (in.readBoolean()) {
                keyColumnSortOrder = ArrayUtil.readBooleanArray(in);
            }
			keyColumnTypes = ArrayUtil.readIntArray(in);
            if (in.readBoolean()) {
                keyDecodingMap = ArrayUtil.readIntArray(in);
            }
			accessedKeys = (FormatableBitSet) in.readObject();
			if (in.readBoolean())
				indexName = in.readUTF();
			if (in.readBoolean())				
				tableVersion = in.readUTF();
		}
		
		public static TableScannerBuilder getTableScannerBuilderFromBase64String(String base64String) throws IOException, StandardException {
			if (base64String == null)
				throw new IOException("tableScanner base64 String is null");
			return (TableScannerBuilder) SerializationUtils.deserialize(Base64.decode(base64String));
		}

		public String getTableScannerBuilderBase64String() throws IOException, StandardException {
			return Base64.encodeBytes(SerializationUtils.serialize(this));
		}

		public Scan getScan() {
			return scan;
		}

		@Override
		public String toString() {
			return String.format("template=%s, scan=%s, rowColumnMap=%s, txn=%s, "
					+ "keyColumnEncodingOrder=%s, keyColumnSortOrder=%s, keyColumnTypes=%s, keyDecodingMap=%s, "
					+ "accessedKeys=%s, indexName=%s, tableVerson=%s",
					template,scan,rowColumnMap!=null?Arrays.toString(rowColumnMap):"NULL",
							txn,
							keyColumnEncodingOrder!=null?Arrays.toString(keyColumnEncodingOrder):"NULL",
							keyColumnSortOrder!=null?Arrays.toString(keyColumnSortOrder):"NULL",
							keyColumnTypes!=null?Arrays.toString(keyColumnTypes):"NULL",
							keyDecodingMap!=null?Arrays.toString(keyDecodingMap):"NULL",
							accessedKeys,indexName,tableVersion);
		}

		public int[] getExecRowTypeFormatIds() {
			return execRowTypeFormatIds;
		}

}
