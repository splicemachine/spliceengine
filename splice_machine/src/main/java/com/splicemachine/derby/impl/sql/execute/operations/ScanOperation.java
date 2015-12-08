package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.ScanInformation;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableSet;

public abstract class ScanOperation extends SpliceBaseOperation {
		private static Logger LOG = Logger.getLogger(ScanOperation.class);
		private static long serialVersionUID=7l;
		public int lockMode;
		public int isolationLevel;
		protected boolean oneRowScan;
		protected ScanInformation scanInformation;
		protected String tableName;
		protected String indexName;
		public boolean isConstraint;
		public boolean forUpdate;
		protected ExecRow currentTemplate;
		protected int[] columnOrdering;
		protected int[] getColumnOrdering;
		protected EntryPredicateFilter predicateFilter;
		protected int[] keyDecodingMap;
        protected TransactionalRegion txnRegion;
        protected Scan scan;
        protected boolean scanSet = false;
        protected String scanQualifiersField;
        protected String tableVersion;

		public ScanOperation () {
				super();
		}

		public ScanOperation (long conglomId, Activation activation, int resultSetNumber,
													GeneratedMethod startKeyGetter, int startSearchOperator,
													GeneratedMethod stopKeyGetter, int stopSearchOperator,
													boolean sameStartStopPosition,
                                                    boolean rowIdKey,
													String scanQualifiersField,
													GeneratedMethod resultRowAllocator,
													int lockMode, boolean tableLocked, int isolationLevel,
													int colRefItem,
													int indexColItem,
													boolean oneRowScan,
													double optimizerEstimatedRowCount,
													double optimizerEstimatedCost, String tableVersion) throws StandardException {
				super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				this.lockMode = lockMode;
				this.isolationLevel = isolationLevel;
				this.oneRowScan = oneRowScan;
                this.scanQualifiersField = scanQualifiersField;
                this.tableVersion = tableVersion;
				this.scanInformation = new DerbyScanInformation(resultRowAllocator.getMethodName(),
								startKeyGetter!=null? startKeyGetter.getMethodName(): null,
								stopKeyGetter!=null ? stopKeyGetter.getMethodName(): null,
								scanQualifiersField!=null? scanQualifiersField : null,
								conglomId,
								colRefItem,
								indexColItem,
								sameStartStopPosition,
								startSearchOperator,
								stopSearchOperator,
                                rowIdKey,
                                tableVersion
				);
		}

		protected int[] getColumnOrdering() throws StandardException{
				if (columnOrdering == null) {
						columnOrdering = scanInformation.getColumnOrdering();
				}
				return columnOrdering;
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				super.readExternal(in);
				oneRowScan = in.readBoolean();
				lockMode = in.readInt();
				isolationLevel = in.readInt();
				scanInformation = (ScanInformation)in.readObject();
                tableVersion = in.readUTF();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeBoolean(oneRowScan);
				out.writeInt(lockMode);
				out.writeInt(isolationLevel);
				out.writeObject(scanInformation);
                out.writeUTF(tableVersion);
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "init called");
				super.init(context);
				scanInformation.initialize(context);
				try {
						ExecRow candidate = scanInformation.getResultRow();
						currentRow = operationInformation.compactRow(candidate, scanInformation);
						currentTemplate = currentRow.getClone();
						if (currentRowLocation == null)
								currentRowLocation = new HBaseRowLocation();
                        this.txnRegion = context.getTransactionalRegion();
				} catch (Exception e) {
						SpliceLogUtils.logAndThrowRuntime(LOG, "Operation Init Failed!", e);
				}
		}

		@Override
		public SpliceOperation getLeftOperation() {
				return null;
		}

		protected void initIsolationLevel() {
				SpliceLogUtils.trace(LOG, "initIsolationLevel");
		}

		public Scan getNonSIScan() throws StandardException {
				/*
				 * Intended to get a scan which does NOT set up SI underneath us (since
				 * we are doing it ourselves).
				 */
				Scan scan = buildScan();
				if (oneRowScan) {
                    		scan.setSmall(true);
				            scan.setCaching(2); // Limit the batch size for performance
                    		// Setting caching to 2 instead of 1 removes an extra RPC during Single Row Result Scans
				}
				deSiify(scan);
				return scan;
		}

        public Scan getReversedNonSIScan() throws StandardException {
            Scan scan = getNonSIScan();
            scan.setReversed(true);
            return scan;
        }

		/**
		 * Get the Stored format ids for the columns in the key. The returned int[] is ordered
		 * by the encoding order of the keys.
		 *
		 * @return the format ids for the columns in the key.
		 * @throws StandardException
		 */
		protected int[] getKeyFormatIds() throws StandardException {
				int[] keyColumnEncodingOrder = scanInformation.getColumnOrdering();
				if(keyColumnEncodingOrder==null) return null; //no keys to worry about
				int[] allFormatIds = scanInformation.getConglomerate().getFormat_ids();
				int[] keyFormatIds = new int[keyColumnEncodingOrder.length];
				for(int i=0,pos=0;i<keyColumnEncodingOrder.length;i++){
						int keyColumnPosition = keyColumnEncodingOrder[i];
						if(keyColumnPosition>=0){
								keyFormatIds[pos] = allFormatIds[keyColumnPosition];
								pos++;
						}
				}
				return keyFormatIds;
		}

		/**
		 * @return a map from the accessed (desired) key columns to their position in the decoded row.
		 * @throws StandardException
		 */
		protected int[] getKeyDecodingMap() throws StandardException {
				if(keyDecodingMap ==null){
						FormatableBitSet pkCols = scanInformation.getAccessedPkColumns();

						int[] keyColumnEncodingOrder = scanInformation.getColumnOrdering();
						int[] baseColumnMap = operationInformation.getBaseColumnMap();

						int[] kDecoderMap = new int[keyColumnEncodingOrder.length];
						Arrays.fill(kDecoderMap, -1);
						for(int i=0;i<keyColumnEncodingOrder.length;i++){
								int baseKeyColumnPosition = keyColumnEncodingOrder[i]; //the position of the column in the base row
								if(pkCols.get(i)){
										kDecoderMap[i] = baseColumnMap[baseKeyColumnPosition];
										baseColumnMap[baseKeyColumnPosition] = -1;
								}else
										kDecoderMap[i] = -1;
						}


						keyDecodingMap = kDecoderMap;
				}
				return keyDecodingMap;
		}


		protected void deSiify(Scan scan) {
				/*
				 * Remove SI-specific behaviors from the scan, so that we can handle it ourselves correctly.
				 */
				//exclude this from SI treatment, since we're doing it internally
				scan.setAttribute(SIConstants.SI_NEEDED,null);
                scan.setMaxVersions();
				Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
				if(familyMap!=null){
						NavigableSet<byte[]> bytes = familyMap.get(SpliceConstants.DEFAULT_FAMILY_BYTES);
						if(bytes!=null)
								bytes.clear(); //make sure we get all columns
				}
		}

		protected Scan buildScan() throws StandardException {
		    return getScan();
		}

		protected Scan getScan() throws StandardException {
			return scanInformation.getScan(getCurrentTransaction(),
					((BaseActivation) activation).getScanStartOverride(), getKeyDecodingMap(), null, ((BaseActivation) activation).getScanStopOverride());
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) {
				return operationInformation.getBaseColumnMap();
		}

		@Override
		public boolean isReferencingTable(long tableNumber){
				return tableName.equals(String.valueOf(tableNumber));
		}

		/**
		 * Returns the relational table name for the scan, e.g. LINEITEM,
		 * not the conglomerate number.
		 */
		public String getDisplayableTableName() {
		    // TODO: purge this method. It worked on dev branch but not any more
		    return tableName;
//		    if (scanInformation == null) return tableName; // avoid NPE
//            try {
//                return scanInformation.getConglomerate().getString();
//            } catch (StandardException e) {
//                SpliceLogUtils.logAndThrowRuntime(LOG,e);
//                return null;
//    		}
		}
		
		public String getTableName(){
				return this.tableName;
		}

		public String getIndexName() {
				return this.indexName;
		}

		@Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n"+ Strings.repeat("\t",indentLevel);
				return new StringBuilder("Scan:")
								.append(indent).append("resultSetNumber:").append(resultSetNumber)
								.append(indent).append("optimizerEstimatedCost:").append(optimizerEstimatedCost).append(",")
								.append(indent).append("optimizerEstimatedRowCount:").append(optimizerEstimatedRowCount).append(",")								
								.append(indent).append("scanInformation:").append(scanInformation)
								.append(indent).append("tableName:").append(tableName)
								.toString();
		}

		public int[] getKeyColumns() {
				return columnOrdering;
		}

    public void setScan(Scan scan) {
        this.scan = scan;
        this.scanSet = true;
    }

	@Override
	public ExecIndexRow getStartPosition() throws StandardException {
		return scanInformation.getStartPosition();
	}

    public String getTableVersion() {
        return tableVersion;
    }
}
