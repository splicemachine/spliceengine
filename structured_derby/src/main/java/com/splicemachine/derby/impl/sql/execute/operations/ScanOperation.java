package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.Qualifier;
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
		protected EntryDecoder rowDecoder;
		protected MultiFieldDecoder keyDecoder;
		protected EntryPredicateFilter predicateFilter;
		protected int[] keyDecodingMap;
    protected TransactionalRegion txnRegion;
        protected Scan scan;
        protected boolean scanSet = false;
        protected String scanQualifiersField;

		public ScanOperation () {
				super();
		}

		public ScanOperation (long conglomId, Activation activation, int resultSetNumber,
													GeneratedMethod startKeyGetter, int startSearchOperator,
													GeneratedMethod stopKeyGetter, int stopSearchOperator,
													boolean sameStartStopPosition,
													String scanQualifiersField,
													GeneratedMethod resultRowAllocator,
													int lockMode, boolean tableLocked, int isolationLevel,
													int colRefItem,
													boolean oneRowScan,
													double optimizerEstimatedRowCount,
													double optimizerEstimatedCost) throws StandardException {
				super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				this.lockMode = lockMode;
				this.isolationLevel = isolationLevel;
				this.oneRowScan = oneRowScan;
                this.scanQualifiersField = scanQualifiersField;

				this.scanInformation = new DerbyScanInformation(resultRowAllocator.getMethodName(),
								startKeyGetter!=null? startKeyGetter.getMethodName(): null,
								stopKeyGetter!=null ? stopKeyGetter.getMethodName(): null,
								scanQualifiersField!=null? scanQualifiersField : null,
								conglomId,
								colRefItem,
								sameStartStopPosition,
								startSearchOperator,
								stopSearchOperator
				);
		}

		public ScanOperation(ScanInformation scanInformation,
												 OperationInformation operationInformation,
												 int lockMode,
												 int isolationLevel) throws StandardException {
				super(operationInformation);
				this.lockMode = lockMode;
				this.isolationLevel = isolationLevel;
				this.scanInformation = scanInformation;
		}

		protected MultiFieldDecoder getKeyDecoder() {
				if (keyDecoder == null)
						keyDecoder = MultiFieldDecoder.create();
				return keyDecoder;
		}

		protected EntryDecoder getRowDecoder() {
				if(rowDecoder==null)
						rowDecoder = new EntryDecoder();
				return rowDecoder;
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
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeBoolean(oneRowScan);
				out.writeInt(lockMode);
				out.writeInt(isolationLevel);
				out.writeObject(scanInformation);
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
                        if(shouldRecordStats()) {
                            if(scanQualifiersField != null ) {
                                getScanQualifiersInStringFormat();
                            }
                        }
				} catch (Exception e) {
						SpliceLogUtils.logAndThrowRuntime(LOG, "Operation Init Failed!", e);
				}
		}

        private void getScanQualifiersInStringFormat() throws StandardException{
            Qualifier[][] scanQualifiers;

            try {
                scanQualifiers = (Qualifier[][]) activation.getClass().getField(scanQualifiersField).get(activation);
            } catch (Exception e) {
                throw StandardException.unexpectedUserException(e);
            }

            if (scanQualifiers != null) {
                String qualifiersString = "Scan filter:";
                for (Qualifier[] qualifiers : scanQualifiers) {
                    for (Qualifier q : qualifiers) {
                        String text = q.getText();
                        if (text != null) {
                            qualifiersString += text;
                        }
                    }
                }
                if (info == null) {
                    info = qualifiersString;
                }
                else if (!info.contains(qualifiersString)) {
                    info +=", " + qualifiersString;
                }
            }
        }
		@Override
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG, "executeScan");
				try {
						return new SpliceNoPutResultSet(activation,this, getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), runtimeContext));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

		@Override
		public SpliceOperation getLeftOperation() {
				return null;
		}

		protected void initIsolationLevel() {
				SpliceLogUtils.trace(LOG, "initIsolationLevel");
		}

		public Scan getNonSIScan(SpliceRuntimeContext spliceRuntimeContext) {
				/*
				 * Intended to get a scan which does NOT set up SI underneath us (since
				 * we are doing it ourselves).
				 */
				Scan scan = buildScan(spliceRuntimeContext);
				if (oneRowScan) {
					scan.setCaching(1); // Limit the batch size for performance
				}
				deSiify(scan);
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

		protected Scan buildScan(SpliceRuntimeContext ctx) {
				try{
						return getScan(ctx);
				} catch (StandardException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG,e);
				}
				return null;
		}


		protected Scan getScan(SpliceRuntimeContext ctx) throws StandardException {
				return scanInformation.getScan(operationInformation.getTransaction(), ctx.getScanStartOverride(),getKeyDecodingMap());
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) {
				return operationInformation.getBaseColumnMap();
		}

		@Override
		public boolean isReferencingTable(long tableNumber){
				return tableName.equals(String.valueOf(tableNumber));
		}

		public FormatableBitSet getAccessedCols()  {
				try {
						return scanInformation.getAccessedColumns();
				} catch (StandardException e) {
						LOG.error(e);
						throw new RuntimeException(e);
				}
		}

		public Qualifier[][] getScanQualifiers()  {
				try {
						return scanInformation.getScanQualifiers();
				} catch (StandardException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG,e);
						return null;
				}
		}

		/*@Override
		public String getName() {
				/*
				 * TODO -sf- tableName and indexName are actually conglomerate ids, not
				 * human-readable table names. Unfortunately, there doesn't appear
				 * to be any mechanism to get the human readable name short of
				 * issuing a query to find it (which isn't really an option). For now,
				 * we'll just set the conglomerate id on here, and then allow people
				 * to look for them later; at some point the Query Optimizer will
				 * need to be invoked to ensure that the human readable name gets
				 * passed through.
				 *
				String baseName =  super.getName();
				if(this.tableName!=null){
						baseName+="(table="+tableName+")";
				}else if(this.indexName!=null)
						baseName+="(index="+indexName+")";
				return baseName;
		}*/

		public String getTableName(){
				return this.tableName;
		}

		public String getIndexName() {
				return this.indexName;
		}

		@Override
		public void close() throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "closing");
				super.close();
		}

		public String printStartPosition() {
				try {
						return scanInformation.printStartPosition(numOpens);
				} catch (StandardException e) {
						throw new RuntimeException(e);
				}
		}

		public String printStopPosition() {
				try {
						return scanInformation.printStopPosition(numOpens);
				} catch (StandardException e) {
						throw new RuntimeException(e);
				}
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
}
