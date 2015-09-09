package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.concurrent.SameThreadExecutorService;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.derby.stream.temporary.WriteReadUtils;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.NoOpKeyHashDecoder;
import com.splicemachine.derby.utils.marshall.SkippingKeyDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 4/11/14
 */
public class IndexRowReaderBuilder implements Externalizable {
		private Iterator source;
		private int lookupBatchSize;
		private int numConcurrentLookups = -1;
		private ExecRow  outputTemplate;
		private long mainTableConglomId = -1;
		private int[] mainTableRowDecodingMap;
		private FormatableBitSet mainTableAccessedRowColumns;
		private int[] mainTableKeyColumnEncodingOrder;
		private boolean[] mainTableKeyColumnSortOrder;
		private int[] mainTableKeyDecodingMap;
		private FormatableBitSet mainTableAccessedKeyColumns;
        private int[] execRowTypeFormatIds;
		/*
		 * A Map from the physical location of the Index columns in the INDEX scanned row
		 * and the decoded output row.
		 */
		private int[] indexCols;
		private String tableVersion;
		private int[] mainTableKeyColumnTypes;
        private TxnView txn;

    public IndexRowReaderBuilder indexColumns(int[] indexCols){
				this.indexCols = indexCols;
				return this;
		}

    public IndexRowReaderBuilder execRowTypeFormatIds(int[] execRowTypeFormatIds){
        this.execRowTypeFormatIds = execRowTypeFormatIds;
        return this;
    }


    public IndexRowReaderBuilder mainTableVersion(String mainTableVersion){
				this.tableVersion = mainTableVersion;
				return this;
		}

		public IndexRowReaderBuilder source(Iterator source) {
				this.source = source;
				return this;
		}

		public IndexRowReaderBuilder lookupBatchSize(int lookupBatchSize) {
				this.lookupBatchSize = lookupBatchSize;
				return this;
		}

		public IndexRowReaderBuilder numConcurrentLookups(int numConcurrentLookups) {
				this.numConcurrentLookups = numConcurrentLookups;
				return this;
		}

		public IndexRowReaderBuilder outputTemplate(ExecRow outputTemplate) {
				this.outputTemplate = outputTemplate;
				return this;
		}

    public IndexRowReaderBuilder transaction(TxnView transaction) {
        this.txn = transaction;
        return this;
    }

		public IndexRowReaderBuilder mainTableConglomId(long mainTableConglomId) {
				this.mainTableConglomId = mainTableConglomId;
				return this;
		}

		public IndexRowReaderBuilder mainTableAccessedRowColumns(FormatableBitSet mainTableAccessedRowColumns){
				this.mainTableAccessedRowColumns = mainTableAccessedRowColumns;
				return this;
		}

		public IndexRowReaderBuilder mainTableRowDecodingMap(int[] mainTableRowDecodingMap) {
				this.mainTableRowDecodingMap = mainTableRowDecodingMap;
				return this;
		}

		public IndexRowReaderBuilder mainTableKeyColumnEncodingOrder(int[] mainTableKeyColumnEncodingOrder) {
				this.mainTableKeyColumnEncodingOrder = mainTableKeyColumnEncodingOrder;
				return this;
		}

		public IndexRowReaderBuilder mainTableKeyColumnTypes(int[] mainTableKeyColumnTypes) {
				this.mainTableKeyColumnTypes = mainTableKeyColumnTypes;
				return this;
		}

		public IndexRowReaderBuilder mainTableKeyColumnSortOrder(boolean[] mainTableKeyColumnSortOrder) {
				this.mainTableKeyColumnSortOrder = mainTableKeyColumnSortOrder;
				return this;
		}

		public IndexRowReaderBuilder mainTableKeyDecodingMap(int[] mainTableKeyDecodingMap) {
				this.mainTableKeyDecodingMap = mainTableKeyDecodingMap;
				return this;
		}

		public IndexRowReaderBuilder mainTableAccessedKeyColumns(FormatableBitSet mainTableAccessedKeyColumns) {
				this.mainTableAccessedKeyColumns = mainTableAccessedKeyColumns;
				return this;
		}

		public IndexRowReader build() throws StandardException{
				assert txn!=null: "No Transaction specified!";
				assert mainTableRowDecodingMap!=null: "No Row decoding map specified!";
				assert mainTableConglomId>=0: "No Main table conglomerate id specified!";
				assert outputTemplate!=null: "No output template specified!";
				assert source!=null: "No source specified";
				assert indexCols!=null: "No index columns specified!";
				ExecutorService lookupService;
				if(numConcurrentLookups<0)
						lookupService = SameThreadExecutorService.instance();
				else{
						ThreadFactory factory = new ThreadFactoryBuilder()
										.setNameFormat("index-lookup-%d").build();
						lookupService = new ThreadPoolExecutor(numConcurrentLookups,numConcurrentLookups,
										60, TimeUnit.SECONDS,new SynchronousQueue<Runnable>(),factory,
										new ThreadPoolExecutor.CallerRunsPolicy());
				}

				BitSet rowFieldsToReturn = new BitSet(mainTableAccessedRowColumns.getNumBitsSet());
				for(int i=mainTableAccessedRowColumns.anySetBit();i>=0;i=mainTableAccessedRowColumns.anySetBit(i)){
						rowFieldsToReturn.set(i);
				}
				EntryPredicateFilter epf = new EntryPredicateFilter(rowFieldsToReturn, ObjectArrayList.<Predicate>newInstanceWithCapacity(0));
				byte[] epfBytes = epf.toBytes();

				DescriptorSerializer[] templateSerializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(outputTemplate);
				KeyHashDecoder keyDecoder;
				if(mainTableKeyColumnEncodingOrder==null || mainTableAccessedKeyColumns==null||mainTableAccessedKeyColumns.getNumBitsSet()<=0)
						keyDecoder = NoOpKeyHashDecoder.INSTANCE;
				else{
						keyDecoder = SkippingKeyDecoder.decoder(VersionedSerializers.typesForVersion(tableVersion),
										templateSerializers,
										mainTableKeyColumnEncodingOrder,
										mainTableKeyColumnTypes,
										mainTableKeyColumnSortOrder,
										mainTableKeyDecodingMap,
										mainTableAccessedKeyColumns);
				}

				KeyHashDecoder rowDecoder = new EntryDataDecoder(mainTableRowDecodingMap,null,templateSerializers);

				return new IndexRowReader(lookupService,
								source,
								outputTemplate,
								txn,
								lookupBatchSize,
								Math.max(numConcurrentLookups,0),
								mainTableConglomId,
								epfBytes,
								keyDecoder,
								rowDecoder,
								indexCols);
		}

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        try {
            TransactionOperations.getOperationFactory().writeTxn(txn, out);
            out.writeInt(lookupBatchSize);
            out.writeInt(numConcurrentLookups);
            ArrayUtil.writeIntArray(out, WriteReadUtils.getExecRowTypeFormatIds(outputTemplate));
            out.writeLong(mainTableConglomId);
            ArrayUtil.writeIntArray(out, mainTableRowDecodingMap);
            out.writeObject(mainTableAccessedRowColumns);
            ArrayUtil.writeIntArray(out, mainTableKeyColumnEncodingOrder);
			if (mainTableKeyColumnSortOrder != null) {
				out.writeBoolean(true);
				ArrayUtil.writeBooleanArray(out, mainTableKeyColumnSortOrder);
			} else {
				out.writeBoolean(false);
			}
            ArrayUtil.writeIntArray(out, mainTableKeyDecodingMap);
            out.writeObject(mainTableAccessedKeyColumns);
            ArrayUtil.writeIntArray(out, indexCols);
            out.writeUTF(tableVersion);
            ArrayUtil.writeIntArray(out, mainTableKeyColumnTypes);
        } catch (StandardException se) {
            throw new IOException(se);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        txn = TransactionOperations.getOperationFactory().readTxn(in);
        lookupBatchSize = in.readInt();
        numConcurrentLookups = in.readInt();
        execRowTypeFormatIds = ArrayUtil.readIntArray(in);
        outputTemplate = WriteReadUtils.getExecRowFromTypeFormatIds(execRowTypeFormatIds);
        mainTableConglomId = in.readLong();
        mainTableRowDecodingMap = ArrayUtil.readIntArray(in);
        mainTableAccessedRowColumns = (FormatableBitSet) in.readObject();
        mainTableKeyColumnEncodingOrder = ArrayUtil.readIntArray(in);
		if (in.readBoolean()) {
			mainTableKeyColumnSortOrder = ArrayUtil.readBooleanArray(in);
		}
        mainTableKeyDecodingMap = ArrayUtil.readIntArray(in);
        mainTableAccessedKeyColumns = (FormatableBitSet) in.readObject();
        indexCols = ArrayUtil.readIntArray(in);
        tableVersion = in.readUTF();
        mainTableKeyColumnTypes = ArrayUtil.readIntArray(in);
    }
}
