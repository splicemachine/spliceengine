package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.sql.execute.actions.UpdateConstantOperation;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.ForwardRecordingCallBuffer;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import com.splicemachine.stats.Counter;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author jessiezhang
 * @author Scott Fines
 */
public class UpdateOperation extends DMLWriteOperation{
		private static final Logger LOG = Logger.getLogger(UpdateOperation.class);

		private ResultSupplier resultSupplier;

        private DataValueDescriptor[] kdvds;

		@SuppressWarnings("UnusedDeclaration")
		public UpdateOperation() {
				super();
		}

		int[] pkCols;
		FormatableBitSet pkColumns;
		public UpdateOperation(SpliceOperation source, GeneratedMethod generationClauses,
													 GeneratedMethod checkGM, Activation activation)
						throws StandardException {
				super(source, generationClauses, checkGM, activation);
				init(SpliceOperationContext.newContext(activation));
				recordConstructorTime();
		}

		@Override
		public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				ExecRow nextSinkRow = super.getNextSinkRow(spliceRuntimeContext);
				if(nextSinkRow==null&&resultSupplier!=null)
						resultSupplier.close();
				return nextSinkRow;
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException{
				SpliceLogUtils.trace(LOG,"init with regionScanner %s",regionScanner);
				super.init(context);
				heapConglom = writeInfo.getConglomerateId();

				pkCols = writeInfo.getPkColumnMap();
				pkColumns = writeInfo.getPkColumns();

                SpliceConglomerate conglomerate = (SpliceConglomerate)((SpliceTransactionManager)activation.getTransactionController()).findConglomerate(heapConglom);
                int[] format_ids = conglomerate.getFormat_ids();
                int[] columnOrdering = conglomerate.getColumnOrdering();
                kdvds = new DataValueDescriptor[columnOrdering.length];
                for (int i = 0; i < columnOrdering.length; ++i) {
                    kdvds[i] = LazyDataValueFactory.getLazyNull(format_ids[columnOrdering[i]]);
                }

				startExecutionTime = System.currentTimeMillis();
		}

		private int[] getColumnPositionMap(FormatableBitSet heapList) {
				final int[] colPositionMap = new int[heapList.size()];
				for(int i = heapList.anySetBit(),pos=heapList.getNumBitsSet();i!=-1;i=heapList.anySetBit(i),pos++){
						colPositionMap[i] = pos;
				}
				return colPositionMap;
		}

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				DataHash hash;
				FormatableBitSet heapList = getHeapList();
				if(!modifiedPrimaryKeys(heapList)){
						hash = new DataHash<ExecRow>() {
								private ExecRow currentRow;
								@Override
								public void setRow(ExecRow rowToEncode) {
										this.currentRow = rowToEncode;
								}

								@Override
								public byte[] encode() throws StandardException, IOException {
										return ((RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
								}

								@Override public void close() throws IOException {  }

								@Override
								public KeyHashDecoder getDecoder() {
										return NoOpKeyHashDecoder.INSTANCE;
								}
						};
				}else{
						//TODO -sf- we need a sort order here for descending columns, don't we?
						//hash = BareKeyHash.encoder(getFinalPkColumns(getColumnPositionMap(heapList)),null);
                    hash = new PkDataHash(getFinalPkColumns(getColumnPositionMap(heapList)), kdvds,writeInfo.getTableVersion());
				}
				return new KeyEncoder(NoOpPrefix.INSTANCE,hash,NoOpPostfix.INSTANCE);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				FormatableBitSet heapList = getHeapList();
				boolean modifiedPrimaryKeys = modifiedPrimaryKeys(heapList);

        /*
	       * heapList is the position of the columns in the original row (e.g. if cols 2 and 3 are being modified,
				 * then heapList = {2,3}). We have to take that position and convert it into the actual positions
				 * in nextRow.
				 *
				 * nextRow looks like {old,old,...,old,new,new,...,new,rowLocation}, so suppose that we have
				 * heapList = {2,3}. Then nextRow = {old2,old3,new2,new3,rowLocation}. Which makes our colPositionMap
				 * look like
				 *
				 * colPositionMap[2] = 2;
				 * colPositionMap[3] = 3;
				 *
				 * But if heapList = {2}, then nextRow looks like {old2,new2,rowLocation}, which makes our colPositionMap
				 * look like
				 *
				 * colPositionMap[2] = 1
				 *
				 * in general, then
				 *
				 * colPositionMap[i= heapList.anySetBit()] = nextRow[heapList.numSetBits()]
				 * colPositionMap[heapList.anySetBit(i)] = nextRow[heapList.numSetBits()+1]
				 * ...
				 *
				 * and so forth
				 */
				final int[] colPositionMap = getColumnPositionMap(heapList);

				//if we haven't modified any of our primary keys, then we can just change it directly
				DescriptorSerializer[] serializers = VersionedSerializers.forVersion(writeInfo.getTableVersion(),true).getSerializers(getExecRowDefinition());
				if(!modifiedPrimaryKeys){
						return new NonPkRowHash(colPositionMap,null, serializers, heapList);
				}

				int[] finalPkColumns = getFinalPkColumns(colPositionMap);

				resultSupplier = new ResultSupplier(new BitSet(),spliceRuntimeContext);
				return new PkRowHash(finalPkColumns,null,heapList,colPositionMap,resultSupplier,serializers);
		}

		private int[] getFinalPkColumns(int[] colPositionMap) {
				int[] finalPkColumns;
				if(pkCols!=null){
						finalPkColumns =new int[pkCols.length];
						for(int i= pkColumns.anySetBit();i!=-1;i=pkColumns.anySetBit(i)){
								finalPkColumns[i] = colPositionMap[i+1];
						}
				}else{
						finalPkColumns = new int[0];
				}
				return finalPkColumns;
		}

		private FormatableBitSet getHeapList() {
				FormatableBitSet heapList = ((UpdateConstantOperation)writeInfo.getConstantAction()).getBaseRowReadList();
				if(heapList==null){
						int[] changedCols = ((UpdateConstantOperation)writeInfo.getConstantAction()).getChangedColumnIds();
						heapList = new FormatableBitSet(changedCols.length);
						for(int colPosition:changedCols){
								heapList.grow(colPosition+1);
								heapList.set(colPosition);
						}
				}
				return heapList;
		}

		private boolean modifiedPrimaryKeys(FormatableBitSet heapList) {
				boolean modifiedPrimaryKeys = false;
				if(pkColumns!=null){
						for(int pkCol = pkColumns.anySetBit();pkCol!=-1;pkCol= pkColumns.anySetBit(pkCol)){
								if(heapList.isSet(pkCol+1)){
										modifiedPrimaryKeys = true;
										break;
								}
						}
				}
				return modifiedPrimaryKeys;
		}


		@Override
		public RecordingCallBuffer<KVPair> transformWriteBuffer(RecordingCallBuffer<KVPair> bufferToTransform) throws StandardException {
				if(modifiedPrimaryKeys(getHeapList())){
						return new ForwardRecordingCallBuffer<KVPair>(bufferToTransform){
								@Override
								public void add(KVPair element) throws Exception {
										byte[] oldLocation = ((RowLocation) currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
                                        if (!Bytes.equals(oldLocation, element.getRow())) {
                                            // only add the delete if we aren't overwriting the same row
										    delegate.add(new KVPair(oldLocation, HConstants.EMPTY_BYTE_ARRAY, KVPair.Type.DELETE));
                                        }
										delegate.add(element);
								}
						};
				} else return bufferToTransform;
		}

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				if(resultSupplier!=null){
						//add supplier metrics
						Timer timer = resultSupplier.getTimer;
						stats.addMetric(OperationMetric.REMOTE_GET_ROWS,timer.getNumEvents());
						stats.addMetric(OperationMetric.REMOTE_GET_BYTES, resultSupplier.getBytes.getTotal());
						TimeView getView = timer.getTime();
						stats.addMetric(OperationMetric.REMOTE_GET_WALL_TIME,getView.getWallClockTime());
						stats.addMetric(OperationMetric.REMOTE_GET_CPU_TIME,getView.getCpuTime());
						stats.addMetric(OperationMetric.REMOTE_GET_USER_TIME,getView.getUserTime());
				}
				if(timer!=null){
						//same number of inputs as outputs
						stats.addMetric(OperationMetric.INPUT_ROWS,timer.getNumEvents());
				}
		}

		@Override
		public String toString() {
				return "Update{destTable="+heapConglom+",source=" + source + "}";
		}

		private class ResultSupplier{
				private KeyValue result;

				private byte[] location;
				private byte[] filterBytes;

				private HTableInterface htable;

				private Timer getTimer;
				public Counter getBytes;

				private ResultSupplier(BitSet interestedFields,MetricFactory metricFactory) {
						//we need the index so that we can transform data without the information necessary to decode it
						EntryPredicateFilter predicateFilter = new EntryPredicateFilter(interestedFields,new ObjectArrayList<Predicate>(),true);
						this.filterBytes = predicateFilter.toBytes();
						this.getTimer = metricFactory.newTimer();
						this.getBytes = metricFactory.newCounter();
				}

				public void setLocation(byte[] location){
						this.location = location;
						this.result = null;
				}

				public void setResult(EntryDecoder decoder) throws IOException {
						if(result==null) {
								//need to fetch the latest results
								if(htable==null){
										htable = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(Long.toString(heapConglom)));
								}

								getTimer.startTiming();
								Get remoteGet = SpliceUtils.createGet(getTransactionID(),location);
								remoteGet.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES);
								remoteGet.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,filterBytes);

								Result r = htable.get(remoteGet);
								//we assume that r !=null, because otherwise, what are we updating?
								KeyValue[] rawKvs = r.raw();
								for(KeyValue kv:rawKvs){
										getBytes.add(kv.getLength());
										if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES)){
												result = kv;
												break;
										}
								}
								//we also assume that PACKED_COLUMN_KEY is properly set by the time we get here
								getTimer.tick(1);
						}
						decoder.set(result.getBuffer(),result.getValueOffset(),result.getValueLength());
				}

				public void close() throws IOException {
						if(htable!=null)
								htable.close();
				}
		}


		/*
				 * Entity for encoding rows when the Primary Key has been modified
				 */
		private static class PkRowHash extends EntryDataHash{
				private ResultSupplier supplier;
				private EntryDecoder resultDecoder;
				private final FormatableBitSet finalHeapList;
				private final int[] colPositionMap;

				public PkRowHash(int[] keyColumns,
												 boolean[] keySortOrder,
												 FormatableBitSet finalHeapList,
												 int[] colPositionMap,
												 ResultSupplier supplier,
												 DescriptorSerializer[] serializers) {
						super(keyColumns, keySortOrder,serializers);
						this.finalHeapList = finalHeapList;
						this.colPositionMap = colPositionMap;
						this.supplier = supplier;
				}

				@Override
				public byte[] encode() throws StandardException, IOException {
						if(entryEncoder==null)
								entryEncoder = buildEntryEncoder();

						RowLocation location= (RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject(); //the location to update is always at the end
						//convert Result into put under the new row key
						supplier.setLocation(location.getBytes());

						if(resultDecoder==null)
								resultDecoder = new EntryDecoder(SpliceDriver.getKryoPool());

						supplier.setResult(resultDecoder);

						entryEncoder.reset(resultDecoder.getCurrentIndex());
						pack(entryEncoder.getEntryEncoder(),currentRow);
						return entryEncoder.encode();
				}

				@Override
				protected void pack(MultiFieldEncoder updateEncoder,
														ExecRow currentRow) throws StandardException, IOException {
						BitIndex index = resultDecoder.getCurrentIndex();
						MultiFieldDecoder getFieldDecoder = resultDecoder.getEntryDecoder();
						for(int pos=index.nextSetBit(0);pos>=0;pos=index.nextSetBit(pos+1)){
								if(finalHeapList.isSet(pos+1)){
										DataValueDescriptor dvd = currentRow.getRowArray()[colPositionMap[pos+1]];
										DescriptorSerializer serializer = serializers[colPositionMap[pos+1]];
										serializer.encode(updateEncoder,dvd,false);
//										if(dvd==null||dvd.isNull()){
//												updateEncoder.encodeEmpty();
//										}else{
//												DerbyBytesUtil.encodeInto(updateEncoder, dvd,false);
//										}
										resultDecoder.seekForward(getFieldDecoder, pos);
								}else{
										//use the index to get the correct offsets
										int offset = getFieldDecoder.offset();
										resultDecoder.seekForward(getFieldDecoder,pos);
										int limit = getFieldDecoder.offset()-1-offset;
										updateEncoder.setRawBytes(getFieldDecoder.array(),offset,limit);
								}
						}
				}
				public void close() throws IOException {
						if(supplier!=null)
								supplier.close();
						if(resultDecoder!=null)
								resultDecoder.close();
						super.close();
				}
		}

		/*
		 * Entity for encoding rows when primary keys have not been modified
		 */
		private class NonPkRowHash extends EntryDataHash{
				private final FormatableBitSet finalHeapList;

				public NonPkRowHash(int[] keyColumns,
														boolean[] keySortOrder,
														DescriptorSerializer[] serializers,
														FormatableBitSet finalHeapList) {
						super(keyColumns, keySortOrder,serializers);
						this.finalHeapList = finalHeapList;
				}

				@Override
				protected void pack(MultiFieldEncoder encoder, ExecRow currentRow) throws StandardException {
						encoder.reset();
						DataValueDescriptor[] dvds = currentRow.getRowArray();
						for(int i=finalHeapList.anySetBit();i>=0;i=finalHeapList.anySetBit(i)){
								int position = keyColumns[i];
								DataValueDescriptor dvd = dvds[position];
								DescriptorSerializer serializer = serializers[position];
								serializer.encode(encoder,dvd,false);
//								//we know that derby never spits out a null field here--we hope.
//								if(dvd.isNull())
//										DerbyBytesUtil.encodeTypedEmpty(encoder,dvd,false,true);
//								else
//										DerbyBytesUtil.encodeInto(encoder, dvd, false);
						}
				}

				@Override
				protected EntryEncoder buildEntryEncoder() {
						BitSet notNullFields = new BitSet(finalHeapList.size());
						BitSet scalarFields = new BitSet();
						BitSet floatFields = new BitSet();
						BitSet doubleFields = new BitSet();
						for(int i=finalHeapList.anySetBit();i>=0;i=finalHeapList.anySetBit(i)){
								notNullFields.set(i - 1);
								DataValueDescriptor dvd = currentRow.getRowArray()[keyColumns[i]];
								if(DerbyBytesUtil.isScalarType(dvd, null)){
										scalarFields.set(i-1);
								}else if(DerbyBytesUtil.isFloatType(dvd)){
										floatFields.set(i-1);
								}else if(DerbyBytesUtil.isDoubleType(dvd)){
										doubleFields.set(i-1);
								}
						}
						return EntryEncoder.create(SpliceDriver.getKryoPool(),currentRow.nColumns(),
										notNullFields,scalarFields,floatFields,doubleFields);
				}

				@Override
				protected BitSet getNotNullFields(ExecRow row, BitSet notNullFields) {
						notNullFields.clear();
						for(int i=finalHeapList.anySetBit();i>=0;i=finalHeapList.anySetBit(i)){
								notNullFields.set(i-1);
						}
						return notNullFields;
				}
		}

        private class PkDataHash implements DataHash<ExecRow> {
						private final String tableVersion;
						private ExecRow currentRow;
            private byte[] rowKey;
            private int[] keyColumns;
            private MultiFieldEncoder encoder;
            private MultiFieldDecoder decoder;
            private DataValueDescriptor[] kdvds;
						private DescriptorSerializer[] serializers;

            public PkDataHash(int[] keyColumns, DataValueDescriptor[] kdvds,String tableVersion) {
                this.keyColumns = keyColumns;
                this.kdvds = kdvds;
								this.tableVersion = tableVersion;
            }

            @Override
            public void setRow(ExecRow rowToEncode) {
                this.currentRow = rowToEncode;
            }

            @Override
            public byte[] encode() throws StandardException, IOException {
                rowKey = ((RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
                if (encoder == null)
                    encoder = MultiFieldEncoder.create(keyColumns.length);
                if (decoder == null)
                    decoder = MultiFieldDecoder.create();
                pack();
                return encoder.build();
            }

            private void pack() throws StandardException {
                encoder.reset();
                decoder.set(rowKey);
								if(serializers==null)
										serializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(currentRow);
                int i = 0;
                for (int col:keyColumns) {
                    if (col > 0) {
                        DataValueDescriptor dvd = currentRow.getRowArray()[col];
												DescriptorSerializer serializer = serializers[col];
												serializer.encode(encoder,dvd,false);
//                        if(dvd==null||dvd.isNull()){
//                            encoder.encodeEmpty();
//                        }else{
//                            DerbyBytesUtil.encodeInto(encoder, dvd,false);
//                        }
                        DerbyBytesUtil.skip(decoder,kdvds[i++]);
                    } else {
                        int offset = decoder.offset();
                        DerbyBytesUtil.skip(decoder,kdvds[i++]);
                        int limit = decoder.offset()-1-offset;
                        encoder.setRawBytes(decoder.array(),offset,limit);
                    }
                }
            }

						@Override
						public void close() throws IOException {
								for(DescriptorSerializer serializer:serializers){
										Closeables.closeQuietly(serializer);
								}
						}

						@Override
            public KeyHashDecoder getDecoder() {
                return NoOpKeyHashDecoder.INSTANCE;
            }
        }
}
