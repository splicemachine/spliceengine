package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SIFilterFactory;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.RowAccumulator;
import com.splicemachine.stats.Metrics;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Snowflake;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests specific scenarios around the SITableScanner (as opposed to the randomized testing types)
 *
 * @author Scott Fines
 * Date: 4/9/14
 */
public class FixedSITableScannerTest {

		@Test
		public void testScansBackSkipsSecondPrimaryKey() throws Exception {
				int[] keyDecodingMap = new int[]{1,-1};
				int[] keyColumnOrder = new int[]{1,0};
				int[] keyEncodingMap = new int[]{2,1};
				DataValueDescriptor[] data = new DataValueDescriptor[]{
								new SQLInteger(1),
//								new SQLDouble(Double.parseDouble("-8.98846567431158E307")), //encodes weirdly, so exercises our type checking
								new SQLReal(25f),
								new SQLVarchar("Hello")
				};
				int[] rowDecodingMap = new int[]{0,-1,-1,2};
				testScansProperly(keyDecodingMap,keyColumnOrder,null,keyEncodingMap,data,rowDecodingMap);
		}

		@Test
		public void testScansBackSkipsFirstPrimaryKey() throws Exception {
				int[] keyDecodingMap = new int[]{-1,1};
				int[] keyColumnOrder = new int[]{1,0};
				int[] keyEncodingMap = new int[]{2,1};
				DataValueDescriptor[] data = new DataValueDescriptor[]{
								new SQLInteger(1),
								new SQLDouble(Double.parseDouble("-8.98846567431158E307")), //encodes weirdly, so exercises our type checking
								new SQLVarchar("Hello")
				};
				int[] rowDecodingMap = new int[]{0,-1,-1,2};
				testScansProperly(keyDecodingMap,keyColumnOrder,null,keyEncodingMap,data,rowDecodingMap);
		}

		@Test
		public void testScansBackSkipsFirstPrimaryKeyDescendingAscending() throws Exception {
				int[] keyDecodingMap = new int[]{-1,1};
				int[] keyColumnOrder = new int[]{1,0};
				int[] keyEncodingMap = new int[]{2,1};
				DataValueDescriptor[] data = new DataValueDescriptor[]{
								new SQLInteger(1),
								new SQLDouble(Double.parseDouble("-8.98846567431158E307")), //encodes weirdly, so exercises our type checking
								new SQLVarchar("Hello")
				};
				int[] rowDecodingMap = new int[]{0,-1,-1,2};
				boolean[] keySortOrder = new boolean[]{false,true};
				testScansProperly(keyDecodingMap,keyColumnOrder,keySortOrder,keyEncodingMap,data,rowDecodingMap);
		}

		@Test
		public void testScansBackSkipsFirstPrimaryKeyAscendingDescending() throws Exception {
				int[] keyDecodingMap = new int[]{-1,1};
				int[] keyColumnOrder = new int[]{1,0};
				int[] keyEncodingMap = new int[]{2,1};
				DataValueDescriptor[] data = new DataValueDescriptor[]{
								new SQLInteger(1),
								new SQLDouble(Double.parseDouble("-8.98846567431158E307")), //encodes weirdly, so exercises our type checking
								new SQLVarchar("Hello")
				};
				int[] rowDecodingMap = new int[]{0,-1,-1,2};
				boolean[] keySortOrder = new boolean[]{true,false};
				testScansProperly(keyDecodingMap,keyColumnOrder,keySortOrder,keyEncodingMap,data,rowDecodingMap);
		}

		@Test
		public void testWorksWithTwoOutOfOrderPrimaryKeys() throws Exception {
				int[] keyDecodingMap = new int[]{2,1};
				int[] keyColumnOrder = new int[]{1,0};
				int[] keyEncodingMap = new int[]{2,1};
				testScansProperly(keyDecodingMap, keyColumnOrder,null,keyEncodingMap,null,null);
		}

		@Test
		public void testWorksWithTwoOutOfOrderPrimaryKeysDescendingAscending() throws Exception {
				int[] keyDecodingMap = new int[]{2,1};
				int[] keyColumnOrder = new int[]{1,0};
				int[] keyEncodingMap = new int[]{2,1};
				boolean[] keySortOrder = new boolean[]{false,true};
				testScansProperly(keyDecodingMap, keyColumnOrder,keySortOrder,keyEncodingMap,null,null);
		}

		@Test
		public void testWorksWithOneFloatKeyDescending() throws Exception {
				/*
				 * Test that the scanner properly decodes an entire row with a primary key
				 */
				int[] keyColumnPositionMap = new int[]{2};
				int[] keyColumnOrder = new int[]{0};
				boolean[] ascDescInfo = new boolean[]{false};
				testScansProperly(keyColumnPositionMap,keyColumnOrder,ascDescInfo,null,null,null);
		}

		@Test
		public void testWorksWithOneFloatKey() throws Exception {
				/*
				 * Test that the scanner properly decodes an entire row with a primary key
				 */
				int[] keyColumnPositionMap = new int[]{2};
				int[] keyColumnOrder = new int[]{0};
				testScansProperly(keyColumnPositionMap,keyColumnOrder);
		}

		@Test
		public void testWorksWithOneDoublePrimaryKey() throws Exception {
				/*
				 * Test that the scanner properly decodes an entire row with a primary key
				 */
				int[] keyColumnPositionMap = new int[]{1};
				int[] keyColumnOrder = new int[]{0};
				testScansProperly(keyColumnPositionMap,keyColumnOrder);
		}

		@Test
		public void testWorksWithNoPrimaryKeys() throws Exception {
				/*
				 * Test the situation where there are no primary keys. Hence, all data is stored in a row
				 */
				testScansProperly(null,null);
		}

		private static class MockFilter implements SIFilter{
				private RowAccumulator accumulator;

				private MockFilter(EntryAccumulator accumulator,
													 EntryDecoder decoder,
													 EntryPredicateFilter predicateFilter,
													 boolean isCountStar) {
						this.accumulator = new HRowAccumulator(predicateFilter,decoder,accumulator,isCountStar);
				}

				@Override public void nextRow() {  }

				@Override
				public RowAccumulator getAccumulator() {
						return accumulator;
				}

				@Override
				public Filter.ReturnCode filterKeyValue(KeyValue kv) throws IOException {
						if(!com.splicemachine.hbase.KeyValueUtils.singleMatchingQualifier(kv,SpliceConstants.PACKED_COLUMN_BYTES))
								return Filter.ReturnCode.SKIP;
						if(!accumulator.isFinished() && accumulator.isOfInterest(kv)){
								if(!accumulator.accumulate(kv))
										return Filter.ReturnCode.NEXT_ROW;
								return Filter.ReturnCode.INCLUDE;
						}else return Filter.ReturnCode.INCLUDE;
				}
		}

		protected void testScansProperly(int[] keyDecodingMap, int[] keyColumnOrder) throws StandardException, IOException {
				testScansProperly(keyDecodingMap, keyColumnOrder,null,null,null,null);

		}
		protected void testScansProperly(int[] keyDecodingMap,
																		 int[] keyColumnOrder,
																		 boolean[] keySortOrder,
																		 int[] keyEncodingMap,
																		 DataValueDescriptor[] correct,
																		 int[] rowDecodingMap) throws StandardException, IOException {
		/*
		 * Test that the scanner properly decodes an entire row with a primary key
		 */
				DataValueDescriptor[] data = new DataValueDescriptor[]{
								new SQLInteger(1),
								new SQLDouble(Double.parseDouble("-8.98846567431158E307")), //encodes weirdly, so exercises our type checking
								new SQLReal(25f),
								new SQLVarchar("Hello")
				};
				ExecRow row = new ValueRow(data.length);
				row.setRowArray(data);
				DescriptorSerializer[] serializers;
//				if(correct==null)
						serializers = VersionedSerializers.latestVersion(true).getSerializers(data);
//				else
//						serializers = VersionedSerializers.latestVersion(true).getSerializers(correct);
				byte[] key;
				int[] rowEncodingMap;
				int[] keyColumnTypes = null;
				if(keyColumnOrder!=null){
						if(keyEncodingMap==null){
								keyEncodingMap = new int[keyColumnOrder.length];
								for(int i=0;i<keyColumnOrder.length;i++){
										keyEncodingMap[i] = keyDecodingMap[keyColumnOrder[i]];
								}
						}
						keyColumnTypes = new int[keyColumnOrder.length];
						for(int i=0;i<keyEncodingMap.length;i++){
								if(keyEncodingMap[i]<0) continue;
								keyColumnTypes[i] = data[keyEncodingMap[i]].getTypeFormatId();
						}
						rowEncodingMap = IntArrays.count(data.length);
						for(int pkCol:keyEncodingMap){
								rowEncodingMap[pkCol] = -1;
						}
						if(rowDecodingMap==null)
								rowDecodingMap = rowEncodingMap;

						KeyEncoder encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(keyEncodingMap, keySortOrder, serializers), NoOpPostfix.INSTANCE);
						key = encoder.getKey(row);
				}else{
						key = new Snowflake((short)1).nextUUIDBytes();
						rowEncodingMap = IntArrays.count(data.length);
						rowDecodingMap = rowEncodingMap;
				}

				EntryDataHash hash = new EntryDataHash(rowEncodingMap,null,serializers);
				hash.setRow(row);
				byte[] value = hash.encode();
				final KeyValue dataKv = new KeyValue(key, SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES,1l,value);
				final KeyValue siKv = new KeyValue(key,SpliceConstants.DEFAULT_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,1l, HConstants.EMPTY_BYTE_ARRAY);
				final boolean[] returned = new boolean[]{false};

				RegionScanner scanner = mock(RegionScanner.class);
				Answer<Boolean> rowReturnAnswer = new Answer<Boolean>() {

						@Override
						public Boolean answer(InvocationOnMock invocation) throws Throwable {
								Assert.assertFalse("Attempted to call next() twice!", returned[0]);

								@SuppressWarnings("unchecked") List<KeyValue> kvs = (List<KeyValue>) invocation.getArguments()[0];
								kvs.add(siKv);
								kvs.add(dataKv);
								returned[0] = true;
								return false;
						}
				};
				//noinspection unchecked
				when(scanner.nextRaw(any(List.class),any(String.class))).thenAnswer(rowReturnAnswer);
				//noinspection unchecked
				when(scanner.next(any(List.class))).thenAnswer(rowReturnAnswer);

				Scan scan = mock(Scan.class);
				TableScannerBuilder builder = new TableScannerBuilder()
								.scan(scan)
								.scanner(scanner)
								.metricFactory(Metrics.noOpMetricFactory())
								.transactionID("1")
								.tableVersion("2.0")
								.rowDecodingMap(rowDecodingMap)
								.filterFactory(
												new SIFilterFactory() {
														@Override
														public SIFilter newFilter(EntryPredicateFilter predicateFilter,
																											EntryDecoder rowEntryDecoder,
																											EntryAccumulator accumulator,
																											boolean isCountStar) throws IOException {
																return new MockFilter(accumulator, rowEntryDecoder, predicateFilter, isCountStar);
														}
												});

				if(correct!=null){
						ExecRow returnedRow = new ValueRow(correct.length);
						returnedRow.setRowArray(correct);
						builder = builder.template(returnedRow.getNewNullRow());
				}else
						builder = builder.template(row.getNewNullRow());

				if(keyColumnOrder!=null){
						FormatableBitSet accessedKeyCols = new FormatableBitSet(2);
						for(int i=0;i<keyColumnOrder.length;i++){
								if(keyDecodingMap[i]>=0)
										accessedKeyCols.set(i);
						}
						builder = builder
										.keyColumnEncodingOrder(keyColumnOrder)
										.keyColumnTypes(keyColumnTypes)
										.keyColumnSortOrder(keySortOrder)
										.keyDecodingMap(keyDecodingMap)
										.accessedKeyColumns(accessedKeyCols);
				}
				SITableScanner tableScanner = builder.build();

				SpliceRuntimeContext ctx = mock(SpliceRuntimeContext.class);
				ExecRow next = tableScanner.next(ctx);
				if(correct==null)
						correct = row.getRowArray();
				Assert.assertArrayEquals("Incorrect scan decoding!",correct,next.getRowArray());
		}
}
