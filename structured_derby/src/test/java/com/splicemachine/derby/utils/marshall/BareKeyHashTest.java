package com.splicemachine.derby.utils.marshall;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.derby.impl.load.ImportTestUtils;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.mockito.Mockito.mock;

/**
 * @author Scott Fines
 *         Date: 11/15/13
 */
@RunWith(Parameterized.class)
public class BareKeyHashTest {
		private static final int numRandomValues=1;
		private static final KryoPool kryoPool = mock(KryoPool.class);

		@Parameterized.Parameters
		public static Collection<Object[]> data() throws StandardException {
				Collection<Object[]> data = Lists.newArrayList();

				Random random = new Random(0l);

				//do single-entry rows
				for(TestingDataType type: TestingDataType.values()){
						DataValueDescriptor dvd = type.getDataValueDescriptor();
						//set a null entry
						dvd.setToNull();
						data.add(new Object[]{
										new TestingDataType[]{type},
										new DataValueDescriptor[]{dvd}
						});

						//set some random fields
						for(int i=0;i<numRandomValues;i++){
								dvd = type.getDataValueDescriptor();
								type.setNext(dvd,type.newObject(random));
								data.add(new Object[]{
												new TestingDataType[]{type},
												new DataValueDescriptor[]{dvd}
								});
						}
				}

				//do two-entry rows
				for(TestingDataType firstType: TestingDataType.values()){
						DataValueDescriptor firstDvd = firstType.getDataValueDescriptor();
						DataValueDescriptor[] dvds = new DataValueDescriptor[2];
						TestingDataType[] dts = new TestingDataType[2];
						//set a null entry
						firstDvd.setToNull();
						dvds[0] = firstDvd;
						dts[0] = firstType;

						for(TestingDataType secondType: TestingDataType.values()){
								//set a null entry
								DataValueDescriptor secondDvd = secondType.getDataValueDescriptor();
								secondDvd.setToNull();
								dvds[1] = secondDvd;
								dts[1] = secondType;
								data.add(new Object[]{dts,dvds});

								//set some random fields
								for(int i=0;i<numRandomValues;i++){
										secondDvd = secondType.getDataValueDescriptor();
										secondType.setNext(secondDvd,secondType.newObject(random));
										dvds[1] = secondDvd;
										data.add(new Object[]{dts,dvds});
								}
						}

						//set some random fields
						for(int i=0;i<numRandomValues;i++){
								firstDvd = firstType.getDataValueDescriptor();
								firstType.setNext(firstDvd,firstType.newObject(random));
								dvds[0] = firstDvd;
								for(TestingDataType secondType: TestingDataType.values()){
										//set a null entry
										DataValueDescriptor secondDvd = secondType.getDataValueDescriptor();
										secondDvd.setToNull();
										dvds[1] = secondDvd;
										dts[1] = secondType;
										data.add(new Object[]{dts,dvds});

										//set some random fields
										for(int j=0;j<numRandomValues;j++){
												secondDvd = secondType.getDataValueDescriptor();
												secondType.setNext(secondDvd,secondType.newObject(random));
												dvds[1] = secondDvd;
												data.add(new Object[]{dts,dvds});
										}
								}
						}
				}
				return data;
		}

		private final TestingDataType[] dataTypes;
		private final DataValueDescriptor[] row;

		public BareKeyHashTest(TestingDataType[] dataTypes, DataValueDescriptor[] row) {
				this.dataTypes = dataTypes;
				this.row = row;
		}

		@Test
		public void testProperlyEncodesValues() throws Exception {
				ExecRow execRow = new ValueRow(dataTypes.length);
				for(int i=0;i<dataTypes.length;i++){
						execRow.setColumn(i + 1, row[i].cloneValue(true));
				}

				int[] keyColumns= new int[dataTypes.length];
				for(int i=0;i<keyColumns.length;i++){
						keyColumns[i] = i;
				}
				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(true).getSerializers(execRow);
				DataHash bareKeyHash = BareKeyHash.encoder(keyColumns,null,serializers);
				bareKeyHash.setRow(execRow);
				byte[] data = bareKeyHash.encode();


				MultiFieldDecoder decoder = MultiFieldDecoder.wrap(data);

				for(int i=0;i<dataTypes.length;i++){
						TestingDataType dt = dataTypes[i];
						DataValueDescriptor field = execRow.getColumn(i+1);
						if(row[i].isNull()){
								String errorMsg = "Incorrectly encoded a null value!";
								if(dt==TestingDataType.REAL)
										Assert.assertTrue(errorMsg,decoder.nextIsNullFloat());
								else if(dt==TestingDataType.DOUBLE)
										Assert.assertTrue(errorMsg,decoder.nextIsNullDouble());
								else
										Assert.assertTrue(errorMsg,decoder.nextIsNull());
								continue;
						}
						String errorMsg = "Incorrectly encoded a non-null field as null!";
						if(dt==TestingDataType.REAL)
								Assert.assertFalse(errorMsg,decoder.nextIsNullFloat());
						else if(dt==TestingDataType.DOUBLE)
								Assert.assertFalse(errorMsg,decoder.nextIsNullDouble());
						else
								Assert.assertFalse(errorMsg,decoder.nextIsNull());

						dt.decodeNext(field,decoder);
						Assert.assertEquals("Row<"+ Arrays.toString(row)+">Incorrect serialization of field "+ row[i],row[i],field);
				}
		}

		@Test
		public void testCanEncodeAndDecodeProperly() throws Exception {
				ExecRow execRow = new ValueRow(dataTypes.length);
				for(int i=0;i<dataTypes.length;i++){
						execRow.setColumn(i + 1, row[i].cloneValue(true));
				}


				int[] keyColumns= new int[dataTypes.length];
				for(int i=0;i<keyColumns.length;i++){
						keyColumns[i] = i;
				}
				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(true).getSerializers(execRow);
				DataHash bareKeyHash = BareKeyHash.encoder(keyColumns,null,serializers);
				bareKeyHash.setRow(execRow);
				byte[] data = bareKeyHash.encode();

				KeyHashDecoder decoder = bareKeyHash.getDecoder();

				ExecRow clone = execRow.getNewNullRow();
				decoder.set(data,0,data.length);
				decoder.decode(clone);

				ImportTestUtils.assertRowsEquals(execRow,clone);
		}

		@Test
		public void testCanEncodeAndDecodeProperlyOutOfOrderKeyCols() throws Exception {
				ExecRow execRow = new ValueRow(dataTypes.length);
				for(int i=0;i<dataTypes.length;i++){
						execRow.setColumn(i + 1, row[i].cloneValue(true));
				}

				int[] keyColumns= new int[dataTypes.length];
				Set<Integer> columns = Sets.newHashSet();
				for(int i=0;i<10;i++)
						columns.add(i);

				Iterator<Integer> colIt = columns.iterator();
				for(int i=0;i<keyColumns.length;i++){
						keyColumns[i] = colIt.next();
						colIt.remove();
				}

				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(true).getSerializers(execRow);
				DataHash bareKeyHash = BareKeyHash.encoder(keyColumns,null,serializers);
				bareKeyHash.setRow(execRow);
				byte[] data = bareKeyHash.encode();

				KeyHashDecoder decoder = bareKeyHash.getDecoder();

				ExecRow clone = execRow.getNewNullRow();
				decoder.set(data,0,data.length);
				decoder.decode(clone);

				ImportTestUtils.assertRowsEquals(execRow,clone);
		}

}
