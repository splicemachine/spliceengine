package com.splicemachine.derby.utils.marshall;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.load.ImportTestUtils;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
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
public class PairEncoderTest {
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
								data.add(new Object[]{dts.clone(),dvds.clone()});

								//set some random fields
								for(int i=0;i<numRandomValues;i++){
										secondDvd = secondType.getDataValueDescriptor();
										secondType.setNext(secondDvd,secondType.newObject(random));
										dvds[1] = secondDvd;
										data.add(new Object[]{dts.clone(),dvds.clone()});
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
										data.add(new Object[]{dts.clone(),dvds.clone()});

										//set some random fields
										for(int j=0;j<numRandomValues;j++){
												secondDvd = secondType.getDataValueDescriptor();
												secondType.setNext(secondDvd,secondType.newObject(random));
												dvds[1] = secondDvd;
												data.add(new Object[]{dts.clone(),dvds.clone()});
										}
								}
						}
				}
				return data;
		}

		private final TestingDataType[] dataTypes;
		private final DataValueDescriptor[] row;

		public PairEncoderTest(TestingDataType[] dataTypes, DataValueDescriptor[] row) {
				System.out.println(Arrays.toString(dataTypes));
				this.dataTypes = dataTypes;
				this.row = row;
		}

		@Test
		public void testProperlyEncodesValues() throws Exception {
				ExecRow execRow = new ValueRow(dataTypes.length);
				for(int i=0;i<dataTypes.length;i++){
						execRow.setColumn(i + 1, row[i].cloneValue(true));
				}

				int keyLength = dataTypes.length/2;
				if(keyLength==0)
						keyLength++;
				int[] keyColumns= new int[keyLength];
				int pos=0;
				for(int i=0;i<dataTypes.length;i++){
						if(i%2==0){
								keyColumns[pos] = i;
								pos++;
						}
				}

				int[] rowColumns = IntArrays.complement(keyColumns,execRow.nColumns());

				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(true).getSerializers(execRow);
				KeyEncoder keyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE,BareKeyHash.encoder(keyColumns,null,serializers),NoOpPostfix.INSTANCE);
				DataHash rowEncoder = BareKeyHash.encoder(rowColumns,null,serializers);
				PairEncoder encoder = new PairEncoder(keyEncoder,rowEncoder, KVPair.Type.INSERT);

				KVPair pair = encoder.encode(execRow);

				ExecRow clone = execRow.getNewNullRow();
				PairDecoder decoder = encoder.getDecoder(clone);

				KeyValue kv = new KeyValue(pair.getRow(), SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY,pair.getValue());

				ExecRow decodedRow = decoder.decode(kv);
				ImportTestUtils.assertRowsEquals(execRow,decodedRow);
		}

        @Test
        public void testProperlyEncodesSortedValues() throws Exception {
            ExecRow execRow = new ValueRow(dataTypes.length);
            for (int i = 0; i < dataTypes.length; i++) {
                execRow.setColumn(i + 1, row[i].cloneValue(true));
            }

            int keyLength = dataTypes.length / 2;
            if (keyLength == 0)
                keyLength++;
            int[] keyColumns = new int[keyLength];
            int pos = 0;
            for (int i = 0; i < dataTypes.length; i++) {
                if (keyLength == 1 && i == 0){
                    keyColumns[pos] = i;
                } else if (i % 2 == 1) {
                    keyColumns[pos] = i;
                    pos++;
                }
            }

            boolean[] keySortOrder = new boolean[keyLength];
            keySortOrder[0] = false;

            int[] rowColumns = IntArrays.complement(keyColumns, execRow.nColumns());

						DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(true).getSerializers(execRow);
            KeyEncoder keyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(keyColumns, keySortOrder,serializers), NoOpPostfix.INSTANCE);
            DataHash rowEncoder = BareKeyHash.encoder(rowColumns, null,serializers);
            PairEncoder encoder = new PairEncoder(keyEncoder, rowEncoder, KVPair.Type.INSERT);

            KVPair pair = encoder.encode(execRow);

            ExecRow clone = execRow.getNewNullRow();
            PairDecoder decoder = encoder.getDecoder(clone);

            KeyValue kv = new KeyValue(pair.getRow(), SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY, pair.getValue());

            ExecRow decodedRow = decoder.decode(kv);
            ImportTestUtils.assertRowsEquals(execRow, decodedRow);
        }

        private static final Snowflake.Generator snowGen = new Snowflake((short)0xFAFF).newGenerator(64);
        private static final byte[] uniquePostBytes = Bytes.toBytes((long)0x7FFF7833FFAB7833l);
        private static final byte[] prefixBytes = Bytes.toBytes((long)0x7FAF7933FFA07853l);
        {
            // generate some Snowflake ids to make sure first byte is not 0x00
            for (int i = 0; i < 10; i++) {
                snowGen.next();
            }
        }

        @Test
        public void testProperlyEncodesSortedValuesWithPrefixAndPostfix() throws Exception {
            ExecRow execRow = new ValueRow(dataTypes.length);
            for (int i = 0; i < dataTypes.length; i++) {
                execRow.setColumn(i + 1, row[i].cloneValue(true));
            }

            int keyLength = dataTypes.length / 2;
            if (keyLength == 0)
                keyLength++;
            int[] keyColumns = new int[keyLength];
            int pos = 0;
            for (int i = 0; i < dataTypes.length; i++) {
                if (keyLength == 1 && i == 0){
                    keyColumns[pos] = i;
                } else if (i % 2 == 1) {
                    keyColumns[pos] = i;
                    pos++;
                }
            }

            boolean[] keySortOrder = new boolean[keyLength];
            int[] rowColumns = IntArrays.complement(keyColumns, execRow.nColumns());

            FixedBucketPrefix prefix = new FixedBucketPrefix((byte)0x08,
                    new FixedPrefix(prefixBytes));
            UniquePostfix postfix = new UniquePostfix(uniquePostBytes, snowGen);
						DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(execRow);
            KeyEncoder keyEncoder = new KeyEncoder(prefix, BareKeyHash.encoder(keyColumns, keySortOrder,serializers), postfix);
            DataHash rowEncoder = BareKeyHash.encoder(rowColumns, null,serializers);
            PairEncoder encoder = new PairEncoder(keyEncoder, rowEncoder, KVPair.Type.INSERT);

            KVPair pair = encoder.encode(execRow);

            ExecRow clone = execRow.getNewNullRow();
            PairDecoder decoder = encoder.getDecoder(clone);

            KeyValue kv = new KeyValue(pair.getRow(), SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY, pair.getValue());

            ExecRow decodedRow = decoder.decode(kv);
            ImportTestUtils.assertRowsEquals(execRow, decodedRow);
        }

}
