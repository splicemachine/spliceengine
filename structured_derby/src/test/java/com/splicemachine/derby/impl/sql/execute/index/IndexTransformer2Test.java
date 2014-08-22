package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.storage.EntryEncoder;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * @author Scott Fines
 * Date: 4/17/14
 */
public class IndexTransformer2Test {

		@Test
		public void testCanTranslateUniqueWithDuplicateNullsNoSourceKeyColumns() throws Exception {
				BitSet nonNullFields = new BitSet();
				nonNullFields.set(0,4);
				BitSet scalarFields = new BitSet();
				scalarFields.set(0,4);
				BitSet floatFields = new BitSet();
				BitSet doubleFields = new BitSet();

				EntryEncoder row = EntryEncoder.create(SpliceKryoRegistry.getInstance(),4,
								nonNullFields,scalarFields,floatFields,doubleFields);
				row.getEntryEncoder().encodeEmpty().encodeNext(2).encodeNext(3).encodeNext(4);

				byte[] rowData = row.encode();

				KVPair kvPair = new KVPair(new byte[]{},rowData);

				int[] indexKeyMap = new int[]{0,-1,-1,-1};
				boolean[] sourceAscDescInfo = new boolean[]{true,true,true,true};
                int srcColumnTypes[] = new int[] { StoredFormatIds.SQL_INTEGER_ID,  StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID };

				IndexTransformer2 idx = new IndexTransformer2(true,true,"1.0",null,srcColumnTypes,null,indexKeyMap,sourceAscDescInfo);

				KVPair translated = idx.translate(kvPair);

				byte[] key = translated.getRow();
				Assert.assertNotNull("No row key set!", key);
				assertTrue("No bytes in the row key!", key.length > 0);

				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create();
				keyDecoder.set(key);

				assertTrue("Incorrect row key!", keyDecoder.nextIsNull());
				keyDecoder.skipLong();

				//need to check with a duplicate null entry
				byte[] mainLoc = keyDecoder.decodeNextBytesUnsorted();
				assertArrayEquals("Incorrect row reference!", kvPair.getRow(), mainLoc);
		}

		@Test
		public void testCanTranslateUniqueNoSourceKeyColumns() throws Exception {
				BitSet nonNullFields = new BitSet();
				nonNullFields.set(0,4);
				BitSet scalarFields = new BitSet();
				scalarFields.set(0,4);
				BitSet floatFields = new BitSet();
				BitSet doubleFields = new BitSet();

				EntryEncoder row = EntryEncoder.create(SpliceKryoRegistry.getInstance(),4,
								nonNullFields,scalarFields,floatFields,doubleFields);
				row.getEntryEncoder().encodeNext(1).encodeNext(2).encodeNext(3).encodeNext(4);

				byte[] rowData = row.encode();

				KVPair kvPair = new KVPair(new byte[]{},rowData);

				int[] indexKeyMap = new int[]{0,-1,-1,-1};
				boolean[] sourceAscDescInfo = new boolean[]{true,true,true,true};
                int srcColumnTypes[] = new int[] { StoredFormatIds.SQL_INTEGER_ID,  StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID };

				IndexTransformer2 idx = new IndexTransformer2(true,false,"1.0",null,srcColumnTypes,null,indexKeyMap,sourceAscDescInfo);

				KVPair translated = idx.translate(kvPair);

				byte[] key = translated.getRow();
				Assert.assertNotNull("No row key set!", key);
				assertTrue("No bytes in the row key!", key.length > 0);

				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create();
				keyDecoder.set(key);

				int keyField = keyDecoder.decodeNextInt();
				assertEquals("incorrect key value!", 1, keyField);
				assertFalse("Data is still present in the key!", keyDecoder.available());
		}

		@Test
		public void testCanTranslateNonUniqueNoKeyColumns() throws Exception {
				BitSet nonNullFields = new BitSet();
				nonNullFields.set(0,4);
				BitSet scalarFields = new BitSet();
				scalarFields.set(0,4);
				BitSet floatFields = new BitSet();
				BitSet doubleFields = new BitSet();

				EntryEncoder row = EntryEncoder.create(SpliceKryoRegistry.getInstance(),4,
								nonNullFields,scalarFields,floatFields,doubleFields);
				row.getEntryEncoder().encodeNext(1).encodeNext(2).encodeNext(3).encodeNext(4);

				byte[] rowData = row.encode();

				KVPair kvPair = new KVPair(new byte[]{},rowData);

				int[] indexKeyMap = new int[]{0,-1,-1,-1};
				boolean[] sourceAscDescInfo = new boolean[]{true,true,true,true};
                int srcColumnTypes[] = new int[] { StoredFormatIds.SQL_INTEGER_ID,  StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID };

				IndexTransformer2 idx = new IndexTransformer2(false,false,"1.0",null,srcColumnTypes,null,indexKeyMap,sourceAscDescInfo);

				KVPair translated = idx.translate(kvPair);

				byte[] key = translated.getRow();
				Assert.assertNotNull("No row key set!", key);
				assertTrue("No bytes in the row key!", key.length > 0);

				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create();
				keyDecoder.set(key);

				int keyField = keyDecoder.decodeNextInt();
				assertEquals("incorrect key value!", 1, keyField);
				byte[] mainLoc = keyDecoder.decodeNextBytesUnsorted();
				assertArrayEquals("Incorrect row reference!", kvPair.getRow(), mainLoc);
		}

		@Test
		public void testCanTranslateNonUniqueTwoFieldsNoKeyColumns() throws Exception {
				BitSet nonNullFields = new BitSet();
				nonNullFields.set(0,4);
				BitSet scalarFields = new BitSet();
				scalarFields.set(0,4);
				BitSet floatFields = new BitSet();
				BitSet doubleFields = new BitSet();

				EntryEncoder row = EntryEncoder.create(SpliceKryoRegistry.getInstance(),4,
								nonNullFields,scalarFields,floatFields,doubleFields);
				row.getEntryEncoder().encodeNext(1).encodeNext(2).encodeNext(3).encodeNext(4);

				byte[] rowData = row.encode();

				KVPair kvPair = new KVPair(new byte[]{},rowData);

				int[] indexKeyMap = new int[]{0,1,-1,-1};
				boolean[] sourceAscDescInfo = new boolean[]{true,true,true,true};
                int[] sourceKeyTypes = new int[]{StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID};


                IndexTransformer2 idx = new IndexTransformer2(false,false,"1.0",null,sourceKeyTypes,null,indexKeyMap,sourceAscDescInfo);

				KVPair translated = idx.translate(kvPair);

				byte[] key = translated.getRow();
				Assert.assertNotNull("No row key set!", key);
				assertTrue("No bytes in the row key!", key.length > 0);

				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create();
				keyDecoder.set(key);

				int keyField = keyDecoder.decodeNextInt();
				assertEquals("incorrect key value!", 1, keyField);
				assertEquals("incorrect key value!", 2, keyDecoder.decodeNextInt());
				byte[] mainLoc = keyDecoder.decodeNextBytesUnsorted();
				assertArrayEquals("Incorrect row reference!", kvPair.getRow(), mainLoc);
		}

		@Test
		public void testCanTranslateNonUniqueOneFieldsOneKeyColumn() throws Exception {
				BitSet nonNullFields = new BitSet();
				nonNullFields.set(0,4);
				nonNullFields.clear(0);
				BitSet scalarFields = new BitSet();
				scalarFields.set(0,4);
				BitSet floatFields = new BitSet();
				BitSet doubleFields = new BitSet();

				EntryEncoder row = EntryEncoder.create(SpliceKryoRegistry.getInstance(),4,
								nonNullFields,scalarFields,floatFields,doubleFields);
				row.getEntryEncoder().encodeNext(2).encodeNext(3).encodeNext(4);

				byte[] rowData = row.encode();

				KVPair kvPair = new KVPair(Encoding.encode(1),rowData);

				int[] indexKeyMap = new int[]{0,-1,-1,-1};
				boolean[] sourceAscDescInfo = new boolean[]{true,true,true,true};
				int[] sourceKeyEncodingOrder = new int[]{0};
				int[] sourceKeyTypes = new int[]{StoredFormatIds.SQL_INTEGER_ID};

				IndexTransformer2 idx = new IndexTransformer2(false,false,"1.0",
								sourceKeyEncodingOrder,sourceKeyTypes,new boolean[]{true},
								indexKeyMap,sourceAscDescInfo);

				KVPair translated = idx.translate(kvPair);

				byte[] key = translated.getRow();
				Assert.assertNotNull("No row key set!", key);
				assertTrue("No bytes in the row key!", key.length > 0);

				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create();
				keyDecoder.set(key);

				int keyField = keyDecoder.decodeNextInt();
				assertEquals("incorrect key value!", 1, keyField);
//				Assert.assertEquals("incorrect key value!",2,keyDecoder.decodeNextInt());
				byte[] mainLoc = keyDecoder.decodeNextBytesUnsorted();
				assertArrayEquals("Incorrect row reference!", kvPair.getRow(), mainLoc);
		}

		@Test
		public void testCanTranslateNonUniqueTwoFieldsOneKeyColumnNullSourceKey() throws Exception {
				BitSet nonNullFields = new BitSet();
				nonNullFields.set(0,4);
				nonNullFields.clear(0);

                BitSet scalarFields = new BitSet();
				scalarFields.set(0,4);
				BitSet floatFields = new BitSet();
				BitSet doubleFields = new BitSet();

				EntryEncoder srcValueEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(), 4 ,
								nonNullFields, scalarFields, floatFields, doubleFields);

				srcValueEncoder.getEntryEncoder().encodeNext(2).encodeNext(3).encodeNext(4);

                byte[] srcRowKey = Encoding.encode(1);
				byte[] srcValue = srcValueEncoder.encode();

                KVPair srcKvPair = new KVPair(srcRowKey, srcValue);

				int[] indexKeyMap = new int[]{0,-1,-1,-1};
				boolean[] sourceAscDescInfo = new boolean[]{true,true,true,true};
				int[] sourceKeyTypes = new int[]{StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID};

				IndexTransformer2 transformer = new IndexTransformer2(false,false,"1.0",null,sourceKeyTypes,null,
                        indexKeyMap,sourceAscDescInfo);

				KVPair translated = transformer.translate(srcKvPair);

				byte[] indexRowKey = translated.getRow();
				Assert.assertNotNull("No row key set!", indexRowKey);
				assertTrue("No bytes in the row key!", indexRowKey.length > 0);

				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create();
				keyDecoder.set(indexRowKey);

				assertTrue("Incorrectly missed a null entry!", keyDecoder.nextIsNull());
				keyDecoder.skipLong();
				byte[] mainLoc = keyDecoder.decodeNextBytesUnsorted();
				assertArrayEquals("Incorrect row reference!", srcKvPair.getRow(), mainLoc);
		}

		@Test
		public void testCanTranslateNonUniqueTwoFieldsOneKeyColumnNullSourceKeyAscDescInfo() throws Exception {
				BitSet nonNullFields = new BitSet();
				nonNullFields.set(0,4);
				nonNullFields.clear(0);
				BitSet scalarFields = new BitSet();
				scalarFields.set(0,4);
				BitSet floatFields = new BitSet();
				BitSet doubleFields = new BitSet();

				EntryEncoder row = EntryEncoder.create(SpliceKryoRegistry.getInstance(),4,
								nonNullFields,scalarFields,floatFields,doubleFields);
				row.getEntryEncoder().encodeNext(2).encodeNext(3).encodeNext(4);

				byte[] rowData = row.encode();

				KVPair kvPair = new KVPair(Encoding.encode(1),rowData);

				int[] indexKeyMap = new int[]{0,1,-1,-1};
				boolean[] sourceAscDescInfo = new boolean[]{true,true,true,true};
				int[] sourceKeyEncodingOrder = new int[]{0};
                int srcColumnTypes[] = new int[] { StoredFormatIds.SQL_INTEGER_ID,  StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID };


            IndexTransformer2 idx = new IndexTransformer2(false,false,"1.0",
								sourceKeyEncodingOrder,srcColumnTypes,null,
								indexKeyMap,sourceAscDescInfo);

				KVPair translated = idx.translate(kvPair);

				byte[] key = translated.getRow();
				Assert.assertNotNull("No row key set!", key);
				assertTrue("No bytes in the row key!", key.length > 0);

				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create();
				keyDecoder.set(key);

				int keyField = keyDecoder.decodeNextInt();
				assertEquals("incorrect key value!", 1, keyField);
				assertEquals("incorrect key value!", 2, keyDecoder.decodeNextInt());
				byte[] mainLoc = keyDecoder.decodeNextBytesUnsorted();
				assertArrayEquals("Incorrect row reference!", kvPair.getRow(), mainLoc);
		}

		@Test
		public void testCanTranslateNonUniqueTwoFieldsOneKeyColumn() throws Exception {
				BitSet nonNullFields = new BitSet();
				nonNullFields.set(0,4);
				nonNullFields.clear(0);
				BitSet scalarFields = new BitSet();
				scalarFields.set(0,4);
				BitSet floatFields = new BitSet();
				BitSet doubleFields = new BitSet();

				EntryEncoder row = EntryEncoder.create(SpliceKryoRegistry.getInstance(),4,
								nonNullFields,scalarFields,floatFields,doubleFields);
				row.getEntryEncoder().encodeNext(2).encodeNext(3).encodeNext(4);

				byte[] rowData = row.encode();

				KVPair kvPair = new KVPair(Encoding.encode(1),rowData);

				int[] indexKeyMap = new int[]{0,1,-1,-1};
				boolean[] sourceAscDescInfo = new boolean[]{true,true,true,true};
				int[] sourceKeyEncodingOrder = new int[]{0};
                int srcColumnTypes[] = new int[] { StoredFormatIds.SQL_INTEGER_ID,  StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID };


            IndexTransformer2 idx = new IndexTransformer2(false,false,"1.0",
								sourceKeyEncodingOrder,srcColumnTypes,new boolean[]{true},
								indexKeyMap,sourceAscDescInfo);

				KVPair translated = idx.translate(kvPair);

				byte[] key = translated.getRow();
				Assert.assertNotNull("No row key set!", key);
				assertTrue("No bytes in the row key!", key.length > 0);

				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create();
				keyDecoder.set(key);

				int keyField = keyDecoder.decodeNextInt();
				assertEquals("incorrect key value!", 1, keyField);
				assertEquals("incorrect key value!", 2, keyDecoder.decodeNextInt());
				byte[] mainLoc = keyDecoder.decodeNextBytesUnsorted();
				assertArrayEquals("Incorrect row reference!", kvPair.getRow(), mainLoc);
		}

}
