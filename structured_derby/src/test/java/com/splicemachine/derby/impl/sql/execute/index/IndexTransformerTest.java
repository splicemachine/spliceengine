package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author Scott Fines
 * Date: 2/10/14
 */
public class IndexTransformerTest {

		@Test
		public void testTransformsDataProperly() throws Exception {
				BitSet indexedColumns = new BitSet(1);
				indexedColumns.set(1);

				int[] mainColToIndexPosMap = new int[]{-1,0};

				BitSet descColumns = new BitSet();
                int[] formatIds = new int[]{79,80,80,79};
				IndexTransformer transformer = IndexTransformer.newTransformer(indexedColumns,
								mainColToIndexPosMap, descColumns, false, false, KryoPool.defaultPool(), null, formatIds);


				BitSet mainDataFields = new BitSet(4);
				mainDataFields.set(0,4);
				BitSet scalarFields = new BitSet(4);
				scalarFields.set(1,3);
				BitSet floatFields = new BitSet(4);
				floatFields.set(0);
				BitSet doubleFields = new BitSet(4);
				doubleFields.set(3);
				EntryEncoder mainEntryEncoder = EntryEncoder.create(KryoPool.defaultPool(),4,
								mainDataFields,scalarFields,floatFields,doubleFields);

				MultiFieldEncoder fieldEncoder = mainEntryEncoder.getEntryEncoder();
				fieldEncoder.encodeNext(1.0f).encodeNext(1).encodeNext(0d).encodeNext(2);

				Snowflake snowflake = new Snowflake((short)1);

				KVPair mainPair = new KVPair(snowflake.nextUUIDBytes(),mainEntryEncoder.encode());

				KVPair indexPair = transformer.translate(mainPair);

				/*row data validation*/
				byte[] indexData = indexPair.getValue();

				EntryDecoder entryDecoder = new EntryDecoder(KryoPool.defaultPool());
				entryDecoder.set(indexData);

				//make sure index is correct. Row should not contain data
				BitIndex indexIndex = entryDecoder.getCurrentIndex();
				Assert.assertEquals("Incorrect index size!",0,indexIndex.length());

				/*row key validation*/

				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create(KryoPool.defaultPool());
				keyDecoder.set(indexPair.getRow());
				Assert.assertEquals("Incorrect key element in position 0",1,keyDecoder.decodeNextInt());
				Assert.assertArrayEquals("Incorrect key element in position 1", mainPair.getRow(),
                        Encoding.decodeBytesUnsortd(keyDecoder.array(), keyDecoder.offset(), keyDecoder.array().length-keyDecoder.offset()));
		}

		@Test
		public void testTransformsDataProperlyWithDescColumn() throws Exception {
				BitSet indexedColumns = new BitSet(1);
				indexedColumns.set(1);

				int[] mainColToIndexPosMap = new int[]{-1,0};

				BitSet descColumns = new BitSet();
				descColumns.set(0);
                int[] formatIds = new int[]{79,80,80,79};

				IndexTransformer transformer = IndexTransformer.newTransformer(indexedColumns,
								mainColToIndexPosMap,descColumns,false,false,KryoPool.defaultPool(), null, formatIds);


				BitSet mainDataFields = new BitSet(4);
				mainDataFields.set(0,4);
				BitSet scalarFields = new BitSet(4);
				scalarFields.set(1,3);
				BitSet floatFields = new BitSet(4);
				floatFields.set(0);
				BitSet doubleFields = new BitSet(4);
				doubleFields.set(3);
				EntryEncoder mainEntryEncoder = EntryEncoder.create(KryoPool.defaultPool(),4,
								mainDataFields,scalarFields,floatFields,doubleFields);

				MultiFieldEncoder fieldEncoder = mainEntryEncoder.getEntryEncoder();
				fieldEncoder.encodeNext(1.0f).encodeNext(1).encodeNext(0d).encodeNext(2);

				Snowflake snowflake = new Snowflake((short)1);

				KVPair mainPair = new KVPair(snowflake.nextUUIDBytes(),mainEntryEncoder.encode());

				KVPair indexPair = transformer.translate(mainPair);

				/*row data validation*/
				byte[] indexData = indexPair.getValue();

				EntryDecoder entryDecoder = new EntryDecoder(KryoPool.defaultPool());
				entryDecoder.set(indexData);

				//make sure index is correct. The row should not contain any data
				BitIndex indexIndex = entryDecoder.getCurrentIndex();
				Assert.assertEquals("Incorrect index size!",0,indexIndex.length());

				/*row key validation*/
				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create(KryoPool.defaultPool());
				keyDecoder.set(indexPair.getRow());
				Assert.assertEquals("Incorrect key element in position 0",1,keyDecoder.decodeNextInt(true));
				Assert.assertArrayEquals("Incorrect key element in position 1",mainPair.getRow(),
                        Encoding.decodeBytesUnsortd(keyDecoder.array(), keyDecoder.offset(), keyDecoder.array().length-keyDecoder.offset()));
		}
}
