package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.KVPair;
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

				IndexTransformer transformer = IndexTransformer.newTransformer(indexedColumns,
								mainColToIndexPosMap,descColumns,false,false,KryoPool.defaultPool());


				BitSet mainDataFields = new BitSet(4);
				mainDataFields.set(0,4);
				BitSet scalarFields = new BitSet(4);
				scalarFields.set(1,3);
				BitSet floatFields = new BitSet(4);
				floatFields.set(0);
				BitSet doubleFields = new BitSet(4);
				doubleFields.set(2);
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

				//make sure index is correct
				BitIndex indexIndex = entryDecoder.getCurrentIndex();
				Assert.assertEquals("Incorrect index size!",2,indexIndex.length());
				Assert.assertTrue("Incorrect data value type in index",indexIndex.isScalarType(0));

				//make sure row includes 2 fields--entry 1, and the row key
				MultiFieldDecoder fieldDecoder= entryDecoder.getEntryDecoder();
				Assert.assertEquals("Incorrect data element in position 0",1,fieldDecoder.decodeNextInt());
				Assert.assertArrayEquals("Incorrect data element in position 1",mainPair.getRow(),fieldDecoder.decodeNextBytesUnsorted());

				/*row key validation*/

				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create(KryoPool.defaultPool());
				keyDecoder.set(indexPair.getRow());
				Assert.assertEquals("Incorrect key element in position 0",1,keyDecoder.decodeNextInt());

				Assert.assertArrayEquals("Incorrect key element in position 1",mainPair.getRow(),keyDecoder.slice(8));
		}

		@Test
		public void testTransformsDataProperlyWithDescColumn() throws Exception {
				BitSet indexedColumns = new BitSet(1);
				indexedColumns.set(1);

				int[] mainColToIndexPosMap = new int[]{-1,0};

				BitSet descColumns = new BitSet();
				descColumns.set(0);

				IndexTransformer transformer = IndexTransformer.newTransformer(indexedColumns,
								mainColToIndexPosMap,descColumns,false,false,KryoPool.defaultPool());


				BitSet mainDataFields = new BitSet(4);
				mainDataFields.set(0,4);
				BitSet scalarFields = new BitSet(4);
				scalarFields.set(1,3);
				BitSet floatFields = new BitSet(4);
				floatFields.set(0);
				BitSet doubleFields = new BitSet(4);
				doubleFields.set(2);
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

				//make sure index is correct
				BitIndex indexIndex = entryDecoder.getCurrentIndex();
				Assert.assertEquals("Incorrect index size!",2,indexIndex.length());
				Assert.assertTrue("Incorrect data value type in index",indexIndex.isScalarType(0));

				//make sure row includes 2 fields--entry 1, and the row key
				MultiFieldDecoder fieldDecoder= entryDecoder.getEntryDecoder();
				Assert.assertEquals("Incorrect data element in position 0",1,fieldDecoder.decodeNextInt());
				Assert.assertArrayEquals("Incorrect data element in position 1",mainPair.getRow(),fieldDecoder.decodeNextBytesUnsorted());

				/*row key validation*/

				MultiFieldDecoder keyDecoder = MultiFieldDecoder.create(KryoPool.defaultPool());
				keyDecoder.set(indexPair.getRow());
				Assert.assertEquals("Incorrect key element in position 0",1,keyDecoder.decodeNextInt(true));

				Assert.assertArrayEquals("Incorrect key element in position 1",mainPair.getRow(),keyDecoder.slice(8));
		}
}
